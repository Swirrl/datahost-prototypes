(ns tpximpact.datahost.ldapi.delta-handler
  (:require [clojure.string :as str]
            [ring.util.io :as ring-io]
            [tablecloth.api :as tc]
            [tpximpact.datahost.ldapi.util.data-validation :as data-validation])
  (:import (net.openhft.hashing LongHashFunction)))

(def default-schema
  {:columns [{:name "Area"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? (fn [v] (= 9 (count v)))}
             {:name "Period"
              :datatype :period
              :coltype :qb/dimension
              :valid-value? (constantly true)}
             {:name "Sex"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? #{"Male" "Female" "Other"}}
             {:name "Life Expectancy"
              :datatype :double
              :coltype :qb/measure
              :valid-value? double?}]})

(def ^:private long-hash-fn-instance (LongHashFunction/xx128low))

(def ^:private right-column-prefix-pattern #"^right.")

(def obs-coordinate-cols
  (->> default-schema
       :columns
       (filter (comp #{:qb/dimension :qb/attribute} :coltype))
       (map :name)))

(def right-obs-coordinate-cols (map #(str "right." %) obs-coordinate-cols))

(defn debug [x]
  (println x)
  x)

(defn hash-fn [dims]
  (->> (apply str (interpose \| dims))
       (.hashChars long-hash-fn-instance)))

(defn get-measure-column-name [schema]
  (->> (:columns schema)
       (some #(when ((comp #{:qb/measure} :coltype) %) %))
       :name))

(defn- rename-column-mappings [pattern column-names]
  (->> (map #(vector % (str/replace % pattern ""))
            column-names)
       (into {})))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn add-user-schema-ids [ds measure-column-name obs-coordinate-cols]
  (tc/map-columns ds :id
                  (conj obs-coordinate-cols measure-column-name)
                  ;; map-fn arity must match number of columns selected
                  (fn map-fn [& dims]
                    (println "Hashing: " dims "," (hash-fn dims))
                    (hash-fn dims))))

(defn join-and-partition-deltas
  [base-ds delta-ds measure-column-name right-measure-column-name]
  ;; the hashing function used here is only for the observation coordinates (no measure)
  (-> (tc/full-join base-ds delta-ds obs-coordinate-cols {:hashing hash-fn})
      (tc/map-columns :operation
                      [measure-column-name right-measure-column-name]
                      (fn [old-measure-val new-measure-val]
                        (cond
                          (nil? old-measure-val) :append
                          (nil? new-measure-val) :retract
                          (not= old-measure-val new-measure-val) :modified)))
      (tc/group-by :operation)
      (tc/groups->map)))

(defn derive-deltas [base-ds delta-ds]
  (let [measure-column-name (get-measure-column-name default-schema)
        right-measure-column-name (str "right." measure-column-name)
        column-names (tc/column-names base-ds)
        right-column-names (map #(str "right." %) column-names)

        labelled-ds (join-and-partition-deltas base-ds delta-ds measure-column-name right-measure-column-name)

        append-dataset (-> (add-user-schema-ids (:append labelled-ds) right-measure-column-name right-obs-coordinate-cols)
                           (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names)))

        retracted-dataset (-> (tc/select-columns (:retract labelled-ds)
                                                 (concat [:operation] column-names [measure-column-name]))
                              (add-user-schema-ids measure-column-name obs-coordinate-cols))

        corrections-dataset (add-user-schema-ids (:modified labelled-ds) measure-column-name obs-coordinate-cols)

        amended-appends-ds (-> (tc/map-columns corrections-dataset :operation [right-measure-column-name] (constantly :append))
                               ;; copy left :id column for the :correction_for pointer pairing
                               ((fn [dataset]
                                  (tc/add-column dataset :correction_for (dataset :id))))
                               ;; add tx id for right measure user schema
                               (add-user-schema-ids right-measure-column-name obs-coordinate-cols)
                               (tc/drop-columns measure-column-name)
                               (tc/rename-columns (rename-column-mappings right-column-prefix-pattern
                                                                          right-column-names)))

        amended-retracts-ds (-> (tc/select-columns corrections-dataset (cons :id column-names))
                                (tc/map-columns :operation [measure-column-name] (constantly :retract)))]
    (debug
     (-> (tc/concat retracted-dataset
                    amended-retracts-ds
                    amended-appends-ds
                    append-dataset)
         (tc/reorder-columns (concat [:id :operation] column-names [:correction_for]))))))

(defn post-delta-files
  [{{{:keys [base-csv delta-csv]} :multipart} :parameters :as _request}]
  (let [diff-results (derive-deltas (data-validation/slurpable->dataset (:tempfile base-csv)
                                                                        {:file-type :csv :encoding "UTF-8"})
                                    (data-validation/slurpable->dataset (:tempfile delta-csv)
                                                                        {:file-type :csv :encoding "UTF-8"}))]
    {:status 200
     :body (write-dataset-to-outputstream diff-results)}))

(defn delta-tool-route-config []
  {:handler (partial post-delta-files)
   :parameters {:multipart [:map
                            [:base-csv reitit.ring.malli/temp-file-part]
                            [:delta-csv reitit.ring.malli/temp-file-part]]}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Differences between input files calculated"
                    :content {"text/csv" any?}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

; Curl command used to test the delta route:
;
; curl -X 'POST' 'http://localhost:3000/delta' -H 'accept: text/csv' \
;   -H 'Content-Type: multipart/form-data' \
;   -F 'base-csv=@./env/test/resources/test-inputs/delta/orig.csv;type=text/csv' \
;   -F 'delta-csv=@./env/test/resources/test-inputs/delta/new.csv;type=text/csv' \
;   --output ./deltas.csv
;
