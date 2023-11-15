(ns tpximpact.datahost.ldapi.handlers.delta ;TODO: move to *.handlers.delta
  "Contains functionality for diffing datasets.

  TODO: proper explanation."
  (:require [clojure.string :as str]
            [ring.util.io :as ring-io]
            [ring.util.response :as util.response]
            [reitit.ring.malli :as ring.malli]
            [tablecloth.api :as tc]
            [tpximpact.datahost.ldapi.db :as db]
            [tpximpact.datahost.ldapi.store :as store]
            [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
            [tpximpact.datahost.ldapi.util.data.compilation :as data.compilation]
            [tpximpact.datahost.ldapi.util.data.internal :as data.internal])
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

(defn- tag-change
  [old-measure-val new-measure-val]
  (cond
    (nil? old-measure-val) :append
    (nil? new-measure-val) :retract
    (not= old-measure-val new-measure-val) :modify))

;; name,age,dh/kind,dh/id,dh/tx,dh/parent

(defn- row-changed? [row] (some? (get row data.internal/op-column-name)))

(defn delta-dataset
  "Returns a dataset with extra columns: TODO(finalise names)

  Value of the operation column can be TODO | TODO | TODO.

  Unchanged rows are not present in the delta dataset.

  Assumptions:

  - rows of both datasets conform to row-schema"
  [base-ds new-ds {:keys [row-schema hashable-columns] measure-column-name :measure-column :as _ctx}]
  (let [right-measure-column-name (data.internal/r-joinable-name (tc/dataset-name new-ds)
                                                                 measure-column-name)
        base-ds (data.internal/ensure-id-column base-ds row-schema)
        new-ds (data.internal/ensure-id-column new-ds row-schema)
        ]
    (-> (tc/full-join base-ds new-ds data.internal/hash-column-name {:operation-space :int64})
        (tc/map-columns data.internal/op-column-name [measure-column-name right-measure-column-name] tag-change)
        (tc/select-rows row-changed?)
        ;; (tc/select-columns (conj (vec hashable-columns)
        ;;                          op-column-name
        ;;                          id-column-name))
        )))

(defn derive-deltas
  "TODO
  
  Assumptions:
  - the datasets were already validated/created against the same schema."
  [ds-left
   ds-right
   {measure-column :measure-column
    hashable-columns :hashable-columns
    row-schema :row-schema
    :as ctx}]
  (let [right-measure-column-name (data.internal/r-joinable-name measure-column (tc/dataset-name ds-right))
        column-names (tc/column-names ds-left)
        right-column-names (map #(data.internal/r-joinable-name (tc/dataset-name ds-right) %) column-names)

        delta-ds (delta-dataset ds-left ds-right ctx)

        append-dataset (-> (add-user-schema-ids (:append delta-ds)
                                                right-measure-column-name
                                                (remove (fn [col-name]
                                                          (= col-name right-measure-column-name))
                                                        right-column-names))
                           (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names)))

        retracted-dataset (-> (tc/select-columns (:retract delta-ds)
                                                 (concat [:operation] column-names [measure-column]))
                              (add-user-schema-ids measure-column obs-coordinate-cols))

        corrections-dataset (add-user-schema-ids (:modify delta-ds) measure-column obs-coordinate-cols)

        amended-appends-ds (-> (tc/map-columns corrections-dataset :operation [right-measure-column-name] (constantly :append))
                               ;; copy left :id column for the :correction_for pointer pairing
                               ((fn [dataset]
                                  (tc/add-column dataset :correction_for (dataset :id))))
                               ;; add tx id for right measure user schema
                               (add-user-schema-ids right-measure-column-name obs-coordinate-cols)
                               (tc/drop-columns measure-column)
                               (tc/rename-columns (rename-column-mappings right-column-prefix-pattern
                                                                          right-column-names)))

        amended-retracts-ds (-> (tc/select-columns corrections-dataset (cons :id column-names))
                                (tc/map-columns :operation [measure-column] (constantly :retract)))]
    (-> (tc/concat retracted-dataset
                   amended-retracts-ds
                   amended-appends-ds
                   append-dataset)
        (tc/reorder-columns (concat [:id :operation] column-names [:correction_for])))))

(defn error-no-revisions
  []
  (-> (util.response/response "This release has no revisions yet")
      (util.response/status 422)
      (util.response/header "content-type" "text/plain")))

(defmulti -post-delta-files (fn [sys {{:strs [accept]} :headers}] accept))

(defmethod -post-delta-files "application/x-datahost-tx-csv" [sys request]
  ;; (let [{:keys [triplestore change-store]} sys
  ;;       {{{:keys [csv]} :multipart} :parameters
  ;;        {release-uri :dh/Release} :datahost.request/uris} request

  ;;       schema (db/get-release-schema triplestore release-uri)

  ;;       change-infos (db/get-changes-info triplestore release-uri)
  ;;       _ (when (empty? change-infos)
  ;;           (throw (ex-info "This release has no revisions"
  ;;                           {:type :tpximpact.datahost.ldapi.errors/exception})))
        
  ;;       {snapshot-key :snapshotKey rev-uri :rev} (last change-infos)
  ;;       _ (when (nil? snapshot-key)
  ;;           (throw (ex-info (format "Missing :snapshotKey for '%s'" rev-uri)
  ;;                           {:type :tpximpact.datahost.ldapi.errors/exception})))
        
  ;;       row-schema (data.validation/make-row-schema schema)
  ;;       opts {:store change-store :file-type :csv :enforce-schema row-schema}
  ;;       ds-release (data.validation/as-dataset snapshot-key opts)
  ;;       ds-input (data.validation/as-dataset (:tempfile csv) opts)
        
  ;;       ctx (data.compilation/make-schema-context row-schema)
  ;;       diff-results (derive-deltas ds-release ds-input ctx)]
  ;;   {:status 200
  ;;    :body (write-dataset-to-outputstream diff-results)})
  )

(defn post-delta-files [sys request]    ;TODO: rename this fn
  ;; TODO: add basic validation for incoming dataset, (e.g.
  ;; `data.compilation/validate-row-uniqueness`) after ns reorg
  (-post-delta-files sys request))

; Curl command used to test the delta route:
;
; curl -X 'POST' 'http://localhost:3000/delta' -H 'accept: text/csv' \
;   -H 'Content-Type: multipart/form-data' \
;   -F 'base-csv=@./env/test/resources/test-inputs/delta/orig.csv;type=text/csv' \
;   -F 'delta-csv=@./env/test/resources/test-inputs/delta/new.csv;type=text/csv' \
;   --output ./deltas.csv
;
