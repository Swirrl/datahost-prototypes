(ns tpximpact.datahost.ldapi.delta-handler
  (:require [clojure.string :as str]
            [ring.util.io :as ring-io]
            [tablecloth.api :as tc]
            [tech.v3.dataset.zip :as zip]
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

(def id-columns
  (->> default-schema
       :columns
       (filter (comp #{:qb/dimension :qb/attribute} :coltype))
       (map :name)))

(defn hash-chars [^String s]
  (.hashChars long-hash-fn-instance s))

(defn hash-fn [& dims]
  (-> (apply str (interpose "|" dims))
      hash-chars))

(defn add-ids [ds]
  (-> ds
      ;; adds a new column named "ID" by applying `hash-fn` to the
      ;; vals in the selected columns
      (tc/map-columns "ID" id-columns hash-fn)))

(defn get-measure-column-name [schema]
  (->> schema
       :columns
       (some #(when ((comp #{:qb/measure} :coltype) %) %))
       :name))

(defn- rename-column-mappings [pattern column-names]
  (->> (map #(vector % (str/replace % pattern ""))
            column-names)
       (into {})))

(defn derive-deltas [original-ds new-ds]
  (let [old (add-ids original-ds)
        new (add-ids new-ds)
        measure-column-name (get-measure-column-name default-schema)
        right-measure-column-name (str "right." measure-column-name)
        column-names (tc/column-names original-ds)
        right-column-names (map #(str "right." %) column-names)
        labelled-ds (tc/map-columns (tc/full-join old new "ID")
                                    :status
                                    [measure-column-name right-measure-column-name]
                                    (fn [old-measure-val new-measure-val]
                                      (cond
                                        (nil? old-measure-val) :added
                                        (nil? new-measure-val) :deleted
                                        (not= old-measure-val new-measure-val) :modified)))]
    [(-> labelled-ds
         (tc/select-rows (comp #(= :added %) :status))
         (tc/select-columns right-column-names)
         (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names))
         (tc/set-dataset-name "additions"))
     (-> labelled-ds
         (tc/select-rows (comp #(= :deleted %) :status))
         (tc/select-columns column-names)
         (tc/set-dataset-name "deletions"))
     (-> labelled-ds
         (tc/select-rows (comp #(= :modified %) :status))
         (tc/select-columns (conj column-names (str "right." measure-column-name)))
         (tc/drop-columns measure-column-name)
         (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names))
         (tc/set-dataset-name "modifications"))]))

(defn post-delta-files
  [{{{:keys [base-csv delta-csv]} :multipart} :parameters :as _request}]
  (let [diff-results (derive-deltas (data-validation/slurpable->dataset (:tempfile base-csv)
                                                                        {:file-type :csv :encoding "UTF-8"})
                                    (data-validation/slurpable->dataset (:tempfile delta-csv)
                                                                        {:file-type :csv :encoding "UTF-8"}))]
    {:status 200
     :body (ring-io/piped-input-stream
            (fn [out-stream]
              (zip/dataset-seq->zipfile! out-stream {:file-type :csv} diff-results)))}))

(defn delta-tool-route-config []
  {:handler (partial post-delta-files)
   :parameters {:multipart [:map
                            [:base-csv reitit.ring.malli/temp-file-part]
                            [:delta-csv reitit.ring.malli/temp-file-part]]}
   :openapi {:security [{"basic" []}]}
   :responses {201 {:description "Differences between input files calculated"
                    :content {"application/json"
                              {:body string?}}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
