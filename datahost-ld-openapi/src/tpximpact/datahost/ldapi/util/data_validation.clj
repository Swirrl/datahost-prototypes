(ns tpximpact.datahost.ldapi.util.data-validation
  (:require
   [clojure.set :as set]
   [malli.core :as m]
   [malli.error :as me]
   [malli.util :as mu]
   [clojure.tools.logging :as log]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [tpximpact.datahost.ldapi.resource :as resource])
  (:import (clojure.lang ExceptionInfo)
           (java.io ByteArrayInputStream File)
           [java.net URL]
           [java.net URI]))

(def MakeRowSchemaOptions
  [:map])

(def ValidateOptions
  [:map
   [:fail-fast? {:default false :optional true} :boolean]])

(def ^:private make-row-schema-options-valid? (m/validator MakeRowSchemaOptions))

(def ^:private validate-dataset-options-valid? (m/validator ValidateOptions))

(defn column-key
  [k]
  (case k
    :datatype (URI. "http://www.w3.org/ns/csvw#datatype")
    :titles (URI. "http://www.w3.org/ns/csvw#titles")
    :required (URI. "http://www.w3.org/ns/csvw#required")))

(defn- extract-column-datatype
  "Returns a malli schema for column's value.

  At the moment only the simplest cases are supported, and the
  \"csvw:datatype\" field can be set to \"string\",\"double\",
  \"integer\". Additionally, the schema can contain
  \"csvw:required\".

  Example:
  - {\"csvw:datatype\": \"string\"} -> [:string]
  - {\"csvw:datatype\": \"double\", \"csvw:required\": false} -> [:maybe :double]

  "
  [csvw-col-schema]
  (let [datatype (first (get csvw-col-schema (column-key :datatype)))
        schema (get {"integer" :int "string" :string "double" :double} datatype datatype)]
    (if-not (get csvw-col-schema (column-key :required))
      [:maybe schema]
      schema)))

(defn- unpack-column-title
  "Extracts column title from the schema."
  [json-cols title]
  (cond
    ;;(string? title) title

    (and (set? title) (= 1 (count title)))
    (first title)

    :default (throw (ex-info (format "Unsupported column name value: %s" title)
                             {:title title
                              :columns json-cols}))))

(defn- schema-columns
  [json-ld-schema]
  (let [{subjects-map :subjects} json-ld-schema
        ^URI id (resource/id json-ld-schema)
        col-key (URI. "https://publishmydata.com/def/datahost/columns")
        column-keys (get-in subjects-map [id col-key])]
    (map subjects-map column-keys)))

(defn make-row-schema
  "Make a malli schema for a tuple of row values specified by
  column-names. If column names were not specified, uses all columns
  from JSON-LD schema."
  ([json-ld-schema]
   (make-row-schema json-ld-schema nil {}))
  ([json-ld-schema column-names]
   (make-row-schema json-ld-schema column-names {}))
  ([json-ld-schema column-names options]
   {:pre [(make-row-schema-options-valid? options)]
    :post [(every? some? (next %))]}
   (let [titles-key (column-key :titles)
         json-cols (schema-columns json-ld-schema)
         ;; NOTE: titles passed as a JS Array -> #{["foo" "bar"]} ?
         ;; we throw if the array has more than one element.
         fix-title (fn [schema] (update schema titles-key (partial unpack-column-title json-cols)))
         indexed (set/index (into #{} (map fix-title) json-cols) [titles-key])
         _ (assert (= (count json-cols) (count indexed))
                   "column names in schema should be distinct")
         m (reduce-kv (fn reducer [r k v]
                        ;; we know there's only item per index key,
                        ;; so we can safely unpack the value
                        (let [col-name (get k titles-key)
                              schema (extract-column-datatype (first v))]
                          (assoc r col-name
                                 (mu/update-properties schema assoc :dataset.column/name col-name))))
                      {}
                      indexed)]
     (into [:tuple] (map #(get m %) (or column-names (keys m)))))))

(defn row-schema->column-names
  "Returns a seq of column names supported by the row schema."
  [row-schema]
  (map (comp :dataset.column/name m/properties) 
       (m/children row-schema)))

(defn- validate-found-columns
  "Verifies that we could extract the required columns from the dataset.
  Throws when it's not possible."
  [schema column-names ds-column-names]
  (let [schema-col-names (set column-names)
        ds-col-names (set ds-column-names)]
    (when (not= schema-col-names
                (set/intersection ds-col-names schema-col-names))
      (log/info "column mismatch" {:schema-column-names schema-col-names
                                   :dataset-column-names ds-col-names})
      (throw (ex-info "The expected columns are not in the dataset"
                      {:schema-column-names schema-col-names
                       :dataset-column-names ds-col-names
                       :schema schema})))))

(defn validate-dataset
  "Validates a dataset row by row using the passed schema.
  Returns:
  - {:dataset DS} dataset with invalid rows with extra 'valid' colum
    when :fail-fast? = false or when the dataset is valid.
  - {:explanation ...} on failure, only when :fail-fast = true. Explanation,
    will contain the schema error as returned by `me/humanize`

  The `schema` parameter should be malli tuple schema, where each
  component is a schema for appropriate column. Each component schema
  should have `:dataset.column/name` in its
  properties (see [[make-row-schema]], [[malli.core/properties]]).

  Options:
  
  - :fail-fast? (boolean) - terminate validation on first failure."
  [dataset schema {:keys [fail-fast?] :as options}]
  (when-not (validate-dataset-options-valid? options)
    (throw (ex-info "Invalid options" {:options options})))
  (let [validator (m/validator schema)
        column-names (row-schema->column-names schema)
        validator-fn (if fail-fast?
                       (fn [cols]
                         (when-not (validator cols)
                           (throw (ex-info "Invalid row" {:type :dataset.validation/error
                                                          :schema schema
                                                          :columns cols
                                                          :explanation (-> (m/explain schema cols)
                                                                           (me/humanize))}))))
                       validator)]
    (assert (every? some? column-names)
            "Could not extract column names from row schema")
    (validate-found-columns schema column-names (tc/column-names dataset))
    (try
      {:dataset (-> dataset
                    (tc/map-columns "valid"
                                    :boolean
                                    column-names
                                    (fn col-mapper [& args]
                                      (validator-fn (vec args))))
                    (ds/filter (fn [{:strs [valid]}] (not valid))))}
      (catch ExceptionInfo ex
        (if-not (= :dataset.validation/error (-> ex ex-data :type))
          (throw ex)
          (ex-data ex))))))

(defmulti -as-dataset
  "Coerce given value (e.g. CSV string or CSV java.io.File ) to a dataset"
  (fn [v _opts] (type v)))

(defn- slurpable->dataset
  "If `slurp` can handle, so does this fn. Returns a dataset."
  [v {:keys [file-type encoding]}]
  (-> v
      slurp
      (.getBytes ^String encoding)
      (ByteArrayInputStream.)
      (tc/dataset {:file-type file-type})))

(defmethod -as-dataset File [v opts]
  (tc/set-dataset-name (slurpable->dataset v opts) (.getPath ^File v)))

(defmethod -as-dataset URL [v opts]
  (tc/set-dataset-name (slurpable->dataset v opts) (.getPath ^URL v)))

(def AsDatasetOpts
  [:map
   [:file-type {:optional true} [:enum :csv]]
   [:encoding {:optional true} [:enum "UTF-8"]]])

(def ^:private as-dataset-opts-valid? (m/validator AsDatasetOpts))

(defn as-dataset
  "Tries to turn passed value into a dataset.

  The value can be:
  - `java.io.File`
  - `java.net.URL` (e.g. a resource)
  
  See also: [[tc/dataset]]"
  [v opts]
  {:pre [(as-dataset-opts-valid? opts)]}
  (-as-dataset v (merge {:file-type :csv :encoding "UTF-8"} opts)))
