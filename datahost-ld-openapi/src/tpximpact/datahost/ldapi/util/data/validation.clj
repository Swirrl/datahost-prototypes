(ns tpximpact.datahost.ldapi.util.data.validation
  (:require
   [clojure.set :as set]
   [malli.core :as m]
   [malli.error :as me]
   [malli.util :as mu]
   [clojure.tools.logging :as log]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [tpximpact.datahost.uris :as uris]
   [tpximpact.datahost.ldapi.compact :as compact]
   [tpximpact.datahost.ldapi.util.data.internal :as data.internal]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.common :as s.common])
  (:import (clojure.lang ExceptionInfo)
           (java.io ByteArrayInputStream File)
           (java.net URI URL)
           (org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil)))

(def MakeRowSchemaOptions
  [:map])

(def ValidateOptions
  [:map
   [:fail-fast? {:default false :optional true} :boolean]])

(def ^:private make-row-schema-options-valid? (m/validator MakeRowSchemaOptions))

(def ^:private validate-dataset-options-valid? (m/validator ValidateOptions))

(def ^:private validate-ld-release-schema-input-valid? (m/validator routes-shared/LdSchemaInput))

(def column-uri-keys
  {:datatype (URI. "http://www.w3.org/ns/csvw#datatype")
   :name (URI. "http://www.w3.org/ns/csvw#name")
   :titles (URI. "http://www.w3.org/ns/csvw#titles")
   :required (URI. "http://www.w3.org/ns/csvw#required")
   :type (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")})

(defmacro make-column-datatype-extractor
  "Creates a function taking in a JSON or LD column schema (e.g. returned from
  triplestore) and returning a malli schema for the column's value.

  At the moment only the simplest cases are supported, and the
  \"csvw:datatype\" field can be set to \"string\",\"double\", \"int\",
  \"integer\". Additionally, the schema can contain
  \"csvw:required\".

  Example:
  - {\"csvw:datatype\": \"string\"} -> [:string]
  - {\"csvw:datatype\": \"double\", \"csvw:required\": false} -> [:maybe :double]

  "
  [{:keys [input-format] :as opts}]
  {:pre [(contains? #{:ld :json} input-format)]}
  (let [{:keys [datatype required]}
        (case input-format
          :json {:datatype #(get % "csvw:datatype")
                 :required #(get % "csvw:required")}
          :ld {:datatype #(first (get % (:datatype column-uri-keys)))
               :required #(get % (:required column-uri-keys))})
        in-schema (gensym "in-schema-")]
    `(fn extractor-fn# [~in-schema]
       (let [datatype# (~datatype ~in-schema)
             schema# (get {"int" :int
                           "integer" :int
                           "string" :string
                           "double" :double} datatype# datatype#)]
         (if-not (~required ~in-schema)
           [:maybe schema#]
           schema#)))))

(def json-column-datatype-extractor (make-column-datatype-extractor {:input-format :json}))

(def ld-column-datatype-extractor (make-column-datatype-extractor {:input-format :ld}))

(defn- schema-columns
  [json-ld-schema]
  (let [{subjects-map :subjects} json-ld-schema
        ^URI id (resource/id json-ld-schema)
        col-key (URI. "https://publishmydata.com/def/datahost/columns")
        column-keys (get-in subjects-map [id col-key])]
    (map subjects-map column-keys)))

(def DatasetRow
  "Dataset row schema should be a malli tuple where each child schema
  has required properties set.

  Example schema for dataset with two columns:

  ```
    [:tuple [:maybe {:dataset.column/name \"Foo\"
                     :dataset.column/type <URI>
                     :dataset.column/datatype :string]
            [:int {:dataset.column/name \"My Measure\"
                   :dataset.column/type <URI>
                   :dataset.column/datatype :int}]]
  ```"
  (let [props-schema [:map
                      [:dataset.column/name :string]
                      [:dataset.column/type (into [:enum ] (vals uris/column-types))]
                      [:dataset.column/datatype
                       {:description "Keyword that [[tablecloth.api]] can use as a column type"}
                       :keyword]]
        validate-seq (m/validator (m/schema [:sequential props-schema]
                                            {:registry s.common/registry}))]
    [:and
     [:fn {:error/message "Not a malli schema."} m/schema?]
     [:fn {:error/message "Not a non-empty tuple schema"}
      (fn [s]
        (let [children (m/children s)]
         (and (sequential? children)
              (seq children))))]
     [:fn {:error/message "Properties of children schemas are invalid"}
      (fn props-pred [s]
        (validate-seq (map m/properties (m/children s))))]]))

(def ^:private row-schema-valid?
  "This validator is meant to be used in dev environment as assertion
  condition."
  (let [validator (m/validator DatasetRow)]
    (fn row-schema-validator [schema]
      (or (validator schema)
          (throw (java.lang.AssertionError.
                  "Invalid DatasetRow schema"
                  (ex-info "Invalid DatasetRow schema"
                           {:malli/explanation (me/humanize (m/explain DatasetRow schema))})))))))


(defn- set-col-schema-props
  [datatype-schema col-name col-type]
  (mu/update-properties datatype-schema assoc
                        :dataset.column/datatype (if (vector? datatype-schema)
                                                   (second datatype-schema)
                                                   datatype-schema)
                        :dataset.column/name col-name
                        :dataset.column/type col-type))

(defn- make-row-schema--cont
  [components]
  (vary-meta (m/schema (into [:tuple] components))
             assoc :datahost/type :datahost.types/dataset-row-schema))

(defn- make-missing-components-error
  [exception-data]
  (ex-info "Could not find desired column names in passed in schema."
           exception-data))

(defn- validate-no-column-name-duplicates
  [cols indexed]
  (when (not= (count cols) (count indexed))
    (throw (ex-info (format "column names in schema should be distinct: schema=%s, indexed=%s"
                            (count cols) (count indexed))
                    {:columns cols}))))

(defn- make-row-schema*
  [columns
   column-names
   {:keys [name-key type-key column-datatype-extractor uri-lookup unpack?]
    :as options}]
  {:pre [(make-row-schema-options-valid? options)]
   :post [(row-schema-valid? %)]}
  (let [get-name (cond->> #(get % name-key)
                   unpack? (comp first))
        indexed (set/index (set columns) [name-key])
        _ (validate-no-column-name-duplicates columns indexed)
        m (reduce-kv (fn reducer [result index-key v]
                       ;; we validated that there's only item per
                       ;; index key, so we can safely unpack the value
                       (let [item (first v)
                             col-name (get-name index-key)
                             schema (column-datatype-extractor item)]
                         (assoc result col-name
                                (set-col-schema-props schema
                                                      col-name
                                                      (uri-lookup (get item type-key))))))
                     {}
                     indexed)
        components (map #(get m %) (or column-names (keys m)))]
    (when-not (every? some? components)
      (throw (make-missing-components-error {:extracted-components components
                                             :column-names column-names
                                             :schema-cols columns
                                             :keys (keys m)})))
    (make-row-schema--cont components)))

(defn make-row-schema
  "Make a malli schema for a tuple of row values specified by
  column-names. If column names were not specified, uses all columns
  from JSON-LD schema."
  ([json-ld-schema]
   (make-row-schema json-ld-schema nil {}))
  ([json-ld-schema column-names]
   (make-row-schema json-ld-schema column-names {}))
  ([json-ld-schema column-names options]
   (make-row-schema* (schema-columns json-ld-schema)
                     column-names
                     {:name-key (:name column-uri-keys)
                      :type-key (:type column-uri-keys)
                      :column-datatype-extractor ld-column-datatype-extractor
                      :uri-lookup first
                      :unpack? true} )))

(defn make-row-schema-from-json
  "Make a malli schema for a tuple of row values specified by
  column-names. If column names were not specified, uses all columns
  from the JSON schema.

  The input json should conform to `routes-shared/LdSchemaInput`"
  ([json]
   (make-row-schema-from-json json nil))
  ([json column-names]
   (when-not (validate-ld-release-schema-input-valid? json)
     (throw (ex-info "Invalid input release schema" {:release-schema json})))
   (make-row-schema* (get json "dh:columns")
                     column-names
                     {:name-key "csvw:name"
                      :type-key "@type"
                      :column-datatype-extractor json-column-datatype-extractor
                      :uri-lookup uris/json-column-types})))

(defn parsing-errors [column]
  (assert (instance? tech.v3.dataset.impl.column.Column column))
  (-> column meta :unparsed-data))

(defn dataset-errors?
  [ds]
  (contains? (set (tc/column-names ds)) :$error))

(defmulti -dataset-ctor-opts
  "Create dataset creation options based on :datahost/type."
  (comp :datahost/type meta))

(defn dataset-ctor-opts [value]
  (-dataset-ctor-opts value))

(defn parse-double*
  "Returns a double or nil."
  [v]
  (try
    (XMLDatatypeUtil/parseDouble v)
    (catch NumberFormatException ex
      nil)))

(defn- datatype+parse-fn
  "Returns a tuple of [datatype parse-fn] as required
  by [[tablecloth.api]]"
  [{:dataset.column/keys [name datatype] :as props}]
  (let [parse-col (fn parse-col* [parse-fn v]
                    ;;NOTE: the parse-fn should return parsed value or nil
                    (cond
                      (= "" v) :tech.v3.dataset/missing
                      :else (or (parse-fn v) :tech.v3.dataset/parse-failure)))]
    [name
     (cond
       (= :int datatype)
       [datatype (partial parse-col #(clojure.core/parse-long %))]

       (= :double datatype)
       [datatype (partial parse-col parse-double*)]


       (= :string datatype)
       [datatype (partial parse-col str)]

       :else
       (throw (ex-info (str "Unsupported datatype: " datatype)
                       {:props props})))]))

(defmethod -dataset-ctor-opts :datahost.types/dataset-row-schema [schema]
  (let []
    {:parser-fn (into {} (comp (map m/properties)
                               (map datatype+parse-fn))
                      (m/children schema))}))

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
    (when-not (every? some? column-names)
      (throw (ex-info "Could not extract column names from row schema"
                      {:row-schema schema :column-names column-names})))
    (validate-found-columns schema column-names (tc/column-names dataset))
    (try
      {:dataset (-> dataset
                    (tc/map-columns "valid"
                                    :boolean
                                    column-names
                                    (fn col-mapper [& args]
                                      (validator-fn (vec args))))
                    (ds/filter (fn [{:strs [valid]}] (not valid)))
                    (cond-> fail-fast? (ds/remove-column "valid")))}
      (catch ExceptionInfo ex
        (if-not (= :dataset.validation/error (-> ex ex-data :type))
          (throw ex)
          (ex-data ex))))))

(defmulti -as-dataset
  "Coerce given value (e.g. CSV string or CSV java.io.File ) to a dataset"
  (fn [v _opts] (type v)))

(defn slurpable->dataset
  "If `slurp` can handle, so does this fn. Returns a dataset."
  [v {:keys [encoding] :as opts}]
  (-> v
      slurp
      (.getBytes ^String (or encoding "UTF-8"))
      (ByteArrayInputStream.)
      (tc/dataset opts)))

(defmethod -as-dataset File [v opts]
  (tc/set-dataset-name (slurpable->dataset v opts) (.getPath ^File v)))

(defmethod -as-dataset URL [v opts]
  (tc/set-dataset-name (slurpable->dataset v opts) (.getPath ^URL v)))

(defmethod -as-dataset java.nio.file.Path [^java.nio.file.Path v opts]
  (tc/set-dataset-name (slurpable->dataset (.toFile v) opts)
                       (.getFileName v)))

(defmethod -as-dataset java.io.InputStream [^java.io.InputStream v opts]
  (slurpable->dataset v opts))

(defmethod -as-dataset :datahost.types/seq-of-maps [v _opts]
  (tc/dataset v))

(def AsDatasetOpts
  [:map
   [:file-type {:optional true} [:enum :csv]]
   [:encoding {:optional true} [:enum "UTF-8"]]
   [:store {:optional true} [:fn some?]]
   [:enforce-schema
    {:optional true
     :description "Schema to validate against on dataset creation."}
    [:and
     [:fn m/schema?]
     [:fn #(-> % meta :datahost/type)]]]
   [:convert-types {:optional true} [:map
                                     [:row-schema DatasetRow]]]])

(def ^:private as-dataset-opts-valid? (m/validator AsDatasetOpts))


(defn convert-dataset-types
  "Returns a dataset with column types converted to ones specified in
  row-schema."
  [dataset row-schema]
  {:pre [(m/validate DatasetRow row-schema)]}
  (let [cols (for [col (m/children row-schema)]
               (m/properties col))]
    (tc/convert-types dataset
                      (map :dataset.column/name cols)
                      (map :dataset.column/datatype cols))))

(defn as-dataset
  "Tries to turn passed value into a dataset.

  Pass :enforce-schema option to ensure the created dataset conforms
  to a particular schema. The value should have an implementation of
  `-dataset-ctor-opts`. When the data does not conform to the schema,
  an exception with :type ::dataset-creation is thrown.

  The value can be:
  - `java.io.File`
  - `java.net.URL` (e.g. a resource)

  See also: [[AsDatasetOpts]], [[tc/dataset]]"
  [v opts]
  {:pre [(as-dataset-opts-valid? opts)]}
  (let [{convert-opts :convert-types row-schema :enforce-schema} opts
        opts (cond-> opts
               row-schema (merge (dataset-ctor-opts row-schema)))
        ds (-as-dataset v (merge {:file-type :csv :encoding "UTF-8"} opts))]
    (when (dataset-errors? ds)
      (throw (ex-info "Dataset creation failure. See dataset ':$error' column."
                      {:type ::dataset-creation :options opts})))
    (when row-schema
      (let [err-samples (into {} (comp (map #(tc/column ds %))
                                       (keep (fn [col]
                                               ;; get a sample of values causing trouble
                                               (when-let [errs (parsing-errors col)]
                                                 [(-> col meta :name) (take 3 errs)]))))
                              (row-schema->column-names row-schema))]
        (when-not (empty? err-samples)
         (throw (ex-info (str "Dataset creation failure: failures in columns: " err-samples)
                         {:type ::dataset-creation
                          :error-samples err-samples
                          :options opts})))))

    (cond-> ds
      convert-opts (convert-dataset-types (:row-schema convert-opts)))))

(defn validate-ld-release-schema-input [ld-schema]
  (when-not (validate-ld-release-schema-input-valid? ld-schema)
    (throw (ex-info "Invalid Release schema input" {:ld-schema ld-schema}))))

(defn validate-row-uniqueness
  "Throws when number of dataset's unique ids != number of rows."
  ([ds hash-col-name] (validate-row-uniqueness ds hash-col-name nil))
  ([ds hash-col-name ex-data-payload]
   (when-not (= (tc/row-count (tc/unique-by ds hash-col-name))
                (tc/row-count ds))
     (throw (ex-info "Possible data issue: are combinations of all non-measure values unique?"
                     (cond-> {:type :dataset.validation/error
                              :coords-column-name hash-col-name
                              :dataset-name (tc/dataset-name ds)}
                       ex-data-payload (merge ex-data-payload)))))))

(defn validate-row-coords-uniqueness
  "Throws when number of dataset's unique coords != number of rows.

  'coords' refers to the combination of attribute/dimension values of
  a row. "
  [ds row-schema]
  (validate-row-uniqueness (data.internal/add-coords-column ds row-schema)
                           data.internal/coords-column-name))
