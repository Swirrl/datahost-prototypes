(ns tpximpact.datahost.ldapi.util.data-compilation
  "Functionality related to compiling a dataset for a Revision/Release.

  At the moment the only supported input/output type is CSV files."
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [tablecloth.api :as tc]
   [tpximpact.datahost.uris :as uris]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.util.data-validation
    :refer [-as-dataset as-dataset]
    :as data-validation])
  (:import
   (net.openhft.hashing LongHashFunction)))

(def hash-column-name
  "Name of the column holding the hash of the row (excluding the measurement itself.)"
  "datahost.row/hash")

(defmethod -as-dataset tech.v3.dataset.impl.dataset.Dataset [v _opts]
  v)

;;; use content hash (a store key) to get the data

(defmethod -as-dataset String [v {:keys [store] :as opts}]
  (assert (= 64 (count v)))
  (assert store)
  (with-open [is (store/get-data store v)]
   (as-dataset is opts)))

(def ^:private hash-fn (LongHashFunction/xx128low 1337))

(defn make-columnwise-hasher
  "Returns a function seq -> long"
  []
  (fn hasher* [& columns]
    (let [^StringBuilder sb (StringBuilder.)
          h ^LongHashFunction hash-fn]
      (.append sb "|")
      (loop [columns columns]
        (if-not (seq columns)
          (.hashChars h sb)
          (do
            (.append sb (first columns))
            (.append sb "|")
            (recur (next columns))))))))

(def ^:private measure-type-uri (:measure uris/column-types))

(defn- make-column-info-xform
  [filter-fn]
  (comp (map m/properties)
        (filter filter-fn)
        (map #(get % :dataset.column/name))))

(def ^:private tuple-schema-component-names-xform
  "Transducer transforming elements of [[m/children]] output into strings."
  (make-column-info-xform #(not= measure-type-uri (get % :dataset.column/type))))

(def ^:private tuple-schema-measure-names-xform
  "Transducer transforming elements of [[mu/children]] output into strings."
  (make-column-info-xform #(= measure-type-uri (get % :dataset.column/type))))

(defn check-dataset-has-columns
  [dataset column-names]
  (when-not (every? #(tc/has-column? dataset %) column-names)
    (throw (ex-info "Not every required column is in the dataset"
                    {:dataset-columns (tc/column-names dataset)
                     :schema-columns column-names}))))

(defn- compile--extract-component-column-names
  "Returns a seq of column names. Throws when any of the component
  columns extracted from release-schema are not present in the
  dataset."
  [row-schema]
  (sort (sequence tuple-schema-component-names-xform
                  ;;(mu/subschemas row-schema)
                  (m/children row-schema))))

(defn- compile--extract-measure-column-name
  [row-schema]
  {:post [some?]}
  (first (sort (sequence tuple-schema-measure-names-xform
                         ;;(mu/subschemas row-schema)
                         (m/children row-schema)))))

(defn- add-id-column
  "Adds a column column containing unique id of the measurment
  (based on component names from row-schema)."
  [dataset row-schema]
  (assert row-schema)
  (let [col-names (compile--extract-component-column-names row-schema)
        hasher (make-columnwise-hasher)]
    (check-dataset-has-columns dataset col-names)
    (-> dataset
        (tc/map-columns hash-column-name
                        :long
                        col-names
                        hasher)
        (vary-meta assoc ::hash-column hash-column-name))))

;;; main functionality

(def dataset-input-type-valid? (m/validator [:or
                                             ;; specify :type in metadata
                                             [:qualified-keyword]
                                             ;; or we can accept an object of some class
                                             [:fn #(instance? java.lang.Class %)]]))

(def ChangeInfo
  [:map
   {:description "Description of a change to a revision."}
   [:datahost.change.data/ref {:description "Reference/key to the data (e.g. CSV file)."}
    [:fn (comp dataset-input-type-valid? type)]]
   [:datahost.change.data/format {:description "Format of the input data, e.g. 'text/csv'"
                                  :json-schema/example "text/csv"}
    ;; 'native' means we're passing native Clojure data structures.
    [:string {:description "Mime type of the referenced data. Values as in 'dcterms:format'"}]]
   [:datahost.change/kind s.common/ChangeKind]])

(def CompileDatasetOptions
  (m/schema
   [:map
    [:changes [:sequential ChangeInfo]]
    [:store {:optional true} [:fn some?]]
    [:row-schema data-validation/DatasetRow]]))

(defn- has-hashed-column? [ds]
  (tc/has-column? ds hash-column-name))

(defmulti -apply-change
  "Returns a dataset.

  Assumptions:
  
  - the base dataset already has column with hashed (named in `hash-column-name`)
  - the input and result datasets fit in memory."
  (fn dispatch [ctx base-ds change-ds] (:datahost.change/kind ctx)))

(defmethod -apply-change :dh/ChangeKindAppend [ctx base-ds change-ds]
  (assert (has-hashed-column? base-ds))
  (let [{:keys [row-schema]} ctx]
    (-> base-ds
        (tc/concat (add-id-column change-ds row-schema))
        ;; it's an 'append', so we remove already existing entries
        (tc/unique-by [hash-column-name] {:strategy :first}))))

(defmethod -apply-change :dh/ChangeKindRetract [{:keys [row-schema] :as _ctx} base-ds change-ds]
  (assert (has-hashed-column? base-ds))
  (let [change-ds+id (add-id-column change-ds row-schema)]
    (tc/difference base-ds change-ds+id)))

(defmethod -apply-change :dh/ChangeKindCorrect [ctx base-ds change-ds]
  (assert (has-hashed-column? base-ds))
  (let [{:keys [measure-column row-schema]} ctx
        change-ds-name (tc/dataset-name change-ds)
        right-col-name (if (= "_unnamed" change-ds-name)
                         (str "right." measure-column)
                         (str change-ds-name "." measure-column))
        change-ds+id (add-id-column change-ds row-schema)
        corrections-ds (-> (tc/inner-join change-ds+id base-ds [hash-column-name])
                           (tc/select-columns (-> (tc/column-names base-ds)
                                                  set
                                                  (disj right-col-name))))]
    (-> base-ds
        (tc/concat corrections-ds)
        (tc/unique-by [hash-column-name] {:strategy :last}))))

(defn make-change-context
  [change-kind row-schema]
  {:row-schema row-schema
   :datahost.change/kind change-kind
   :hashable-columns (compile--extract-component-column-names row-schema)
   :measure-column (compile--extract-measure-column-name row-schema)})

(defn apply-change
  [context base-ds change-ds]
  {:pre [(has-hashed-column? base-ds)]}
  (-apply-change context base-ds change-ds))

(defn- ensure-components-hash-column
  [dataset row-schema]
  (if (tc/has-column? dataset hash-column-name)
    dataset
    (add-id-column dataset row-schema)))

(def ^:private compile-dataset-opts-valid? (m/validator CompileDatasetOptions))

(defn- compile-reducer
  "Reducer function for creating a dataset out of a seq of [[ChangeInfo]]s"
  [{row-schema :row-schema :as opts} ds change]
  (let [{:datahost.change/keys [kind]} change
        ctx (make-change-context kind row-schema)
        dataset (as-dataset (:datahost.change.data/ref change) opts)]
    (log/trace "compile-reducer:"
               {:new {:dataset (tc/dataset-name dataset)
                      :row-count (tc/row-count dataset) :kind kind}
                :old {:dataset (tc/dataset-name ds)
                      :row-count (tc/row-count ds)}})
    (apply-change ctx (ensure-components-hash-column ds row-schema) dataset)))

(defn compile-dataset
  "Returns a dataset.

  'opts' should conform to `CompileDatasetOptions` schema."
  [opts]
  (when-not (compile-dataset-opts-valid? opts)
    (throw (ex-info "Illegal options" {:opts opts
                                       :malli/explanation (-> (m/explain CompileDatasetOptions opts)
                                                              (me/humanize))})))
  (let [{:keys [changes]} opts
        change (when-let [change (first changes)]
                 (when (not= :dh/ChangeKindAppend (:datahost.change/kind change))
                   (throw (ex-info "First change kind should be an 'append'"
                                   {:kinds (map :datahost.change/kind changes)})))
                 change)
        ds-opts opts
        base-ds (-> change :datahost.change.data/ref (as-dataset ds-opts))
        _ (when (tc/has-column? base-ds hash-column-name)
            (throw (ex-info (format "Base dataset already contains '%s' column"
                                    hash-column-name)
                            {:columns (vec (tc/column-names base-ds))})))
        base-ds (ensure-components-hash-column base-ds (:row-schema opts))]
    ;; let's ensure data issues in dev are immediately revealed
    (assert (= (tc/row-count (tc/unique-by base-ds hash-column-name))
               (tc/row-count base-ds))
            "Possible data issue: are combinations of all non-measure values unique?")
    (-> (reduce (partial compile-reducer ds-opts)
                base-ds
                (next changes))
        ;; only return columns that teh caller passed in
        (tc/select-columns (disj (set (tc/column-names base-ds))
                                 hash-column-name)))))
