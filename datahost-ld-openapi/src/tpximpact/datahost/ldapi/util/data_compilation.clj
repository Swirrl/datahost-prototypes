(ns tpximpact.datahost.ldapi.util.data-compilation
  "Functionality related to compiling a dataset for a Revision/Release.

  At the moment the only supported input/output type is CSV files."
  (:require
   [malli.core :as m]
   [malli.error :as me]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.util.data-validation
    :refer [-as-dataset as-dataset]]))

(def registry
  (merge
   (m/class-schemas)
   (m/comparator-schemas)
   (m/base-schemas)
   (m/type-schemas)
   {:datahost.types/uri #(instance? java.net.URI %)
    :datahost.types/file #(instance? java.io.File %)
    :datahost.types/path #(instance? java.nio.file.Path %)
    :datahost.types/seq-of-maps #(= :datahost.types/seq-of-maps (type %))}))

(def dataset-input-type-valid? (m/validator [:qualified-keyword]))

(def ChangeInfo
  [:map
   {:description "Description of a change to a revision."}
   [:datahost.change.data/ref {:description "Reference/key to the data (e.g. CSV file)."}
    [:fn (comp dataset-input-type-valid? type)]]
   [:datahost.change.data/format {:description "Format of the input data, e.g. 'text/csv'"
                                  :json-schema/example "text/csv"}
    ;; 'native' means we're passing native Clojure data structures.
    [:string {:description "Mime type of the referenced data. Values as in 'dcterms:format'"}]]
   [:datahost.change/kind [:enum
                           :datahost.change.kind/append
                           :datahost.change.kind/retract]]])

(def CompileDatasetOptions
  (m/schema
   [:map
    [:changes [:sequential ChangeInfo]]]))

(def ^:private compile-dataset-opts-valid? (m/validator CompileDatasetOptions))

(defn- compile-reducer
  [ds change]
  (let [{:datahost.change/keys [kind input-format]} change]
    (case (:datahost.change/kind change)
      :datahost.change.kind/append (tc/union ds (as-dataset (:datahost.change.data/ref change) {}))
      :datahost.change.kind/retract (tc/difference ds (as-dataset (:datahost.change.data/ref change) {})))))

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
                 (when (not= :datahost.change.kind/append (:datahost.change/kind change))
                   (throw (ex-info "First change kind should be an 'append'"
                                   {:kind (:datahost.change/kind (first change))})))
                 change)]
    (reduce compile-reducer
            (as-dataset (:datahost.change.data/ref change) {})
            (next changes))))
