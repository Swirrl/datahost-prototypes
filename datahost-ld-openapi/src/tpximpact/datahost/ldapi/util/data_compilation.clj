(ns tpximpact.datahost.ldapi.util.data-compilation
  "Functionality related to compiling a dataset for a Revision/Release.

  At the moment the only supported input/output type is CSV files."
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.util.data-validation
    :refer [-as-dataset as-dataset]]))

(defmethod -as-dataset tech.v3.dataset.impl.dataset.Dataset [v _opts]
  v)

;;; use content hash (a store key) to get the data

(defmethod -as-dataset String [v {:keys [store] :as opts}]
  (assert (= 64 (count v)))
  (assert store)
  (with-open [is (store/get-data store v)]
   (as-dataset is opts)))

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
   [:datahost.change/kind [:enum
                           :dh/ChangeKindAppend
                           :dh/ChangeKindRetract]]])

(def CompileDatasetOptions
  (m/schema
   [:map
    [:changes [:sequential ChangeInfo]]
    [:store {:optional true} [:fn some?]]]))

(def ^:private compile-dataset-opts-valid? (m/validator CompileDatasetOptions))

(defn- compile-reducer
  [opts ds change]
  (let [{:datahost.change/keys [kind input-format]} change
        dataset (as-dataset (:datahost.change.data/ref change) opts)]
    (log/trace "reducer:"
               {:new {:dataset (tc/dataset-name dataset) :row-count (tc/row-count dataset) :kind kind}
                :old {:dataset (tc/dataset-name ds) :row-count (tc/row-count ds)}})
    (case (:datahost.change/kind change)
      :dh/ChangeKindAppend (tc/concat ds dataset)
      :dh/ChangeKindRetract (tc/difference ds dataset))))

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
        ds-opts opts]
    (reduce (partial compile-reducer ds-opts)
            (as-dataset (:datahost.change.data/ref change) ds-opts)
            (next changes))))
