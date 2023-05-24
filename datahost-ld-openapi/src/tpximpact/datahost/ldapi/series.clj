(ns tpximpact.datahost.ldapi.series
  (:require
   [clojure.set :as set]
   [clojure.tools.logging :as log]
   [tpximpact.datahost.ldapi.util :as util]
   [malli.core :as m]
   [malli.error :as me])
  (:import
   [java.net URI]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it. It should have a trailing slash."
  (URI. "https://example.org/data/"))

(defn dataset-series-key [series-slug]
  (str (.getPath ld-root) series-slug))

(def SeriesApiParams [:map
                      [:series-slug :series-slug-string]
                      [:title {:optional true} :string]
                      [:description {:optional true} :string]])

(def registry
  (merge
   (m/class-schemas)
   (m/comparator-schemas)
   (m/base-schemas)
   (m/type-schemas)
   {:series-slug-string [:and :string [:re {:error/message "should contain alpha numeric characters and hyphens only."}
                                       #"^[a-z,A-Z,\-,0-9]+$"]]
    :url-string (m/-simple-schema {:type :url-string :pred
                                   (fn [x]
                                     (and (string? x)
                                          (try (URI. x)
                                               true
                                               (catch Exception ex
                                                 false))))})}))

(defn normalise-context [ednld]
  (let [normalised-context ["https://publishmydata.com/def/datahost/context"
                            {"@base" (str ld-root)}]]
    (if-let [context (get ednld "@context")]
      (cond
        (= context "https://publishmydata.com/def/datahost/context") normalised-context
        (= context normalised-context) normalised-context

        :else (throw (ex-info "Invalid @context" {:supplied-context context
                                                  :valid-context normalised-context})))

      ;; return normalised-context if none provided
      normalised-context)))

(defn validate-id [{:keys [series-slug] :as _api-params} cleaned-doc]
  (let [id-in-doc (get cleaned-doc "@id")]
    (cond
      (nil? id-in-doc) cleaned-doc

      (= series-slug id-in-doc) cleaned-doc

      :else (throw
             (ex-info "@id should for now be expressed as a slugged style suffix, and if present match that supplied as the API slug."
                      {:supplied-id id-in-doc
                       :expected-id series-slug})))))

(defn validate-series-context [ednld]
  (if-let [base-in-doc (get-in ednld ["@context" 1 "@base"])]
    (if (= (str ld-root) base-in-doc)
      (update ednld "@context" normalise-context)
      (throw (ex-info
              (str "@base for the dataset-series must currently be set to the linked-data root '" ld-root "'")
              {:type :validation-error
               :expected-value (str ld-root)
               :actual-value base-in-doc})))
    (update ednld "@context" normalise-context)))

(defn merge-params-with-doc [{:keys [series-slug] :as api-params} jsonld-doc]
  (let [merged-doc (merge (set/rename-keys api-params
                                           {:title "dcterms:title"
                                            :description "dcterms:description"})
                          jsonld-doc)
        cleaned-doc (-> merged-doc
                        (util/dissoc-by-key keyword?)
                        #_(update "@context" normalise-context))]

    cleaned-doc))

(defn normalise-series
  "Takes api params and an optional json-ld document of metadata, and
  returns a normalised EDN form of the JSON-LD, with the API
  parameters applied, validated and some structures like the context
  normalised."
  ([api-params]
   (normalise-series api-params nil))
  ([{:keys [series-slug] :as api-params} jsonld-doc]
   (when-not (m/validate SeriesApiParams api-params {:registry registry})
     (throw (ex-info "Invalid API parameters"
                     {:type :validation-error
                      :validation-error (-> (m/explain SeriesApiParams api-params {:registry registry})
                                            (me/humanize))})))

   (let [cleaned-doc (merge-params-with-doc api-params jsonld-doc)

         validated-doc (-> (validate-id api-params cleaned-doc)
                           (validate-series-context))

         final-doc (assoc validated-doc
                            ;; add any managed params
                          "@type" "dh:DatasetSeries"
                          "@id" series-slug
                          "dh:baseEntity" (str ld-root series-slug "/") ;; coin base-entity to serve as the @base for nested resources
                          )]
     final-doc)))

(defn- update-series [_old-series {:keys [api-params jsonld-doc] :as _new-series}]
  (log/info "Updating series " (:series-slug api-params))
  (normalise-series api-params jsonld-doc))

(defn- create-series [api-params jsonld-doc]
  (log/info "Updating series " (:series-slug api-params))
  (normalise-series api-params jsonld-doc))

(defn upsert-series
  "Takes a derefenced db state map with the shape {path jsonld} and
  upserts a new dataset series into it.

  Intended usage is via swap!

  ```
  (swap! db upsert-series {:api-params {:series-slug \"my-dataset-series\"}
                           :jsonld-doc {}})
  ```

  An upsert will insert a new series if it isn't there already.

  If the series exists already it will replace the majority of the
  fields with those newly supplied, but may ignore or manage some
  fields specially on update.

  For example a `dcterms:issued` time should not change after a
  document is updated (TODO: https://github.com/Swirrl/datahost-prototypes/issues/57)
  "
  [db {:keys [api-params jsonld-doc] :as new-series}]
  (let [series-key (dataset-series-key (:series-slug api-params))]
    (if-let [_old-series (get db series-key)]
      (update db series-key update-series new-series)
      (assoc db series-key (create-series api-params jsonld-doc)))))
