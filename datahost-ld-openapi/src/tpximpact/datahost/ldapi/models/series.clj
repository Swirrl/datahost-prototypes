(ns tpximpact.datahost.ldapi.models.series
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
   [tpximpact.datahost.ldapi.schemas.series :as s.series]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]))

(def api-params-valid? (m/validator s.series/ApiParams))

;;; ---- NORMALISE

(defn normalise-series
  "Takes api params and an optional json-ld document of metadata, and
  returns a normalised EDN form of the JSON-LD, with the API
  parameters applied, validated and some structures like the context
  normalised."
  ([api-params]
   (normalise-series api-params nil))
  ([{:keys [series-slug] :as api-params} jsonld-doc]
   (when-not (api-params-valid? api-params)
     (throw (ex-info "Invalid API parameters"
                     {:type :validation-error
                      :validation-error (-> (m/explain s.series/ApiParams
                                                       api-params
                                                       {:registry registry})
                                            (me/humanize))})))
   (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)
         validated-doc (-> (models-shared/validate-id series-slug cleaned-doc)
                           (models-shared/validate-context (str models-shared/ld-root)))

         final-doc (assoc validated-doc
                          ;; add any managed params
                          "@type" "dh:DatasetSeries"
                          "@id" series-slug
                          ;; coin base-entity to serve as the @base for nested resources
                          "dh:baseEntity" (str models-shared/ld-root series-slug "/"))]
     final-doc)))

;;; ---- UPDATE

(defn- update-series [old-series api-params jsonld-doc]
  ;; we don't want the client to overwrite the 'issued' value
  {:pre [(some? jsonld-doc)
         (models-shared/validate-issued-unchanged old-series jsonld-doc)]
   :post [(some? (get % "dcterms:issued"))
          (some? (get % "dcterms:modified"))]}
  (log/info "Updating series " (:series-slug api-params))
  (->> jsonld-doc
       (normalise-series api-params)
       (models-shared/issued+modified-dates api-params old-series)))

;;; ---- CREATE

(defn- create-series [api-params jsonld-doc]
  (log/info "Creating series " (:series-slug api-params))
  (->> jsonld-doc
       (normalise-series api-params)
       (models-shared/issued+modified-dates api-params nil)))

;;; ---- UPSERT

(def ^:private api-query-params-keys (m/explicit-keys s.series/ApiQueryParams))

(defn upsert-series
  "Takes a derefenced db state map with the shape {path jsonld} and
  upserts a new dataset series into it.

  Intended usage is via swap!

  ```
  (swap! db upsert-series {:series-slug \"my-dataset-series\"} {})
  ```

  An upsert will insert a new series if it isn't there already.

  If the series exists already it will replace the majority of the
  fields with those newly supplied, but may ignore or manage some
  fields specially on update.

  For example a `dcterms:issued` time should not change after a
  document is updated."
  ([db api-params jsonld-doc]
   {:pre [(s.series/upsert-args-valid? [db api-params jsonld-doc])]
    :post [(models-shared/validate-issued-unchanged jsonld-doc %)
           (models-shared/validate-modified-changed jsonld-doc %)]}
   (let [series-key (models-shared/dataset-series-key (:series-slug api-params))
         old-series (get db series-key)
         op (models-shared/infer-upsert-op api-query-params-keys api-params 
                                           old-series jsonld-doc)]
     (vary-meta
      (case op
        :noop db
        :update (update db series-key update-series api-params
                        (or jsonld-doc old-series))
        :create (assoc db series-key (create-series api-params jsonld-doc)))
      assoc :op op))))
