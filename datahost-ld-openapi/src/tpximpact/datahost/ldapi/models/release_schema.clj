(ns tpximpact.datahost.ldapi.models.release-schema
  (:require
   [malli.core :as m]
   [malli.error :as me]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
   [tpximpact.datahost.ldapi.schemas.release-schema :as s.release-schema]
   [tpximpact.datahost.ldapi.models.shared :as models.shared])
  (:import [java.net URLEncoder]))

(defn normalise-schema
  [base-entity api-params jsonld-doc]
  (when-not (s.release-schema/api-params-valid? api-params)
    (throw (ex-info "Invalid API parameters"
                    {:type :validation-error
                     :validation-error (-> (m/explain s.release-schema/ApiParams
                                                      api-params
                                                      {:registry registry})
                                           (me/humanize))})))
  (let [{:keys [series-slug release-slug]} api-params]
    (as-> jsonld-doc $
      (models.shared/merge-params-with-doc api-params $)
      (models.shared/validate-id release-slug $)
      (models.shared/validate-context $ base-entity)
      (assoc $ "@type" "dh:TableSchema"
             ;;"@type" "dh:DimensionColumn"
             "datahost:appliesToRelease" (format "/data/%s/releases/%s"
                                                 (URLEncoder/encode series-slug "UTF-8") 
                                                 (URLEncoder/encode release-slug "UTF-8"))
             "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180")
      (update $ "dh:columns" (fn [cols]
                               (mapv (fn [col]
                                       (if (= "dh:DimensionColumn" (get col "@type"))
                                         col
                                         (assoc col "@type" "dh:DimensionColumn")))
                                     cols))))))

(def InsertApiParams
  [:map
   [:op.upsert/keys [:map
                     [:series :any]
                     [:release :any]
                     [:release-schema :any]]]])

(def ^:private insert-api-params-valid? (m/validator InsertApiParams))

(defn insert-schema
  "Potentially adds a schema and updates the release with a reference to
  the schema.

  Returned meta data will contain `:op` key."
  [db api-params incoming-jsonld-doc]
  {:pre [(insert-api-params-valid? api-params)]}
  #_(let [{:keys [series-slug release-slug schema-slug]
         {series-key :series 
          release-key :release
          schema-key :release-schema} :op.upsert/keys} api-params
        base (-> db (get series-key) (get "dh:baseEntity"))
        schema (get db schema-key)
        op (models.shared/infer-upsert-op [] api-params schema incoming-jsonld-doc)]
    (vary-meta
     (if (some? schema)
       db
       (-> db
           (assoc schema-key (normalise-schema base 
                                               api-params 
                                               incoming-jsonld-doc))
           (models.release/upsert-release api-params 
                                          (assoc (get db release-key)
                                                 "datahost:hasSchema"
                                                 (format "/data/%s/releases/%s/schemas/%s"
                                                         series-slug release-slug schema-slug)))))
     assoc :op op)))
