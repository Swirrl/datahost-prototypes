(ns tpximpact.datahost.ldapi.models.release
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]
   [tpximpact.datahost.ldapi.schemas.release :as s.release]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]))

(defn normalise-release [base-entity api-params jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        _ (assert base-entity "Expected base entity to be set")]
    (when-not (s.release/api-params-valid? api-params)
      (throw (ex-info "Invalid API parameters"
                      {:type :validation-error
                       :validation-error (-> (m/explain s.release/ApiParams
                                                        api-params
                                                        {:registry registry})
                                             (me/humanize))})))

    (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)

          validated-doc (-> (models-shared/validate-id release-slug cleaned-doc)
                            (models-shared/validate-context base-entity))

          final-doc (assoc validated-doc
                           ;; add managed properties,
                           ;; timestamps, and dcat:seriesMember <release> to series metadata
                           "@type" "dh:Release"
                           "@id" release-slug
                           "dcat:inSeries" (str "../" series-slug))]
      final-doc)))

(defn- update-release [old-release base-entity api-params jsonld-doc]
  (log/info "Updating release " (:series-slug api-params) "/" (:release-slug api-params))
  (->> jsonld-doc
       (normalise-release base-entity api-params)
       (models-shared/issued+modified-dates api-params old-release)))

(defn- create-release [base-entity api-params jsonld-doc]
  (log/info "Creating release " (:series-slug api-params) "/" (:release-slug api-params))
  (->> jsonld-doc 
       (normalise-release base-entity api-params)
       (models-shared/issued+modified-dates api-params nil)))

(def ^:private api-query-params-keys (m/explicit-keys s.release/ApiQueryParams))

(defn upsert-release 
  "Returns potentially updated db value."
  [db api-params jsonld-doc]
  {:pre [(s.release/upsert-args-valid? [db api-params jsonld-doc])]
   :post [(models-shared/validate-issued-unchanged jsonld-doc %)
          (models-shared/validate-modified-changed jsonld-doc %)]}
  (let [{{series-key :series release-key :release} :op.upsert/keys} api-params
        series (get db series-key)
        base-entity (get series "dh:baseEntity")
        old-release (get db release-key)
        op (models-shared/infer-upsert-op api-query-params-keys api-params
                                          old-release jsonld-doc)]

    (vary-meta
     (case op
       :noop db
       :update (update db release-key update-release base-entity api-params
                       (or jsonld-doc old-release))
       :create (assoc db release-key (create-release base-entity api-params jsonld-doc)))
     assoc :op op)))
