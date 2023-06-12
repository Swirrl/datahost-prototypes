(ns tpximpact.datahost.ldapi.models.release
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]))

(def ReleaseApiParams [:map
                       [:series-slug :slug-string]
                       [:release-slug :slug-string]
                       [:title {:optional true} :string]
                       [:description {:optional true} :string]])

(defn validate-release-context [ednld]
  ;; TODO
  ;; (if-let [base-in-doc (get-in ednld ["@context" 1 "@base"])]

  ;;   )
  ednld
  )

(defn normalise-release [base-entity api-params jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        _ (assert base-entity "Expected base entity to be set")
        context ["https://publishmydata.com/def/datahost/context"
                 {"@base" base-entity}]]
    (when-not (m/validate ReleaseApiParams
                          api-params
                          {:registry models-shared/registry})
      (throw (ex-info "Invalid API parameters"
                      {:type :validation-error
                       :validation-error (-> (m/explain ReleaseApiParams
                                                        api-params
                                                        {:registry models-shared/registry})
                                             (me/humanize))})))

    (let [cleaned-doc (models-shared/merge-params-with-doc api-params jsonld-doc)
          validated-doc (-> (models-shared/validate-id api-params cleaned-doc)
                            (validate-release-context))])

    (->
     (assoc "@context" context
            "@id" release-slug
            "dcat:inSeries" (str "../" series-slug)))))

(defn- update-release [_old-release base-entity api-params jsonld-doc]
  (normalise-release base-entity api-params jsonld-doc))

(defn- update-release [_old-release base-entity api-params jsonld-doc]
  (log/info "Updating release " (:series-slug api-params) "/" (:release-slug api-params))
  (normalise-release base-entity api-params jsonld-doc))

(defn- create-release [base-entity api-params jsonld-doc]
  (log/info "Creating release " (:series-slug api-params) "/" (:release-slug api-params))
  (normalise-release base-entity api-params jsonld-doc))

(defn upsert-release [db api-params jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        release-key (models-shared/release-key series-slug release-slug)
        series-key (models-shared/dataset-series-key series-slug)
        series (get db series-key)
        base-entity (get series "dh:baseEntity")]

    (if-let [_old-release (get db release-key)]
      (update db release-key update-release base-entity api-params jsonld-doc)
      (assoc db release-key (create-release base-entity api-params jsonld-doc)))))
