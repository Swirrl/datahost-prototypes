(ns tpximpact.datahost.ldapi.models.release
  (:require
   [tpximpact.datahost.ldapi.models.series :as series]))

;; (defn upsert-release [db {:keys [api-params jsonld-doc] :as new-release}]
;;     ;[db {:keys [series-slug release-slug] :as api-params} jsonld-doc]
;;    (let [{:keys [series-slug release-slug]} api-params
;;          release-path (str (.getPath series/ld-root) series-slug "/" release-slug)
;;          series-path (str (.getPath series/ld-root) series-slug)
;;          series (get db series-path)
;;          base-entity (get series "dh:baseEntity")]

;;      (if-let [old-release (get db release-path)]
;;        (update db release-path update-release base-entity new-release)
;;        (assoc db release-path (normalise-release base-entity new-release)))))


(defn normalise-release [base-entity {:keys [api-params jsonld-doc]}]
  (let [{:keys [series-slug release-slug]} api-params
        _ (assert base-entity "Expected base entity to be set")
        context ["https://publishmydata.com/def/datahost/context"
                 {"@base" base-entity}]]

    (-> (series/merge-params-with-doc api-params jsonld-doc)
        (assoc "@context" context
               "@id" release-slug
               "dcat:inSeries" (str "../" series-slug)))))

(defn- update-release [_old-release base-entity {:keys [api-params _jsonld-doc] :as new-release}]
  (log/info "Updating release " (:series-slug api-params) "/" (:release-slug api-params))
  (normalise-release base-entity new-release))

(defn upsert-release [db {:keys [api-params jsonld-doc] :as new-release}] ;[db {:keys [series-slug release-slug] :as api-params} jsonld-doc]
  (let [{:keys [series-slug release-slug]} api-params
        release-path (str (.getPath series/ld-root) series-slug "/" release-slug)
        series-path (str (.getPath series/ld-root) series-slug)
        series (get db series-path)
        base-entity (get series "dh:baseEntity")]

    (if-let [old-release (get db release-path)]
      (update db release-path update-release base-entity new-release)
      (assoc db release-path (normalise-release base-entity new-release)))))
