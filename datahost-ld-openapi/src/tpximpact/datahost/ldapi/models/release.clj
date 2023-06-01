(ns tpximpact.datahost.ldapi.models.release)

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
