(ns tpximpact.datahost.ldapi.db
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]
   [tpximpact.datahost.ldapi.series :as series]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.db/db [_ {:keys [storage-type opts]}]
  (when storage-type
    (da/duratom storage-type opts)))

(defn upsert-series! [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [series-key (series/dataset-series-key series-slug)
        ;; TODO: could handle this more nicely after
        ;; https://github.com/Swirrl/datahost-prototypes/issues/57
        ;; by comparing modified/issued times..
        series-exists? (get @db series-key)
        op (if series-exists? :update :create)
        updated-db (swap! db series/upsert-series api-params incoming-jsonld-doc)]
    {:op op
     :jsonld-doc (get updated-db series-key)}))
