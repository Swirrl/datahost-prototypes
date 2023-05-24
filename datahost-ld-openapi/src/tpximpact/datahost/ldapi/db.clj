(ns tpximpact.datahost.ldapi.db
  (:require
   [duratom.core :as da]
   [integrant.core :as ig]
   [tpximpact.datahost.ldapi.series :as series]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.db/db [_ {:keys [storage-type opts]}]
  (da/duratom storage-type opts))

(defn upsert-series! [db {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [new-series (series/normalise-series api-params incoming-jsonld-doc)
        series-key (series/dataset-series-key series-slug)
        updated-db (swap! db series/upsert-series {:api-params api-params
                                                   :jsonld-doc incoming-jsonld-doc})]
    (get updated-db series-key)))
