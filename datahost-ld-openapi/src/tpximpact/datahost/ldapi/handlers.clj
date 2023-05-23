(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.series :as series]
   [tpximpact.datahost.ldapi.db :as db]))

(defn get-dataset-series [{{:keys [series-slug]} :path-params}]
  (let [key (series/dataset-series-key series-slug)
        jsonld-doc (get @db/db key)]
    {:status 200
     :body jsonld-doc}))

(defn put-dataset-series [{:keys [body-params path-params query-params]}]
  (try
    (swap! db/db series/upsert-series {:api-params (merge path-params
                                                          query-params)
                                       :jsonld-doc body-params})
    {:status 200
     :body {:status "success"}}

    (catch Throwable e
      (let [message (ex-message e)]
        {:status 500
         :body {:status "error"
                :message message}}))))
