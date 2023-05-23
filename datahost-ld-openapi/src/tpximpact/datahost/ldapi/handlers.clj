(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.series :as series]))

(defn get-dataset-series [db {{:keys [series-slug]} :path-params}]
  (let [key (series/dataset-series-key series-slug)
        jsonld-doc (get @db key)]
    (if jsonld-doc
      {:status 200
       :body jsonld-doc}
      {:status 404
       :body "Not found"})))

(defn put-dataset-series [db {:keys [body-params path-params query-params] :as request}]
  (try
    (swap! db series/upsert-series {:api-params (merge path-params
                                                          query-params)
                                       :jsonld-doc body-params})
    {:status 200
     :body {:status "success"}}

    (catch Throwable e
      (let [message (ex-message e)]
        {:status 500
         :body {:status "error"
                :message message}}))))
