(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.series :as series]
   [tpximpact.datahost.ldapi.db :as db]))

(defn get-dataset-series [{{:keys [series-slug]} :path-params}]
  ;; (sc.api/spy)
  {:status 200
   :body {:name "foo"
          :size 123}})

(defn put-dataset-series [{:keys [body-params path-params query-params]
                           {:keys [series-slug]} :path-params
                           {:keys [title description]} :query-params :as request}]
  (try
    (swap! db/db series/upsert-series {:api-params (merge path-params
                                                          query-params)
                                       :jsonld-doc body-params})
    {:status 200
     :body {:status "success"}}

    (catch Throwable e
      (let [{:keys [type expected-value actual-value]} (ex-data e)
            message (ex-message e)]
        {:status 500
         :body (str "Error: " message)}))))
