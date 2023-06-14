(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.db :as db]))

(def not-found-response
  {:status 404
   :body "Not found"})

(defn error-response [error]
  (let [message (ex-message error)]
    {:status 500
     :body {:status "error"
            :message message}}))

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn get-dataset-series [db {{:keys [series-slug]} :path-params}]
  (if-let [series (db/get-series db series-slug)]
    {:status 200
     :body series}
    not-found-response))

(defn put-dataset-series [db {:keys [body-params] :as request}]
  (try
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-series! db api-params incoming-jsonld-doc)
          response-code (case op
                          :create 201
                          :update 200)]
      {:status response-code
       :body jsonld-doc})

    (catch Throwable e
      (error-response e))))

(defn get-release [db {{:keys [series-slug release-slug]} :path-params :as path-params}]
  (if-let [release (db/get-release db series-slug release-slug)]
    {:status 200
     :body release}
    not-found-response))

(defn put-release [db {{:keys [series-slug]} :path-params
                       body-params :body-params :as request}]
  (try
    (if-let [_series (db/get-series db series-slug)]
      (try
        (let [api-params (get-api-params request)
              incoming-jsonld-doc body-params
              {:keys [op jsonld-doc]} (db/upsert-release! db api-params incoming-jsonld-doc)
              response-code (case op
                              :create 201
                              :update 200)]
          {:status response-code
           :body jsonld-doc})

        (catch Throwable e
          (error-response e)))
      {:status 422
       :body "Series does not exist"})

    (catch Throwable e
      (error-response e))))
