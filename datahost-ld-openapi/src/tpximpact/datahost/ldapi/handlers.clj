(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]))

(defn get-dataset-series [db {{:keys [series-slug]} :path-params}]
  (let [key (models-shared/dataset-series-key series-slug)
        jsonld-doc (get @db key)]
    (if jsonld-doc
      {:status 200
       :body jsonld-doc}
      {:status 404
       :body "Not found"})))

(defn put-dataset-series [db {:keys [body-params path-params query-params]}]
  (try
    (let [api-params (-> query-params (update-keys keyword) (merge path-params))
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-series! db api-params incoming-jsonld-doc)
          response-code (case op
                          :create 201
                          :update 200)]
      {:status response-code
       :body jsonld-doc})

    (catch Throwable e
      (let [message (ex-message e)]
        {:status 500
         :body {:status "error"
                :message message}}))))

(defn get-release [db params]
  {:status 200})

(defn put-release [db params]
  (try
    (let []
      {:status 200 ;; response-code
       :body  "";; jsonld-doc
       }))
  {:status 200})
