(ns  tpximpact.datahost.ldapi.handlers)

(defn get-dataset-series [{{:keys [dataset-series]} :path-params}]
  ;; (sc.api/spy)
  {:status 200
   :body {:name "foo"
          :size 123}})

(defn put-dataset-series [{:keys [body-params]
                           {:keys [dataset-series]} :path-params
                           {:keys [title description]} :query :as request}]
  (let [json-ld-doc body-params])
  ;; (sc.api/spy)
  {:status 200
   :body {:name "foo"
          :size 123}})
