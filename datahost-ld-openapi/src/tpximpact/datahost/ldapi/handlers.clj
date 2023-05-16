(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.series :as series]
   [tpximpact.datahost.ldapi.db :as db]))

(defn get-dataset-series [{{:keys [dataset-series]} :path-params}]
  ;; (sc.api/spy)
  {:status 200
   :body {:name "foo"
          :size 123}})



(defn put-dataset-series [{:keys [body-params path-params query-params]
                           {:keys [dataset-series]} :path-params
                           {:keys [title description]} :query-params :as request}]
  (let [json-ld-doc body-params]
    (series/upsert-series @db/db (merge path-params
                                        query-params
                                        {:jsonld-doc body-params})))
  {:status 200
   :body {:name "foo"
          :size 123}})
