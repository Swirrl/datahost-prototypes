(ns tpximpact.datahost.ldapi.handlers
  (:require
   [malli.core :as m]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]))

(def not-found-response
  {:status 404
   :body "Not found"})

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn get-dataset-series [triplestore {{:keys [series-slug]} :path-params}]
  (if-let [series (db/get-series-by-slug triplestore series-slug)]
    {:status 200
     :body (db/series->response-body series)}
    not-found-response))

(defn op->response-code
  "Takes [s.api/UpsertOp] and returns a HTTP status code (number)."
  [op]
  {:pre [(s.api/upsert-op-valid? op)]}
  (case op
    :create 201
    :update 200
    :noop   200))

(defn put-dataset-series [clock triplestore {:keys [body-params] :as request}]
  (let [api-params (get-api-params request)
        incoming-jsonld-doc body-params
        {:keys [op jsonld-doc]} (db/upsert-series! clock triplestore api-params incoming-jsonld-doc)]
    {:status (op->response-code op)
     :body jsonld-doc}))

(defn get-release [db {{:keys [series-slug release-slug]} :path-params :as path-params}]
  (if-let [release (db/get-release db series-slug release-slug)]
    {:status 200
     :body release}
    not-found-response))

(defn put-release [db triplestore {{:keys [series-slug]} :path-params
                       body-params :body-params :as request}]
  (if-let [_series (db/get-series-by-slug triplestore series-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-release! db api-params incoming-jsonld-doc)]
      {:status (op->response-code op)
       :body jsonld-doc})
    {:status 422
     :body "Series for this release does not exist"}))

(defn get-revision [db {{:keys [series-slug release-slug revision-id]} :path-params :as _path-params}]
  (if-let [rev (db/get-revision db series-slug release-slug revision-id)]
    {:status 200
     :body rev}
    not-found-response))

(defn post-revision [db {{:keys [series-slug release-slug]} :path-params
                         body-params :body-params :as request}]
  (if-let [_release (db/get-release db series-slug release-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [_op jsonld-doc]} (db/insert-revision! db api-params incoming-jsonld-doc)]
      {:status 201
       :body jsonld-doc})

    {:status 422
     :body "Release for this revision does not exist"}))