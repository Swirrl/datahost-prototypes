(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]))

(def not-found-response
  {:status 404
   :body "Not found"})

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn get-dataset-series [db {{:keys [series-slug]} :path-params}]
  (if-let [series (db/get-series db series-slug)]
    {:status 200
     :body series}
    not-found-response))

(defn op->response-code
  "Takes [s.api/UpsertOp] and returns a HTTP status code (number)."
  [op]
  {:pre [(s.api/upsert-op-valid? op)]}
  (case op
    :create 201
    :update 200
    :noop   200))

(defn put-dataset-series [db {:keys [body-params] :as request}]
  (let [api-params (get-api-params request)
        incoming-jsonld-doc body-params
        {:keys [op jsonld-doc]} (db/upsert-series! db api-params incoming-jsonld-doc)]
    {:status (op->response-code op)
     :body jsonld-doc}))

(defn get-release [db {{:keys [series-slug release-slug]} :path-params :as path-params}]
  (if-let [release (db/get-release db series-slug release-slug)]
    {:status 200
     :body release}
    not-found-response))

(defn put-release [db {{:keys [series-slug]} :path-params
                       body-params :body-params :as request}]
  (if-let [_series (db/get-series db series-slug)]
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
          {:keys [_op jsonld-doc resource-id]} (db/insert-revision! db api-params incoming-jsonld-doc)]
      {:status 201
       :headers {"Location" (str "/data/" series-slug "/release/" release-slug "/revisions/" resource-id)}
       :body jsonld-doc})

    {:status 422
     :body "Release for this revision does not exist"}))

;; TODO: Uploaded change data should be validated against a supplied schema
(defn post-revision-changes [db {{:keys [series-slug release-slug revision-id]} :path-params
                                 {{:keys [appends]} :multipart} :parameters
                                 body-params :body-params :as request}]
  (if-let [_revision (db/get-revision db series-slug release-slug revision-id)]
    (let [incoming-jsonld-doc body-params
          api-params (-> (get-api-params request)
                         ;; TODO: coercion doesn't seem to be working on revision-id for some reason
                         (update :revision-id parse-long))
          {:keys [_op resource-id jsonld-doc]} (db/insert-change! db api-params incoming-jsonld-doc appends)]
    {:status 201
       ; /data/:series-slug/release/:release-slug/revisions/1/changes/:auto-incrementing-change-id
       :headers {"Location" (str "/data/" series-slug "/release/" release-slug
                                 "/revisions/" revision-id "/changes/" resource-id)}
       :body jsonld-doc})

    {:status 422
     :body "Revision for this change does not exist"}))