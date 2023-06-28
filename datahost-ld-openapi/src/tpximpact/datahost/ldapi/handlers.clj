(ns tpximpact.datahost.ldapi.handlers
  (:require
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]
   [tpximpact.datahost.ldapi.models.revision :as revision-model]))

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

(defn get-release [db {{:keys [series-slug release-slug]} :path-params
                       {:strs [accept]} :headers}]
  (if-let [release (db/get-release db series-slug release-slug)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=release.csv"}
       :body (or (revision-model/release->csv-stream db release) "")}
      {:status 200
       :body release})
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

(defn put-release-schema
  [db {{:keys [series-slug] :as params} :path-params
       incoming-jsonld-doc :body-params 
       :as request}]
  (if (not (db/get-series-by-slug db series-slug))
    not-found-response
    
    (let [{:keys [op jsonld-doc]} (db/upsert-release-schema! db (get-api-params request) incoming-jsonld-doc)]
      {:status (op->response-code op)
       :body jsonld-doc})))

;;; we don't have the schema slug, and we don't use a real db in the
;;; prototype, so we have to extract it from the path that's in the
;;; release.

(defn- schema-path->schema-slug
  "Returns a string or nil"
  [path]
  (let [[_ slug] (re-find #"\/data.*\/schemas\/(\S+)$" path)]
    slug))

(defn get-release-schema 
  [db {{:keys [series-slug release-slug] :as path-params} :path-params}]
  (let [schema-path (some-> db
                            (db/get-release series-slug release-slug) 
                            (get "datahost:hasSchema"))]
    (if-let [schema (db/get-release-schema db (assoc path-params :schema-slug (schema-path->schema-slug schema-path)))]
      {:status 200
       :body schema}
      {:status 404
       :body {:status "error"
              :message "Not found"}})))

(defn get-revision [db {{:keys [series-slug release-slug revision-id]} :path-params
                        {:strs [accept]} :headers :as _request}]
  (if-let [rev (db/get-revision db series-slug release-slug revision-id)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=revision.csv"}
       :body (or (revision-model/revision->csv-stream db rev) "")}

      {:status 200
       :headers {"content-type" "application/json"}
       :body rev})
    not-found-response))

(defn post-revision [db {{:keys [series-slug release-slug]} :path-params
                         body-params :body-params :as request}]
  (if-let [_release (db/get-release db series-slug release-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [_op jsonld-doc resource-id]} (db/insert-revision! db api-params incoming-jsonld-doc)]
      {:status 201
       :headers {"Location" (str "/data/" series-slug "/releases/" release-slug "/revisions/" resource-id)}
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
       ; /data/:series-slug/releases/:release-slug/revisions/1/changes/:auto-incrementing-change-id
       :headers {"Location" (str "/data/" series-slug "/releases/" release-slug
                                 "/revisions/" revision-id "/changes/" resource-id)}
       :body jsonld-doc})

    {:status 422
     :body "Revision for this change does not exist"}))

(defn get-change [db {{:keys [series-slug release-slug revision-id change-id]} :path-params
                        {:strs [accept]} :headers :as _request}]
  (if-let [change (db/get-change db series-slug release-slug revision-id change-id)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=change.csv"}
       :body (or (revision-model/change->csv-stream db change) "")}

      {:status 406
       :headers {}
       :body "Only text/csv format is available at this time."})
    not-found-response))
