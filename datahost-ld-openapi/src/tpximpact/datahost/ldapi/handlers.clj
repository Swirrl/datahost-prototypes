(ns tpximpact.datahost.ldapi.handlers
  (:require
   [clojure.tools.logging :as log]
   [malli.core :as m]
   [malli.error :as me]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]
   [tpximpact.datahost.ldapi.models.revision :as revision-model]
   [tpximpact.datahost.ldapi.models.shared :as models.shared]
   [tpximpact.datahost.ldapi.util.data-validation :as data-validation]))

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

(defn get-release [triplestore change-store {{:keys [series-slug release-slug]} :path-params
                       {:strs [accept]} :headers}]
  (if-let [release (db/get-release triplestore series-slug release-slug)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=release.csv"}
       :body (or (revision-model/release->csv-stream triplestore change-store release) "")}
      {:status 200
       :body (db/release->response-body release)})
    not-found-response))

(defn put-release [clock triplestore {{:keys [series-slug]} :path-params
                       body-params :body-params :as request}]
  (if-let [series (db/get-series-by-slug triplestore series-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-release! clock triplestore series api-params incoming-jsonld-doc)]
      {:status (op->response-code op)
       :body jsonld-doc})
    {:status 422
     :body "Series for this release does not exist"}))

(defn put-release-schema
  [clock triplestore {{:keys [series-slug] :as params} :path-params
                      incoming-jsonld-doc :body-params
                      :as request}]
   (if (not (db/get-series-by-slug triplestore series-slug))
    not-found-response

    (let [{:keys [op jsonld-doc]} (db/upsert-release-schema! clock triplestore (get-api-params request) incoming-jsonld-doc)]
      {:status (op->response-code op)
       :body jsonld-doc})))

(defn get-release-schema
    [triplestore {{:keys [series-slug release-slug] :as path-params} :path-params}]
    (let [release-uri (models.shared/release-uri-from-slugs series-slug release-slug)]
      (if-let [schema (db/get-release-schema triplestore release-uri)]
        {:status 200
         :body (db/schema->response-body schema)}
        {:status 404
         :body {:status "error"
                :message "Not found"}})))

(defn get-revision [triplestore change-store {{:keys [series-slug release-slug revision-id]} :path-params
                        {:strs [accept]} :headers :as _request}]
  (if-let [rev (db/get-revision triplestore series-slug release-slug revision-id)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=revision.csv"}
       :body (or (revision-model/revision->csv-stream triplestore change-store rev) "")}

      {:status 200
       :headers {"content-type" "application/json"}
       :body (db/revision->response-body rev)})
    not-found-response))

(defn post-revision [triplestore {{:keys [series-slug release-slug]} :path-params
                         body-params :body-params :as request}]
  (if-let [_release (db/get-release triplestore series-slug release-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [jsonld-doc resource-id]} (db/insert-revision! triplestore api-params incoming-jsonld-doc)]
      {:status 201
       :headers {"Location" (str "/data/" series-slug "/releases/" release-slug "/revisions/" resource-id)}
       :body jsonld-doc})

    {:status 422
     :body   "Release for this revision does not exist"}))

(defn- dataset-validation-error!
  "Returns nill on success, error response on validation failure."
  [release-schema appends]
  (try
    (let [dataset (data-validation/as-dataset (:tempfile appends) {})
          schema (data-validation/make-row-schema release-schema)
          {:keys [explanation]} (data-validation/validate-dataset dataset schema
                                                                  {:fail-fast? true})]
      (when (some? explanation)
        {:status 400
         :body explanation}))))

(defn post-change [triplestore
                   change-store
                   {{:keys [series-slug release-slug revision-id]} :path-params
                    {{:keys [appends]} :multipart}                 :parameters
                    body-params                                    :body-params :as request}]
  ;; TODO This could be an ASK of the Revision
  (if-let [_revision (db/get-revision triplestore series-slug release-slug revision-id)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          release-schema (db/get-release-schema triplestore (models.shared/release-uri-from-slugs series-slug release-slug))
          validation-err (when (some? release-schema)
                           (dataset-validation-error! release-schema appends))
          {:keys [jsonld-doc resource-id]} (when-not validation-err
                                             (db/insert-change! triplestore change-store api-params
                                                                incoming-jsonld-doc appends))]
      (log/info (format "post-change: validation: found-schema? = %s change-valid? = "
                        (some? release-schema) (nil? validation-err)))
      (if validation-err
        validation-err
        {:status  201
         :headers {"Location" (str "/data/" series-slug "/releases/" release-slug
                                   "/revisions/" revision-id "/changes/" resource-id)}
         :body    jsonld-doc}))

    {:status 422
     :body   "Revision for this change does not exist"}))

(defn get-change [triplestore change-store {{:keys [series-slug release-slug revision-id change-id]} :path-params
                        {:strs [accept]} :headers :as _request}]
  (if-let [change (db/get-change triplestore series-slug release-slug revision-id change-id)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=change.csv"}
       :body (or (revision-model/change->csv-stream change-store change) "")}

      {:status 406
       :headers {}
       :body "Only text/csv format is available at this time."})
    not-found-response))
