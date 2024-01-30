(ns tpximpact.datahost.ldapi.handlers
  (:require
   [clojure.tools.logging :as log]
   [clojure.data.csv :as csv]
   [clojure.string :as string]
   [grafter.matcha.alpha :as matcha]
   [ring.util.io :as ring-io]
   [tablecloth.api :as tc]
   [reitit.core :as rc]
   [next.jdbc :as jdbc]
   [ring.util.request :as util.request]
   [ring.util.response :as util.response]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.ldapi.compact :as cmp]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.errors :as errors]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.json-ld :as json-ld]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.routes.shared :as shared]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]
   [tpximpact.datahost.ldapi.store.sql.interface :as sql.interface]
   [tpximpact.datahost.ldapi.store.sql :as store.sql]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
   [tpximpact.datahost.ldapi.util.triples
    :refer [triples->ld-resource triples->ld-resource-collection]]
   [tpximpact.datahost.ldapi.models.release :as m.release]
   [clojure.java.io :as io])
  (:import
   (java.net URI)
   (java.io ByteArrayInputStream
            ByteArrayOutputStream
            BufferedWriter
            InputStream
            OutputStream)))

(defn- box [x]
  (if (coll? x) x [x]))

(defn- as-json-ld [response]
  (assoc-in response [:headers "content-type"] "application/ld+json"))

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
    (fn [out-stream]
      (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn- change-store-to-ring-io-writer
  "Use as argument to `ring-io/piped-input-stream`."
  [change-store data-key ^OutputStream out-stream]
  (with-open [in ^InputStream (store/get-data change-store data-key)]
    (.transferTo in out-stream)))

(defn- snapshot-maps-to-ring-io-writer
  "Use as argument to `ring-io/piped-input-stream`"
  [commit-uri observations ^OutputStream out-stream]
  (try
    (let [ks (keys (first observations))
          out (BufferedWriter. (java.io.OutputStreamWriter. out-stream))
          make-row (fn [record] (map #(get record %) ks))]
      (csv/write-csv  out [(map name ks)])
      (doseq [batch (partition-all 100 (map make-row observations))]
        (csv/write-csv out batch))
      (.flush out))
    (catch Exception ex
      (log/warn ex "Failure to write CSV response" commit-uri)
      (throw ex))))

(defn- make-row-vec
  [row]
  (into [] (map #(str (nth row %))) (range (count row))))

(defn- snapshot-arrays-to-ring-io-writer
  "Use as argument to `ring-io/piped-input-stream`"
  [data-source executor commit-uri ostore temp-table ^OutputStream out-stream]
  (try
    (.get (sql.interface/submit
           executor
           (fn materialise []
             (with-open [conn (jdbc/get-connection data-source)]
               (jdbc/with-transaction [tx conn]
                 (store.sql/replay-commits tx {:store ostore
                                               :commit-uri commit-uri
                                               :snapshot-table temp-table})
                 (let [out ^BufferedWriter (BufferedWriter. (java.io.OutputStreamWriter. out-stream))
                       rs (m.release/stream-materialized-snapshot tx {:snapshot-table temp-table}
                                                                  {:store ostore})]
                   (doseq [s (interpose "," (:required-columns ostore))]
                     (.write out s))
                   (.newLine out)
                   
                   (reduce (fn [_ row]
                             (let [v (make-row-vec row)]
                               (doseq [s (interpose "," v)]
                                 (.write out s)))
                             (.newLine out))
                           out
                           rs)
                   (.flush out)))))))
    (catch Exception ex
      (log/warn ex "Failure to write CSV response" commit-uri)
      (throw ex))))

(defn commit-data-to-ring-io-writer
  [data-source executor ])

(defn csv-file-location->dataset [change-store key]
  (with-open [^java.io.Closeable in (store/get-data change-store key)]
    (data.validation/as-dataset in {:file-type :csv})))

(defn op->response-code
  "Takes [s.api/UpsertOp] and returns a HTTP status code (number)."
  [op]
  {:pre [(s.api/upsert-op-valid? op)]}
  (case op
    :create 201
    :update 200
    :noop   200))

(defn get-dataset-series [triplestore system-uris {{:keys [series-slug]} :path-params :as request}]
  (if-let [series (->> (db/get-dataset-series triplestore (su/dataset-series-uri system-uris series-slug))
                       (matcha/index-triples)
                       (triples->ld-resource))]
    (as-json-ld {:status 200
                 :body (-> (json-ld/compact series (json-ld/simple-context system-uris))
                           (.toString))})
    (errors/not-found-response request)))

(defn put-dataset-series [clock triplestore system-uris {:keys [body-params] :as request}]
  (let [api-params (get-api-params request)
        incoming-jsonld-doc body-params
        {:keys [op jsonld-doc]} (db/upsert-series! clock triplestore system-uris api-params incoming-jsonld-doc)]
    (as-json-ld {:status (op->response-code op)
                 :body jsonld-doc})))

(defn- release-uris
  "Returns a seq of URIs for a series."
  [triplestore series-uri]
  (let [issued-uri (cmp/expand :dcterms/issued)
        has-schema-uri (cmp/expand :dh/hasSchema)
        releases (->> (db/get-releases triplestore series-uri)
                      (matcha/index-triples)
                      (triples->ld-resource-collection)
                      (sort-by #(get % issued-uri))
                      (reverse))]
    (into #{} (comp (filter #(get % has-schema-uri))
                    (map resource/id))
        releases)))

(defn delete-dataset-series
  [{:keys [triplestore change-store system-uris store-factory data-source db-executor]}
   {{:keys [series-slug]} :path-params
    {series-uri :dh/DatasetSeries} :datahost.request/uris
    {_series :dh/DatasetSeries} :datahost.request/entities
    :as _request}]
  (let [release-uris (release-uris triplestore series-uri)]
    (log/infof "delete-dataset-series: series-path=%s  %s associated releases"
               (.getPath ^URI series-uri) (count release-uris))
    ;; potentially delete the DB tables
    (when (and data-source (seq release-uris))
      ;; TODO(rosado): replace `jdbc/get-connection` with a wrapper that accepts release-uri
      (with-open [conn (jdbc/get-connection data-source)]
        (try
          (-> db-executor
              (sql.interface/submit
               (fn drop-release-tables []
                 (doseq [uri release-uris
                         :let [params {:release-uri uri}]]
                   (log/debug {:delete-dataset-series/release-uri uri})
                   (m.release/drop-release-tables-and-data conn params {}))))
              (.get))
          (catch Throwable err
            (log/debug {:delete-dataset-series/release-uris release-uris})
            (log/warn (ex-cause err) "delete-dataset-series: sql cleanup failed")
            (throw err)))
        (log/debugf "delete-dataset-series: series-path=%s release tables deleted"
                    (.getPath ^URI series-uri))))

    (let [orphaned-change-keys (db/delete-series! triplestore system-uris series-slug)]
      (doseq [change-key orphaned-change-keys]
        (store/-delete change-store change-key))
      {:status 204})))

(defn get-release
  [triplestore _change-store system-uris
   {{:keys [extension] :as path-params} :path-params
    {release-uri :dh/Release} :datahost.request/uris
    :as request}]
  (if-let [release (->> release-uri
                        (db/get-release-by-uri triplestore)
                        (matcha/index-triples)
                        (triples->ld-resource))]
    (if (= extension "csv")
      (let [change-info (db/get-latest-change-info triplestore release-uri)]
        (cond
          (nil? change-info) (-> (util.response/response "This release has no revisions yet")
                                 (util.response/status 422)
                                 (util.response/header "content-type" "text/plain"))
          :else (let [rev-uri ^URI (:rev change-info)]
                  (-> (.getPath rev-uri)
                      (util.response/redirect)
                      (shared/set-csvm-header request)))))
      (as-json-ld {:status 200
                   :body (-> (json-ld/compact release (json-ld/simple-context system-uris))
                             (.toString))}))
    (errors/not-found-response request)))


(defn put-release [clock triplestore system-uris {path-params :path-params
                                                  body-params :body-params :as request}]

  (if (db/resource-exists? triplestore (su/dataset-series-uri* system-uris path-params))
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-release! clock triplestore system-uris api-params incoming-jsonld-doc)]
      (as-json-ld {:status (op->response-code op)
                   :body jsonld-doc}))
    {:status 422
     :body "Series for this release does not exist"}))

(defn post-release-schema
  "Saves the schema in the triplestore and creates necessary tables in SQL db."
  [{:keys [clock triplestore system-uris db-executor data-source] :as sys}
   {{schema-file :body} :parameters
    {release-uri :dh/Release} :datahost.request/uris
    :as request}]
  {:pre [schema-file]}
  (let [incoming-jsonld-doc schema-file
        api-params (get-api-params request)
        insert-result (db/upsert-release-schema! clock triplestore system-uris incoming-jsonld-doc api-params)
        row-schema (data.validation/make-row-schema-from-json incoming-jsonld-doc)]
    (when data-source
      (with-open [conn (jdbc/get-connection data-source)]
        (-> db-executor
            (sql.interface/submit
             #(m.release/create-release-tables conn {:release-uri release-uri :row-schema row-schema} {}))
            (.get))))
    (as-json-ld {:status (op->response-code (:op insert-result))
                 :body (:jsonld-doc insert-result)})))

(defn- get-schema-id [matcha-db]
  ((grafter.matcha.alpha/select-1 [?schema]
                                  [[?schema (cmp/expand :dh/columns) ?col]])
   matcha-db))

(defn get-release-schema
  [triplestore system-uris {path-params :path-params :as request}]
  (let [release-uri (su/dataset-release-uri* system-uris path-params)
        matcha-db (matcha/index-triples (db/get-release-schema-statements triplestore release-uri))]
    (if-let [schema-id (get-schema-id matcha-db)]
      (let [schema-resource (triples->ld-resource matcha-db schema-id)
            csvw-number-uri (cmp/expand :csvw/number)
            columns (->> (box (get schema-resource (cmp/expand :dh/columns)))
                         (map #(triples->ld-resource matcha-db %))
                         (sort-by #(get % csvw-number-uri)))
            schema-ld-with-columns (assoc schema-resource (cmp/expand :dh/columns) columns)]
        (as-json-ld {:status 200
                     :body (-> (json-ld/compact schema-ld-with-columns
                                                (merge (json-ld/simple-context system-uris)
                                                       {"dh:columns" {"@container" "@set"}}))
                               (.toString))}))
      (errors/not-found-response request))))

(defn- revision-number
  "Returns a number or throws."
  [rev-id]
  (let [path (.getPath ^URI rev-id)]
    (try
      (Long/parseLong (-> (re-find #"^.*\/revision\/(\d+)\/{0,1}.*$" path) next first))
      (catch NumberFormatException ex
        (throw (ex-info (format "Could not extract revision number from given id: %s" rev-id)
                        {:revision-id rev-id} ex))))))

(defn get-revision
  [{:keys [triplestore
           system-uris
           store-factory
           data-source
           db-executor]
    :as _system}
   {{revision :dh/Revision} :datahost.request/entities
    {release-uri :dh/Release} :datahost.request/uris
    {:strs [accept]} :headers
    :as request}]
  (let [revision-ld (->> revision matcha/index-triples triples->ld-resource)]
    (if (= accept "text/csv")
      (-> {:status 200
           :headers {"content-type" "text/csv"
                     "content-disposition" "attachment ; filename=revision.csv"}
           :body (let [release-schema (db/get-release-schema triplestore release-uri)
                       change-infos (db/get-changes-info triplestore release-uri (revision-number (resource/id revision-ld)))]
                   (if (empty? change-infos)
                     "" ;TODO: return table header here, need to get schema first``
                     ;; ELSE
                     (let [ostore (store-factory release-uri (data.validation/make-row-schema release-schema))
                           commit-uri (-> change-infos last :snapshotKey (URI.))
                           ;; TODO: one temp table per request - this obviously can cause disk usage
                           ;; to grow significantly if not capped.
                           temp-table (str "test_" (random-uuid))]
                       (ring-io/piped-input-stream
                        (partial snapshot-arrays-to-ring-io-writer
                                 data-source
                                 db-executor
                                 commit-uri
                                 ostore
                                 temp-table)))))}
          (shared/set-csvm-header request))

      (as-json-ld {:status 200
                   :body (-> (json-ld/compact revision-ld (json-ld/simple-context system-uris))
                             (.toString))}))))

(defn- wrap-ld-collection-contents [coll]
  {"https://publishmydata.com/def/datahost/collection-contents" coll})

(defn get-series-list [triplestore system-uris _request]
  (let [issued-uri (cmp/expand :dcterms/issued)
        series (->> (db/get-all-series triplestore)
                    (matcha/index-triples)
                    (triples->ld-resource-collection)
                    (sort-by #(get % issued-uri))
                    (reverse))
        response-body (-> (wrap-ld-collection-contents series)
                          (json-ld/compact (json-ld/simple-collection-context system-uris))
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-revision-list [triplestore system-uris {path-params :path-params}]
  (let [revisions (->> (su/dataset-release-uri* system-uris path-params)
                       (db/get-revisions triplestore)
                       (matcha/index-triples)
                       (triples->ld-resource-collection)
                       (sort-by (comp str (keyword "@id")))
                       (reverse))
        response-body (-> (wrap-ld-collection-contents revisions)
                          (json-ld/compact (json-ld/simple-collection-context system-uris))
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-release-list [triplestore system-uris {{:keys [series-slug]} :path-params}]
  (let [issued-uri (cmp/expand :dcterms/issued)
        releases (->> (db/get-releases triplestore (su/dataset-series-uri system-uris series-slug))
                      (matcha/index-triples)
                      (triples->ld-resource-collection)
                      (sort-by #(get % issued-uri))
                      (reverse))
        response-body (-> (wrap-ld-collection-contents releases)
                          (json-ld/compact (json-ld/simple-collection-context system-uris))
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn post-revision [triplestore system-uris {{:keys [series-slug release-slug]} :path-params
                                              router :reitit.core/router
                                              body-params :body-params :as request}]
  (let [api-params (get-api-params request)
        release-uri (su/dataset-release-uri system-uris (su/dataset-series-uri system-uris series-slug) release-slug)]
    (if (db/resource-exists? triplestore release-uri)
      (let [next-revision-id (db/fetch-next-child-resource-number triplestore release-uri :dh/hasRevision)
            revision-uri (su/revision-uri system-uris series-slug release-slug next-revision-id)
            {:keys [jsonld-doc resource-id]} (db/insert-revision! triplestore api-params
                                                                  body-params (su/rdf-base-uri system-uris)
                                                                  release-uri revision-uri next-revision-id)]
        (as-json-ld {:status 201
                     :headers {"Location" (-> (rc/match-by-name router
                                                                :tpximpact.datahost.ldapi.router/revision
                                                                {:series-slug series-slug
                                                                 :release-slug release-slug
                                                                 :revision-id resource-id})
                                              (rc/match->path))}
                     :body jsonld-doc}))

      {:status 422
       :body "Release for this revision does not exist"})))

(defn ->byte-array-input-stream [input-stream]
  (with-open [intermediate (ByteArrayOutputStream.)]
    (io/copy input-stream intermediate)
    (ByteArrayInputStream. (.toByteArray intermediate))))

(defn ->tmp-file
  [^InputStream input-stream]
  (let [tmp (java.io.File/createTempFile "inputcsv" ".tmp")]
    (with-open [out (java.io.FileOutputStream. tmp)]
      (io/copy input-stream out))
    tmp))

(defn post-change
  [{:keys [triplestore system-uris data-source store-factory] :as sys}
   change-kind
   {router :reitit.core/router
    {:keys [series-slug release-slug revision-id]} :path-params
    _query-params :query-params
    appends :body
    {:keys [release-schema]} :datahost.request/entities
    {^URI release-uri :dh/Release :as request-uris} :datahost.request/uris
    :as request}]
  (let [^java.io.File appends-file (with-open [^java.io.Closeable a appends]
                                     (->tmp-file a))
        insert-req (store.sql/make-observation-insert-request! release-uri appends-file change-kind)
        content-type (util.request/content-type request)
        ;; insert relevant triples
        {:keys [inserted-jsonld-doc
                change-id
                ^URI change-uri
                message]} (db/insert-change! triplestore
                                             system-uris
                                             {:api-params (assoc (get-api-params request)
                                                                 :format content-type)
                                              :store-key (:key insert-req)
                                              :datahost.change/kind change-kind
                                              :datahost.request/uris request-uris})]
    (log/info (format "post-change: '%s'  insert-ok? = %s" change-uri  (nil? message)))
    (try
      (cond
        (some? message) {:status 422 :body message}

        :else
        (do
          ;; store the change
          (log/debug (format "post-change: '%s' stored-change: '%s'" (.getPath change-uri) (:key insert-req)))
          (let [row-schema (data.validation/make-row-schema release-schema)]
            (when (and data-source store-factory)
              (try
                (jdbc/with-transaction [tx (jdbc/get-connection data-source)]
                  (let [obs-store (store-factory release-uri row-schema)
                        tx-store (assoc obs-store :db tx)
                        insert-req (assoc insert-req :commit-uri change-uri)
                        {import-status :status} (store.sql/execute-insert-request tx-store insert-req)]
                    (log/debug "import-status: " import-status)
                    ;; TODO(rosado): complete-import should probably take :status 

                    (when (= :import.status/success import-status)
                      (store.sql/complete-import tx-store insert-req))

                    (if (store.sql/create-commit? import-status)
                      (store.sql/create-commit tx-store insert-req)
                      (throw (ex-info (format "Could not create commit: %s" change-uri)
                                      {:commit-uri change-uri :import-status import-status})))))
                (catch Exception ex
                  ;; TODO(rosado): handle error, tag the change entity as failed/error in the triplestore (or delete it)
                  (throw ex))))
            (db/tag-with-snapshot triplestore change-uri {:new-snapshot-key (str change-uri)})
            
            (as-json-ld {:status 201
                         :headers {"Location" (-> (rc/match-by-name router
                                                                    :tpximpact.datahost.ldapi.router/commit
                                                                    {:series-slug series-slug
                                                                     :release-slug release-slug
                                                                     :revision-id revision-id
                                                                     :commit-id change-id})
                                                  (rc/match->path))}
                         :body inserted-jsonld-doc}))))
      (finally
        (.delete appends-file)))))

(defn change->csv-stream [change-store change]
  (let [appends (get change (cmp/expand :dh/updates))]
    (tap> {:change change})
    (when-let [dataset (csv-file-location->dataset change-store appends)]
      (write-dataset-to-outputstream dataset))))

(defn get-change
  "Returns data for commit (and not the snapshot dataset)."
  [triplestore
   change-store
   system-uris
   {{:keys [series-slug release-slug revision-id commit-id]} :path-params
    {:strs [accept]} :headers :as request}]
  (if-let [change (->> (su/commit-uri system-uris series-slug release-slug revision-id commit-id)
                       (db/get-change triplestore)
                       matcha/index-triples
                       triples->ld-resource)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=change.csv"}
       :body (or (change->csv-stream change-store change) "")}

      {:status 406
       :headers {}
       :body "Only text/csv format is available at this time."})
    (errors/not-found-response request)))
