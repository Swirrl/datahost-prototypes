(ns tpximpact.datahost.ldapi.db
  "Note: current persistence is a temporary solution.

  Current persistence layer has same interface as Clojure's atom. At
  the moment we make use of the fact that each upsert results in an
  update of the atom value if the value didn't change. In other words:
  the atom's' value may be equal but not `identical?`, because the
  metadata is always updated with `:op` entry.
  status (`[:enum :noop :update :create]`)

  See: https://github.com/jimpil/duratom"
  (:require
    [duratom.core :as da]
    [grafter-2.rdf.protocols :as pr]
    [grafter-2.rdf4j.repository :as repo]
    [integrant.core :as ig]
    [malli.core :as m]
    [meta-merge.core :as mm]
    [com.yetanalytics.flint :as f]
    [clojure.data.json :as json]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]
    [tpximpact.datahost.ldapi.models.series :as series]
    [tpximpact.datahost.ldapi.models.release :as release]
    [tpximpact.datahost.ldapi.models.revision :as revision]
    [tpximpact.datahost.ldapi.schemas.release :as s.release]
    [tpximpact.datahost.ldapi.schemas.series :as s.series]
    [tpximpact.datahost.ldapi.native-datastore :as datastore]
    [tpximpact.datahost.time :as time]
    [tpximpact.datahost.ldapi.compact :as compact]
    [tpximpact.datahost.ldapi.resource :as resource])
  (:import
   [java.time ZoneId ZonedDateTime Instant OffsetDateTime ZoneOffset]
   [java.net URI]))

(def db-defaults
  {:storage-type :local-file
   :opts {:commit-mode :sync
          :init {:revision-id-counter 0}}})

(defmethod ig/prep-key ::db
  [_ options]
  (mm/meta-merge db-defaults options))

(defmethod ig/init-key ::db [_ {:keys [storage-type opts]}]
    (da/duratom storage-type opts))

(defn- get-series-query [series-url]
  (let [bgps [[series-url 'a :dh/DatasetSeries]
              [series-url :dcterms/title '?title]
              [series-url :dcterms/description '?description]
              [series-url :dh/baseEntity '?baseentity]
              [series-url :dcterms/modified '?modified]
              [series-url :dcterms/issued '?issued]]]
    {:prefixes {:dcterms "<http://purl.org/dc/terms/>"
                :dh "<https://publishmydata.com/def/datahost/>"}
     :construct bgps
     :where bgps}))

(def prefixes {:dcterms (URI. "http://purl.org/dc/terms/")
               :dh (URI. "https://publishmydata.com/def/datahost/")})

(defn- map-properties [prop-mapping m]
  (reduce-kv (fn [props k v]
               (if-let [prop-uri (get prop-mapping k)]
                 (resource/add-property props prop-uri v)
                 props))
             {}
             m))
(defn- series-params->properties [query-params]
  (let [prop-mapping {:title (compact/expand :dcterms/title)
                      :description (compact/expand :dcterms/description)}]
    (map-properties prop-mapping query-params)))

(defn get-series-by-uri [triplestore series-uri]
  (let [q (get-series-query series-uri)
        statements (datastore/eager-query triplestore (f/format-query q :pretty? true))]
    (when (seq statements)
      (resource/from-statements statements))))

(defn get-series-by-slug [triplestore series-slug]
  (let [series-uri (models-shared/dataset-series-uri series-slug)]
    (get-series-by-uri triplestore series-uri)))

(defn get-release [db series-slug release-slug]
  (let [key (models-shared/release-key series-slug release-slug)]
    (get @db key)))

(defn get-revision [db series-slug release-slug revision-id]
  (let [key (models-shared/revision-key series-slug release-slug revision-id)]
    (get @db key)))

(def ^:private UpsertInternalParams
  [:map
   [:op.upsert/keys
    [:or s.series/UpsertKeys s.release/UpsertKeys]]])

(def ^:private upsert-internal-params-valid? (m/validator UpsertInternalParams))

(defn- upsert-doc!
  "Applies upsert of the JSON-LD document and mutates the db-ref.
  Returns the value of the db-ref after the upsert."
  [db-ref update-fn api-params incoming-jsonld-doc]
  {:pre [(upsert-internal-params-valid? api-params)]}
  (let [ts (ZonedDateTime/now (ZoneId/of "UTC"))]
    (swap! db-ref
           update-fn
           (assoc api-params :op/timestamp ts)
           incoming-jsonld-doc)))

(defn- diff-resource [r1 r2 property-uris])

(defn- series-changed? [old-series new-series]
  true)

(defn- input-context []
  (assoc (update-vals @compact/default-context str)
    "@base" (str models-shared/ld-root)))

(defn- output-context []
  (assoc (update-vals (compact/sub-context ["dh" "dcterms" "rdf"]) str)
    "@base" (str models-shared/ld-root)))

(defn- annotate-json-resource [json-doc resource-uri resource-type]
  (assoc json-doc
    "@id" (str resource-uri)
    "@type" (str resource-type)
    "@context" (input-context)))

(defn- request->series [{:keys [series-slug] :as api-params} json-doc]
  (let [series-uri (models-shared/dataset-series-uri series-slug)
        series-doc (annotate-json-resource json-doc series-uri (compact/expand :dh/DatasetSeries))
        doc-resource (resource/from-json-ld-doc series-doc)
        param-properties (series-params->properties api-params)]
    (resource/set-properties doc-resource param-properties)))

(defn- series-update-query [series]
  (let [series-uri (resource/id series)
        title (resource/get-property1 series (compact/expand :dcterms/title))
        description (resource/get-property1 series (compact/expand :dcterms/description))
        modified-at (resource/get-property1 series (compact/expand :dcterms/modified))]
    (println "title" title ", description" description ", modified" modified-at)
    (flush)
    {:prefixes {:dcterms "<http://purl.org/dc/terms/>"}
     :delete [[series-uri :dcterms/title '?title]
              [series-uri :dcterms/description '?description]
              [series-uri :dcterms/modified '?modified]]
     :insert [[series-uri :dcterms/title title]
              [series-uri :dcterms/description description]
              [series-uri :dcterms/modified modified-at]]
     :where [[series-uri :dcterms/title '?title]
             [series-uri :dcterms/description '?description]
             [series-uri :dcterms/modified '?modified]]}))

(defn- update-series [triplestore series]
  (let [q (series-update-query series)
        qs (f/format-update q :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

(defn- merge-series-updates [clock old-series new-properties]
  (let [diff-properties [(compact/expand :dcterms/title)
                         (compact/expand :dcterms/description)]
        new-diff-properties (resource/get-properties new-properties diff-properties)]
    (if (= (resource/get-properties old-series diff-properties)
           new-diff-properties)
      [false old-series]
      (let [updated (-> old-series
                        (resource/set-properties new-diff-properties)
                        (resource/set-property1 (compact/expand :dcterms/modified) (time/now clock)))]
        [true updated]))))

(defn- set-timestamps [clock series]
  (let [now (time/now clock)]
    (-> series
        (resource/set-property1 (compact/expand :dcterms/issued) now)
        (resource/set-property1 (compact/expand :dcterms/modified) now))))

(defn- set-base-entity [series]
  (resource/set-property1 series (compact/expand :dh/baseEntity) (resource/id series)))

(defn- insert-series [clock triplestore series]
  ;; TODO: move setting default properties outside?
  (let [series (->> series (set-timestamps clock) set-base-entity)
        to-insert (resource/->statements series)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn to-insert))
    series))

;; TODO: move this!
(defn series->response-body [series]
  (resource/->json-ld series (output-context)))

;; TODO: return series directly instead of formatting json-ld doc here
(defn upsert-series!
  "Returns a map {:op ... :jsonld-doc ...}, where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [clock triplestore {:keys [series-slug] :as api-params} incoming-jsonld-doc]
  (let [request-series (request->series api-params incoming-jsonld-doc)]
    (if-let [existing-series (get-series-by-slug triplestore series-slug)]
      (let [[changed? new-series] (merge-series-updates clock existing-series request-series)]
        (when changed?
          (update-series triplestore new-series))
        {:op (if changed? :update :noop)
         :jsonld-doc (series->response-body new-series)})
      (let [created-series (insert-series clock triplestore request-series)]
        {:op :create :jsonld-doc (series->response-body created-series)}))))

(defn upsert-release!
  "Returns a map {:op ... :jsonld-doc ...} where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [release-key (models-shared/release-key series-slug release-slug)
        api-params (assoc api-params :op.upsert/keys
                          {:series (models-shared/dataset-series-key series-slug)
                           :release release-key})
        updated-db (upsert-doc! db release/upsert-release api-params incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db release-key)}))

(defn insert-revision! [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [auto-revision-id (-> (swap! db update :revision-id-counter inc) :revision-id-counter)
        revision-key (models-shared/revision-key series-slug release-slug auto-revision-id)
        updated-db (swap! db revision/insert-revision api-params auto-revision-id incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db revision-key)}))


