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
    [meta-merge.core :as mm]
    [com.yetanalytics.flint :as f]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]
    [tpximpact.datahost.ldapi.models.revision :as revision]
    [tpximpact.datahost.ldapi.native-datastore :as datastore]
    [tpximpact.datahost.ldapi.models.change :as change]
    [tpximpact.datahost.ldapi.models.release-schema :as release-schema]
    [tpximpact.datahost.time :as time]
    [tpximpact.datahost.ldapi.compact :as compact]
    [tpximpact.datahost.ldapi.resource :as resource])
  (:import [java.net URI]))

(def db-defaults
  {:storage-type :local-file
   :opts {:commit-mode :sync
          :init {}}})

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

(defn- get-release-query [release-uri]
  (let [bgps [[release-uri 'a :dh/Release]
              [release-uri :dcterms/title '?title]
              [release-uri :dcterms/description '?description]
              [release-uri :dcat/inSeries '?series]
              [release-uri :dcterms/modified '?modified]
              [release-uri :dcterms/issued '?issued]]]
    {:prefixes (compact/as-flint-prefixes)
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
(defn- params->title-description-properties [query-params]
  (let [prop-mapping {:title (compact/expand :dcterms/title)
                      :description (compact/expand :dcterms/description)}]
    (map-properties prop-mapping query-params)))

(defn get-resource-by-construct-query [triplestore query]
  (let [statements (datastore/eager-query triplestore (f/format-query query :pretty? true))]
    (when (seq statements)
      (resource/from-statements statements))))

(defn get-series-by-uri [triplestore series-uri]
  (let [q (get-series-query series-uri)]
    (get-resource-by-construct-query triplestore q)))

(defn get-release-by-uri [triplestore release-uri]
  (let [q (get-release-query release-uri)]
    (get-resource-by-construct-query triplestore q)))

(defn get-series-by-slug [triplestore series-slug]
  (let [series-uri (models-shared/dataset-series-uri series-slug)]
    (get-series-by-uri triplestore series-uri)))

(defn get-release [triplestore series-slug release-slug]
  (let [series-uri (models-shared/dataset-series-uri series-slug)
        release-uri (models-shared/dataset-release-uri series-uri release-slug)]
    (get-release-by-uri triplestore release-uri)))

(def ^:private get-release-schema-params-valid?
  (m/validator [:map
                [:series-slug :string]
                [:release-slug :string]
                [:schema-slug :string]]))

(defn get-release-schema
  [db params]
  {:pre [(get-release-schema-params-valid? params)]}
  (get @db (models-shared/release-schema-key params)))

(defn get-revision [db series-slug release-slug revision-id]
  (let [key (models-shared/revision-key series-slug release-slug revision-id)]
    (get @db key)))

(defn get-change [db series-slug release-slug revision-id change-id]
  (let [key (models-shared/change-key series-slug release-slug revision-id change-id)]
    (get @db key)))

(defn- input-context []
  (assoc (update-vals @compact/default-context str)
    "@base" (str models-shared/ld-root)))

(defn- output-context [prefixes]
  (assoc (update-vals (compact/sub-context prefixes) str)
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
        param-properties (params->title-description-properties api-params)]
    (resource/set-properties doc-resource param-properties)))
(defn- request->release [series-uri {:keys [release-slug] :as api-params} json-doc]
  (let [release-uri (models-shared/dataset-release-uri series-uri release-slug)
        release-doc (annotate-json-resource json-doc release-uri (compact/expand :dh/Release))
        doc-resource (resource/from-json-ld-doc release-doc)
        param-properties (params->title-description-properties api-params)
        base-release (resource/set-properties doc-resource param-properties)]
    (resource/set-property1 base-release (compact/expand :dcat/inSeries) series-uri)))

(defn- update-resource-title-description-modified-query [resource]
  (let [resource-uri (resource/id resource)
        resource-type (resource/get-property1 resource (compact/expand :rdf/type))
        title (resource/get-property1 resource (compact/expand :dcterms/title))
        description (resource/get-property1 resource (compact/expand :dcterms/description))
        modified-at (resource/get-property1 resource (compact/expand :dcterms/modified))]
    {:prefixes {:dcterms "<http://purl.org/dc/terms/>"}
     :delete [[resource-uri :dcterms/title '?title]
              [resource-uri :dcterms/description '?description]
              [resource-uri :dcterms/modified '?modified]]
     :insert [[resource-uri :dcterms/title title]
              [resource-uri :dcterms/description description]
              [resource-uri :dcterms/modified modified-at]]
     :where [[resource-uri 'a resource-type]
             [resource-uri :dcterms/title '?title]
             [resource-uri :dcterms/description '?description]
             [resource-uri :dcterms/modified '?modified]]}))

(defn- update-resource-title-description-modified [triplestore resource]
  (let [q (update-resource-title-description-modified-query resource)
        qs (f/format-update q :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

(defn- update-series [triplestore series]
  (update-resource-title-description-modified triplestore series))

(defn- update-release [triplestore release]
  (update-resource-title-description-modified triplestore release))

(defn- modified-if-properties-changed [clock existing-resource request-resource diff-properties]
  (let [request-properties (resource/get-properties request-resource diff-properties)
        ;; NOTE: may only be a partial update so only fetch properties defined on the request
        existing-properties (resource/get-properties existing-resource (keys request-properties))]
    (if (= existing-properties request-properties)
      [false existing-resource]
      (let [updated (-> existing-resource
                        (resource/set-properties request-properties)
                        (resource/set-property1 (compact/expand :dcterms/modified) (time/now clock)))]
        [true updated]))))

(defn- modified-if-title-description-changed [clock existing-resource request-resource]
  (let [diff-properties [(compact/expand :dcterms/title)
                         (compact/expand :dcterms/description)]]
    (modified-if-properties-changed clock existing-resource request-resource diff-properties)))

(defn- merge-series-updates [clock old-series new-properties]
  (modified-if-title-description-changed clock old-series new-properties))

(defn- merge-release-updates [clock existing-release request-release]
  (modified-if-title-description-changed clock existing-release request-release))

(defn- set-timestamps [clock series]
  (let [now (time/now clock)]
    (-> series
        (resource/set-property1 (compact/expand :dcterms/issued) now)
        (resource/set-property1 (compact/expand :dcterms/modified) now))))

(defn- set-base-entity [series]
  (resource/set-property1 series (compact/expand :dh/baseEntity) (resource/id series)))

(defn- insert-resource [triplestore resource]
  (with-open [conn (repo/->connection triplestore)]
    (pr/add conn (resource/->statements resource))))

(defn- insert-series [clock triplestore series]
  ;; TODO: move setting default properties outside?
  (let [series (->> series (set-timestamps clock) set-base-entity)]
    (insert-resource triplestore series)
    series))

(defn- insert-release [clock triplestore release]
  (let [release (set-timestamps clock release)]
    (insert-resource triplestore release)
    release))

;; TODO: move this!
(defn series->response-body [series]
  (resource/->json-ld series (output-context ["dh" "dcterms" "rdf"])))

(defn release->response-body [release]
  (resource/->json-ld release (output-context ["dh" "dcterms" "rdf" "dcat"])))

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
  [clock triplestore series api-params incoming-jsonld-doc]
  (let [request-release (request->release (resource/id series) api-params incoming-jsonld-doc)]
    (if-let [existing-release (get-release-by-uri triplestore (resource/id request-release))]
      (let [[changed? new-release] (merge-release-updates clock existing-release request-release)]
        (when changed?
          (update-release triplestore new-release))
        {:op (if changed? :update :noop)
         :jsonld-doc (release->response-body new-release)})
      (let [created-release (insert-release clock triplestore request-release)]
        {:op :create :jsonld-doc (release->response-body created-release)}))))

(defn new-child-id [db parent-key child-predicate]
  "Looks at child keys on parent collection. Assumes keys are strings of format
  /x/y/.../1, i.e. paths that end in a stringified integer."
  (let [child-resources (some->> (get-in @db [parent-key child-predicate])
                                 (map #(get @db %)))
        child-key-fn #(get % "@id")]
    (if (empty? child-resources)
      1
      (->> child-resources
           (sort-by child-key-fn)
           last
           (child-key-fn)
           inc))))

(defn insert-revision! [db {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [auto-revision-id (new-child-id db
                                       (models-shared/release-key series-slug release-slug)
                                       "dh:hasRevision")
        revision-key (models-shared/revision-key series-slug release-slug auto-revision-id)
        updated-db (swap! db revision/insert-revision api-params auto-revision-id incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :resource-id auto-revision-id
     :jsonld-doc (get updated-db revision-key)}))


(defn insert-change! [db {:keys [series-slug release-slug revision-id] :as api-params} incoming-jsonld-doc appends-tmp-file]
  (let [auto-change-id (new-child-id db
                                     (models-shared/revision-key series-slug release-slug revision-id)
                                     "dh:hasChange")
        change-key (models-shared/change-key series-slug release-slug revision-id auto-change-id)
        updated-db (swap! db change/insert-change api-params auto-change-id incoming-jsonld-doc appends-tmp-file)]
    {:op (-> updated-db meta :op)
     :resource-id auto-change-id
     :jsonld-doc (get updated-db change-key)}))

(defn upsert-release-schema!
  [db {:keys [series-slug release-slug schema-slug] :as api-params} incoming-jsonld-doc]
  #_(let [upsert-keys {:series (models-shared/dataset-series-key series-slug)
                     :release (models-shared/release-key series-slug release-slug)
                     :release-schema (models-shared/release-schema-key series-slug release-slug schema-slug)}
        updated-db (upsert-doc! db release-schema/insert-schema
                                (assoc api-params :op.upsert/keys upsert-keys)
                                incoming-jsonld-doc)]
    {:op (-> updated-db meta :op)
     :jsonld-doc (get updated-db (:release-schema upsert-keys))}))

