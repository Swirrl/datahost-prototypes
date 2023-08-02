(ns tpximpact.datahost.ldapi.db
  (:require
    [clojure.data.json :as json]
    [grafter-2.rdf.protocols :as pr]
    [grafter-2.rdf4j.repository :as repo]
    [com.yetanalytics.flint :as f]
    [tpximpact.datahost.ldapi.models.shared :as models-shared]
    [tpximpact.datahost.ldapi.native-datastore :as datastore]
    [tpximpact.datahost.time :as time]
    [tpximpact.datahost.ldapi.compact :as compact]
    [tpximpact.datahost.ldapi.resource :as resource]
    [tpximpact.datahost.ldapi.store :as store])
  (:import [java.net URI]))

(def prefixes {:dcterms (URI. "http://purl.org/dc/terms/")
               :dh (URI. "https://publishmydata.com/def/datahost/")})

(defn- get-series-query [series-url]
  (let [bgps [[series-url 'a :dh/DatasetSeries]
              [series-url :dcterms/title '?title]
              [series-url :dh/baseEntity '?baseentity]
              [series-url :dcterms/modified '?modified]
              [series-url :dcterms/issued '?issued]]]
    {:prefixes {:dcterms "<http://purl.org/dc/terms/>"
                :dh "<https://publishmydata.com/def/datahost/>"}
     :construct (conj bgps [series-url :dcterms/description '?description])
     :where (conj bgps [:optional [[series-url :dcterms/description '?description]]])}))

(defn- get-release-query [release-uri]
  {:prefixes (compact/as-flint-prefixes)
   :construct [[release-uri 'a :dh/Release]
               [release-uri :dcterms/title '?title]
               [release-uri :dcterms/description '?description]
               [release-uri :dcat/inSeries '?series]
               [release-uri :dh/hasRevision '?revision]
               [release-uri :dh/hasSchema '?schema]
               [release-uri :dcterms/modified '?modified]
               [release-uri :dcterms/issued '?issued]]
   :where [[release-uri 'a :dh/Release]
           [release-uri :dcterms/title '?title]
           [release-uri :dcat/inSeries '?series]
           [:optional [[release-uri :dh/hasRevision '?revision]]]
           [:optional [[release-uri :dh/hasSchema '?schema]]]
           [:optional [[release-uri :dcterms/description '?description]]]
           [release-uri :dcterms/modified '?modified]
           [release-uri :dcterms/issued '?issued]]})

(defn- get-change-query [change-uri]
  (let [bgps [[change-uri 'a :dh/Change]
              [change-uri :dcterms/description '?description]
              [change-uri :dh/appends '?appends]
              [change-uri :dh/appliesToRevision '?revision]]]
    {:prefixes prefixes
     :construct bgps
     :where bgps}))

(defn- get-release-schema-query [release-uri]
  {:prefixes prefixes
   :construct [['?schema '?p '?o]]
   :where [[release-uri :dh/hasSchema '?schema]
           ['?schema '?p '?o]]})

(defn- get-schema-columns-query [schema-uri]
  {:prefixes prefixes
   :construct [['?col '?p '?o]]
   :where [[schema-uri :dh/columns '?col]
           ['?col '?p '?o]]})

(defn- map-properties [prop-mapping m]
  (reduce-kv (fn [props k v]
               (if-let [prop-uri (get prop-mapping k)]
                 (resource/add-properties-property props prop-uri v)
                 props))
             (resource/empty-properties)
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

(defn get-release-schema
  [triplestore release-uri]
  (let [q (get-release-schema-query release-uri)]
    (when-let [schema (get-resource-by-construct-query triplestore q)]
      (assert (resource/id schema))
      (let [columns-query (get-schema-columns-query (resource/id schema))
            qs (f/format-query columns-query :pretty? true)
            column-statements (datastore/eager-query triplestore qs)]
        (resource/add-statements schema column-statements)))))

(defn get-revision
  ([triplestore revision-uri]
   (let [q (let [bgps [[revision-uri 'a :dh/Revision]
                       [revision-uri :dcterms/title '?title]
                       [revision-uri :dh/appliesToRelease '?release]]]
             {:prefixes prefixes
              :construct (conj bgps
                               [revision-uri :dh/hasChange '?change]
                               [revision-uri :dcterms/description '?description])
              :where (conj bgps
                           [:optional [[revision-uri :dh/hasChange '?change]]]
                           [:optional [[revision-uri :dcterms/description '?description]]])})]
     (get-resource-by-construct-query triplestore q)))

  ([triplestore series-slug release-slug revision-id]
   (get-revision triplestore
                 (models-shared/revision-uri series-slug release-slug revision-id))))

(defn get-change
  ([triplestore change-uri]
   (get-resource-by-construct-query triplestore
                                    (get-change-query change-uri)))
  ([triplestore series-slug release-slug revision-id change-id]
   (let [change-uri (models-shared/change-uri series-slug release-slug revision-id change-id)]
     (get-change triplestore change-uri))))

(defn revision-appends-file-locations
  "Given a Revision as a hash map, returns appends file locations"
  [triplestore revision]
  (some->> (resource/get-property revision (compact/expand :dh/hasChange))
           (map #(get-change triplestore %))
           (map #(resource/get-property1 % (compact/expand :dh/appends)))))

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
    {:prefixes prefixes
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

(defn- submit-update [triplestore update-query]
  (let [qs (f/format-update update-query :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

(defn- submit-updates [triplestore update-queries]
  (let [qs (f/format-updates update-queries :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

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

(defn- set-timestamps [clock resource]
  (let [now (time/now clock)]
    (-> resource
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

(defn revision->response-body [revision]
  (resource/->json-ld revision (output-context ["dh" "dcterms" "rdf"])))

(defn- map-by [f items]
  (into {} (map (fn [v] [(f v) v]) items)))

(defn- column-number [col]
  (Integer/parseInt (get col "csvw:number")))
(defn schema->response-body [schema]
  ;; NOTE: Schema documents are modified from the standard json-ld serialisation
  ;; The top-level @graph node is removed and the column nodes are inlined within
  ;; the schema node
  (let [json-ld-str (resource/->json-ld schema (output-context ["dh" "dcterms" "csvw" "appropriate-csvw"]))
        json-ld-doc (json/read-str json-ld-str)
        nodes (get json-ld-doc "@graph")
        is-schema-node? (fn [n] (= "dh:TableSchema" (get n "@type")))
        schema-node (first (filter is-schema-node? nodes))
        column-nodes (remove is-schema-node? nodes)
        base-uri-str (get-in json-ld-doc ["@context" "@base"])
        node-uri->node (map-by (fn [col] (str base-uri-str (get col "@id"))) column-nodes)
        inlined-schema (update schema-node "dh:columns" (fn [col-uris]
                                                          (->> col-uris
                                                               (map node-uri->node)
                                                               (sort-by column-number)
                                                               (vec))))
        inlined-doc (assoc inlined-schema "@context" (get json-ld-doc "@context"))]
    (json/write-str inlined-doc)))

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

(defn delete-series-revisions-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?revision '?p '?o]
            ['?release :dh/hasRevision '?revision]]
   :where [['?release :dcat/inSeries series-uri]
           ['?revision :dh/appliesToRelease '?release]
           ['?revision '?p '?o]]})

(defn delete-series-releases-schemas-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?schema '?p '?o]
            ['?release :dh/hasSchema '?schema]]
   :where [['?release :dcat/inSeries series-uri]
           ['?release :dh/hasSchema '?schema]
           ['?schema '?p '?o]]})

(defn delete-series-releases-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?release '?p '?o]]
   :where [['?release :dcat/inSeries series-uri]
           ['?release '?p '?o]]})

(defn delete-series-query [series-uri]
  {:delete [[series-uri '?p '?o]]
   :where [[series-uri '?p '?o]]})

(defn delete-series!
  [triplestore series-slug]
  (let [series-uri (models-shared/dataset-series-uri series-slug)
        queries [(delete-series-releases-schemas-query series-uri)
                 (delete-series-revisions-query series-uri)
                 (delete-series-releases-query series-uri)
                 (delete-series-query series-uri)]]
    (submit-updates triplestore queries)))

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

(defn- select-auto-increment-query [parent-uri child-pred]
  {:prefixes {:dh "<https://publishmydata.com/def/datahost/>"
              :xsd "<http://www.w3.org/2001/XMLSchema#>"}
   :select ['?next]
   :where [[:where {:select '[[(max (:xsd/integer (replace (str ?child) "^.*/([^/]*)$" "$1"))) ?highest]]
                    :where [[parent-uri child-pred '?child]]}]
           [:bind ['(coalesce (+ ?highest 1) 1) '?next]]]})

(defn- fetch-next-child-resource-number [triplestore parent-uri child-pred]
  (let [q (select-auto-increment-query parent-uri child-pred)
        qs (f/format-query q :pretty? true)
        bindings (with-open [conn (repo/->connection triplestore)]
                   (doall (repo/query conn qs)))]
    (if-let [bs (first bindings)]
      (:next bs)
      (throw (ex-info "Couldn't fetch new child number for parent resource" {:resource-uri parent-uri})))))

(defn- generate-revision-number [triplestore {:keys [series-slug release-slug] :as _api-params}]
  (let [series-uri (models-shared/dataset-series-uri series-slug)
        release-uri (models-shared/dataset-release-uri series-uri release-slug)]
    (fetch-next-child-resource-number triplestore release-uri :dh/hasRevision)))

(defn- generate-change-number [triplestore {:keys [series-slug release-slug revision-id] :as _api-params}]
  (let [series-uri (models-shared/dataset-series-uri series-slug)
        release-uri (models-shared/dataset-release-uri series-uri release-slug)
        revision-uri (models-shared/dataset-revision-uri release-uri revision-id)]
    (fetch-next-child-resource-number triplestore revision-uri :dh/hasChange)))

(defn- request->revision [revision-number {:keys [series-slug release-slug] :as api-params} json-doc]
  (let [revision-uri (models-shared/revision-uri series-slug release-slug revision-number)
        series-uri (models-shared/dataset-series-uri series-slug)
        release-uri (models-shared/dataset-release-uri series-uri release-slug)
        revision-doc (annotate-json-resource json-doc revision-uri (compact/expand :dh/Revision))
        doc-resource (resource/from-json-ld-doc revision-doc)
        param-properties (params->title-description-properties api-params)]
    (-> doc-resource
        (resource/set-properties param-properties)
        (resource/set-property1 (compact/expand :dh/appliesToRelease) release-uri))))

(defn- request->change [change-number {:keys [series-slug release-slug revision-id] :as api-params} json-doc appends-tmp-file]
  (let [change-uri (models-shared/change-uri series-slug release-slug revision-id change-number)
        series-uri (models-shared/dataset-series-uri series-slug)
        release-uri (models-shared/dataset-release-uri series-uri release-slug)
        revision-uri (models-shared/dataset-revision-uri release-uri revision-id)
        change-doc (annotate-json-resource json-doc change-uri (compact/expand :dh/Change))
        doc-resource (resource/from-json-ld-doc change-doc)
        param-properties (params->title-description-properties api-params)]
    (-> doc-resource
        (resource/set-properties param-properties)
        (resource/set-property1 (compact/expand :dh/appliesToRevision) revision-uri))))

(defn- request->schema [{:keys [series-slug release-slug schema-slug] :as api-params} json-doc]
  (let [schema-uri (models-shared/release-schema-uri series-slug release-slug schema-slug)
        release-uri (models-shared/release-uri-from-slugs series-slug release-slug)
        schema-doc (annotate-json-resource json-doc schema-uri (compact/expand :dh/TableSchema))
        schema-doc (update schema-doc "dh:columns" (fn [cols]
                                                     (map-indexed (fn [index col]
                                                             (assoc col "@id" (str schema-uri "/columns/" (inc index))
                                                                        "@type" "dh:DimensionColumn"
                                                                        "csvw:number" (inc index)))
                                                           cols)))
        schema-resource (resource/from-json-ld-doc schema-doc)]
    (-> schema-resource
        (resource/set-property1 (compact/expand :dh/appliesToRelease) release-uri)
        (resource/set-property1 (compact/expand :appropriate-csvw/modeling-of-dialect) "UTF-8,RFC4180"))))

(defn- release-revision-statements
  "Returns a collection of triples connecting a release to a revision"
  [revision]
  [(pr/->Triple (resource/get-property1 revision (compact/expand :dh/appliesToRelease))
                (compact/expand :dh/hasRevision)
                (resource/id revision))])

(defn- revision-change-statements
  "Returns a collection of triples connecting a revision to a change"
  [change]
  [(pr/->Triple (resource/get-property1 change (compact/expand :dh/appliesToRevision))
                (compact/expand :dh/hasChange)
                (resource/id change))])

(defn insert-revision! [triplestore api-params incoming-jsonld-doc]
  (let [revision-number (generate-revision-number triplestore api-params)
        revision (request->revision revision-number api-params incoming-jsonld-doc)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn (concat (resource/->statements revision)
                           (release-revision-statements revision))))
    {:resource-id revision-number
     :jsonld-doc (revision->response-body revision)}))

(defn insert-change! [triplestore change-store api-params incoming-jsonld-doc appends-tmp-file]
  (let [change-number (generate-change-number triplestore api-params)
        change (request->change change-number api-params incoming-jsonld-doc appends-tmp-file)
        append-key (store/insert-append change-store appends-tmp-file)
        change (resource/set-property1 change (compact/expand :dh/appends) append-key)]

    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn (concat (resource/->statements change)
                           (revision-change-statements change))))
    {:resource-id change-number
     :jsonld-doc (resource/->json-ld change (output-context ["dh" "dcterms" "rdf"]))}))

(defn- release-schema-statements [schema]
  [(pr/->Triple (resource/get-property1 schema (compact/expand :dh/appliesToRelease))
                (compact/expand :dh/hasSchema)
                (resource/id schema))])
(defn- insert-schema [clock triplestore schema]
  (let [new-schema (set-timestamps clock schema)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn (concat (resource/->statements new-schema)
                           (release-schema-statements new-schema))))
    new-schema))

(defn upsert-release-schema!
  [clock triplestore {:keys [series-slug release-slug] :as api-params} incoming-jsonld-doc]
  (let [request-schema (request->schema api-params incoming-jsonld-doc)
        release-uri (models-shared/release-uri-from-slugs series-slug release-slug)]
    (if-let [existing-schema (get-release-schema triplestore release-uri)]
      {:op :noop
       :jsonld-doc (schema->response-body existing-schema)}
      (let [new-schema (insert-schema clock triplestore request-schema)]
        {:op :create :jsonld-doc (schema->response-body new-schema)}))))

