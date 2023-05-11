(ns tpximpact.datahost.scratch.model-test
  (:require
   [clojure.test :as t]
   [clojure.set :as set]
   [clojure.tools.logging :as log]
   [tpximpact.datahost.scratch.series :as series]))

(def db (atom {}))

(defn update-series [old-series {:keys [api-params jsonld-doc] :as _new-series}]
  (log/info "Updating series " (:series-slug api-params))
  (series/normalise-series api-params jsonld-doc))

(defn create-series [api-params jsonld-doc]
  (log/info "Updating series " (:series-slug api-params))
  (series/normalise-series api-params jsonld-doc))

(defn upsert-series [db {:keys [api-params jsonld-doc] :as new-series}]
  (let [k (str (.getPath series/ld-root) (:series-slug api-params))]
    (if-let [old-series (get db k)]
      (update db k update-series new-series)
      (assoc db k (normalise-series api-params jsonld-doc)))))

(defn normalise-release [{:keys [series-slug release-slug] :as api-params} jsonld-doc]
  (let [series-path (str (.getPath series/ld-root) series-slug)
        series (get @db series-path)
        base-entity (get series "dh:baseEntity")
        _ (assert base-entity "Expected base entity to be set")
        context ["https://publishmydata.com/def/datahost/context"
                 {"@base" base-entity}]]

    (-> (series/merge-params-with-doc api-params jsonld-doc)
        (assoc "@context" context
               "@id" release-slug
               "dcat:inSeries" (str "../" series-slug)))))

(defn- update-release [old-release {:keys [api-params jsonld-doc] :as _new-release}]
  (log/info "Updating release " (:series-slug api-params) "/" (:release-slug api-params))
  (normalise-release api-params jsonld-doc))

(defn upsert-release [db {:keys [series-slug release-slug] :as api-params} jsonld-doc]
  (let [release-path (str (.getPath series/ld-root) series-slug "/" release-slug)]
    (if-let [old-release (get db release-path)]
      (update db release-path update-release {:api-params api-params :jsonld-doc jsonld-doc})
      (assoc db release-path (normalise-release api-params jsonld-doc)))))

;; create-series
(swap! db upsert-series {:api-params {:series-slug "my-dataset-series"}})

;; upsert release
;;(swap! db normalise-release {:series-slug "my-dataset-series" :release-slug "2018"} {})

(swap! db upsert-release {:series-slug "my-dataset-series" :release-slug "2018"} {"dcterms:title" "my release"})

(let [coercion-properties #{"csvw:null" "csvw:default" "csvw:separator" "csvw:ordered"}
      transformation-properties #{"csvw:aboutUrl" "csvw:propertyUrl" "csvw:valueUrl" "csvw:virtual" "csvw:suppressOutput"}]


  (def reserved-schema-properties
    "These csvw properties are currently intentional unsupported, or may
  be supported in the future."
    (set/union coercion-properties transformation-properties)))

(defn normalise-schema [release schema]
  {"@type" #{"dh:TableSchema" "dh:UserSchema"} ; | dh:RevisionSchema
   "@context" ["https://publishmydata.com/def/datahost/context"
               {"@base" "https://example.org/data/my-dataset-series/2018/schema/"}]
   "@id" "2018"
   "dh:columns": [{"csvw:datatype" "string" ;; should support all/most csvw datatype definitions
                   "csvw:name" "sex"
                   "csvw:title" "Sex"
                   "@type" "dh:DimensionColumn" ;; | "dh:MeasureColumn" | "dh:AttributeColumn"
                   ;;"csvw:ordered" false
                   ;;"csvw:virtual" true

                   ;;"csvw:aboutUrl" "uri-template"
                   ;;"csvw:propertyUrl" "uri-template"
                   ;;"csvw:valueUrl" "uri-template"

                   ;;"csvw:required" true
                   ;;"csvw:separator" ";"
                   ;;"csvw:suppressOutput" false
                   ;;"csvw:textDirection" "ltr"
                   ;;"csvw:transformations" []
                   ;;
                   ;;"csvw:default" "default-value"
                   ;;"csvw:null" "n/a"


                   ;;"csvw:lang" "en"


                   }]}
  )

(defn derive-revision-schema
  "Derive a dh:RevisionSchema from a dh:CubeSchema"
  [cube-schema]
  )

(def example-changesets [{:description "first changeset"
                          :commits [{:append [["male" "manchester" 3]]
                                     :description "add obs"}]}
                         {:description "second changeset"
                          :commits [{:append [["female" "manchester" 4]]
                                     :description "add obs"}]}
                         {:description "second changeset"
                          :commits [{:delete [["male" "manchester" 3]]
                                     :append [["male" "manchester" 4]]
                                     :description "correct incorrect observation"}]}
                         ])

(def equivalent-changes-in-one-changeset [{:description "Coin the first release"
                                           :commits [{:append [["male" "manchester" 3]]
                                                      :description "add obs"}
                                                     {:append [["female" "manchester" 4]]
                                                      :description "add obs"}
                                                     {:delete [["male" "manchester" 3]]
                                                      :append [["male" "manchester" 4]]
                                                      :description "correct incorrect observation"}]}

                                          ])

(defn add-changeset [release {:keys [description append delete]}])
