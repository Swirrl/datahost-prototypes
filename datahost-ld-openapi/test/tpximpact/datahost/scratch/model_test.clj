(ns tpximpact.datahost.scratch.model-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.set :as set]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [tpximpact.datahost.scratch.series :as series]
   [tpximpact.datahost.scratch.release :as release]
   [grafter-2.rdf4j.io :as gio]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.core :refer [prefixer]]
   [grafter.vocabularies.dcterms :refer [dcterms:title dcterms:description]]
   [grafter.vocabularies.dcat :refer [dcat:keyword dcat:Dataset dcat]])
  (:import [java.net URI]))




(defn db->matcha [db]
  (->> db
       vals
       (mapcat series/ednld->rdf)
       (matcha/index-triples)))

(def example:my-dataset-series (URI. "https://example.org/data/my-dataset-series"))
(def example:my-release (URI. "https://example.org/data/my-dataset-series/2018"))

(def dh (prefixer "https://publishmydata.com/def/datahost/"))
(def dh:baseEntity (dh "baseEntity"))
(def dcat:inSeries (dcat "inSeries"))


(deftest loading-data-workflow-with-rdf
  (let [db (atom {})] ;; an empty database
    (testing "Constructing the series"
      ;; first make a series
      (swap! db series/upsert-series {:api-params {:series-slug "my-dataset-series" :title "My series"}})

      (is (matcha/ask [[example:my-dataset-series dh:baseEntity ?o]] (db->matcha @db)))
      (is (matcha/ask [[example:my-dataset-series dcterms:title "My series"]] (db->matcha @db)))

      (testing "idempotent - upserting same request again is equivalent to inserting once"

        (let [start-state @db
              end-state (swap! db series/upsert-series {:api-params {:series-slug "my-dataset-series" :title "My series"}})]
          (is (= start-state end-state)))))

    (testing "Constructing a release"
      (swap! db release/upsert-release {:api-params {:series-slug "my-dataset-series" :release-slug "2018"}
                                        :jsonld-doc {"dcterms:title" "2018"}})

      (is (matcha/ask [[example:my-release ?p ?o]] (db->matcha @db)))
      (is (matcha/ask [[example:my-release dcterms:title "2018"]] (db->matcha @db)))

      (swap! db release/upsert-release {:api-params {:series-slug "my-dataset-series" :release-slug "2018"}
                                        :jsonld-doc {"dcterms:title" "2018"}})

      (testing "idempotent - upserting same request again is equivalent to inserting once"
        (let [start-state @db
              end-state (swap! db release/upsert-release {:api-params {:series-slug "my-dataset-series" :release-slug "2018"}
                                                          :jsonld-doc {"dcterms:title" "2018"}})]
          (is (= start-state end-state))))


      (testing "TODO inverse triples see issue: https://github.com/Swirrl/datahost-prototypes/issues/54"

        )

      )))


(let [coercion-properties #{"csvw:null" "csvw:default" "csvw:separator" "csvw:ordered"}
      transformation-properties #{"csvw:aboutUrl" "csvw:propertyUrl" "csvw:valueUrl" "csvw:virtual" "csvw:suppressOutput"}]


  (def reserved-schema-properties
    "These csvw properties are currently intentional unsupported, but some may
  be supported in the future.

  For example we're hoping to automatically define and generate
  aboutUrl's from the CSV when we know the type of all columns in the
  cube."
    (set/union coercion-properties transformation-properties)))

(defn normalise-schema [release schema]
  {"@type" #{"dh:TableSchema" "dh:UserSchema"} ; | dh:RevisionSchema
   "@context" ["https://publishmydata.com/def/datahost/context"
               {"@base" "https://example.org/data/my-dataset-series/2018/schema/"}]
   "@id" "2018"
   "dh:columns" [{"csvw:datatype" "string" ;; should support all/most csvw datatype definitions
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


(deftest ontology-parses
  (is (< 0 (count (gio/statements (io/file "./doc/datahost-ontology.ttl"))))))
