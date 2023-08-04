(ns tpximpact.datahost.ldapi.util.data-validation-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [grafter-2.rdf4j.repository :as repo]
   [malli.core :as m]
   [malli.error :as me]
   [malli.transform :as mt]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.models.shared :as models.shared]
   [tpximpact.datahost.ldapi.routes.shared :refer [LdSchemaInput]]
   [tpximpact.datahost.ldapi.util.data-validation :as util.data-validation]
   [tpximpact.datahost.time :as time]))

(defn- explain
  "Returns nil on success, a value on validation error."
  [v]
  (-> (m/explain LdSchemaInput v) (me/humanize)))

(deftest validation-test
  (let [repo (repo/sail-repo)
        t (time/parse "2023-07-20T11:00:00Z")
        clock (time/manual-clock t)
        slugs {:series-slug "s1" :release-slug "r1" :schema-slug "schema1"}

        schema-json (-> (io/resource "test-inputs/schemas/simple.json")
                       (slurp)
                       (json/read-str {:keywordize? false}))
        ld-schema (m/decode
                   LdSchemaInput
                   schema-json 
                   (mt/transformer
                    mt/json-transformer
                    mt/string-transformer))]

    (testing "Decoding malli schema from JSON-LD"
      (is ld-schema))

    (db/upsert-release-schema! clock repo slugs ld-schema)
    
    (let [schema (db/get-release-schema repo (models.shared/release-uri-from-slugs "s1" "r1"))]
      (testing "Creating malli row schemas from JSON-LD schema"
        (is (= ["Year" "Unit of Measure"]
               (-> (util.data-validation/make-row-schema schema ["Year" "Unit of Measure"])
                   (util.data-validation/row-schema->column-names)))))

      (testing "Validating data"
        (let [ds (util.data-validation/as-dataset (io/resource "test-inputs/revision/2020.csv") {})
              row-schema (util.data-validation/make-row-schema schema)]
          (is (contains? (util.data-validation/validate-dataset
                          ds
                          row-schema
                          {:fail-fast? true})
                         :explanation))

          (is (contains? (util.data-validation/validate-dataset
                          ds
                          row-schema
                          {:fail-fast? false})
                         :dataset)))))))
