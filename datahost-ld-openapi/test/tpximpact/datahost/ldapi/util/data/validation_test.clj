(ns tpximpact.datahost.ldapi.util.data.validation-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [grafter-2.rdf4j.repository :as repo]
   [malli.core :as m]
   [malli.error :as me]
   [malli.transform :as mt]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.routes.shared :refer [LdSchemaInput]]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.time :as time])
  (:import (java.net URI)))

(defn- explain
  "Returns nil on success, a value on validation error."
  [v]
  (-> (m/explain LdSchemaInput v) (me/humanize)))

(defn- json-to-ld
  [schema-json]
  (m/decode
   LdSchemaInput
   schema-json
   (mt/transformer
    mt/json-transformer
    mt/string-transformer)))

(defn- schema-from-resource
  [path]
  (-> (io/resource path)
      (slurp)
      (json/read-str {:keywordize? false})))

(defn- dataset-creation-success?
  [ds]
  (not (data.validation/dataset-errors? ds)))

(defn- unparsed-data [column]
  (:unparsed-data (meta column)))

(defn- optional-failure-message [ds]
  (when (tc/has-column? ds :$error)
    (str (tc/column ds :$error))))

(deftest validation-test
  (let [repo (repo/sail-repo)
        t (time/parse "2023-07-20T11:00:00Z")
        clock (time/manual-clock t)
        system-uris (su/make-system-uris (URI. "https://example.org/data/"))

        slugs {:series-slug "s1" :release-slug "r1" :schema-slug "schema1"}
        slugs2 {:series-slug "s2" :release-slug "r1" :schema-slug "schema1"}

        schema-json (schema-from-resource "test-inputs/schemas/simple.json")
        ld-schema (json-to-ld schema-json)

        schema-int-double-double (-> "test-inputs/schemas/int-double-double.json"
                                     schema-from-resource
                                     json-to-ld)]

    (testing "Decoding malli schema from JSON-LD"
      (is ld-schema))

    (db/upsert-release-schema! clock repo system-uris ld-schema slugs)
    (db/upsert-release-schema! clock repo system-uris schema-int-double-double slugs2)
    
    (let [schema (db/get-release-schema repo (su/dataset-release-uri* system-uris slugs2))
          cols ["Year" "Double" "Other Double"]]
      
      (testing "Creating malli row schemas from JSON-LD schema"
        (is (= cols
               (-> (data.validation/make-row-schema schema cols)
                   (data.validation/row-schema->column-names)))))

      (testing "Data validation using a row-schema"
        ;; NOTE: we're not enforcing the schema on creationi of the dataset
        (let [ds (data.validation/as-dataset (io/resource "test-inputs/revision/one-row-invalid-double.csv") {})
              row-schema (data.validation/make-row-schema schema)]
          (is (contains? (data.validation/validate-dataset ds row-schema
                                                                {:fail-fast? true})
                         :explanation))
          (is (contains? (data.validation/validate-dataset ds row-schema
                                                                {:fail-fast? false})
                         :dataset)))))

    (let [schema (db/get-release-schema repo (su/dataset-release-uri* system-uris slugs2))
          row-schema (data.validation/make-row-schema schema ["Year" "Double"])]
      
      (testing "Create a dataset with well formatted double values"
        (let [ds (-> (io/resource "test-inputs/revision/valid-double-column.csv")
                     (data.validation/as-dataset {:enforce-schema row-schema}))
              validation-result (data.validation/validate-dataset ds row-schema
                                                                       {:fail-fast? true})]
          (is (dataset-creation-success? ds) (optional-failure-message ds))
          (is (empty? (unparsed-data (tc/column ds "Double"))))
          (is (not (contains? validation-result :explanation)))
          (is (contains? validation-result :dataset))))

      (testing "Failure to create a dataset with incorrectly formatted double values"
        (let [csv (io/resource "test-inputs/revision/invalid-double-column.csv")]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"^Dataset creation failure.*"
                                (data.validation/as-dataset csv {:enforce-schema row-schema})))))

      (testing "Create dataset from one-row CSV with invalid double value"
        ;; without the :enforce-schema option, dataset will be created,
        ;; and the "Double" column's type will be set to 'int' by tablecloth
        (let [csv (io/resource "test-inputs/revision/one-row-invalid-double.csv")]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"^Dataset creation failure: failures in columns: .*"
                                (data.validation/as-dataset csv {:enforce-schema row-schema})))

          (let [ex (try (data.validation/as-dataset csv {:enforce-schema row-schema})
                        (catch clojure.lang.ExceptionInfo ex
                          ex))]
            (is (= ["Double"] (:error-columns (ex-data ex))))))))))
