(ns tpximpact.datahost.scratch.series-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing]]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.dcterms :refer [dcterms:title]]
   [tpximpact.datahost.scratch.series :as sut])
  (:import
   (clojure.lang ExceptionInfo)
   (java.io StringReader)
   (java.net URI)))

(defn- coerce-test-json [json-as-edn]
  (StringReader. (json/write-str json-as-edn)))

(deftest normalise-context-test
  (let [expected-context ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]]

    (testing "An empty @context is normalised"
      (is (= expected-context
             (sut/normalise-context {}))))

    (testing "A declared context of 'https://publishmydata.com/def/datahost/context' is normalised"
      (is (= expected-context
             (sut/normalise-context {"@context" "https://publishmydata.com/def/datahost/context"}))))

    (testing "A normalised context is idempotent to itself"
      (is (= expected-context
             (sut/normalise-context {"@context" ["https://publishmydata.com/def/datahost/context",
                                                 {"@base" "https://example.org/data/"}]}))))))

(defn canonicalisation-idempotent? [api-params jsonld]
  (let [canonicalised-form (sut/normalise-series api-params jsonld)]
    (= canonicalised-form (sut/normalise-series api-params canonicalised-form))))

(defn subject= [expected-s]
  (fn [s]
    (comp #(= (URI. expected-s) s)
          :s)))

(deftest normalise-series-test
  (testing "Invalid cases"
    (testing "Missing key detail :series-slug"

      (is (thrown? ExceptionInfo
                   (sut/normalise-series {}
                                         {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (sut/normalise-series {}
                                         {"@id" "my-dataset-series"}))))

    (is (thrown? ExceptionInfo
                 (sut/normalise-series {:series-slug "my-dataset-series"}
                                       {"@context" ["https://publishmydata.com/def/datahost/context"
                                                    {"@base" "https://different.base/is/invalid/"}]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (sut/normalise-series {:series-slug "my-dataset-series"}
                                       {"@context" ["https://publishmydata.com/def/datahost/context",
                                                    {"@base" "https://example.org/data/"}],
                                        "@id" "https://example.org/data/my-dataset-series"}))
        "@id usage in the series document is (for now) restricted to be in slugised form only"))

  (testing "Valid cases"
    (let [returned-value (sut/normalise-series {:series-slug "my-dataset-series"}
                                               {"@id" "my-dataset-series"})]
      (is (= {"@id" "my-dataset-series"
              "@context" ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]
              "dh:baseEntity" "https://example.org/data/my-dataset-series/"}
             returned-value))

      (is (canonicalisation-idempotent? {:series-slug "my-dataset-series"} returned-value)))

    (testing "with title metadata"
      (let [ednld (sut/normalise-series {:series-slug "my-dataset-series"}
                                        {"@context" ["https://publishmydata.com/def/datahost/context"
                                                     {"@base" "https://example.org/data/"}]
                                         "dcterms:title" "My Dataset Series"})]

        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}],
                "dcterms:title" "My Dataset Series",
                "@id" "my-dataset-series",
                "dh:baseEntity" "https://example.org/data/my-dataset-series/"}
               ednld))

        (testing "as RDF"
          (let [triples (matcha/index-triples (sut/ednld->rdf ednld))]
            (testing "All emitted triples have the same expected subject"
              (is (matcha/ask [[(URI. "https://example.org/data/my-dataset-series") dcterms:title ?o]] triples))
              ;; TODO add some more tests
              )))))

    ;; TODO add RDFization tests
    ))

;; PUT /data/:series-slug
;; GET /data/:series-slug
