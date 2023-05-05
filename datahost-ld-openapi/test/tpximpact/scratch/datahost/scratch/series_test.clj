(ns tpximpact.datahost.scratch.series-test
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [grafter-2.rdf4j.io :as rio]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [testing is deftest with-test]]
            [tpximpact.datahost.scratch.series :as sut]
            [malli.core :as m]
            [malli.instrument :as mi]
            [grafter.matcha.alpha :as matcha]
            [grafter.vocabularies.dcterms :refer [dcterms:title]])
  (:import [java.net URI]
           [java.io StringReader]
           [com.github.jsonldjava.core JsonLdProcessor RDFDatasetUtils JsonLdTripleCallback]
           [com.github.jsonldjava.utils JsonUtils]
           [clojure.lang ExceptionInfo]))

(defn- coerce-test-json [json-as-edn]
  (StringReader. (json/write-str json-as-edn)))

(defn normalise-context [ednld]
  (sut/normalise-context (coerce-test-json ednld)))

(deftest normalise-context-test
  (let [expected-context ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]]

    (testing "An empty @context is normalised"
      (is (= expected-context
             (normalise-context {}))))

    (testing "A declared context of 'https://publishmydata.com/def/datahost/context' is normalised"
      (is (= expected-context
             (normalise-context {"@context" "https://publishmydata.com/def/datahost/context"}))))

    (testing "A normalised context is idempotent to itself"
      (is (= expected-context
             (normalise-context {"@context" ["https://publishmydata.com/def/datahost/context",
                                             {"@base" "https://example.org/data/"}]}))))))



(defn normalise-series [api-params json-ld]
  (sut/normalise-series api-params
                        (coerce-test-json json-ld)))

(defn canonicalisation-idempotent? [api-params jsonld]
  (let [canonicalised-form (normalise-series api-params jsonld)]
    (= canonicalised-form (normalise-series api-params canonicalised-form))))

(defn subject= [expected-s]
  (fn [s]
    (comp #(= (URI. expected-s) s)
          :s)))

(deftest normalise-series-test
  (testing "Invalid cases"
    (testing "Missing key detail :slug"

      (is (thrown? ExceptionInfo
                   (normalise-series {}
                                     {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (normalise-series {}
                                     {"@id" "my-dataset-series"}))))

    (is (thrown? ExceptionInfo
                 (normalise-series {:slug "my-dataset-series"}
                                   {"@context" ["https://publishmydata.com/def/datahost/context"
                                                {"@base" "https://different.base/is/invalid/"}]})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (normalise-series {:slug "my-dataset-series"}
                                   {"@context" ["https://publishmydata.com/def/datahost/context",
                                                {"@base" "https://example.org/data/"}],
                                    "@id" "https://example.org/data/my-dataset-series"}))
        "@id usage in the series document is (for now) restricted to be in slugised form only"))

  (testing "Valid cases"
    (let [returned-value (normalise-series {:slug "my-dataset-series"}
                                           {"@id" "my-dataset-series"})]
      (is (= {"@id" "my-dataset-series"
              "@context" ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]
              "dh:base-entity" "https://example.org/data/my-dataset-series/"}
             returned-value))

      (is (canonicalisation-idempotent? {:slug "my-dataset-series"} returned-value)))

    (testing "with title metadata"
      (let [ednld (normalise-series {:slug "my-dataset-series"}
                                    {"@context" ["https://publishmydata.com/def/datahost/context"
                                                 {"@base" "https://example.org/data/"}]
                                     "dcterms:title" "My Dataset Series"})]

        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}],
                "dcterms:title" "My Dataset Series",
                "@id" "my-dataset-series",
                "dh:base-entity" "https://example.org/data/my-dataset-series/"}
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
