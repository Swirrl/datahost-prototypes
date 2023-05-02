(ns tpximpact.datahost.scratch.series-test
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [grafter-2.rdf4j.io :as rio]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [testing is deftest with-test]]
            [tpximpact.datahost.scratch.series :as sut]
            [malli.core :as m]
            [malli.instrument :as mi])
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

    (is (= expected-context
           (normalise-context {})))

    (is (= expected-context
           (normalise-context {"@context" "https://publishmydata.com/def/datahost/context"})))

    (is (= expected-context
           (normalise-context {"@context" ["https://publishmydata.com/def/datahost/context",
                                           {"@base" "https://example.org/data/"}]})))))



(defn normalise-series [api-params json-ld]
  (sut/normalise-series api-params
                        (coerce-test-json json-ld)))

(m/=> normalise-series [:=> [:cat
                             [:map
                              [:slug :string]]
                             [:map]]
                        [:map
                         ["@id" sut/valid-slug?]
                         ["dh:base-entity" ]]])



(deftest normalise-series-test
  (testing "Invalid cases"
    (testing "Missing key details"

      (is (thrown? ExceptionInfo
                   (normalise-series {}
                                     {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (normalise-series {}
                                     {"@id" "my-dataset-series"}))
          )

      ))

  (testing "Valid cases"

    (normalise-series {:slug "my-dataset-series"}
                      {"@id" "my-dataset-series"})

    (is (not (empty?
              (normalise-series {:slug "my-dataset-series"}
                                {"@context" ["https://publishmydata.com/def/datahost/context"
                                             {"@base" "https://example.org/data/"}]
                                 "dcterms:title" "My Dataset Series"}))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (normalise-series {:slug "my-dataset-series"}
                                   {"@context" ["https://publishmydata.com/def/datahost/context",
                                                {"@base" "https://example.org/data/"}],
                                    "@id" "https://example.org/data/my-dataset-series"}))
        "@id usage in document is (for now) restricted to be slugised form only")))
