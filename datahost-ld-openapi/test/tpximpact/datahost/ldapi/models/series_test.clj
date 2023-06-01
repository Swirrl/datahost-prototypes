(ns tpximpact.datahost.ldapi.models.series-test
  (:require
   [clj-http.client :as http]
   [clojure.test :refer [deftest is testing]]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.dcterms :refer [dcterms:title]]
   [tpximpact.datahost.ldapi.models.series :as sut]
   [tpximpact.datahost.ldapi.util :as util]
   [tpximpact.test-helpers :as th]
   [clojure.data.json :as json])
  (:import
   (clojure.lang ExceptionInfo)
   (java.net URI)))

(deftest round-tripping-series-test
  (th/with-system-and-clean-up sys
    (testing "A series that does not exist returns 'not found'"
      (try
        (http/get "http://localhost:3400/data/does-not-exist")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= status 404))
            (is (= body "Not found"))))))

    (let [incoming-jsonld-doc {"@context"
                               ["https://publishmydata.com/def/datahost/context"
                                {"@base" "https://example.org/data/"}],
                               "dcterms:title" "A title"}
          augmented-jsonld-doc (sut/normalise-series {:series-slug "new-series"}
                                                     incoming-jsonld-doc)]
      (testing "A series can be created and retrieved via the API"

        (let [response (http/put
                        "http://localhost:3400/data/new-series"
                        {:content-type :json
                         :body (json/write-str incoming-jsonld-doc)})]
          (is (= (:status response) 201))
          (is (= (json/read-str (:body response)) augmented-jsonld-doc)))

        (let [response (http/get "http://localhost:3400/data/new-series")]
          (is (= (:status response) 200))
          (is (= (json/read-str (:body response)) incoming-jsonld-doc))))

      (testing "A series can be updated via the API"
        (let [response (http/put "http://localhost:3400/data/new-series?title=A%20new%20title")]
          (is (= (:status response) 200)))

        (let [response (http/get "http://localhost:3400/data/new-series")]
          (is (= (:status response) 200))
          (is (= (-> response :body json/read-str (get "dcterms:title")) "A new title")))))))

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
              "@type" "dh:DatasetSeries"
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
                 {"@base" "https://example.org/data/"}]
                "@id" "my-dataset-series"
                "@type" "dh:DatasetSeries"
                "dcterms:title" "My Dataset Series"
                "dh:baseEntity" "https://example.org/data/my-dataset-series/"}
               ednld))

        (testing "as RDF"
          (let [triples (matcha/index-triples (util/ednld->rdf ednld))]
            (testing "All emitted triples have the same expected subject"
              (is (matcha/ask [[(URI. "https://example.org/data/my-dataset-series") dcterms:title ?o]] triples))
              ;; TODO add some more tests
              )))))

    ;; TODO add RDFization tests
    ))

;; PUT /data/:series-slug
;; GET /data/:series-slug
