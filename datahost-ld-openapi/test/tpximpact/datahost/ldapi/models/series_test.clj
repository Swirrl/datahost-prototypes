(ns tpximpact.datahost.ldapi.models.series-test
  (:require
   [clj-http.client :as http]
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing]]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.dcterms :refer [dcterms:title]]
   [tpximpact.datahost.ldapi.models.series :as sut]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]
   [tpximpact.datahost.ldapi.util :as util]
   [tpximpact.test-helpers :as th])
  (:import
   (clojure.lang ExceptionInfo)
   (java.net URI)))

(defn format-date-time
  [dt]
  (.format dt java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))

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
                               "dcterms:title" "A title"
                               "dcterms:identifier" "foobar"}
          timestamp (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC"))
          augmented-jsonld-doc (sut/normalise-series {:series-slug "new-series"
                                                      :op/timestamp timestamp}
                                                     incoming-jsonld-doc)]
      (testing "A series can be"

        (testing "created via API"
         (let [response (http/put
                         "http://localhost:3400/data/new-series"
                         {:content-type :json
                          :body (json/write-str incoming-jsonld-doc)})]
           (is (= (:status response) 201))
           (let [resp-body (json/read-str (:body response))
                 timestamp-str (format-date-time timestamp)
                 added-fields {"dcterms:issued" timestamp-str
                               "dcterms:modified" timestamp-str}]
             (is (= augmented-jsonld-doc 
                    (dissoc resp-body "dcterms:issued" "dcterms:modified")))
             (is (= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified"))))))

        (testing "retrieved via the API"
          (let [response (http/get "http://localhost:3400/data/new-series")
                resp-body (json/read-str (:body response))]
            (is (= (:status response) 200))
            (is  (= augmented-jsonld-doc
                    (dissoc resp-body  "dcterms:issued" "dcterms:modified")))
            (is (= (get resp-body "dcterms:issued")
                   (get resp-body "dcterms:modified"))))))

      (testing "A series can be updated via the API"
        (let [response (http/put "http://localhost:3400/data/new-series?title=A%20new%20title")]
          (is (= (:status response) 200)))
        
        (let [response (http/get "http://localhost:3400/data/new-series")
              resp-body (-> response :body json/read-str)]
          (is (= (:status response) 200))
          (is (= (get resp-body "dcterms:title") "A new title"))
          (is (not= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified")))
          (is (= "foobar" (get resp-body "dcterms:identifier"))
              "The identifier should be left untouched."))))))

(deftest normalise-context-test
  (let [expected-context ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]]

    (testing "An empty @context is normalised"
      (is (= expected-context
             (models-shared/normalise-context {}))))

    (testing "A declared context of 'https://publishmydata.com/def/datahost/context' is normalised"
      (is (= expected-context
             (models-shared/normalise-context {"@context" "https://publishmydata.com/def/datahost/context"}))))

    (testing "A normalised context is idempotent to itself"
      (is (= expected-context
             (models-shared/normalise-context {"@context" ["https://publishmydata.com/def/datahost/context",
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
    (let [timestamp (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC"))
          returned-value (sut/normalise-series {:series-slug "my-dataset-series"
                                                :op/timestamp timestamp}
                                               {"@id" "my-dataset-series"})]
      (is (= {"@id" "my-dataset-series"
              "@context" ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]
              "@type" "dh:DatasetSeries"
              "dh:baseEntity" "https://example.org/data/my-dataset-series/"
              ;; "dcterms:issued" (format-date-time timestamp)
              }
             returned-value))

      (is (canonicalisation-idempotent? {:series-slug "my-dataset-series"
                                         :op/timestamp timestamp}
                                        returned-value)))

    (testing "with title metadata"
      (let [timestamp (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC"))
            ednld (sut/normalise-series {:series-slug "my-dataset-series"
                                         :op/timestamp timestamp}
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
