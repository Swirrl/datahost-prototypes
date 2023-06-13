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
  (.format ^java.time.ZonedDateTime dt java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))

(deftest round-tripping-series-test
  (th/with-system-and-clean-up sys
    (testing "A series that does not exist returns 'not found'"
      (try
        (http/get "http://localhost:3400/data/does-not-exist")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= status 404))
            (is (= body "Not found"))))))

    (let [request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}],
                         "dcterms:title" "A title"
                         "dcterms:identifier" "foobar"}
          timestamp (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC"))
          normalised-ednld {"@context"
                            ["https://publishmydata.com/def/datahost/context"
                             {"@base" "https://example.org/data/"}],
                            "@type" "dh:DatasetSeries"
                            "@id" "new-series"
                            "dh:baseEntity" "https://example.org/data/new-series/"
                            "dcterms:title" "A title"
                            "dcterms:issued" timestamp
                            "dcterms:modified" timestamp}]
      (testing "A series can be created and retrieved via the API"

        (let [response (http/put
                        "http://localhost:3400/data/new-series"
                        {:content-type :json
                         :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))
              timestamp-str (format-date-time timestamp)]
          (is (= (:status response) 201))
          (is (= (json/read-str (:body response)) normalised-ednld))

          (is (= (get resp-body "dcterms:issued")
                 (get resp-body "dcterms:modified")
                 timestamp-str)))
;; TODO NOW: make this a separate test
;; testing "retrieved via the API"
        (let [response (http/get "http://localhost:3400/data/new-series")]
          (is (= (:status response) 200))
          (is (= (json/read-str (:body response)) normalised-ednld))))

      (testing "A series can be updated via the API, query params take precedence"
        (let [response (http/put "http://localhost:3400/data/new-series?title=A%20new%20title")
              resp-body (json/read-str (:body response))]
          (is (= (:status response) 200))
          (is (= normalised-ednld resp-body)
          (is (= (get resp-body "dcterms:issued")
                 (get resp-body "dcterms:modified"))))

        (let [response (http/get "http://localhost:3400/data/new-series")]
          (is (= (:status response) 200))
          ;; TODO NOW get resp-body "dcterms:title"
          ;; (is (not= (get resp-body "dcterms:issued")
                    ;; (get resp-body "dcterms:modified")))
          ;; (is (= "foobar" (get resp-body "dcterms:identifier"))
               ;; "The identifier should be left untouched.")))

          (is (= (-> response :body json/read-str (get "dcterms:title")) "A new title")))))))

(deftest normalise-context-test
  (let [expected-context ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}]]

    (testing "An empty @context is normalised"
      (is (= expected-context
             (models-shared/normalise-context {}
                                              (str models-shared/ld-root)))))

    (testing "A declared context of 'https://publishmydata.com/def/datahost/context' is normalised"
      (is (= expected-context
             (models-shared/normalise-context {"@context" "https://publishmydata.com/def/datahost/context"}
                                              (str models-shared/ld-root)))))

    (testing "A normalised context is idempotent to itself"
      (is (= expected-context
             (models-shared/normalise-context {"@context" ["https://publishmydata.com/def/datahost/context",
                                                           {"@base" "https://example.org/data/"}]}
                                              (str models-shared/ld-root)))))))

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
