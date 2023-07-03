(ns tpximpact.datahost.ldapi.models.series-test
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.dcterms :refer [dcterms:title]]
   [tpximpact.datahost.ldapi.models.series :as sut]
   [tpximpact.datahost.ldapi.models.shared :as models-shared]
   [tpximpact.datahost.ldapi.util :as util]
   [tpximpact.test-helpers :as th])
  (:import
    (clojure.lang ExceptionInfo)
    (java.net URI)
    (java.util UUID)))

(defn format-date-time
  [dt]
  (.format ^java.time.ZonedDateTime dt java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))

(deftest round-tripping-series-test
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client :as _sys}
    (testing "A series that does not exist returns 'not found'"
      (try
        (GET "/data/does-not-exist")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (let [new-series-id (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-id)
          request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/"}],
                         "dcterms:title" "A title"
                         "dcterms:identifier" "foobar"}
          normalised-ednld {"@context"
                            ["https://publishmydata.com/def/datahost/context"
                             {"@base" "https://example.org/data/"}],
                            "@type" "dh:DatasetSeries"
                            "dcterms:identifier" "foobar"
                            "@id" new-series-id
                            "dh:baseEntity" (str "https://example.org" new-series-path "/")
                            "dcterms:title" "A title"}]

      (testing "A series can be created"
        (let [response (PUT new-series-path
                            {:content-type :json
                             :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "dcterms:issued" "dcterms:modified")))
          (is (= (get resp-body "dcterms:issued")
                 (get resp-body "dcterms:modified")))))

      (testing "A series can be retrieved via the API"
        (let [response (GET new-series-path)
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "dcterms:issued" "dcterms:modified")))))

      (testing "A series can be updated via the API, query params take precedence"
        (let [response (PUT (str new-series-path "?title=A%20new%20title")
                            {:content-type :json
                                  :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (not= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified"))))

        (let [response (GET new-series-path)
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (not= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified")))
          (is (= "foobar" (get resp-body "dcterms:identifier"))
              "The identifier should be left untouched.")
          (is (= (get resp-body "dcterms:title") "A new title"))

          (testing "No update when query params same as in existing doc"
            (let [{body' :body :as response} (PUT (str new-series-path "?title=A%20new%20title")
                                                  {:content-type :json :body nil})
                  body' (json/read-str body')]
              (is (= 200 (:status response)))
              (is (= "A new title" (get body' "dcterms:title")))
              (is (= (select-keys resp-body ["dcterms:issued" "dcterms:modified"])
                     (select-keys body' ["dcterms:issued" "dcterms:modified"]))
                  "The document shouldn't be modified"))))))))

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
