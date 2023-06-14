(ns tpximpact.datahost.ldapi.models.release-test
  (:require
   [clj-http.client :as http]
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing]]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.models.release :as sut])
  (:import (clojure.lang ExceptionInfo)))

(defn- put-series []
  (let [jsonld {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}]
                "dcterms:title" "A title"}]
    (http/put
     "http://localhost:3400/data/new-series"
     {:content-type :json
      :body (json/write-str jsonld)})))

(deftest round-tripping-release-test
  (th/with-system-and-clean-up sys
    (testing "Fetching a release for a series that does not exist returns 'not found'"
      (try
        (http/get "http://localhost:3400/data/does-not-exist/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Fetching a release that does not exist returns 'not found'"
      (try
        (http/get "http://localhost:3400/data/new-series/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Creating a release for a series that is not found fails gracefully"
      (let [jsonld {"@context"
                    ["https://publishmydata.com/def/datahost/context"
                     {"@base" "http://example.org/data/"}]
                    "dcterms:title" "Example Release"}]
        (try
          (http/put "http://localhost:3400/data/new-series/release/release-1"
                    {:content-type :json
                     :body (json/write-str jsonld)})

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 422 status))
              (is (= "Series does not exist" body)))))))

    (put-series)

    (let [request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/new-series/"}]
                         "dcterms:title" "Example Release"}
          normalised-ednld {"@context"
                            ["https://publishmydata.com/def/datahost/context"
                             {"@base" "https://example.org/data/new-series/"}],
                            "dcterms:title" "Example Release",
                            "@type" "dh:Release"
                            "@id" "release-1",
                            "dcat:inSeries" "../new-series"}]

      (testing "Creating a release for a series that does exist works"
        (let [response (http/put "http://localhost:3400/data/new-series/release/release-1"
                                 {:content-type :json
                                  :body (json/write-str request-ednld)})]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (json/read-str (:body response))))))

      (testing "Fetching a release that does exist works"
        (let [response (http/get "http://localhost:3400/data/new-series/release/release-1 ")]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (json/read-str (:body response))))))

      (testing "A release can be updated, query params take precedence"
        (let [response (http/put "http://localhost:3400/data/new-series/release/release-1?title=A%20new%20title"
                                 {:content-type :json
                                  :body (json/write-str request-ednld)})]
          (is (= 200 (:status response)))
          (is (= "A new title" (-> response :body json/read-str (get "dcterms:title")))))))))

(deftest normalise-release-test
  (testing "invalid cases"
    (testing "missing :series-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {}
                                          {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {}
                                          {"@id" "release-1"}))))

    (testing "missing :release-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"}
                                          {}))))

    (testing "different base is invalid"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"}
                                          {"@context" ["https://publishmydata.com/def/datahost/context"
                                                       {"@base" "https://different.base/is/invalid/"}]}))))

    (testing "@id must be slugised in the doc"
      (is (thrown? clojure.lang.ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"
                                           :release-slug "2023"}
                                          {"@context" ["https://publishmydata.com/def/datahost/context",
                                                       {"@base" "https://example.org/data/"}],
                                           "@id" "https://example.org/data/my-dataset-series"}))
          "@id usage in the series document is (for now) restricted to be in slugised form only")))

  (testing "valid cases"
    (let [returned-value (sut/normalise-release "https://example.org/data/series-1"
                                                {:series-slug "my-dataset-series"
                                                 :release-slug "release-1"}
                                                {})]
      (testing "canonicalisation works"
        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/series-1"}]
                "@id" "release-1"
                "@type" "dh:Release"
                "dcat:inSeries" "../my-dataset-series"}
               returned-value)))

      (testing "canonicalisation is idempotent"
        (is (= returned-value (sut/normalise-release "https://example.org/data/series-1"
                                                     {:series-slug "my-dataset-series"
                                                      :release-slug "release-1"}
                                                     returned-value))))

      (testing "with title metadata"
        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/series-1"}]
                "@id" "release-1"
                "@type" "dh:Release"
                "dcterms:title" "My Release"
                "dcat:inSeries" "../my-dataset-series"}
               (sut/normalise-release "https://example.org/data/series-1"
                                      {:series-slug "my-dataset-series"
                                       :release-slug "release-1"}
                                      {"@context" ["https://publishmydata.com/def/datahost/context"
                                                    {"@base" "https://example.org/data/series-1"}]
                                        "dcterms:title" "My Release"})))))))
