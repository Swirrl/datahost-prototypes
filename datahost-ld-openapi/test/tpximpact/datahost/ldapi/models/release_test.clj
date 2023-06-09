(ns tpximpact.datahost.ldapi.models.release-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing]]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.models.release :as sut])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID)))


(deftest round-tripping-release-test
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client
                                :as sys}

    (let [new-series-id (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-id)]
      (testing "Fetching a release for a series that does not exist returns 'not found'"
        (try

          (GET (str new-series-path "/releases/release-1"))

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 404 status))
              (is (= "Not found" body))))))

      (testing "Fetching a release that does not exist returns 'not found'"
        (try
          (GET (str new-series-path "/releases/release-1"))

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 404 status))
              (is (= "Not found" body))))))

      (testing "Creating a release for a series that is not found fails gracefully"
        (let [jsonld {"@context"
                      ["https://publishmydata.com/debf/datahost/context"
                       {"@base" "http://example.org/data/"}]
                      "dcterms:title" "Example Release"}]
          (try
            (PUT (str new-series-path "/releases/release-1")
                 {:content-type :json
                  :body (json/write-str jsonld)})

            (catch Throwable ex
              (let [{:keys [status body]} (ex-data ex)]
                (is (= 422 status))
                (is (= "Series for this release does not exist" body)))))))

      (let [jsonld {"@context"
                    ["https://publishmydata.com/def/datahost/context"
                     {"@base" "https://example.org/data/"}]
                    "dcterms:title" "A title"}]
        (PUT new-series-path
                {:content-type :json
                 :body (json/write-str jsonld)}))

      (let [request-ednld {"@context"
                           ["https://publishmydata.com/def/datahost/context"
                            {"@base" (str "https://example.org" new-series-path "/")}]
                           "dcterms:title" "Example Release"}
            normalised-ednld {"@context"
                              ["https://publishmydata.com/def/datahost/context"
                               {"@base" (str "https://example.org" new-series-path "/")}],
                              "dcterms:title" "Example Release",
                              "@type" "dh:Release"
                              "@id" "release-1",
                              "dcat:inSeries" new-series-path}]

        (testing "Creating a release for a series that does exist works"
          (let [response (PUT (str new-series-path "/releases/release-1")
                              {:content-type :json
                               :body (json/write-str request-ednld)})
                body (json/read-str (:body response))]
            (is (= 201 (:status response)))
            (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))
            (is (= (get body "dcterms:issued")
                   (get body "dcterms:modified")))))

        (testing "Fetching a release that does exist works"
          (let [response (GET (str new-series-path "/releases/release-1"))
                body (json/read-str (:body response))]
            (is (= 200 (:status response)))
            (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))))

        (testing "A release can be updated, query params take precedence"
          (let [{body-str-before :body} (GET (str new-series-path "/releases/release-1"))
                {:keys [body] :as response} (PUT (str new-series-path
                                                      "/releases/release-1?title=A%20new%20title")
                                                 {:content-type :json
                                                  :body (json/write-str request-ednld)})
                body-before (json/read-str body-str-before)
                body (json/read-str body)]
            (is (= 200 (:status response)))
            (is (= "A new title" (get body "dcterms:title")))
            (is (= (get body-before "dcterms:issued")
                   (get body "dcterms:issued")))
            (is (not= (get body "dcterms:modified")
                      (get body-before "dcterms:modified")))

            (testing "No update when query params same as in existing doc"
              (let [response (PUT (str new-series-path
                                       "/releases/release-1?title=A%20new%20title")
                                  {:content-type :json
                                   :body nil})
                    body' (-> response :body json/read-str)]
                (is (= 200 (:status response)))
                (is (= "A new title" (get body' "dcterms:title")))
                (is (= (select-keys body ["dcterms:issued" "dcterms:modified"])
                       (select-keys body' ["dcterms:issued" "dcterms:modified"]))
                    "The document shouldn't be modified")))))))))

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
                "dcat:inSeries" "/data/my-dataset-series"}
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
                "dcat:inSeries" "/data/my-dataset-series"}
               (sut/normalise-release "https://example.org/data/series-1"
                                      {:series-slug "my-dataset-series"
                                       :release-slug "release-1"}
                                      {"@context" ["https://publishmydata.com/def/datahost/context"
                                                    {"@base" "https://example.org/data/series-1"}]
                                        "dcterms:title" "My Release"})))))))
