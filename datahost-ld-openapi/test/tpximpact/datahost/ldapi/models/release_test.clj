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
            (is (= status 404))
            (is (= body "Not found"))))))

    (testing "Fetching a release that does not exist returns 'not found'"
      (try
        (http/get "http://localhost:3400/data/new-series/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= status 404))
            (is (= body "Not found"))))))

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
              (is (= status 422))
              (is (= body "Series does not exist")))))))

    (put-series)

    (testing "Creating a release for a series that does exist works"
      (let [jsonld {"@context"
                    ["https://publishmydata.com/def/datahost/context"
                     {"@base" "http://example.org/data/"}]
                    "dcterms:title" "Example Release"}
            response (http/put "http://localhost:3400/data/new-series/release/release-1"
                               {:content-type :json
                                :body (json/write-str jsonld)})]
        (is (= (:status response) 201))
        (is (= (json/read-str (:body response)) ))
        ))

    (testing "Fetching a release that does exist works")

    (testing "A release can be updated"))

;; (th/with-system-and-clean-up sys
  ;;   (testing "A series that does not exist returns 'not found'"
  ;;     (try
  ;;       (http/get "http://localhost:3400/data/does-not-exist")

  ;;       (catch Throwable ex
  ;;         (let [{:keys [status body]} (ex-data ex)]
  ;;           (is (= status 404))
  ;;           (is (= body "Not found"))))))

  ;;   (let [incoming-jsonld-doc {"@context"
  ;;                              ["https://publishmydata.com/def/datahost/context"
  ;;                               {"@base" "https://example.org/data/"}],
  ;;                              "dcterms:title" "A title"}
  ;;         augmented-jsonld-doc (sut/normalise-series {:series-slug "new-series"}
  ;;                                                    incoming-jsonld-doc)]
  ;;     (testing "A series can be created and retrieved via the API"

  ;;       (let [response (http/put
  ;;                       "http://localhost:3400/data/new-series"
  ;;                       {:content-type :json
  ;;                        :body (json/write-str incoming-jsonld-doc)})]
  ;;         (is (= (:status response) 201))
  ;;         (is (= (json/read-str (:body response)) augmented-jsonld-doc)))

  ;;       (let [response (http/get "http://localhost:3400/data/new-series")]
  ;;         (is (= (:status response) 200))
  ;;         (is (= (json/read-str (:body response)) augmented-jsonld-doc))))

  ;;     (testing "A series can be updated via the API"
  ;;       (let [response (http/put "http://localhost:3400/data/new-series?title=A%20new%20title")]
  ;;         (is (= (:status response) 200)))

  ;;       (let [response (http/get "http://localhost:3400/data/new-series")]
  ;;         (is (= (:status response) 200))
  ;;         (is (= (-> response :body json/read-str (get "dcterms:title")) "A new title"))))))
  )

(deftest normalise-release-test
  (testing "invalid cases"
    (testing "missing :series-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release {}
                                          {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (sut/normalise-release {}
                                          {"@id" "my-dataset-series"}))))

    (testing "missing :release-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release {:series-slug "my-dataset-series"}
                                          {"@context" ["https://publishmydata.com/def/datahost/context"
                                                       {"@base" "https://different.base/is/invalid/"}]}))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (sut/normalise-release {:series-slug "my-dataset-series"
                                         :release-slug "2023"}
                                        {"@context" ["https://publishmydata.com/def/datahost/context",
                                                     {"@base" "https://example.org/data/"}],
                                         "@id" "https://example.org/data/my-dataset-series"}))
        "@id usage in the series document is (for now) restricted to be in slugised form only"))

  (testing "valid cases"
    (testing "canonicalisation works")
    (testing "canonicalisation is idempotent")

    (testing "with title metadata")
    (testing "with description metadata")))
