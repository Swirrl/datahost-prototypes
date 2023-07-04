(ns tpximpact.datahost.ldapi.models.release-test
  (:require
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]))

(defn- put-series [put-fn]
  (let [jsonld {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}]
                "dcterms:title" "A title" "dcterms:description" "Description"}]
    (put-fn "/data/new-series"
            {:content-type :json
             :body (json/write-str jsonld)})))

(defn- create-series [handler]
  (let [request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri "/data/new-series"
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        response (handler request)
        series-doc (json/read-str (:body response))]
    (get series-doc "@id")))

(defn- create-put-request [series-slug release-slug properties]
  {:uri (format "/data/%s/release/%s" series-slug release-slug)
   :request-method :put
   :headers {"content-type" "application/json"}
   :body (json/write-str properties)})

(defn- temp-repo []
  (repo/sparql-repo "http://localhost:5820/test/query" "http://localhost:5820/test/update"))

(t/deftest put-release-create-test
  (let [repo (repo/sail-repo)
        t (time/parse "2023-07-03T11:16:16Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo (atom {}))
        series-slug (create-series handler)

        request-json {"dcterms:title" "Release title" "dcterms:description" "Description"}
        release-request (create-put-request series-slug "test-release" request-json)
        {:keys [body status] :as response} (handler release-request)
        release-doc (json/read-str body)]
    (t/is (= status 201))

    (t/is (= "Release title" (get release-doc "dcterms:title")))
    (t/is (= "Description" (get release-doc "dcterms:description")))
    (t/is (= (str t) (get release-doc "dcterms:issued")))
    (t/is (= (str t) (get release-doc "dcterms:modified")))
    (t/is (= (format "https://example.org/data/%s" series-slug) (get release-doc "dcat:inSeries")))))

(t/deftest put-release-update-test
  (let [repo (repo/sail-repo)
        t1 (time/parse "2023-07-03T14:35:55Z")
        t2 (time/parse "2023-07-03T16:02:34Z")
        clock (time/manual-clock t1)
        handler (router/handler clock repo (atom {}))

        series-slug (create-series handler)
        create-request (create-put-request series-slug "test-release" {"dcterms:title" "Initial title" "dcterms:description" "Initial description"})
        _create-response (handler create-request)

        _ (time/set-now clock t2)

        update-request {:uri (format "/data/%s/release/test-release" series-slug)
                        :request-method :put
                        :headers {"content-type" "application/json"}
                        :body (json/write-str {"dcterms:title" "Updated title" "dcterms:description" "Updated description"})}
        {:keys [status body] :as update-response} (handler update-request)
        updated-doc (json/read-str body)]
    (t/is (= 200 status))
    (t/is (= "Updated title" (get updated-doc "dcterms:title")))
    (t/is (= "Updated description" (get updated-doc "dcterms:description")))
    (t/is (= (str t1) (get updated-doc "dcterms:issued")))
    (t/is (= (str t2) (get updated-doc "dcterms:modified")))))

(t/deftest put-release-no-changes-test
  (let [repo (repo/sail-repo)
        t1 (time/parse "2023-07-04T08:54:11Z")
        t2 (time/parse "2023-07-04T10:33:24Z")
        clock (time/manual-clock t1)
        handler (router/handler clock repo (atom {}))

        series-slug (create-series handler)

        properties {"dcterms:title" "Title" "dcterms:description" "Description"}
        create-request (create-put-request series-slug "new-series" properties)
        create-response (handler create-request)
        initial-doc (json/read-str (:body create-response))

        _ (time/set-now clock t2)

        update-request (create-put-request series-slug "new-series" properties)
        update-response (handler update-request)
        updated-doc (json/read-str (:body update-response))]
    (t/is (= initial-doc updated-doc))))

(deftest round-tripping-release-test
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client
                                :as sys}
    (testing "Fetching a release for a series that does not exist returns 'not found'"
      (try
        
        (GET "/data/does-not-exist/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Fetching a release that does not exist returns 'not found'"
      (try
        (GET "/data/new-series/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Creating a release for a series that is not found fails gracefully"
      (let [jsonld {"@context"
                    ["https://publishmydata.com/debf/datahost/context"
                     {"@base" "http://example.org/data/"}]
                    "dcterms:title" "Example Release"
                    "dcterms:description" "Description"}]
        (try
          (PUT "/data/new-series/release/release-1"
               {:content-type :json
                :body (json/write-str jsonld)})

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 422 status))
              (is (= "Series for this release does not exist" body)))))))

    (put-series PUT)

    (let [request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/new-series/"}]
                         "dcterms:title" "Example Release"
                         "dcterms:description" "Description"}
          normalised-ednld {"dcterms:title" "Example Release",
                            "dcterms:description" "Description"
                            "@type" "dh:Release"}]

      (testing "Creating a release for a series that does exist works"
        (let [response (PUT "/data/new-series/release/release-1"
                            {:content-type :json
                             :body (json/write-str request-ednld)})
              body (json/read-str (:body response))]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (select-keys body (keys normalised-ednld))))
          (is (= (get body "dcterms:issued")
                 (get body "dcterms:modified")))))

      (testing "Fetching a release that does exist works"
        (let [response (GET "/data/new-series/release/release-1")
              body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (select-keys body (keys normalised-ednld))))))

      (testing "A release can be updated, query params take precedence"
        (let [{body-str-before :body} (GET "/data/new-series/release/release-1")
              {:keys [body] :as response} (PUT (str "/data/new-series"
                                                    "/release/release-1?title=A%20new%20title")
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
            (let [response (PUT (str "/data/new-series"
                                     "/release/release-1?title=A%20new%20title")
                                {:content-type :json
                                 :body nil})
                  body' (-> response :body json/read-str)]
              (is (= 200 (:status response)))
              (is (= "A new title" (get body' "dcterms:title")))
              (is (= (select-keys body ["dcterms:issued" "dcterms:modified"])
                     (select-keys body' ["dcterms:issued" "dcterms:modified"]))
                  "The document shouldn't be modified"))))))))
