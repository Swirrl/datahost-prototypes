(ns tpximpact.datahost.ldapi.models.series-test
  (:require
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th])
  (:import
    [java.util UUID]))

(defn format-date-time
  [dt]
  (.format ^java.time.ZonedDateTime dt java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))

(defn- create-put-request [series-slug body]
  {:uri (str "/data/" series-slug)
   :request-method :put
   :headers {"content-type" "application/json"}
   :body (json/write-str body)})

(t/deftest put-series-create-test
  (let [repo (repo/sail-repo)
        t (time/parse "2023-06-29T10:11:07Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo)
        request (create-put-request "new-series" {"dcterms:title" "A title"
                                                  "dcterms:description" "Description"})
        {:keys [status body]} (handler request)
        new-series-doc (json/read-str body)]
    (t/is (= 201 status))
    (t/is (= "A title" (get new-series-doc "dcterms:title")))
    (t/is (= "Description" (get new-series-doc "dcterms:description")))
    (t/is (= (str t) (get new-series-doc "dcterms:modified")))
    (t/is (= (str t) (get new-series-doc "dcterms:issued")))
    (t/is (= (get new-series-doc "dh:baseEntity") "https://example.org/data/new-series"))

    ;; fetch created series
    (let [request {:uri "/data/new-series"
                   :request-method :get}
          {:keys [status] :as response} (handler request)
          series-doc (json/read-str (:body response))]
      (t/is (= 200 status))
      (t/is (= new-series-doc series-doc)))

    (let [series2-slug "new-series-without-description"
          request (create-put-request series2-slug {"dcterms:title" "Another title"})
          {:keys [status]} (handler request)]
      (t/is (= 201 status)
            "Should create series without optional dcterms:description")

      (let [request {:uri (str "/data/" series2-slug)
                     :request-method :get}
            {:keys [status]} (handler request)]
        (t/is (= 200 status)
              "Should retrieve a series without optional dcterms:description")))))

(t/deftest put-series-update-test
  (let [repo (repo/sail-repo)
        t1 (time/parse "2023-06-30T11:36:18Z")
        t2 (time/parse "2023-06-30T14:25:33Z")
        clock (time/manual-clock t1)
        handler (router/handler clock repo)
        create-request (create-put-request "new-series" {"dcterms:title" "Initial Title"
                                                         "dcterms:description" "Initial Description"})
        _initial-response (handler create-request)

        _ (time/set-now clock t2)
        update-request (create-put-request "new-series" {"dcterms:title" "Updated Title"
                                                         "dcterms:description" "Updated Description"})
        {:keys [status body] :as update-response} (handler update-request)
        updated-doc (json/read-str body)]
    (t/is (= 200 status))
    (t/is (= "Updated Title" (get updated-doc "dcterms:title")))
    (t/is (= "Updated Description" (get updated-doc "dcterms:description")))
    (t/is (= (str t1) (get updated-doc "dcterms:issued")))
    (t/is (= (str t2) (get updated-doc "dcterms:modified")))))

(t/deftest put-series-no-changes-test
  (let [repo (repo/sail-repo)
        t1 (time/parse "2023-06-30T13:37:00Z")
        t2 (time/parse "2023-06-30T15:08:03Z")
        clock (time/manual-clock t1)
        handler (router/handler clock repo)

        properties {"dcterms:title" "Title" "dcterms:description" "Description"}
        create-request (create-put-request "new-series" properties)
        create-response (handler create-request)
        initial-doc (json/read-str (:body create-response))

        _ (time/set-now clock t2)

        update-request (create-put-request "new-series" properties)
        update-response (handler update-request)
        updated-doc (json/read-str (:body update-response))]

    (t/is (= initial-doc updated-doc))))

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
          request-ednld {"dcterms:title" "A title"
                         "dcterms:description" "foobar"}
          normalised-ednld {"@type" "dh:DatasetSeries"
                            "dcterms:description" "foobar"
                            "@id" new-series-id
                            "dh:baseEntity" (str "https://example.org" new-series-path)
                            "dcterms:title" "A title"}]

      (testing "A series can be created"
        (let [response (PUT new-series-path
                            {:content-type :json
                             :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "@context" "dcterms:issued" "dcterms:modified")))
          (is (= (get resp-body "dcterms:issued")
                 (get resp-body "dcterms:modified")))))

      (testing "A series can be retrieved via the API"
        (let [response (GET new-series-path)
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "@context" "dcterms:issued" "dcterms:modified")))))

      (testing "A series can be updated via the API, query params take precedence"
        (let [response (PUT (str new-series-path "?title=A%20new%20title")
                            {:content-type :json
                                  :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (println response)
          (is (= 200 (:status response)))
          (is (not= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified"))))

        (let [response (GET new-series-path)
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (not= (get resp-body "dcterms:issued")
                    (get resp-body "dcterms:modified")))
          (is (= "foobar" (get resp-body "dcterms:description"))
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

