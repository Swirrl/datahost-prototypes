(ns tpximpact.datahost.ldapi.models.series-test
  (:require
    [clojure.data :refer [diff]]
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]
    [clojure.test.check.generators :as gen]
    [tpximpact.datahost.ldapi.generators :as rgen]
    [tpximpact.datahost.ldapi.client.ring :as ring-client])
  (:import [java.util UUID]))

(defn- create-put-request [series-slug body]
  {:uri (str "/data/" series-slug)
   :request-method :put
   :headers {"content-type" "application/json"}
   :body (json/write-str body)})

(defn create-op [resource at]
  {:op :create
   :resource resource
   :at at})

(defn update-op [resource at]
  {:op :update
   :resource resource
   :at at})

(defn delete-op [resource at]
  {:op :delete
   :resource resource
   :at at})

(defn flatten-resource [resource]
  (loop [result (list resource)
         current resource]
    (if-let [p (:parent current)]
      (recur (conj result p) p)
      result)))

(defn find-parent [{:keys [parent type] :as resource} parent-type]
  (cond
    (= type parent-type) resource
    (some? parent) (recur parent parent-type)
    :else (throw (ex-info "Could not find parent with type" {:type parent-type}))))

(t/deftest creaty-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-25T14:05:21Z")
          series (gen/generate rgen/series-gen)
          op (create-op series start-time)
          series-doc (ring-client/submit-op client op)
          fetched-doc (ring-client/get-resource client series)
          expected (merge (select-keys series ["dcterms:title" "dcterms:description"])
                          {"dcterms:issued" (str start-time)
                           "dcterms:modified" (str start-time)})
          [missing _ _] (diff expected fetched-doc)]
      (t/is (= nil missing))
      (t/is (= series-doc fetched-doc)))))

(t/deftest mutaty-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-25T14:05:21Z")
          initial (gen/generate rgen/series-gen)
          updates (gen/generate (rgen/series-updates-gen initial))
          update-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count updates)))
          ops (concat [(create-op initial start-time)]
                      (mapv update-op updates update-times))
          last-update-time (last update-times)
          last-series (last updates)
          expected (merge (select-keys last-series ["dcterms:title" "dcterms:description"])
                          {"dcterms:issued" (str start-time)
                           "dcterms:modified" (str last-update-time)})]
      (doseq [op ops]
        (ring-client/submit-op client op))

      (let [series (ring-client/get-resource client initial)
            [missing _ _] (diff expected series)]
        (t/is (= nil missing))))))

(t/deftest create-deletey-test
  (with-open [client (ring-client/create-client)]
    (let [create-time (time/parse "2023-07-26T17:21:03Z")
          delete-time (gen/generate (rgen/tick-gen create-time))
          series (gen/generate rgen/series-gen)
          _series-doc (ring-client/submit-op client (create-op series create-time))]
      (ring-client/submit-op client (delete-op series delete-time))

      (let [fetched-doc (ring-client/get-resource client series)]
        (t/is (nil? fetched-doc))))))

(t/deftest create-release-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          release (gen/generate rgen/release-deps-gen)
          resources (flatten-resource release)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count resources)))
          create-ops (mapv create-op resources create-times)]
      (doseq [op create-ops]
        (ring-client/submit-op client op))

      ;; all resources should be created
      (doseq [resource resources]
        (let [fetched (ring-client/get-resource client resource)
              expected (into {} (remove (fn [[k _v]] (keyword? k)) fetched))
              [missing _ _] (diff expected fetched)]
          (t/is (= nil missing)))))))

(t/deftest create-release-delete-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          release (gen/generate rgen/release-deps-gen)
          resources (flatten-resource release)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count resources)))
          delete-time (gen/generate (rgen/tick-gen (last create-times)))
          create-ops (mapv create-op resources create-times)
          series (find-parent release :series)]
      (doseq [op create-ops]
        (ring-client/submit-op client op))

      (ring-client/submit-op client (delete-op series delete-time))

      ;; all resources should be deleted
      (doseq [resource resources]
        (let [fetched (ring-client/get-resource client resource)]
          (t/is (nil? fetched)))))))

(defn format-date-time
  [dt]
  (.format ^java.time.ZonedDateTime dt java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))

(t/deftest put-series-create-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t (time/parse "2023-06-29T10:11:07Z")
          clock (time/manual-clock t)
          handler (router/handler clock repo temp-store)
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
                "Should retrieve a series without optional dcterms:description"))))))

(t/deftest put-series-update-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t1 (time/parse "2023-06-30T11:36:18Z")
          t2 (time/parse "2023-06-30T14:25:33Z")
          clock (time/manual-clock t1)
          handler (router/handler clock repo temp-store)
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
      (t/is (= (str t2) (get updated-doc "dcterms:modified"))))))

(t/deftest put-series-no-changes-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t1 (time/parse "2023-06-30T13:37:00Z")
          t2 (time/parse "2023-06-30T15:08:03Z")
          clock (time/manual-clock t1)
          handler (router/handler clock repo temp-store)

          properties {"dcterms:title" "Title" "dcterms:description" "Description"}
          create-request (create-put-request "new-series" properties)
          create-response (handler create-request)
          initial-doc (json/read-str (:body create-response))

          _ (time/set-now clock t2)

          update-request (create-put-request "new-series" properties)
          update-response (handler update-request)
          updated-doc (json/read-str (:body update-response))]

      (t/is (= initial-doc updated-doc)))))

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

