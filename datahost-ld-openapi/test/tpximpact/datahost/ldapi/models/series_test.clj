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
    [com.gfredericks.test.chuck.generators :as cgen]
    [malli.core :as m]
    [malli.generator :as mg])
  (:import
    [java.lang AutoCloseable]
    [java.util UUID]
    [java.time Duration Instant LocalDateTime OffsetDateTime ZoneOffset ZonedDateTime]))

(defn- create-put-request [series-slug body]
  {:uri (str "/data/" series-slug)
   :request-method :put
   :headers {"content-type" "application/json"}
   :body (json/write-str body)})

(def title-gen gen/string-alphanumeric)
(def description-gen gen/string-alphanumeric)

(def series-slug-gen (cgen/string-from-regex #"\w(-?\w{0,5}){0,2}"))

(def series-gen
  (gen/hash-map :type (gen/return :series)
                :slug series-slug-gen
                "dcterms:title" title-gen
                "dcterms:description" description-gen))

(defn- update-key-gen [resource k value-gen]
  (let [existing-value (get resource k)
        new-value-gen (gen/such-that (fn [v] (not= v existing-value)) value-gen)]
    (gen/fmap (fn [new-value]
                (assoc resource k new-value))
              new-value-gen)))

(defn update-title-gen [resource]
  (update-key-gen resource "dcterms:title" title-gen))

(defn update-description-gen [resource]
  (update-key-gen resource "dcterms:description" description-gen))

(defn mutate-title-description-gen [resource]
  (let [mutators [update-title-gen
                  update-description-gen]]
    (gen/bind (gen/elements mutators) (fn [mf] (mf resource)))))

(defn- maybe-add-mutation-gen [versions mutator decision-gen]
  {:pre [(seq versions)]}
  (let [last-version (last versions)]
    (gen/bind decision-gen (fn [mutate?]
                             (if mutate?
                               (gen/bind (mutator last-version) (fn [version] (maybe-add-mutation-gen (conj versions version) mutator decision-gen)))
                               (gen/return versions))))))
(defn mutations-gen [initial mutator]
  (maybe-add-mutation-gen [initial] mutator gen/boolean))

(defn series-updates-gen [series]
  (mutations-gen series mutate-title-description-gen))

(defn- instant-gen
  ([]
   (let [local-now (ZonedDateTime/now)
         min-date (.minusWeeks local-now 1)
         max-date (.plusWeeks local-now 1)]
     (instant-gen (Instant/from min-date) (Instant/from max-date))))
  ([^Instant min  ^Instant max]
   (gen/fmap (fn [secs] (Instant/ofEpochSecond secs)) (gen/choose (.getEpochSecond min) (.getEpochSecond max)))))

(defn- instant->utc-offset-datetime [^Instant i]
  (OffsetDateTime/ofInstant i ZoneOffset/UTC))

(defn- datetime-gen
  ([] (gen/fmap instant->utc-offset-datetime (instant-gen)))
  ([^OffsetDateTime min ^OffsetDateTime max]
   (gen/fmap instant->utc-offset-datetime (instant-gen (.toInstant min) (.toInstant max)))))

(defn- duration-gen
  ([] (duration-gen 0 (.getSeconds (Duration/ofDays 1))))
  ([min-seconds max-seconds]
   (gen/fmap (fn [secs] (Duration/ofSeconds secs)) (gen/choose min-seconds max-seconds))))

(defn- tick-gen [^OffsetDateTime current]
  (gen/fmap (fn [^Duration d] (.plus current d)) (duration-gen)))

(defn- n-mutations-gen [initial mutator n]
  (letfn [(gen-remaining [versions n]
            (if (pos? n)
              (gen/bind (mutator (last versions)) (fn [next]
                                                    (gen-remaining (conj versions next) (dec n))))
              (gen/return versions)))]
    (gen/bind (mutator initial) (fn [first] (gen-remaining [first] (dec n))))))

(defn create-op [resource at]
  {:op :create
   :resource resource
   :at at})

(defn update-op [resource at]
  {:op :update
   :resource resource
   :at at})

(defrecord RingClient [handler clock store]
  AutoCloseable
  (close [_this]
    (.close store)))

(defn submit-op [{:keys [handler clock] :as client} {:keys [op resource at]}]
  (time/set-now clock at)
  (let [request (case op
                  :delete {:request-method :delete
                           :uri (str "/data/" (:slug resource))}
                  (let [body (into {} (remove (fn [[k v]] (keyword? k)) resource))]
                    (create-put-request (:slug resource) body)))
        {:keys [status body] :as response} (handler request)]
    (cond
      (>= status 400)
      (throw (ex-info "Request failed" {:request request :response response}))

      (= :delete op)
      nil

      :else
      (json/read-str body))))

(defn delete-op [resource at]
  {:op :delete
   :resource resource
   :at at})

(defn get-series [{:keys [handler]} series-slug]
  (let [request {:request-method :get
                 :uri (str "/data/" series-slug)
                 :headers {"accept" "application/json"}}
        {:keys [status body] :as reponse} (handler request)]
    (when (= 200 status)
      (json/read-str body))))

(defn- create-client []
  (let [store (tfstore/create-temp-file-store)
        repo (repo/sail-repo)
        t (time/parse "2023-06-29T10:11:07Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo store)]
    (->RingClient handler clock store)))

(t/deftest creaty-test
  (with-open [client (create-client)]
    (let [start-time (time/parse "2023-07-25T14:05:21Z")
          series (gen/generate series-gen)
          op (create-op series start-time)
          series-doc (submit-op client op)
          fetched-doc (get-series client (:slug series))
          expected (merge (select-keys series ["dcterms:title" "dcterms:description"])
                          {"dcterms:issued" (str start-time)
                           "dcterms:modified" (str start-time)})
          [missing _ _] (diff expected fetched-doc)]
      (t/is (= nil missing))
      (t/is (= series-doc fetched-doc)))))

(t/deftest mutaty-test
  (with-open [client (create-client)]
    (let [start-time (time/parse "2023-07-25T14:05:21Z")
          initial (gen/generate series-gen)
          updates (gen/generate (series-updates-gen initial))
          update-times (gen/generate (n-mutations-gen start-time tick-gen (count updates)))
          ops (concat [(create-op initial start-time)]
                      (mapv update-op updates update-times))
          last-update-time (last update-times)
          last-series (last updates)
          expected (merge (select-keys last-series ["dcterms:title" "dcterms:description"])
                          {"dcterms:issued" (str start-time)
                           "dcterms:modified" (str last-update-time)})]
      (doseq [op ops]
        (submit-op client op))

      (let [series (get-series client (:slug initial))
            [missing _ _] (diff expected series)]
        (t/is (= nil missing))))))

(t/deftest create-deletey-test
  (with-open [client (create-client)]
    (let [create-time (time/parse "2023-07-26T17:21:03Z")
          delete-time (gen/generate (tick-gen create-time))
          series (gen/generate series-gen)
          _series-doc (submit-op client (create-op series create-time))]
      (submit-op client (delete-op series delete-time))

      (let [fetched-doc (get-series client (:slug series))]
        (t/is (nil? fetched-doc))))))

{:op :create
 :resource {:type :series
            :slug "foo-bar"
            "dcterms:title" "title"
            "dcterms:description" "desc"}
 :submitted-at nil}

{:op :update
 :previous {}
 :resource {}
 :submitted-at nil}

{:op :delete
 :resource {}
 :submitted-at nil}

(def Series
  [:map
   [:slug [:re nil]]
   ["dcterms:title" [:string {:min 1 :max 200}]]
   ["dcterms:description" [:string {:min 1 :max 200}]]])

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

