(ns tpximpact.datahost.ldapi.models.series-test
  (:require
    [clojure.data :refer [diff]]
    [clojure.data.json :as json]
    [clojure.pprint :as pprint]
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

(t/deftest create-schema-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          release (gen/generate rgen/schema-deps-gen)
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
          (t/is (empty? missing)))))))

(t/deftest create-delete-schema-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          schema (gen/generate rgen/schema-deps-gen)
          resources (flatten-resource schema)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count resources)))
          delete-time (gen/generate (rgen/tick-gen (last create-times)))
          create-ops (mapv create-op resources create-times)
          series (find-parent schema :series)]
      (doseq [op create-ops]
        (ring-client/submit-op client op))

      (ring-client/submit-op client (delete-op series delete-time))

      ;; all resources should be deleted
      (doseq [resource resources]
        (let [fetched (ring-client/get-resource client resource)]
          (t/is (nil? fetched)))))))

(defn create-resources [client resources create-times]
  (mapv (fn [resource create-time]
          (println resource)
          (ring-client/submit-op client (create-op resource create-time)))
        resources
        create-times))
(t/deftest create-revision-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          revision (gen/generate rgen/revision-deps-gen)
          resources (flatten-resource revision)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count resources)))
          resources (create-resources client resources create-times)]

      ;; all resources should be created
      (doseq [resource resources]
        (let [fetched (ring-client/get-resource client resource)
              expected (into {} (remove (fn [[k _v]] (keyword? k)) fetched))
              [missing _ _] (diff expected fetched)]
          (t/is (empty? missing)))))))

(t/deftest create-delete-revision-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          revision (gen/generate rgen/revision-deps-gen)
          release (find-parent revision :release)
          resources (flatten-resource revision)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (count resources)))
          delete-time (gen/generate (rgen/tick-gen (last create-times)))
          series (find-parent revision :series)
          resources (create-resources client resources create-times)]

      (ring-client/submit-op client (delete-op series delete-time))

      ;; all resources should be deleted
      (doseq [resource resources]
        (let [fetched (ring-client/get-resource client resource)]
          (t/is (nil? fetched)))))))

(defn- get-children [resource]
  (case (:type resource)
    :series (:releases resource)
    :release (let [{:keys [schema revisions]} resource]
               (if (some? schema)
                 (cons schema revisions)
                 revisions))
    :revision (:changes resource)
    nil))

(defn- get-descendent-count [resource]
  (let [children (get-children resource)
        child-counts (map get-descendent-count children)]
    (inc (apply + child-counts))))

(defn- big-flatten-resources [root]
  (tree-seq (fn [resource]
              (contains? #{:series :release :revision} (:type resource)))
            get-children
            root))

(defn- split-all [splits coll]
  (let [result (reduce (fn [{:keys [remaining acc]} n]
                         (let [[x rest] (split-at n remaining)]
                           {:remaining rest :acc (conj acc x)}))
                       {:remaining coll :acc (vector)}
                       splits)]
    (:acc result)))

(defmulti create-resource (fn [_client resource _create-times] (:type resource)))
(defmethod create-resource :series [client {:keys [releases] :as series} create-times]
  (let [[[create-series] rest] (split-at 1 create-times)
        created-series (ring-client/submit-op client (create-op series create-series))
        subtree-sizes (map get-descendent-count releases)
        subtree-create-times (split-all subtree-sizes rest)
        created-releases (mapv (fn [release create-times]
                                 (let [with-parent (rgen/set-parent release created-series)]
                                   (create-resource client with-parent create-times)))
                               releases
                               subtree-create-times)]
    (assoc created-series :releases created-releases)))

(defmethod create-resource :release [client {:keys [schema revisions] :as release} create-times]
  (let [[[create-release-time] create-children-times] (split-at 1 create-times)
        created-release (ring-client/submit-op client (create-op release create-release-time))
        [created-schema create-revision-times] (if (nil? schema)
                                                 [nil create-children-times]
                                                 (let [with-parent (rgen/set-parent schema created-release)
                                                       [[create-schema-time] rest] (split-at 1 create-children-times)
                                                       created-schema (ring-client/submit-op client (create-op with-parent create-schema-time))]
                                                   [created-schema rest]))
        subtree-sizes (map get-descendent-count revisions)
        subtree-create-times (split-all subtree-sizes create-revision-times)
        created-revisions (mapv (fn [revision create-times]
                                  (let [with-parent (rgen/set-parent revision created-release)]
                                    (create-resource client with-parent create-times)))
                                revisions
                                subtree-create-times)]
    (assoc created-release :schema created-schema :revisions created-revisions)))

(defmethod create-resource :schema [client schema create-times]
  (let [create-time (first create-times)]
    (ring-client/submit-op client (create-op schema create-time))))

(defmethod create-resource :revision [client {:keys [changes] :as revision} create-times]
  (let [[[create-revision-time] create-change-times] (split-at 1 create-times)
        created-revision (ring-client/submit-op client (create-op revision create-revision-time))
        created-changes (mapv (fn [change create-time]
                                (let [with-parent (rgen/set-parent change created-revision)]
                                  (create-resource client with-parent [create-time])))
                              changes
                              create-change-times)]
    (assoc created-revision :changes created-changes)))

(defmethod create-resource :change [client change create-times]
  (let [create-time (first create-times)]
    (ring-client/submit-op client (create-op change create-time))))

(defmethod create-resource :default [_client resource _create-times]
  #_(pprint/pprint resource)
  (throw (ex-info "Unsupported resource type" resource)))

(t/deftest create-changes-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          series (gen/generate rgen/big-series-gen)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (get-descendent-count series)))
          series (create-resource client series create-times)]

      (println "Created" (get-descendent-count series) "resources")

      ;; all resources should be created
      (doseq [resource (big-flatten-resources series)]
        (let [fetched (ring-client/get-resource client resource)
              expected (ring-client/resource->doc resource)
              [missing _ _] (diff expected fetched)]
          (when (seq missing)
            (println "Expected:")
            (pprint/pprint expected)
            (println "Actual:")
            (pprint/pprint fetched)
            (println "Missing:")
            (pprint/pprint missing))
          (t/is (= nil (seq missing))))))))

(t/deftest create-delete-change-test
  (with-open [client (ring-client/create-client)]
    (let [start-time (time/parse "2023-07-27T18:02:32Z")
          series (gen/generate rgen/big-series-gen)
          create-times (gen/generate (rgen/n-mutations-gen start-time rgen/tick-gen (get-descendent-count series)))
          delete-time (gen/generate (rgen/tick-gen (last create-times)))
          series (create-resource client series create-times)]

      (println "Created" (get-descendent-count series) "resources")
      (ring-client/submit-op client (delete-op series delete-time))

      ;; all resources should be deleted
      (doseq [resource (big-flatten-resources series)]
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

