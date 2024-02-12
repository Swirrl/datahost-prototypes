(ns tpximpact.datahost.ldapi.models.series-test
  (:require
    [clojure.data :as c.data :refer [diff]]
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing] :as t]
    [com.yetanalytics.flint :as fl]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.system-uris :as su]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.client.ring :as ring-client]
    [tpximpact.datahost.ldapi.models.resources :as resources])
  (:import
   (java.net URI)
   (java.time ZonedDateTime Instant)
   (java.time.format DateTimeFormatter)
   [java.util UUID]))

(defn format-date-time
  [dt]
  (.format ^ZonedDateTime dt DateTimeFormatter/ISO_OFFSET_DATE_TIME))

(defn- create-put-request [series-slug body]
  {:uri (str "/data/" series-slug)
   :request-method :put
   :headers {"accept" "application/ld+json"
             "content-type" "application/json"}
   :body (json/write-str body)})

(t/deftest get-data-root-test
  (th/with-system-and-clean-up {{:keys [GET]} :tpximpact.datahost.ldapi.test/http-client :as sys}
    (let [{:keys [headers body] :as _response}
          (GET "/data" {:headers {"accept" "application/ld+json"}})]
      (is (= "application/ld+json" (get headers "content-type")))
      (is (string? body)))))

(t/deftest put-series-create-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t (time/parse "2023-06-29T10:11:07Z")
          clock (time/manual-clock t)
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))
          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})
          request (create-put-request "new-series" {"dcterms:title" "A title"
                                                    "dcterms:description" "Description"
                                                    "rdfs:comment" "Comment"
                                                    "dcterms:publisher" "http://publisher-uri"
                                                    "dcat:theme" "http://theme-uri"
                                                    "dcterms:license" "http://uri-of-a-licence.org"
                                                    "dcat:keywords" #{"keyword1" "keyword2"}
                                                    "dh:nextUpdate" "2024-06-30"
                                                    "dh:relatedLinks" #{"http://related-1" "http://related-2"}
                                                    "dh:contactName" "Rob Chambers"
                                                    "dh:contactEmail" "heyrob@example.com"
                                                    "dh:contactPhone" "123234234234"})
          {:keys [status body]} (handler request)
          new-series-doc (json/read-str body)]
      (t/is (= 201 status))
      (t/is (= "A title" (get new-series-doc "dcterms:title")))
      (t/is (= "Description" (get new-series-doc "dcterms:description")))
      (t/is (= (str t) (get new-series-doc "dcterms:modified")))
      (t/is (= (str t) (get new-series-doc "dcterms:issued")))
      (t/is (= (get new-series-doc "dh:baseEntity") "https://example.org/data/new-series"))

      (t/is (= "Comment" (get new-series-doc "rdfs:comment")))
      (t/is (= "http://publisher-uri" (get new-series-doc "dcterms:publisher")))
      (t/is (= "http://theme-uri" (get new-series-doc "dcat:theme")))
      (t/is (= "http://uri-of-a-licence.org" (get new-series-doc "dcterms:license")))
      (t/is (= #{"keyword1"  "keyword2"} (set (get new-series-doc "dcat:keywords"))))
      (t/is (= "2024-06-30" (get new-series-doc "dh:nextUpdate")))
      (t/is (= ["http://related-1"
                "http://related-2"] (get new-series-doc "dh:relatedLinks")))
      (t/is (= "Rob Chambers" (get new-series-doc "dh:contactName")))
      (t/is (= "heyrob@example.com" (get new-series-doc "dh:contactEmail")))
      (t/is (= "123234234234" (get new-series-doc "dh:contactPhone")))

      ;; fetch created series
      (let [request {:uri "/data/new-series"
                     :request-method :get}
            {:keys [status] :as response} (handler request)
            series-doc (json/read-str (:body response))]
        (t/is (= 200 status))
        (t/is (= (dissoc new-series-doc "@context") (dissoc series-doc "@context"))))

      (let [series2-slug "new-series-without-description"
            request (create-put-request series2-slug
                                        {"dcterms:title" "Another title"
                                         "dcterms:description" "Foo"})
            {:keys [status]} (handler request)]
        (t/is (= 201 status)
              "Should create series without optional dcterms:description")

        (let [request {:uri (str "/data/" series2-slug)
                       :headers {"accept" "application/ld+json"}
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
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))
          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})
          create-request (create-put-request "new-series" {"dcterms:title" "Initial Title"
                                                           "dcterms:description" "Initial Description"})
          _initial-response (handler create-request)

          _ (time/set-now clock t2)
          update-request (create-put-request "new-series" {"dcterms:title" "Updated Title"
                                                           "dcterms:description" "Updated Description"})
          {:keys [status body] :as _update-response} (handler update-request)
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
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))

          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})

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
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client :as sys}
    (testing "A series that does not exist returns 'not found'"
      (let [{:keys [status body]} (GET "/data/does-not-exist")]
        (is (= 404 status))
        (is (= "Not found" body))))

    (let [rdf-base-uri (th/sys->rdf-base-uri sys)
          new-series-id (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-id)
          request-ednld {"dcterms:title" "A title"
                         "dcterms:description" "foobar"}
          normalised-ednld {"@type" "dh:DatasetSeries"
                            "dcterms:description" "foobar"
                            "@id" new-series-id
                            "dh:baseEntity" (str rdf-base-uri new-series-id)
                            "dcterms:title" "A title"}]

      (testing "A series can be created"
        (let [response (PUT new-series-path
                            {:content-type :json
                             :headers {"accept" "application/ld+json"}
                             :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "@context" "dcterms:issued" "dcterms:modified")))
          (is (= (get resp-body "dcterms:issued")
                 (get resp-body "dcterms:modified")))))

      (testing "A series can be retrieved via the API"
        (let [response (GET new-series-path
                            {:headers {"Accept" "application/ld+json"}})
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (dissoc resp-body "@context" "dcterms:issued" "dcterms:modified")))))

      (testing "Multiple series can be retrieved"
        (let [new-series2-id (str "another-new-series-" (UUID/randomUUID))
              new-series-path (str "/data/" new-series2-id)
              _response2 (PUT new-series-path
                             {:content-type :json
                              :body (json/write-str {"dcterms:title" "A second title"
                                                     "dcterms:description" "Description 2"})})
              {:keys [body status]} (GET "/data")
              series-doc (json/read-str body)
              series-coll (get series-doc "contents")
              [missing1 _extra1 _matching1] (->> (first series-coll)
                                                 (c.data/diff {"dcterms:title" "A second title"}))
              [missing2 _extra2 _matching2] (->> (second series-coll)
                                                 (c.data/diff {"dcterms:title" "A title"}))]
          (t/is (= status 200))
          (t/is (= nil missing1))
          (t/is (= nil missing2))))

      (testing "A series can be updated via the API, query params take precedence"
        (let [response (PUT (str new-series-path "?title=A%20new%20title")
                            {:content-type :json
                                  :body (json/write-str request-ednld)})
              resp-body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (not= (Instant/parse (get resp-body "dcterms:issued"))
                    (Instant/parse (get resp-body "dcterms:modified")))))

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
              (is (= (update-vals (select-keys resp-body ["dcterms:issued" "dcterms:modified"]) #(Instant/parse %))
                     (update-vals (select-keys body' ["dcterms:issued" "dcterms:modified"]) #(Instant/parse %)))
                  "The document shouldn't be modified"))))))))

(defn- create-series [client series-info]
  (ring-client/create-resource client (time/system-now) series-info))

(defn- create-child [client parent child-info]
  (let [child-create (resources/set-parent child-info parent)]
    (ring-client/create-resource client (time/system-now) child-create)))

(defn- resource-exists-in-db? [ring-client resource]
  (let [system-uris (ring-client/system-uris ring-client)
        repo (ring-client/repo ring-client)
        resource-uri (resources/resource-uri system-uris resource)
        q {:ask []
           :where [[resource-uri '?p '?o]]}]
    (with-open [conn (repo/->connection repo)]
      (repo/query conn (fl/format-query q :pretty? true)))))

(t/deftest create-delete-series-test
  (with-open [client (ring-client/create-client)]
    (let [series-info (resources/create-series "test" {"dcterms:title" "Test" "dcterms:description" "Test"})
          series (create-series client series-info)]

      ;; series should exist in db initially
      (t/is (resource-exists-in-db? client series))

      (ring-client/delete-resource client (time/system-now) series)

      ;; series should not be found via API
      (let [fetched-series (ring-client/get-resource client series)]
        (t/is (nil? fetched-series)))

      ;; series should be deleted from db
      (t/is (= false (resource-exists-in-db? client series))))))

(t/deftest create-delete-series-with-releases-test
  (with-open [client (ring-client/create-client)]
    (let [series-info (resources/create-series "test" {"dcterms:title" "Test series" "dcterms:description" "Test"})
          release-info (resources/create-release "release" {"dcterms:title" "Release" "dcterms:description" "Test"})
          series (create-series client series-info)

          release (create-child client series release-info)]

      ;; release should exist in db initially
      (t/is (resource-exists-in-db? client release))

      (ring-client/delete-resource client (time/system-now) series)

      ;; series and release should not be found via API
      (let [fetched-release (ring-client/get-resource client release)
            fetched-series (ring-client/get-resource client series)]
        (t/is (nil? fetched-release))
        (t/is (nil? fetched-series)))

      ;; series and release should not exist in db
      (t/is (= false (resource-exists-in-db? client series)))
      (t/is (= false (resource-exists-in-db? client release))))))

(t/deftest create-delete-series-with-release-with-schema-test
  (with-open [client (ring-client/create-client)]
    (let [series-info (resources/create-series "test" {"dcterms:title" "Series" "dcterms:description" "Test"})
          release-info (resources/create-release "release" {"dcterms:title" "Release" "dcterms:description" "Test"})
          schema-info (resources/create-schema {"dcterms:title" "Fun schema"
                                                "dh:columns"    [{"@type"         "dh:DimensionColumn"
                                                                  "csvw:datatype" "string"
                                                                  "csvw:name"     "foo_bar"
                                                                  "csvw:titles"   "Foo Bar"}
                                                                 {"@type"         "dh:MeasureColumn"
                                                                  "csvw:datatype" "string"
                                                                  "csvw:name"     "height"
                                                                  "csvw:titles"   "Height"}]})

          series (create-series client series-info)
          release (create-child client series release-info)
          schema (create-child client release schema-info)]

      ;; series, release and schema should exist in db
      (t/is (resource-exists-in-db? client series))
      (t/is (resource-exists-in-db? client release))
      (t/is (resource-exists-in-db? client schema))

      (ring-client/delete-resource client (time/system-now) series)

      ;; series, release and schema should not be found via API
      (let [fetched-schema (ring-client/get-resource client schema)
            fetched-release (ring-client/get-resource client release)
            fetched-series (ring-client/get-resource client series)]
        (t/is (nil? fetched-schema))
        (t/is (nil? fetched-release))
        (t/is (nil? fetched-series)))

      ;; series, release and schema should not exist in db
      (t/is (= false (resource-exists-in-db? client series)))
      (t/is (= false (resource-exists-in-db? client release)))
      (t/is (= false (resource-exists-in-db? client schema))))))

(t/deftest create-delete-series-with-revision-test
  (with-open [client (ring-client/create-client)]
    (let [series-info (resources/create-series "test" {"dcterms:title" "Series" "dcterms:description" "Test"})
          release-info (resources/create-release "release" {"dcterms:title" "Release" "dcterms:description" "Test"})
          revision-info (resources/create-revision {"dcterms:title" "Revision"
                                                    "dcterms:description" "Test revision"})

          series (create-series client series-info)
          release (create-child client series release-info)
          revision (create-child client release revision-info)]

      ;; series, release and revision should exist in db
      (t/is (resource-exists-in-db? client series))
      (t/is (resource-exists-in-db? client release))
      (t/is (resource-exists-in-db? client revision))

      (ring-client/delete-resource client (time/system-now) series)

      ;; series, release and revision should not resolve via API
      (let [fetched-revision (ring-client/get-resource client revision)
            fetched-release (ring-client/get-resource client release)
            fetched-series (ring-client/get-resource client series)]
        (t/is (nil? fetched-revision))
        (t/is (nil? fetched-release))
        (t/is (nil? fetched-series)))

      ;; series, release and revision should not exist in db
      (t/is (= false (resource-exists-in-db? client series)))
      (t/is (= false (resource-exists-in-db? client release)))
      (t/is (= false (resource-exists-in-db? client revision))))))

(t/deftest create-delete-series-with-change-test
  (with-open [client (ring-client/create-client)]
    (let [series-info (resources/create-series "test" {"dcterms:title" "Series" "dcterms:description" "Test"})
          release-info (resources/create-release "release" {"dcterms:title" "Release" "dcterms:description" "Test"})
          revision-info (resources/create-revision {"dcterms:title" "Revision"
                                                    "dcterms:description" "Test revision"})
          schema-info (resources/create-schema {"dcterms:title" "Fun schema"
                                                "dh:columns"    [{"@type"         "dh:DimensionColumn"
                                                                  "csvw:datatype" "string"
                                                                  "csvw:name"     "col1"
                                                                  "csvw:titles"   "col1"}
                                                                 {"@type"         "dh:DimensionColumn"
                                                                  "csvw:datatype" "string"
                                                                  "csvw:name"     "col2"
                                                                  "csvw:titles"   "col2"}
                                                                 {"@type"         "dh:MeasureColumn"
                                                                  "csvw:datatype" "string"
                                                                  "csvw:name"     "col3"
                                                                  "csvw:titles"   "col3"}]})
          change-info (resources/create-change :dh/ChangeKindAppend [["col1" "col2" "col3"]
                                                                     ["a" "b" "c"]
                                                                     ["1" "2" "3"]]
                                               {"dcterms:description" "Test change"})

          series (create-series client series-info)
          release (create-child client series release-info)
          schema (create-child client release schema-info)
          revision (create-child client release revision-info)
          change (create-child client revision change-info)]

      ;; series, release, schema, revision and change should exist in db
      (t/is (resource-exists-in-db? client series))
      (t/is (resource-exists-in-db? client release))
      (t/is (resource-exists-in-db? client schema))
      (t/is (resource-exists-in-db? client revision))
      (t/is (resource-exists-in-db? client change))

      (ring-client/delete-resource client (time/system-now) series)

      ;; series, release, schema, revision and change should not resolve via API
      (let [fetched-change (ring-client/get-resource client change)
            fetched-revision (ring-client/get-resource client revision)
            fetched-release (ring-client/get-resource client release)
            fetched-schema (ring-client/get-resource client schema)
            fetched-series (ring-client/get-resource client series)]
        (t/is (nil? fetched-change))
        (t/is (nil? fetched-revision))
        (t/is (nil? fetched-release))
        (t/is (nil? fetched-schema))
        (t/is (nil? fetched-series)))

      ;; series, release, schema, revision and change should not exist in db
      (t/is (= false (resource-exists-in-db? client series)))
      (t/is (= false (resource-exists-in-db? client release)))
      (t/is (= false (resource-exists-in-db? client schema)))
      (t/is (= false (resource-exists-in-db? client revision)))
      (t/is (= false (resource-exists-in-db? client change))))))

(t/deftest delete-series-with-shared-change-test
  ;; test that creates two series with their own releases, revisions and a
  ;; shared schema. Both contain a change with the same contents - these
  ;; changes should have the same address within the store. The test deletes
  ;; one of the series (and therefore its associated change) and checks that
  ;; the contents of the change in the other series can still be fetched
  (with-open [client (ring-client/create-client)]
    (let [series1-info (resources/create-series "series1" {"dcterms:title" "Series 1" "dcterms:description" "Series 1"})
          series2-info (resources/create-series "series2" {"dcterms:title" "Series 2" "dcterms:description" "Series 2"})

          release1-info (resources/create-release "release1" {"dcterms:title" "Release 1" "dcterms:description" "Release 1"})
          release2-info (resources/create-release "release2" {"dcterms:title" "Release 2" "dcterms:description" "Release 2"})

          schema-columns [{"@type"         "dh:DimensionColumn"
                           "csvw:datatype" "string"
                           "csvw:name"     "col1"
                           "csvw:titles"   "col1"}
                          {"@type"         "dh:DimensionColumn"
                           "csvw:datatype" "string"
                           "csvw:name"     "col2"
                           "csvw:titles"   "col2"}
                          {"@type"         "dh:MeasureColumn"
                           "csvw:datatype" "string"
                           "csvw:name"     "col3"
                           "csvw:titles"   "col3"}]

          schema1-info (resources/create-schema {"dcterms:title" "Schema 1"
                                                 "dh:columns" schema-columns})
          schema2-info (resources/create-schema {"dcterms:title" "Schema 2"
                                                 "dh:columns" schema-columns})

          revision1-info (resources/create-revision {"dcterms:title" "Revision 1" "dcterms:description" "Revision 1"})
          revision2-info (resources/create-revision {"dcterms:title" "Revision 2" "dcterms:description" "Revision 2"})

          change-data [["col1" "col2" "col3"]
                       ["a" "b" "c"]
                       ["1" "2" "3"]]

          change1-info (resources/create-change :dh/ChangeKindAppend change-data {"dcterms:description" "Change 1"})
          change2-info (resources/create-change :dh/ChangeKindAppend change-data {"dcterms:description" "Change 2"})

          series1 (ring-client/create-resource client (time/system-now) series1-info)
          release1 (create-child client series1 release1-info)
          schema1 (create-child client release1 schema1-info)
          revision1 (create-child client release1 revision1-info)
          change1 (create-child client revision1 change1-info)

          series2 (ring-client/create-resource client (time/system-now) series2-info)
          release2 (create-child client series2 release2-info)
          schema2 (create-child client release2 schema2-info)
          revision2 (create-child client release2 revision2-info)
          change2 (create-child client revision2 change2-info)]

      (ring-client/delete-resource client (time/system-now) series1)

      ;; change for series2 should still exist
      (let [fetched-change (ring-client/get-resource client change2)]
        (t/is (some? fetched-change))))))
