(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
    [clojure.data :as c.data]
    [clojure.data.json :as json]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [malli.core :as m]
    [malli.error :as me]
    [tpximpact.datahost.system-uris :as su]
    [tpximpact.datahost.ldapi.resource :as resource]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
    [tpximpact.datahost.ldapi.routes.shared :refer [LdSchemaInput]]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.strings :as ld-str])
  (:import [java.net URI]
           [java.util UUID]
           [java.io BufferedReader File StringReader]))

(defn find-first
  [f coll]
  (first (filter f coll)))

(defn- create-series [handler]
  (let [series-slug (str "new-series-" (UUID/randomUUID))
        request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri (str "/data/" series-slug)
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        _response (handler request)]
    series-slug))

(defn- create-release [handler series-slug]
  (let [release-slug (str "test-release-" (UUID/randomUUID))
        request-json {"dcterms:title" "Test release" "dcterms:description" "Description"}
        request {:uri (format "/data/%s/releases/%s" series-slug release-slug)
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        response (handler request)
        release-doc (json/read-str (:body response))]
    [release-slug release-doc]))

(defn- resource-id [resource-doc]
  (let [resource (resource/from-json-ld-doc resource-doc)]
    (resource/id resource)))

(defn- get-release-revisions [release-doc]
  (let [release-revisions (get release-doc "dh:hasRevision")]
    (if (coll? release-revisions)
      (set release-revisions)
      #{release-revisions})))

(t/deftest put-revision-create-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t (time/parse "2023-07-03T11:16:16Z")
          clock (time/manual-clock t)
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))

          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})
          series-slug (create-series handler)
          [release-slug release-doc] (create-release handler series-slug)
          release-uri (resource-id release-doc)
          request1 {:uri (format "/data/%s/releases/%s/revisions" series-slug release-slug)
                    :request-method :post
                    :headers {"content-type" "application/json"}
                    :body (json/write-str {"dcterms:title" "Test revision" "dcterms:description" "Description"})}
          {:keys [status body] :as _response} (handler request1)
          revision-doc (json/read-str body)]
      (t/is (= 201 status)
            "first revision was successfully created")
      (t/is (= "Test revision" (get revision-doc "dcterms:title")))
      (t/is (= "Description" (get revision-doc "dcterms:description")))
      (t/is (= release-uri (URI. (get revision-doc "dh:appliesToRelease"))))
      (t/is (str/ends-with? (get revision-doc "@id") "/revisions/1")
            "auto-increment revision ID is assigned")

      (testing "A single revision can be can be retrieved via the list API"
        ;; NOTE: this test is essential for ensuring that single item collections
        ;; are serialized within an array wrapper and not as a hash-map
        (let [all-revisions-request {:uri (format "/data/%s/releases/%s/revisions" series-slug release-slug)
                                     :headers {"accept" "application/json"}
                                     :request-method :get}
              {:keys [body status]} (handler all-revisions-request)
              release-doc (json/read-str body)
              [missing _extra _matching] (->> (get release-doc "contents")
                                              (first)
                                              (c.data/diff {"dcterms:title" "Test revision"
                                                            "dcterms:description" "Description"
                                                            "@type" "dh:Revision"}))]
          (t/is (= status 200))
          (t/is (= (count (get release-doc "contents")) 1))
          (t/is (= nil missing))))

      (let [request2 {:uri (format "/data/%s/releases/%s/revisions" series-slug release-slug)
                      :request-method :post
                      :headers {"content-type" "application/json"}
                      :body (json/write-str {"dcterms:title" "A second test revision"
                                             "dcterms:description" "Description"})}
            {:keys [status body] :as _response2} (handler request2)
            revision-doc2 (json/read-str body)]
        (t/is (= 201 status)
              "second revision was successfully created")
        (t/is (str/ends-with? (get revision-doc2 "@id") "/revisions/2")
              "subsequent revision has next auto-increment revision ID assigned"))

      (let [release-request {:uri (format "/data/%s/releases/%s" series-slug release-slug)
                             :headers {"accept" "application/json"}
                             :request-method :get}
            {:keys [body]} (handler release-request)
            release-doc (json/read-str body)
            release-revisions (get-release-revisions release-doc)]
        (t/is (= #{(format "https://example.org/data/%s/releases/%s/revisions/1" series-slug release-slug)
                   (format "https://example.org/data/%s/releases/%s/revisions/2" series-slug release-slug)}
                 release-revisions)))

      (testing "Multiple revisions can be can be retrieved via the list API"
        (let [all-revisions-request {:uri (format "/data/%s/releases/%s/revisions" series-slug release-slug)
                                     :headers {"accept" "application/json"}
                                     :request-method :get}
              {:keys [body status]} (handler all-revisions-request)
              release-doc (json/read-str body)
              [missing1 _extra1 _matching1] (->> (get release-doc "contents")
                                                 (first)
                                                 (c.data/diff {"dcterms:title" "A second test revision"}))
              [missing2 _extra2 _matching2] (->> (get release-doc "contents")
                                                 (second)
                                                 (c.data/diff {"dcterms:title" "Test revision"}))]
          (t/is (= status 200))
          (t/is (= (count (get release-doc "contents")) 2))
          (t/is (= nil missing1))
          (t/is (= nil missing2)))))))

(defn- build-csv-multipart [csv-path]
  (let [appends-file (io/file (io/resource csv-path))]
    {:tempfile appends-file
     :size (.length appends-file)
     :filename (.getName appends-file)
     :content-type "text/csv;"}))

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client
                                ld-api-app :tpximpact.datahost.ldapi.router/handler
                                :as sys}
    (let [rdf-base-uri (th/sys->rdf-base-uri sys)
          csv-2019-path "test-inputs/revision/2019.csv"
          csv-2020-path "test-inputs/revision/2020.csv"
          csv-2021-path "test-inputs/revision/2021.csv"
          csv-2021-deletes-path "test-inputs/revision/2021-deletes.csv"
          [csv-2019-seq csv-2020-seq csv-2021-seq] (for [f [csv-2019-path csv-2020-path csv-2021-path]]
                                                     (line-seq (io/reader (io/resource f))))
          series-title "my lovely series"
          series-slug (ld-str/slugify series-title)]

      ;; SERIES
      (PUT (str "/data/" series-slug)
           {:content-type :json
            :body (json/write-str {"dcterms:title" series-title
                                   "dcterms:description" "Description"})})

      (testing "Creating a revision for an existing release and series"
        ;; RELEASE
        (let [release-slug (str "release-" (UUID/randomUUID))
              release-url (str "/data/" series-slug "/releases/" release-slug)
              release-resp (PUT release-url
                                {:content-type :application/json
                                 :body (json/write-str {"dcterms:title" "Release 34"
                                                        "dcterms:description" "Description 34"})})

              schema-req-body {"dcterms:title" "Test schema"
                               "dh:columns"
                               (let [csvw-type (fn [col-name titles datatype]
                                                 {"csvw:datatype" datatype
                                                  "csvw:name" col-name
                                                  "csvw:titles" titles})]
                                 ;; we put schema only on 2 columns
                                 [(csvw-type "measure_type" ["Measure type"] :string)
                                  (csvw-type "year" "Year" :integer)])}

              _validated (when-not (m/validate LdSchemaInput schema-req-body)
                           (throw (ex-info (str (me/humanize (m/explain LdSchemaInput schema-req-body)))
                                           {:schema schema-req-body})))

              temp-schema-file (File/createTempFile "revision-test-schema-1" ".json")

              _ (with-open [file (io/writer temp-schema-file)]
                  (binding [*out* file]
                    (println (json/write-str schema-req-body))))

              json-file-multipart (let [json-file (io/file (.getAbsolutePath temp-schema-file))]
                                    {:tempfile json-file
                                     :size (.length json-file)
                                     :filename (.getName json-file)
                                     :content-type "application/json"})

              _resp (ld-api-app {:request-method :post
                                 :uri (str release-url "/schema")
                                 :multipart-params {:schema-file json-file-multipart}
                                 :content-type "application/json"})]
          (is (= 201 (:status release-resp)))

          ;; REVISION
          (let [revision-title (str "A revision for release " release-slug)
                revision-description "Revision description"
                revision-post-url (str release-url "/revisions")
                revision-ednld {"dcterms:title" revision-title
                                "dcterms:description" revision-description}

                normalised-revision-ld {"dcterms:title" revision-title
                                        "dcterms:description" revision-description
                                        "@type" "dh:Revision"
                                        "dh:appliesToRelease" (str rdf-base-uri series-slug "/releases/" release-slug)}

                revision-resp (POST revision-post-url
                                    {:content-type :application/json
                                     :body (json/write-str revision-ednld)})
                inserted-revision-id (get (json/read-str (:body revision-resp)) "@id")
                new-revision-location (-> revision-resp :headers (get "Location"))
                revision-doc (json/read-str (:body revision-resp))]

            (is (= normalised-revision-ld (select-keys revision-doc (keys normalised-revision-ld)))
                "successful post returns normalised release data")

            (is (str/ends-with? new-revision-location inserted-revision-id)
                "Created with the resource URI provided in the Location header")

            (testing "Fetching an existing Revision as default application/json format works"
              (let [response (GET new-revision-location)
                    revision-doc (json/read-str (:body response))]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (select-keys revision-doc (keys normalised-revision-ld)))
                    "responds with JSON")))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (GET release-url)
                    release-doc (json/read-str (:body release-resp))
                    release-revisions (get-release-revisions release-doc)]
                (t/is (= #{(str rdf-base-uri series-slug "/releases/" release-slug "/revisions/1")} release-revisions))))

            (testing "Changes append resource created with CSV appends file"
              ;; "/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
              (let [change-ednld {"dcterms:description" "A new change"
                                  "dcterms:format" "text/csv"}
                    multipart-temp-file-part (build-csv-multipart csv-2019-path)
                    change-api-response (ld-api-app {:request-method :post
                                                     :uri (str new-revision-location "/changes")
                                                     :multipart-params {:appends multipart-temp-file-part}
                                                     :content-type "application/json"
                                                     :body (json/write-str change-ednld)})
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]


                (is (= 201 (:status change-api-response)))
                (is (= (str new-revision-location "/changes/1")
                       new-change-resource-location)
                    "Created with the resource URI provided in the Location header")

                (testing "Change can be retrieved as CSV with text/csv accepts header"
                  (let [change-response (GET new-change-resource-location {:headers {"accept" "text/csv"}})
                        change-resp-body-seq (line-seq (BufferedReader. (StringReader. (:body change-response))))]
                    (is (= 200 (:status change-response)))
                    (is (= (count csv-2019-seq)
                           (count change-resp-body-seq))
                        "responds CSV contents")))))

            (testing "Ensure we can't add more than 1 change to a revision."
              ; /data/:series-slug/releases/:release-slug/revisions/:revision-id/changes
              (let [change-ednld {"dcterms:description" "A new second change"
                                  "dcterms:format" "text/csv"}
                    multipart-temp-file-part (build-csv-multipart csv-2020-path)
                    change-api-response (ld-api-app {:request-method :post
                                                     :uri (str new-revision-location "/changes")
                                                     :multipart-params {:appends multipart-temp-file-part}
                                                     :content-type "application/json"
                                                     :body (json/write-str change-ednld)})
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]
                (is (= 422 (:status change-api-response)))
                (is (nil? new-change-resource-location))))

            (testing "Fetching Revision as CSV with multiple CSV append changes"
              (let [response (GET new-revision-location {:headers {"accept" "text/csv"}})
                    resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                (is (= 200 (:status response)))
                ;; length of one csv file
                (is (= (count csv-2019-seq)
                       (count resp-body-seq))
                    "responds with concatenated changes from both CSVs")
                (is (= (first resp-body-seq) (first csv-2020-seq)))
                ;;(is (= (second resp-body-seq) (second csv-2020-seq)))
                (is (str/includes? (last resp-body-seq) ",2019,")))))

          (testing "Creation of a second revision for a release"
            (let [revision-title-2 (str "A second revision for release " release-slug)
                  revision-url-2 (str release-url "/revisions")
                  revision-resp-2 (POST revision-url-2
                                        {:content-type :json
                                         ;; NOTE: this revision purposefully DOES NOT have dcterms:description
                                         :body (json/write-str {"dcterms:title" revision-title-2})})
                  inserted-revision-id-2 (get (json/read-str (:body revision-resp-2)) "@id")
                  new-revision-location-2 (-> revision-resp-2 :headers (get "Location"))]

              (is (= 201 (:status revision-resp-2)))
              (is (str/ends-with? new-revision-location-2 inserted-revision-id-2)
                  "Created with the resource URI provided in the Location header")

              (testing "Third Changes append resource created against 2nd Revision"
                (let [change-3-ednld {"dcterms:description" "A new third change"
                                      "dcterms:format" "text/csv"}
                      multipart-temp-file-part (build-csv-multipart csv-2021-path)
                      change-api-response (ld-api-app {:request-method :post
                                                       :uri (str new-revision-location-2 "/changes")
                                                       :multipart-params {:appends multipart-temp-file-part}
                                                       :content-type "application/json"
                                                       :body (json/write-str change-3-ednld)})]
                  (is (= 201 (:status change-api-response)))
                  (is (str/ends-with? (get (json/read-str (:body change-api-response)) "@id")
                                      "/changes/1"))))

              (testing "Fetching Release as CSV with multiple Revision and CSV append changes"
                (let [response (GET release-url {:headers {"accept" "text/csv"}})
                      resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))
                      valid-row-sample "Aged 16 to 64 years level 3 or above qualifications,Merseyside,2021,59.6,per cent,62.7,56.5,"]
                  (is (= 200 (:status response)))
                  ;; length of all csv files minus duplicated headers
                  (is (= (+ (count csv-2019-seq) (- (count csv-2021-seq) 1))
                         (count resp-body-seq))
                      "responds with concatenated changes from all uploaded CSVs")
                  (is (= (first resp-body-seq) (first csv-2019-seq)))
                  (is (str/includes? (last resp-body-seq) ",2021,"))
                  (is (str/includes? (second resp-body-seq) ",2019,"))
                  (is (= (find-first #(= % valid-row-sample) resp-body-seq)
                         valid-row-sample))))

              (testing "Fetching Revision as accumulated CSV from all revisions so far."
                (let [response (GET (str release-url "/revisions/2") {:headers {"accept" "text/csv"}})
                      resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                  (is (= 200 (:status response)))
                  ;; length of all csv files minus duplicated headers
                  (is (= (+ (count csv-2019-seq) (- (count csv-2021-seq) 1))
                         (count resp-body-seq))
                      "responds with concatenated changes from all uploaded CSVs")
                  (is (= (first resp-body-seq) (first csv-2019-seq)))
                  (is (str/includes? (last resp-body-seq) ",2021,"))
                  (is (str/includes? (second resp-body-seq) ",2019,"))))

              (testing "Changes deletes resource created against 2nd Revision"
                (let [revision-resp-3 (POST (str release-url "/revisions")
                                            {:content-type :json
                                             :body (json/write-str {"dcterms:title" (str "A third revision for release "
                                                                                         release-slug)})})
                      new-revision-location-3 (-> revision-resp-3 :headers (get "Location"))
                      change-4-ednld {"dcterms:description" "A new fourth deletes change"
                                      "dcterms:format" "text/csv"}
                      multipart-temp-file-part (build-csv-multipart csv-2021-deletes-path)
                      change-api-response (ld-api-app {:request-method :post
                                                       :uri (str new-revision-location-3 "/deletes")
                                                       :multipart-params {:appends multipart-temp-file-part}
                                                       :content-type "application/json"
                                                       :body (json/write-str change-4-ednld)})
                      response-body-doc (json/read-str (:body change-api-response))]
                  (is (= 201 (:status change-api-response)))
                  (is (= ":dh/ChangeKindRetract" (get response-body-doc "dh:changeKind")))
                  (is (str/ends-with? (get response-body-doc "@id") "/revisions/3/changes/1"))))))


          (testing "Creation of a auto-increment Revision IDs for a release"
            (let [revision-title (str "One of many revisions for release " release-slug)
                  revision-url-2 (str release-url "/revisions")
                  revision-ednld-2 {"dcterms:title" revision-title}
                  new-revision-ids (for [_n (range 1 11)
                                         :let [resp (POST revision-url-2
                                                          {:content-type :json
                                                           :body (json/write-str revision-ednld-2)})]]
                                     (get (json/read-str (:body resp)) "@id"))]
              ;; This Release already has 3 Revisions, so we expect another 10 in the series
              (is (= new-revision-ids (for [i (range 4 14)]
                                        (format "%s/releases/%s/revisions/%d" series-slug release-slug i)))
                  "Expected Revision IDs integers increase in an orderly sequence"))))))))

(deftest csvm-revision-test
  (th/with-system-and-clean-up
      {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client :as sys}

    (let [new-series-slug (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-slug)
          release-1-id (str "release-" (UUID/randomUUID))
          release-1-path (str new-series-path "/releases/" release-1-id)
          revision-post-url (str release-1-path "/revisions")
          _
          (PUT new-series-path
               {:content-type :json
                :body (json/write-str {"dcterms:title" "A title"
                                       "dcterms:description" "Description"})})
          _
          (PUT release-1-path
               {:content-type :json
                :body (json/write-str {"dcterms:title" "Example Release"
                                       "dcterms:description" "Description"})})
          revision-response
          (POST revision-post-url
                {:content-type :application/json
                 :body (json/write-str
                        {"dcterms:title" (str "A revision for release " release-1-id)
                         "dcterms:description" "Revision description"})})

          revision-1-path (-> revision-response :headers (get "Location"))
          revision-1-csvm-path (str revision-1-path "-metadata.json")]

      (testing "Fetching csvw metadata for a revision that does not exist returns 'not found'"

        (let [{:keys [status body]} (GET (str revision-post-url "/does-not-exist"))]
          (is (= 404 status))
          (is (= "Not found" body))))

      (testing "Fetching a release csv that does exist works"
        (let [response (GET revision-1-path {:headers {"accept" "text/csv"}})]
          (is (= 200 (:status response)))
          ;; (is (not (empty? (:body response))))
          ;; TODO: what is the csv release meant to be? `""` empty string
          ;; seems off when the json returns something not-nil
          (is (= (str "<http://localhost:3400" revision-1-csvm-path ">; "
                      "rel=\"describedBy\"; "
                      "type=\"application/csvm+json\""),
                 (get-in response [:headers "link"])))))

      (testing "Fetching csvm for release that does exist works"
        (let [response (GET revision-1-csvm-path)
              body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= {"@context" ["http://www.w3.org/ns/csvw" {"@language" "en"}]}
                 body)))))))
