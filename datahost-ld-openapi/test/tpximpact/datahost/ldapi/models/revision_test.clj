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
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.router :as router]
   [tpximpact.datahost.ldapi.routes.shared :refer [LdSchemaInput]]
   [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore]
   [tpximpact.datahost.ldapi.strings :as ld-str]
   [tpximpact.datahost.ldapi.util.data-validation :as data-validation]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.time :as time]
   [tpximpact.test-helpers :as th])
  (:import [java.io BufferedReader StringReader ByteArrayInputStream]
           [java.net URI]
           [java.util UUID]))

(defn find-first
  [f coll]
  (first (filter f coll)))

(defn id-matches? [body s]
  (assert body)
  (let [body' (if (string? body)
                (json/read-str body)
                body)]
   (str/ends-with? (get body' "@id") s)))

(defn- create-series [handler]
  (let [series-slug (str "new-series-" (UUID/randomUUID))
        request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri (str "/data/" series-slug)
                 :request-method :put
                 :headers {"accept" "application/ld+json"
                           "content-type" "application/json"}
                 :body (json/write-str request-json)}
        _response (handler request)]
    series-slug))

(defn- create-release [handler series-slug]
  (let [release-slug (str "test-release-" (UUID/randomUUID))
        request-json {"dcterms:title" "Test release" "dcterms:description" "Description"}
        request {:uri (format "/data/%s/release/%s" series-slug release-slug)
                 :request-method :put
                 :headers {"accept" "application/ld+json"
                           "content-type" "application/json"}
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


(defn change-test
  "Arguments:

  - change-kind: one of :dh/ChangeKindAppend etc
  - file-path - path to the CSV
  - title - string
  - url-suffix: string of the form \"/revisions/3/changes/1"
  [http-client release-url change-kind revision-title file-path url-suffix]
  (let [{:keys [POST]} http-client
        revision-resp (POST (str release-url "/revisions")
                            {:content-type :application/json
                             :body (json/write-str {"dcterms:title" revision-title})})
        new-revision-location (-> revision-resp :headers (get "Location"))
        [revision-id change-id] (re-find #"\/data\/.*\/revision\/(\d+)\/commit\/(\d+)"
                                         "/data/whatever/revision/3/commit/1")
        _ (assert revision-id)
        _ (assert change-id)
        change-ednld {"description" (str revision-title " -- changes")}

        change-api-response (POST (str new-revision-location
                                       (case change-kind
                                         :dh/ChangeKindAppend "/appends"
                                         :dh/ChangeKindRetract "/retractions"
                                         :dh/ChangeKindCorrect "/corrections"))
                                  {:query-params change-ednld
                                   :headers {"content-type" "text/csv"}
                                   :body (th/file-upload file-path)})
        response-body-doc (json/read-str (:body change-api-response))]
    (is (= 201 (:status change-api-response)))
    (is (= (str change-kind) (get response-body-doc "dh:changeKind")))
    (is (id-matches? response-body-doc url-suffix))

    {:json response-body-doc
     :change-response change-api-response}))

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
          request1 {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                    :request-method :post
                    :headers {"accept" "application/ld+json"
                              "content-type" "application/json"}
                    :body (json/write-str {"dcterms:title" "Test revision" "dcterms:description" "Description"})}
          {:keys [status body] :as _response} (handler request1)
          revision-doc (json/read-str body)]
      (t/is (= 201 status)
            "first revision was successfully created")
      (t/is (= "Test revision" (get revision-doc "dcterms:title")))
      (t/is (= "Description" (get revision-doc "dcterms:description")))
      (t/is (= release-uri (URI. (get revision-doc "dh:appliesToRelease"))))
      (t/is (str/ends-with? (get revision-doc "@id") "/revision/1")
            "auto-increment revision ID is assigned")

      (testing "A single revision can be can be retrieved via the list API"
        ;; NOTE: this test is essential for ensuring that single item collections
        ;; are serialized within an array wrapper and not as a hash-map
        (let [all-revisions-request {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                                     :headers {"accept" "application/ld+json"}
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

      (let [request2 {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                      :request-method :post
                      :headers {"accept" "application/ld+json"
                                "content-type" "application/json"}
                      :body (json/write-str {"dcterms:title" "A second test revision"
                                             "dcterms:description" "Description"})}
            {:keys [status body] :as _response2} (handler request2)
            revision-doc2 (json/read-str body)]
        (t/is (= 201 status)
              "second revision was successfully created")
        (t/is (str/ends-with? (get revision-doc2 "@id") "/revision/2")
              "subsequent revision has next auto-increment revision ID assigned"))

      (let [release-request {:uri (format "/data/%s/release/%s" series-slug release-slug)
                             :headers {"accept" "application/ld+json"}
                             :request-method :get}
            {:keys [body]} (handler release-request)
            release-doc (json/read-str body)
            release-revisions (get-release-revisions release-doc)]
        (t/is (= #{(format "https://example.org/data/%s/release/%s/revision/1" series-slug release-slug)
                   (format "https://example.org/data/%s/release/%s/revision/2" series-slug release-slug)}
                 release-revisions)))

      (testing "Multiple revisions can be can be retrieved via the list API"
        (let [all-revisions-request {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                                     :headers {"accept" "application/ld+json"}
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

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]
                                 :as http-client} :tpximpact.datahost.ldapi.test/http-client
                                :as sys}
    (let [rdf-base-uri (th/sys->rdf-base-uri sys)
          csv-2019-path "test-inputs/revision/2019.csv"
          csv-2020-path "test-inputs/revision/2020.csv"
          csv-2021-path "test-inputs/revision/2021.csv"
          csv-2021-deletes-path "test-inputs/revision/2021-deletes.csv"
          csv-2021-corrections-path "test-inputs/revision/2021-corrections.csv"
          [csv-2019-seq csv-2020-seq csv-2021-seq csv-2021-deletes-seq] (for [f [csv-2019-path csv-2020-path csv-2021-path csv-2021-deletes-path]]
                                                                          (line-seq (io/reader (io/resource f))))
          series-title "my lovely series"
          series-slug (ld-str/slugify series-title)]

      ;; SERIES
      (PUT (str "/data/" series-slug)
        {:content-type :json
         :headers {"accept" "application/ld+json"}
         :body (json/write-str {"dcterms:title" series-title
                                "dcterms:description" "Description"})})

      (testing "Creating a revision for an existing release and series"
        ;; RELEASE
        (let [release-slug (str "release-" (UUID/randomUUID))
              release-url (str "/data/" series-slug "/release/" release-slug)
              release-resp (PUT release-url
                             {:content-type :application/json
                              :headers {"accept" "application/ld+json"}
                              :body (json/write-str {"dcterms:title" "Release 34"
                                                     "dcterms:description" "Description 34"})})

              schema-req-body {"dcterms:title" "Test schema"
                               "dh:columns"
                               (let [csvw-type (fn [typ col-name titles datatype]
                                                 {"@type" typ
                                                  "csvw:datatype" datatype
                                                  "csvw:name" col-name
                                                  "csvw:titles" titles})]
                                 ;; we put schema only on 2 columns
                                 [(csvw-type "dh:DimensionColumn" "Measure type"
                                             ["Measure type"] :string)
                                  (csvw-type "dh:DimensionColumn" "Statistical Geography"
                                             "Statistical Geography" :string)
                                  (csvw-type "dh:DimensionColumn" "Year"
                                             "Year" :integer)
                                  (csvw-type "dh:MeasureColumn"
                                             "Aged 16 to 64 years level 3 or above qualifications"
                                             "Aged 16 to 64 years level 3 or above qualifications"
                                             :double)
                                  (csvw-type "dh:DimensionColumn" "Unit of Measure"
                                             "Unit of Measure" :string)
                                  (csvw-type "dh:DimensionColumn" "Upper Confidence Interval"
                                             "Upper Confidence Interval" :double)
                                  (csvw-type "dh:DimensionColumn" "Lower Confidence Interval"
                                             "Lower Confidence Interval" :double)
                                  (csvw-type "dh:AttributeColumn" "Observation Status"
                                             "Observation Status" :string)])}

              _validated (when-not (m/validate LdSchemaInput schema-req-body)
                           (throw (ex-info (str (me/humanize (m/explain LdSchemaInput schema-req-body)))
                                           {:schema schema-req-body})))

              _resp (POST (str release-url "/schema")
                      (th/jsonld-body-request schema-req-body))]
          (is (= 201 (:status release-resp)))

          ;; REVISION
          (let [revision-title (str "A revision for release " release-slug)
                revision-description "Revision description"
                revision-post-url (str release-url "/revisions")
                revision-ednld {"dcterms:title" revision-title
                                "dcterms:description" revision-description
                                "dh:publicationDate" "2023-09-01"
                                "dcterms:license" "http://license-link"
                                "dh:reasonForChange" "Comment.."}

                normalised-revision-ld {"dcterms:title" revision-title
                                        "dcterms:description" revision-description
                                        "@type" "dh:Revision"
                                        "dh:appliesToRelease" (str rdf-base-uri series-slug "/release/" release-slug)
                                        "dh:publicationDate" "2023-09-01"
                                        "dcterms:license" "http://license-link"
                                        "dh:reasonForChange" "Comment.."}

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
              (let [response (GET new-revision-location
                               {:headers {"accept" "application/ld+json"}})
                    revision-doc (json/read-str (:body response))]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (select-keys revision-doc (keys normalised-revision-ld)))
                    "responds with JSON")))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (GET release-url {:headers {"accept" "application/ld+json"}})
                    release-doc (json/read-str (:body release-resp))
                    release-revisions (get-release-revisions release-doc)]
                (t/is (= #{(str rdf-base-uri series-slug "/release/" release-slug "/revision/1")}
                         release-revisions))))

            (testing "Changes append resource created with CSV appends file"
              ;; "/:series-slug/releases/:release-slug/revisions/:revision-id/appends"
              (let [query-params {"description" "A new change"}
                    change-api-response (POST (str new-revision-location "/appends")
                                          {:query-params query-params
                                           :headers {"content-type" "text/csv"}
                                           :body (th/file-upload csv-2019-path)})
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]

                (is (= 201 (:status change-api-response)))
                (is (= (str new-revision-location "/commit/1")
                       new-change-resource-location)
                    "Created with the resource URI provided in the Location header")

                (testing "Change can be retrieved as CSV with text/csv accepts header"
                  (let [change-response (GET new-change-resource-location {:headers {"accept" "text/csv"}})
                        change-resp-body-seq (line-seq (BufferedReader. (StringReader. (:body change-response))))]
                    (is (= 200 (:status change-response)))
                    (is (= (count csv-2019-seq)
                           (count change-resp-body-seq))
                        "responds CSV contents")))))

            (testing "Ensure we can add more than 1 change to a revision."
              ; /data/:series-slug/release/:release-slug/revision/:revision-id/appends
              (let [query-params {"description" "A second change"}
                    change-api-response (POST (str new-revision-location "/appends")
                                          {:query-params query-params
                                           :headers {"content-type" "text/csv"}
                                           :body (th/file-upload csv-2020-path)})
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]
                (is (= 201 (:status change-api-response)))
                (is new-change-resource-location)))

            (testing "Fetching Revision as CSV after 'appending' a single CSV"
              (let [response (GET new-revision-location {:headers {"accept" "text/csv"}})
                    resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                (is (= 200 (:status response)))
                ;; length of one csv file
                (is (= (+ (count csv-2019-seq)
                          (dec (count csv-2020-seq)))
                       (count resp-body-seq))
                    "responds with concatenated changes from both CSVs")
                (is (= (first resp-body-seq) (first csv-2020-seq)))
                (is (str/includes? (last resp-body-seq) ",2020,")))))

          (let [revision-title-2 (str "A second revision for release " release-slug)
                revision-url-2 (str release-url "/revisions")
                revision-resp-2 (POST revision-url-2
                                  {:content-type :json
                                       ;; NOTE: this revision purposefully DOES NOT have dcterms:description
                                   :body (json/write-str {"dcterms:title" revision-title-2})})
                inserted-revision-id-2 (get (json/read-str (:body revision-resp-2)) "@id")
                new-revision-location-2 (-> revision-resp-2 :headers (get "Location"))]

            (is (= 201 (:status revision-resp-2)))
            (is (re-find #".*\/revision\/2" inserted-revision-id-2))
            (is (str/ends-with? new-revision-location-2 inserted-revision-id-2)
                "Created with the resource URI provided in the Location header")

              (testing "Third Changes append resource created against 2nd Revision"
                (let [query-params {"description" "A new third change"}
                      change-api-response (POST (str new-revision-location-2 "/appends")
                                                {:query-params query-params
                                                 :headers {"content-type" "text/csv"}
                                                 :body (th/file-upload csv-2021-path)})]
                  (is (= 201 (:status change-api-response)))
                  (is (id-matches? (:body change-api-response) "/commit/1"))))

              (testing "Fetching Release as CSV with multiple Revisions and CSV append changes"

                (let [response (GET release-url {:headers {"accept" "text/csv"}})
                      resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))
                      valid-row-sample "Aged 16 to 64 years level 3 or above qualifications,Merseyside,2021,59.6,per cent,62.7,56.5,"
                      [_ _ path :as v] (re-find #"<http:\/\/(.+):\d+(\S+)>; rel=\"describedBy\"; type=\"application\/csvm\+json\""
                                                (get-in response [:headers "link"]))]
                  (is (= 200 (:status response)))
                  (is (some? path))
                  ;; length of all csv files minus duplicated headers
                (is (= (+ (count csv-2019-seq)
                          (dec (count csv-2020-seq))
                          (dec (count csv-2021-seq)))
                       (count resp-body-seq))
                    "responds with concatenated changes from all uploaded CSVs")
                (is (= (first resp-body-seq) (first csv-2019-seq)))
                (is (str/includes? (last resp-body-seq) ",2021,"))
                (is (str/includes? (second resp-body-seq) ",2019,"))
                (is (= (find-first #(= % valid-row-sample) resp-body-seq)
                       valid-row-sample))

                (testing "Fetching csvm for release that does exist works"
                  (let [response (GET path)
                        body (json/read-str (:body response))]
                    (is (= 200 (:status response)))
                    (is (= {"@context" ["http://www.w3.org/ns/csvw" {"@language" "en"}]}
                           body))))))

            (testing "Fetching Revision as accumulated CSV from all revisions so far."
              (let [response (GET (str release-url "/revision/2") {:headers {"accept" "text/csv"}})
                    resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                (is (= 200 (:status response)))
                ;; length of all csv files minus duplicated headers
                (is (= (+ (count csv-2019-seq)
                          (dec (count csv-2020-seq))
                          (dec (count csv-2021-seq)))
                       (count resp-body-seq))
                    "responds with concatenated changes from all uploaded CSVs")
                (is (= (first resp-body-seq) (first csv-2019-seq)))
                (is (str/includes? (last resp-body-seq) ",2021,"))
                (is (str/includes? (second resp-body-seq) ",2019,"))))

            (testing "POST retractions against against 3rd Revision"
              (change-test http-client release-url :dh/ChangeKindRetract "revision #3"
                           csv-2021-deletes-path
                           "/revision/3/commit/1"))

            (testing "POST corrections against 4th Revision"
              (change-test http-client release-url :dh/ChangeKindCorrect "revision #4"
                           csv-2021-corrections-path
                           "/revision/4/commit/1")

              (testing "verifying the snapshot CSV"
                (let [measure-column "Aged 16 to 64 years level 3 or above qualifications"
                      resp (GET (str release-url "/revision/4") {:headers {"accept" "text/csv"}})
                      ds (data-validation/as-dataset (-> (:body resp)
                                                         .getBytes
                                                         (ByteArrayInputStream.))
                                                     {})]
                  (is (= 200 (:status resp)))
                  (is (= (+ (dec (count csv-2019-seq))
                            (dec (count csv-2020-seq))
                            (dec (count csv-2021-seq))
                            (- (dec (count csv-2021-deletes-seq))))
                         (tc/row-count ds)))
                 ;; the corrections set measure column to 99.0'
                  (is (= 4 (tc/row-count (tc/select-rows ds
                                                         (comp #(= 99.0 %)
                                                               #(get % measure-column))))))))))

          (testing "Creation of a auto-increment Revision IDs for a release"
            (let [revision-title (str "One of many revisions for release " release-slug)
                  revision-ednld {"dcterms:title" revision-title}
                  new-revision-ids (for [_ (range 1 11)
                                         :let [resp (POST (str release-url "/revisions")
                                                      {:content-type :json
                                                       :body (json/write-str revision-ednld)})]]
                                     (get (json/read-str (:body resp)) "@id"))]
              ;; This Release already has 4 Revisions, so we expect another 10 in the series
              (is (= (for [i (range 5 15)]
                       (format "%s/release/%s/revision/%d" series-slug release-slug i))
                     new-revision-ids)
                  "Expected Revision IDs integers increase in an orderly sequence")))

          (testing "Create revision with query params only"
            (let [{status :status body :body} (POST (str release-url "/revisions")
                                                {:content-type :json
                                                 :query-params {:title "final revision"}})]
              (is (= 201 status)))))))))

(deftest csvm-revision-test
  (th/with-system-and-clean-up
      {{:keys [GET POST PUT] :as http-client} :tpximpact.datahost.ldapi.test/http-client :as sys}

    (let [new-series-slug (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-slug)
          release-1-id (str "release-" (UUID/randomUUID))
          release-1-path (str new-series-path "/release/" release-1-id)
          revision-post-url (str release-1-path "/revisions")
          revision-get-url (str release-1-path "/revision")
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

        (let [{:keys [status body]} (GET (str revision-get-url "/does-not-exist"))]
          (is (= 404 status))
          (is (= "Not found" body))))

      (testing "Fetching a revision csv that does exist works"
        (let [response (GET revision-1-path {:headers {"accept" "text/csv"}})
              [_ _ path :as v] (re-find #"<http:\/\/(.+):\d+(\S+)>; rel=\"describedBy\"; type=\"application\/csvm\+json\""
                                        (get-in response [:headers "link"]))]
          (is (= 200 (:status response)))
          ;; (is (not (empty? (:body response))))
          ;; TODO: what is the csv release meant to be? `""` empty string
          ;; seems off when the json returns something not-nil
          (is (= revision-1-csvm-path path))))

      (testing "Fetching csvm for revision that does exist works"
        (let [response (GET revision-1-csvm-path)
              body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= {"@context" ["http://www.w3.org/ns/csvw" {"@language" "en"}]}
                 body)))))))
