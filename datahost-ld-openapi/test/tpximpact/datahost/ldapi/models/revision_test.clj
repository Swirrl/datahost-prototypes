(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
    [clojure.data.json :as json]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.resource :as resource]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.strings :as ld-str])
  (:import [java.net URI]
           [java.util UUID]
           [java.io BufferedReader StringReader]))

(defn- create-series [handler]
  (let [series-slug "new-series"
        request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri "/data/new-series"
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        _response (handler request)]
    series-slug))

(defn- create-release [handler series-slug]
  (let [release-slug "test-release"
        request-json {"dcterms:title" "Test release" "dcterms:description" "Description"}
        request {:uri (format "/data/%s/release/%s" series-slug release-slug)
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
  (let [repo (repo/sail-repo)
        t (time/parse "2023-07-03T11:16:16Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo)
        series-slug (create-series handler)
        [release-slug release-doc] (create-release handler series-slug)
        release-uri (resource-id release-doc)
        request1 {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                  :request-method :post
                  :headers {"content-type" "application/json"}
                  :body (json/write-str {"dcterms:title" "Test revision" "dcterms:description" "Description"})}
        request2 {:uri (format "/data/%s/release/%s/revisions" series-slug release-slug)
                  :request-method :post
                  :headers {"content-type" "application/json"}
                  :body (json/write-str {"dcterms:title" "A second test revision" "dcterms:description" "Description"})}
        {:keys [status body] :as _response} (handler request1)
        revision-doc (json/read-str body)]
    (t/is (= 201 status)
          "first revision was successfully created")
    (t/is (= "Test revision" (get revision-doc "dcterms:title")))
    (t/is (= "Description" (get revision-doc "dcterms:description")))
    (t/is (= release-uri (URI. (get revision-doc "dh:appliesToRelease"))))
    (t/is (str/ends-with? (get revision-doc "@id") "/revisions/1")
          "auto-increment revision ID is assigned")

    (let [{:keys [status body] :as _response2} (handler request2)
          revision-doc2 (json/read-str body)]
      (t/is (= 201 status)
            "second revision was successfully created")
      (t/is (str/ends-with? (get revision-doc2 "@id") "/revisions/2")
            "subsequent revision has next auto-increment revision ID assigned"))

    (let [release-request {:uri (format "/data/%s/release/%s" series-slug release-slug)
                           :request-method :get}
          release-response (handler release-request)
          release-doc (json/read-str (:body release-response))
          release-revisions (get-release-revisions release-doc)]
      (t/is (= #{"https://example.org/data/new-series/release/test-release/revisions/1"
                 "https://example.org/data/new-series/release/test-release/revisions/2"}
               release-revisions)))))

(defn- build-csv-multipart [csv-path]
  (let [appends-file (io/file (io/resource csv-path))]
    {:tempfile appends-file
     :size (.length appends-file)
     :filename (.getName appends-file)
     :content-type "text/csv;"}))

(defn- doc-with-context [base rest-of-doc]
  (merge {"@context"
          ["https://publishmydata.com/def/datahost/context"
           {"@base" base}]}
         rest-of-doc))

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client
                                ld-api-app :tpximpact.datahost.ldapi.router/handler
                                :as sys}
    (let [csv-2019-path "test-inputs/revision/2019.csv"
          csv-2020-path "test-inputs/revision/2020.csv"
          csv-2021-path "test-inputs/revision/2021.csv"
          [csv-2019-seq csv-2020-seq csv-2021-seq] (for [f [csv-2019-path csv-2020-path csv-2021-path]]
                                                     (line-seq (io/reader (io/resource f))))
          series-title "my lovely series"
          series-slug (ld-str/slugify series-title)
          series-base (str "https://example.org/data/" series-slug "/")
          base (str "https://example.org/data/" series-slug "/")
          request-ednld {"dcterms:title" series-title
                         "dcterms:description" "Description"}]

      ;; SERIES
      (PUT (str "/data/" (ld-str/slugify series-title))
           {:content-type :json
            :body (json/write-str {"dcterms:title" series-title
                                   "dcterms:description" "Description"})})

      (testing "Creating a revision for an existing release and series"
        ;; RELEASE
        (let [release-id (str "release-" (UUID/randomUUID))
              release-url (str "/data/" series-slug "/releases/" release-id)
              release-resp (PUT release-url
                                {:content-type :json
                                 :body (json/write-str (doc-with-context series-base
                                                                         {"dcterms:title" "Release 34"}))})]
          (is (= 201 (:status release-resp)))

          ;; REVISION
          (let [revision-title (str "A revision for release " release-id)
                revision-description "Revision description"
                revision-url (str release-url "/revisions")
                revision-ednld (doc-with-context series-base {"dcterms:title" revision-title
                                                              "dcterms:description" revision-description})

                normalised-revision-ld {"dcterms:title" revision-title
                                        "dcterms:description" revision-description
                                        "@type" "dh:Revision"
                                        "dh:appliesToRelease" "https://example.org/data/my-lovely-series/release/release-1"}

                revision-resp (POST revision-url
                                    {:content-type :json
                                     :body (json/write-str revision-ednld)})
                inserted-revision-id (get (json/read-str (:body revision-resp)) "@id")
                new-revision-location (-> revision-resp :headers (get "Location"))
                revision-doc (json/read-str (:body revision-resp))
                inserted-revision-url (resource-id revision-doc)]

            (is (= normalised-revision-ld (select-keys revision-doc (keys normalised-revision-ld)))
                "successful post returns normalised release data")

            (is (= new-revision-location
                   (str revision-url "/" inserted-revision-id))
                "Created with the resource URI provided in the Location header")

            (testing "Fetching an existing Revision as default application/json format works"
              (let [response (GET new-revision-location)]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (json/read-str (:body response)))
                    "responds with JSON")))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (GET release-url)
                    release-doc (json/read-str (:body release-resp))
                    release-revisions (get-release-revisions release-doc)]
                (t/is (= #{"https://example.org/data/my-lovely-series/release/release-1/revisions/1"} release-revisions))))

            (testing "Changes resource created with CSV appends file"
              ;"/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
              (let [change-ednld (doc-with-context series-base {"dcterms:description" "A new change"})
                    multipart-temp-file-part (build-csv-multipart csv-2019-path)
                    change-api-response (ld-api-app {:request-method :post
                                                     :uri (str new-revision-location "/changes")
                                                     :multipart-params {:appends multipart-temp-file-part}
                                                     :content-type "application/json"
                                                     :body (json/write-str change-ednld)})
                    change-response-json (json/read-str (slurp (:body change-api-response)))
                    inserted-change-id (get change-response-json "@id")
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]

                (is (= (:status change-api-response) 201))
                (is (= new-change-resource-location
                       (str new-revision-location "/changes/" inserted-change-id))
                    "Created with the resource URI provided in the Location header")

                (testing "Change can be retrieved as CSV with text/csv accepts header"
                  (let [change-response (GET new-change-resource-location {:headers {"accept" "text/csv"}})
                        change-resp-body-seq (line-seq (BufferedReader. (StringReader. (:body change-response))))]
                    (is (= 200 (:status change-response)))
                    (is (= (count csv-2019-seq) (count change-resp-body-seq))
                        "responds CSV contents")))))

            (testing "Second Changes resource created with CSV appends file"
              ;"/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
              (let [change-ednld (doc-with-context series-base {"dcterms:description" "A new second change"})
                    multipart-temp-file-part (build-csv-multipart csv-2020-path)
                    change-api-response (ld-api-app {:request-method :post
                                                     :uri (str new-revision-location "/changes")
                                                     :multipart-params {:appends multipart-temp-file-part}
                                                     :content-type "application/json"
                                                     :body (json/write-str change-ednld)})
                    change-response-json (json/read-str (slurp (:body change-api-response)))
                    inserted-change-id (get change-response-json "@id")
                    new-change-resource-location (-> change-api-response :headers (get "Location"))]

                (is (= (:status change-api-response) 201))
                (is (= 2 inserted-change-id))
                (is (= new-change-resource-location
                       (str new-revision-location "/changes/" inserted-change-id))
                    "Created with the resource URI provided in the Location header")))

            (testing "Fetching Revision as CSV with multiple CSV append changes"
              (let [response (GET new-revision-location {:headers {"accept" "text/csv"}})
                    resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                (is (= 200 (:status response)))
                ;; length of both csv files minus 1 duplicated header
                (is (= (+ (count csv-2019-seq) (- (count csv-2020-seq) 1))
                       (count resp-body-seq))
                    "responds with concatenated changes from both CSVs")
                (is (= (last resp-body-seq) (last csv-2020-seq)))
                (is (= (first resp-body-seq) (first csv-2019-seq))))))

          (testing "Creation of a second revision for a release"
            (let [revision-title-2 (str "A second revision for release " release-id)
                  revision-url-2 (str release-url "/revisions")
                  revision-ednld-2 (doc-with-context series-base {"dcterms:title" revision-title-2})

                  normalised-revision-ld-2 {"@context"
                                            ["https://publishmydata.com/def/datahost/context"
                                             {"@base" series-base}],
                                            "dcterms:title" revision-title-2,
                                            "@type" "dh:Revision"
                                            "@id" 2,
                                            "dh:appliesToRelease" release-url}

                  revision-resp-2 (POST revision-url-2
                                        {:content-type :json
                                         :body (json/write-str revision-ednld-2)})
                  inserted-revision-id-2 (get (json/read-str (:body revision-resp-2)) "@id")
                  new-revision-location-2 (-> revision-resp-2 :headers (get "Location"))]

              (is (= normalised-revision-ld-2 (json/read-str (:body revision-resp-2)))
                  "successful second post returns normalised release data")

              (is (= new-revision-location-2
                     (str revision-url-2 "/" inserted-revision-id-2))
                  "Created with the resource URI provided in the Location header")

              (testing "Fetching a second existing revision as default JSON works"
                (let [response (GET new-revision-location-2)]
                  (is (= 200 (:status response)))
                  (is (= normalised-revision-ld-2 (json/read-str (:body response))))))

              (testing "Third Changes resource created against 2nd Revision"
                (let [change-3-ednld (doc-with-context series-base {"dcterms:description" "A new third change"})
                      multipart-temp-file-part (build-csv-multipart csv-2021-path)
                      change-api-response (ld-api-app {:request-method :post
                                                       :uri (str new-revision-location-2 "/changes")
                                                       :multipart-params {:appends multipart-temp-file-part}
                                                       :content-type "application/json"
                                                       :body (json/write-str change-3-ednld)})
                      change-3-response-json (json/read-str (slurp (:body change-api-response)))
                      inserted-change-id (get change-3-response-json "@id")]
                  (is (= (:status change-api-response) 201))
                  (is (= 1 inserted-change-id))))

              (testing "Fetching Release as CSV with multiple Revision and CSV append changes"
                (let [response (GET release-url {:headers {"accept" "text/csv"}})
                      resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                  (is (= 200 (:status response)))
                  ;; length of all csv files minus 2 duplicated headers
                  (is (= (+ (count csv-2019-seq) (count csv-2020-seq) (- (count csv-2021-seq) 2))
                         (count resp-body-seq))
                      "responds with concatenated changes from all 3 CSVs")
                  (is (= (last resp-body-seq) (last csv-2021-seq)))
                  (is (= (first resp-body-seq) (first csv-2019-seq)))))))

          (testing "Creation of a auto-increment Revision IDs for a release"
            (let [revision-title (str "One of many revisions for release " release-id)
                  revision-url-2 (str release-url "/revisions")
                  revision-ednld-2 (doc-with-context series-base {"dcterms:title" revision-title})
                  new-revision-ids (for [_n (range 1 11)
                                         :let [resp (POST revision-url-2
                                                          {:content-type :json
                                                           :body (json/write-str revision-ednld-2)})]]
                                     (get (json/read-str (:body resp)) "@id"))]
              ;; This Release already has 2 Revisions, so we expect another 10 in the series
              (is (= new-revision-ids (range 3 13))
                  "Expected Revision IDs integers increase in an orderly sequence"))))))))
