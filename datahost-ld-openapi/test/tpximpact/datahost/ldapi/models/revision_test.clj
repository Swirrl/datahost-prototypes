(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
    [clojure.data.json :as json]
    [clojure.java.io :as io]
    [clojure.test :refer [deftest is testing]]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.strings :as ld-str])
  (:import (java.io BufferedReader StringReader)
           (java.util UUID)))

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client
                                ld-api-app :tpximpact.datahost.ldapi.router/handler
                                :as sys}

    (let [csv-1-path "test-inputs/revision/2019.csv"
          csv-2-path "test-inputs/revision/2020.csv"
          series-title "my lovely series"
          series-slug (ld-str/slugify series-title)
          base (str "https://example.org/data/" series-slug "/")
          request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" base}]
                         "dcterms:title" series-title}]

      ;; SERIES
      (PUT (str "/data/" (ld-str/slugify series-title))
           {:content-type :json
            :body (json/write-str {"@context"
                                   ["https://publishmydata.com/def/datahost/context"
                                    {"@base" "https://example.org/data/"}]
                                   "dcterms:title" series-title})})

      (testing "Creating a revision for an existing release and series"
        ;; RELEASE
        (let [release-id (str "release-" (UUID/randomUUID))
              release-url (str "/data/" series-slug "/releases/" release-id)
              release-resp (PUT release-url
                                {:content-type :json
                                 :body (json/write-str request-ednld)})]
          (is (= 201 (:status release-resp)))

          ;; REVISION
          (let [revision-title (str "A revision for release " release-id)
                revision-url (str release-url "/revisions")
                revision-ednld {"@context"
                                ["https://publishmydata.com/def/datahost/context"
                                 {"@base" base}]
                                "dcterms:title" revision-title}

                normalised-revision-ld {"@context"
                                        ["https://publishmydata.com/def/datahost/context"
                                         {"@base" base}],
                                        "dcterms:title" revision-title,
                                        "@type" "dh:Revision"
                                        "@id" 1,
                                        "dh:appliesToRelease" release-url}

                revision-resp (POST revision-url
                                    {:content-type :json
                                     :body (json/write-str revision-ednld)})
                inserted-revision-id (get (json/read-str (:body revision-resp)) "@id")
                new-revision-location (-> revision-resp :headers (get "Location"))]

            (is (= normalised-revision-ld (json/read-str (:body revision-resp)))
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
                    release (json/read-str (:body release-resp))]
                (is (= (str "/data/my-lovely-series/releases/" release-id "/revisions/" inserted-revision-id)
                       (first (get release "dh:hasRevision"))))))

            (testing "Changes resource created with CSV appends file"
              ;"/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
              (let [appends-file (io/file (io/resource csv-1-path))
                    change-ednld {"@context"
                                  ["https://publishmydata.com/def/datahost/context"
                                   {"@base" base}]
                                  "dcterms:description" "A new change"}
                    multipart-temp-file-part {:tempfile appends-file
                                              :size (.length appends-file)
                                              :filename (.getName appends-file)
                                              :content-type "text/csv;"}
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
                    "Created with the resource URI provided in the Location header")))

            (testing "Second Changes resource created with CSV appends file"
              ;"/:series-slug/releases/:release-slug/revisions/:revision-id/changes"
              (let [appends-file (io/file (io/resource csv-2-path))
                    change-ednld {"@context"
                                  ["https://publishmydata.com/def/datahost/context"
                                   {"@base" base}]
                                  "dcterms:description" "A new second change"}
                    multipart-temp-file-part {:tempfile appends-file
                                              :size (.length appends-file)
                                              :filename (.getName appends-file)
                                              :content-type "text/csv;"}
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

            (testing "Fetching revision as CSV with multiple CSV append changes"
              (let [response (GET new-revision-location {:headers {"accept" "text/csv"}})
                    csv-1-seq (line-seq (io/reader (io/resource csv-1-path)))
                    csv-2-seq (line-seq (io/reader (io/resource csv-2-path)))
                    resp-body-seq (line-seq (BufferedReader. (StringReader. (:body response))))]
                (is (= 200 (:status response)))
                ;; length of both csv files minus 1 duplicated header
                (is (= (+ (count csv-1-seq) (- (count csv-2-seq) 1))
                       (count resp-body-seq))
                    "responds with concatenated changes CSVs")
                (is (= (last resp-body-seq) (last csv-2-seq)))
                (is (= (first resp-body-seq) (first csv-1-seq))))))

          (testing "Creation of a second revision for a release"
            (let [revision-title-2 (str "A second revision for release " release-id)
                  revision-url-2 (str release-url "/revisions")
                  revision-ednld-2 {"@context"
                                    ["https://publishmydata.com/def/datahost/context"
                                     {"@base" base}]
                                    "dcterms:title" revision-title-2}

                  normalised-revision-ld-2 {"@context"
                                            ["https://publishmydata.com/def/datahost/context"
                                             {"@base" base}],
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

              (testing "Fetching a second existing revision works"
                (let [response (GET new-revision-location-2)]
                  (is (= 200 (:status response)))
                  (is (= normalised-revision-ld-2 (json/read-str (:body response))))))))
          )))))
