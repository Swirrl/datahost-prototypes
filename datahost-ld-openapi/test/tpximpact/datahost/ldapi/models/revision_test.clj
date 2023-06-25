(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
    [clojure.data.json :as json]
    [clojure.java.io :as io]
    [clojure.test :refer [deftest is testing]]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.strings :as ld-str])
  (:import (java.util UUID)))

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client
                                ld-api-app :tpximpact.datahost.ldapi.router/handler
                                :as sys}

    (let [series-title "my lovely series"
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
              release-url (str "/data/" series-slug "/release/" release-id)
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
                                        "dh:appliesToRelease" (str "../" release-id)}

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

            (testing "Fetching an existing revision works"
              (let [response (GET new-revision-location)]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (json/read-str (:body response))))))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (GET release-url)
                    release (json/read-str (:body release-resp))]
                (is (= (str "/data/my-lovely-series/" release-id "/revisions/" inserted-revision-id)
                       (first (get release "dh:hasRevision"))))))

            ;"/:series-slug/release/:release-slug/revisions/:revision-id/changes"
            (let [appends-file (io/file (io/resource "test-inputs/revision/2019.csv"))
                  change-ednld {"@context"
                                ["https://publishmydata.com/def/datahost/context"
                                 {"@base" base}]
                                "dcterms:description" "A new change"}
                  multipart-temp-file-part {:tempfile appends-file
                                            :size (.length appends-file)
                                            :filename (.getName appends-file)
                                            :content-type "text/csv;"}]

              (is (= (-> {:request-method :post
                          :uri (str new-revision-location "/changes")
                          :multipart-params {:appends multipart-temp-file-part}
                          :content-type "application/json"
                          :body (json/write-str change-ednld)}
                         ld-api-app :status)
                     201)))
            ))))))
