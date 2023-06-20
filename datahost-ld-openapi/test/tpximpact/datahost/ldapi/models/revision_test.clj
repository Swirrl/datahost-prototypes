(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
    [clj-http.client :as http]
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing]]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.strings :as ld-str]))

(defn- put-series [title]
  (let [jsonld {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}]
                "dcterms:title" title}]
    (http/put
      (str "http://localhost:3400/data/" (ld-str/slugify title))
      {:content-type :json
       :body (json/write-str jsonld)})))

(deftest round-tripping-revision-test
  (th/with-system-and-clean-up sys
    ;; SERIES
    (put-series "my lovely series")

    (let [series-title "my lovely series"
          series-slug (ld-str/slugify series-title)
          base (str "https://example.org/data/" series-slug "/")
          request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" base}]
                         "dcterms:title" series-title}]

      (testing "Creating a revision for an existing release and series"
        ;; RELEASE
        (let [release-url (str "http://localhost:3400/data/" series-slug "/release/release-1")
              release-resp (http/put release-url
                                     {:content-type :json
                                      :body (json/write-str request-ednld)})]
          (is (= 201 (:status release-resp)))

          ;; REVISION
          (let [revision-title "A revision for release 1"
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
                                        "dh:appliesToRelease" (str "../release-1")}

                revision-resp (http/post revision-url
                                         {:content-type :json
                                          :body (json/write-str revision-ednld)})
                inserted-revision-id (get (json/read-str (:body revision-resp)) "@id")
                inserted-revision-url (str revision-url "/" inserted-revision-id)]

            (is (= normalised-revision-ld (json/read-str (:body revision-resp)))
                "successful post returns normalised release data")

            (testing "Fetching an existing revision works"
              (let [response (http/get inserted-revision-url)]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (json/read-str (:body response))))))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (http/get release-url)
                    release (json/read-str (:body release-resp))]
                (is (= (str "/data/my-lovely-series/release-1/revisions/" inserted-revision-id)
                       (get release "dh:hasRevision")))))))))))
