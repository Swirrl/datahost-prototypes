(ns tpximpact.datahost.ldapi.models.revision-test
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing] :as t]
   [grafter-2.rdf4j.repository :as repo]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.router :as router]
   [tpximpact.datahost.time :as time]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.strings :as ld-str])
  (:import [java.net URI]))

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
            "subsequent revision has next auto-increment revision ID assigned"))))


(deftest round-tripping-revision-test
  (th/with-system-and-clean-up {{:keys [GET POST PUT]} :tpximpact.datahost.ldapi.test/http-client
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
        (let [release-url (str "/data/" series-slug "/release/release-1")
              release-resp (PUT release-url
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

                revision-resp (POST revision-url
                                    {:content-type :json
                                     :body (json/write-str revision-ednld)})
                inserted-revision-id (get (json/read-str (:body revision-resp)) "@id")
                inserted-revision-url (str revision-url "/" inserted-revision-id)]

            (is (= normalised-revision-ld (json/read-str (:body revision-resp)))
                "successful post returns normalised release data")

            (testing "Fetching an existing revision works"
              (let [response (GET inserted-revision-url)]
                (is (= 200 (:status response)))
                (is (= normalised-revision-ld (json/read-str (:body response))))))

            (testing "Associated Release gets the Revision inverse triple"
              (let [release-resp (GET release-url)
                    release (json/read-str (:body release-resp))]
                (is (= (str "/data/my-lovely-series/release-1/revisions/" inserted-revision-id)
                       (get release "dh:hasRevision")))))))))))
