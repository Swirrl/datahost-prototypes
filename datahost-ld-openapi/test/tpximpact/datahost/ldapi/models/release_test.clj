(ns tpximpact.datahost.ldapi.models.release-test
  (:require
   [clojure.data :as c.data]
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing] :as t]
   [grafter-2.rdf4j.repository :as repo]
   [tpximpact.datahost.ldapi.router :as router]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.time :as time]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.store.temp-file-store :as tfstore])
  (:import (java.net URI)
           (java.time Instant)
           [java.util UUID]))

(defn- put-series [put-fn]
  (let [jsonld {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}]
                "dcterms:title" "A title" "dcterms:description" "Description"}]
    (put-fn "/data/new-series"
            {:content-type :json
             :body (json/write-str jsonld)})))

(defn- create-series [handler]
  (let [request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri "/data/new-series"
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        response (handler request)
        series-doc (json/read-str (:body response))]
    (get series-doc "@id")))

(defn- create-put-request [series-slug release-slug properties]
  {:uri (format "/data/%s/releases/%s" series-slug release-slug)
   :request-method :put
   :headers {"content-type" "application/json"}
   :body (json/write-str properties)})

(t/deftest put-release-create-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t (time/parse "2023-07-03T11:16:16Z")
          clock (time/manual-clock t)
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))
          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})
          series-slug (create-series handler)

          request-json {"dcterms:title" "Release title" "dcterms:description" "Description"}
          release-request (create-put-request series-slug "test-release" request-json)
          {:keys [body status]} (handler release-request)
          release-doc (json/read-str body)]

      (t/is (= status 201))

      (t/is (= "Release title" (get release-doc "dcterms:title")))
      (t/is (= "Description" (get release-doc "dcterms:description")))
      (t/is (= (str t) (get release-doc "dcterms:issued")))
      (t/is (= (str t) (get release-doc "dcterms:modified")))
      (t/is (= (format "https://example.org/data/%s" series-slug) (get release-doc "dcat:inSeries"))))))

(t/deftest put-release-update-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t1 (time/parse "2023-07-03T14:35:55Z")
          t2 (time/parse "2023-07-03T16:02:34Z")
          clock (time/manual-clock t1)

          system-uris (su/make-system-uris (URI. "https://example.org/data/"))
          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})

          series-slug (create-series handler)
          create-request (create-put-request series-slug "test-release" {"dcterms:title" "Initial title" "dcterms:description" "Initial description"})
          _create-response (handler create-request)

          _ (time/set-now clock t2)

          update-request {:uri (format "/data/%s/releases/test-release" series-slug)
                          :request-method :put
                          :headers {"content-type" "application/json"}
                          :body (json/write-str {"dcterms:title" "Updated title"
                                                 "dcterms:description" "Updated description"})}
          {:keys [status body]} (handler update-request)
          updated-doc (json/read-str body)]
      (t/is (= 200 status))
      (t/is (= "Updated title" (get updated-doc "dcterms:title")))
      (t/is (= "Updated description" (get updated-doc "dcterms:description")))
      (t/is (= (str t1) (get updated-doc "dcterms:issued")))
      (t/is (= (str t2) (get updated-doc "dcterms:modified"))))))

(t/deftest put-release-no-changes-test
  (with-open [temp-store (tfstore/create-temp-file-store)]
    (let [repo (repo/sail-repo)
          t1 (time/parse "2023-07-04T08:54:11Z")
          t2 (time/parse "2023-07-04T10:33:24Z")
          clock (time/manual-clock t1)
          system-uris (su/make-system-uris (URI. "https://example.org/data/"))

          handler (router/handler {:clock clock :triplestore repo :change-store temp-store :system-uris system-uris})

          series-slug (create-series handler)

          properties {"dcterms:title" "Title" "dcterms:description" "Description"}
          create-request (create-put-request series-slug "new-series" properties)
          create-response (handler create-request)
          initial-doc (json/read-str (:body create-response))

          _ (time/set-now clock t2)

          update-request (create-put-request series-slug "new-series" properties)
          update-response (handler update-request)
          updated-doc (json/read-str (:body update-response))]
      (t/is (= initial-doc updated-doc)))))

(deftest round-tripping-release-test
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client
                                :as sys}

    (let [new-series-slug (str "new-series-" (UUID/randomUUID))
          new-series-path (str "/data/" new-series-slug)
          release-1-id (str "release-" (UUID/randomUUID))
          release-1-path (str new-series-path "/releases/" release-1-id)]
      (testing "Fetching a release for a series that does not exist returns 'not found'"
        (try

          (GET release-1-path)

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 404 status))
              (is (= "Not found" body))))))

      (testing "Fetching a release that does not exist returns 'not found'"
        (try
          (GET release-1-path)

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 404 status))
              (is (= "Not found" body))))))

      (testing "Creating a release for a series that is not found fails gracefully"
        (try
          (PUT (str "/data/this-series-does-not-exist/releases/release-xyz")
               {:content-type :json
                :body (json/write-str {"dcterms:title" "Example Release"
                                       "dcterms:description" "Description"})})
            (catch Throwable ex
              (let [{:keys [status body]} (ex-data ex)]
                (is (= 422 status))
                (is (= "Series for this release does not exist" body))))))

      (PUT new-series-path
           {:content-type :json
            :body (json/write-str {"dcterms:title" "A title"
                                   "dcterms:description" "Description"})})

      (let [request-ednld {"dcterms:title" "Example Release"
                           "dcterms:description" "Description"}
            normalised-ednld {"@context"
                              {"@base" "https://example.org/data/"
                               "appropriate-csvw" "https://publishmydata.com/def/appropriate-csvw/",
                               "csvw" "http://www.w3.org/ns/csvw#"
                               "dcat" "http://www.w3.org/ns/dcat#"
                               "dcterms" "http://purl.org/dc/terms/"
                               "dh" "https://publishmydata.com/def/datahost/"
                               "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}
                              "@id" (str new-series-slug "/releases/" release-1-id)
                              "@type" "dh:Release"
                              "dcat:inSeries" (str "https://example.org" new-series-path)
                              "dcterms:description" "Description"
                              "dcterms:title" "Example Release"}]

        (testing "Creating a release for a series that does exist works"
          (let [response (PUT release-1-path
                              {:content-type :json
                               :body (json/write-str request-ednld)})
                body (json/read-str (:body response))]
            (is (= 201 (:status response)))
            (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))
            (is (= (get body "dcterms:issued")
                   (get body "dcterms:modified")))))

        (testing "Fetching a release that does exist works"
          (let [response (GET release-1-path)
                body (json/read-str (:body response))]
            (is (= 200 (:status response)))
            (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))))

        (testing "Multiple releases can be can be retrieved via the API"
          (PUT (str new-series-path "/releases/release-2")
               {:content-type :json
                :body (json/write-str {"dcterms:title" "A Second Release"
                                       "dcterms:description" "Description 2"})})

          (let [{:keys [body status]} (GET (str new-series-path "/releases"))
                releases-doc (json/read-str body)
                releases-coll (get releases-doc "contents")
                [missing1 _extra1 _matching1] (->> (first releases-coll)
                                                   (c.data/diff {"dcterms:title" "A Second Release"}))
                [missing2 _extra2 _matching2] (->> (second releases-coll)
                                                   (c.data/diff {"dcterms:title" "Example Release"}))]
            (t/is (= status 200))
            (t/is (= nil missing1))
            (t/is (= nil missing2))))

        (testing "A release can be updated, query params take precedence"
          (let [{body-str-before :body} (GET release-1-path)
                {:keys [body] :as response} (PUT (str release-1-path "?title=A%20new%20title")
                                                 {:content-type :json
                                                  :body (json/write-str request-ednld)})
                body-before (json/read-str body-str-before)
                body (json/read-str body)]
            (is (= 200 (:status response)))
            (is (= "A new title" (get body "dcterms:title")))
            (is (= (Instant/parse (get body-before "dcterms:issued"))
                   (Instant/parse (get body "dcterms:issued"))))
            (is (not= (get body "dcterms:modified")
                      (get body-before "dcterms:modified")))

            (testing "No update when query params same as in existing doc"
              (let [response (PUT (str release-1-path "?title=A%20new%20title")
                                  {:content-type :application/json
                                   :body nil})
                    body' (-> response :body json/read-str)]
                (is (= 200 (:status response)))
                (is (= "A new title" (get body' "dcterms:title")))
                (is (= (select-keys body ["dcterms:issued" "dcterms:modified"])
                       (select-keys body' ["dcterms:issued" "dcterms:modified"]))
                    "The document shouldn't be modified")))))))))
