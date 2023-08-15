(ns tpximpact.datahost.ldapi.models.release-schema-test
  (:require
   [clojure.data.json :as json]
   [clojure.data :refer [diff]]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing is] :as t]
   [tpximpact.test-helpers :as th])
  (:import (java.io File)))

(def http-client :tpximpact.datahost.ldapi.test/http-client)

(defn setup-release
  [n]
  (let [{:keys [PUT]} (get @th/*system* http-client)
        prepare-req (fn [m] (update m :body json/write-str))]
    (PUT (str "/data/my-series-" n)
         (prepare-req {:content-type :json
                       :body {"@context" ["https://publishmydata.com/def/datahost/context"
                                          {"@base" "https://example.org/data/"}],
                              "dcterms:title" "My series"
                              "dcterms:description" (format "some-description-%s" n)}}))
    (PUT (format "/data/my-series-%s/releases/release-%s" n n)
         (prepare-req {:content-type :json
                       :body {"@context" ["https://publishmydata.com/debf/datahost/context"
                                          {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                              "dcterms:title" "Release 1"
                              "dcterms:description" "Description for release 1"}}))))

(t/use-fixtures :each th/with-system-fixture)

(defn schema-doc
  [tag]
  {"@type" "dh:TableSchema"
   "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180"
   "dh:appliesToRelease" (format "https://example.org/data/my-series-%s/releases/release-%s" tag tag)
   "dcterms:title" "Fun schema"
   "dh:columns" [{"@type" "dh:DimensionColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "foo_bar"
                  "csvw:titles" "Foo Bar"}
                 {"@type" "dh:DimensionColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "height"
                  "csvw:titles" "Height"}]})

(defn- build-json-multipart [json-path]
  (let [json-file (io/file json-path)]
    {:tempfile json-file
     :size (.length json-file)
     :filename (.getName json-file)
     :content-type "application/json"}))

(deftest round-tripping-release-schema-from-file-test
  (let [n (format "%03d" (rand-int 100))
        _ (setup-release n)
        {{:keys [GET]} :tpximpact.datahost.ldapi.test/http-client
         ld-api-app :tpximpact.datahost.ldapi.router/handler} @th/*system*

        csvw-type (fn [col-name titles] {"csvw:datatype" "string"
                                         "csvw:name" col-name
                                         "csvw:titles" titles
                                         "@type" "dh:DimensionColumn"})]
    (testing "Creating a schema from file upload"
      (let [schema-path (format "/data/my-series-%s/releases/release-%s/schema" n n)
            schema-uri (str "https://example.org" schema-path)
            temp-schema-file (File/createTempFile "my-schema-1" ".json")

            _ (with-open [file (io/writer temp-schema-file)]
                (binding [*out* file]
                  (println (json/write-str
                            {"@context" ["https://publishmydata.com/def/datahost/context"
                                         {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                             "dcterms:title" "Fun schema"
                             "dh:columns" [(csvw-type "foo_bar" ["Foo Bar"])
                                           (csvw-type "height" ["Height"])]}))))

            json-file-multipart (build-json-multipart (.getAbsolutePath temp-schema-file))
            response (ld-api-app {:request-method :post
                         :uri schema-path
                         :multipart-params {:schema-file json-file-multipart}
                         :content-type "application/json"})
            resp-body (json/read-str (:body response))
            expected-doc (schema-doc n)
            [missing _extra _matching] (diff expected-doc resp-body)]
        (is (= 201 (:status response)))
        (is (= nil missing))

        (testing "The release was updated with a reference to the schema"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s" n n))
                body (json/read-str body)]
            (is (= schema-uri (get body "dh:hasSchema")))))

        (testing "The schema can be retrieved"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s/schema" n n))
                body (json/read-str body)
                [missing _extra _matching] (diff expected-doc body)]
            (is (= nil missing))))))))

(t/deftest one-column-schema-test
  (let [n (format "%03d" (rand-int 1000))
        _ (setup-release n)
        {{:keys [GET]} :tpximpact.datahost.ldapi.test/http-client
         ld-api-app :tpximpact.datahost.ldapi.router/handler} @th/*system*
        schema-path (format "/data/my-series-%s/releases/release-%s/schema" n n)
        schema {"dcterms:title" "Fun schema"
                "dcterms:description" "Description"
                "dh:columns" [{"csvw:datatype" "string"
                               "csvw:name" "test"
                               "csvw:titles" "Test"}]}

        temp-schema-file (File/createTempFile "my-schema-2" ".json")

        _ (with-open [file (io/writer temp-schema-file)]
            (binding [*out* file]
              (println (json/write-str schema))))

        json-file-multipart (build-json-multipart (.getAbsolutePath temp-schema-file))
        _response (ld-api-app {:request-method :post
                              :uri schema-path
                              :multipart-params {:schema-file json-file-multipart}
                              :content-type "application/json"})

        fetch-response (GET (format "/data/my-series-%s/releases/release-%s/schema" n n))
        fetched-doc (json/read-str (:body fetch-response))
        [missing _ _ ] (diff schema fetched-doc)]
    (t/is (= nil missing))))
