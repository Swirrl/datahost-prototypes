(ns tpximpact.datahost.ldapi.models.release-schema-test
  (:require
    [clojure.data.json :as json]
    [clojure.data :refer [diff]]
    [clojure.test :refer [deftest testing is] :as t]
    [tpximpact.test-helpers :as th]))

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

(deftest round-tripping-release-schema-test
  (let [n (format "%03d" (rand-int 100))
        _ (setup-release n)
        {:keys [GET POST]} (get @th/*system* http-client)
        csvw-type (fn [col-name titles] {"csvw:datatype" "string"
                                         "csvw:name" col-name
                                         "csvw:titles" titles})]
    (testing "Creating a schema"
      (let [schema-path (format "/data/my-series-%s/releases/release-%s/schemas/schema-%s" n n n)
            schema-uri (str "https://example.org" schema-path)
            response (POST schema-path
                           {:content-type :json
                            :body (json/write-str
                                   {"@context" ["https://publishmydata.com/def/datahost/context"
                                                {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                                    "dcterms:title" "Fun schema"
                                    "dh:columns" [(csvw-type "foo_bar" ["Foo Bar"])
                                                  (csvw-type "height" ["Height"])]})})
            resp-body (json/read-str (:body response))
            expected-doc (schema-doc n)
            [missing _extra _matching] (diff expected-doc resp-body)]
        (is (= 201 (:status response)))
        (t/is (= nil missing))

        (testing "The release was updated with a reference to the schema"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s" n n))
                body (json/read-str body)]
            (is (= schema-uri (get body "dh:hasSchema")))))

        (testing "The schema can be retrieved"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s/schemas" n n))
                body (json/read-str body)
                [missing _extra _matching] (diff expected-doc body)]
            (is (= nil missing))))))))

(t/deftest one-column-schema-test
  (let [n (format "%03d" (rand-int 100))
        _ (setup-release n)
        {:keys [GET POST]} (get @th/*system* http-client)
        schema-path (format "/data/my-series-%s/releases/release-%s/schemas/schema-%s" n n n)
        schema {"dcterms:title" "Fun schema"
                "dcterms:description" "Description"
                "dh:columns" [{"csvw:datatype" "string"
                               "csvw:name" "test"
                               "csvw:titles" "Test"}]}
        _create-response (POST schema-path
                               {:content-type :json
                                :body (json/write-str schema)})

        fetch-response (GET (format "/data/my-series-%s/releases/release-%s/schemas" n n))
        fetched-doc (json/read-str (:body fetch-response))
        [missing _ _ ] (diff schema fetched-doc)]
    (t/is (= nil missing))))
