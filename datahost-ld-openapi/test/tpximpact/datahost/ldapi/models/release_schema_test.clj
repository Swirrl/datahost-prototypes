(ns tpximpact.datahost.ldapi.models.release-schema-test
  (:require 
   [clojure.data.json :as json]
   [clojure.test :refer [deftest testing is] :as t]
   [tpximpact.datahost.ldapi.models.release :as sut]
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
                              "dcterms:identifier" (format "some-identifier-%s" n)}}))
    (PUT (format "/data/my-series-%s/releases/release-%s" n n)
         (prepare-req {:content-type :json
                       :body {"@context" ["https://publishmydata.com/debf/datahost/context"
                                          {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                              "dcterms:title" "Release 1"}}))))

(t/use-fixtures :each th/with-system-fixture)

(defn schema-doc
  [tag]
  {"@context" ["https://publishmydata.com/def/datahost/context"
               {"@base" (format "https://example.org/data/my-series-%s/" tag)}]
   "@type" "dh:TableSchema"
   "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180"
   "datahost:appliesToRelease" (format "/data/my-series-%s/releases/release-%s" tag tag)
   "dcterms:title" "Fun schema"
   "dh:columns" [{"@type" "dh:DimensionColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "foo_bar"
                  "csvw:titles" ["Foo Bar"]}
                 {"@type" "dh:DimensionColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "height"
                  "csvw:titles" ["Height"]}]})

(deftest round-tripping-release-schema-test
  (let [n (format "%03d" (rand-int 100))
        _ (setup-release n)
        {db :tpximpact.datahost.ldapi.db/db} @th/*system*
        {:keys [GET POST]} (get @th/*system* http-client)
        csvw-type (fn [col-name titles] {"csvw:datatype" "string"
                                         "csvw:name" col-name
                                         "csvw:titles" titles})]
    (testing "Creating a schema"
      (let [schema-path (format "/data/my-series-%s/releases/release-%s/schemas/schema-%s" n n n)
            response (POST schema-path
                           {:content-type :json
                            :body (json/write-str
                                   {"@context" ["https://publishmydata.com/debf/datahost/context"
                                                {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                                    "dcterms:title" "Fun schema"
                                    "dh:columns" [(csvw-type "foo_bar" ["Foo Bar"])
                                                  (csvw-type "height" ["Height"])]})})
            resp-body (json/read-str (:body response))
            expected-doc (schema-doc n)]
        (is (= 201 (:status response)))
        (is (= :create (-> @db meta :op)))
        (is (= expected-doc resp-body))

        (testing "The release was updated with a reference to the schema"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s" n n))
                body (json/read-str body)]
            (is (= schema-path (get body "datahost:hasSchema")))))

        (testing "The schema can be retrieved"
          (let [{body :body} (GET (format "/data/my-series-%s/releases/release-%s/schemas" n n))
                body (json/read-str body)]
            (is (= expected-doc body))))))))
