(ns tpximpact.datahost.ldapi.models.release-schema-test
  (:require
   [clojure.data :refer [diff]]
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is testing] :as t]
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
    (PUT (format "/data/my-series-%s/release/release-%s" n n)
         (prepare-req {:content-type :json
                       :body {"@context" ["https://publishmydata.com/debf/datahost/context"
                                          {"@base" (format "https://example.org/data/my-series-%s/" n)}]
                              "dcterms:title" "Release 1"
                              "dcterms:description" "Description for release 1"}}))))

(t/use-fixtures :each th/with-system-fixture)

(defn schema-doc
  [tag rdf-base-uri]
  {"@type" "dh:TableSchema"
   "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180"
   "dh:appliesToRelease" (format "%smy-series-%s/release/release-%s" rdf-base-uri tag tag)
   "dcterms:title" "Fun schema"
   "dh:columns" [{"@type" "dh:DimensionColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "foo_bar"
                  "csvw:titles" "Foo Bar"}
                 {"@type" "dh:MeasureColumn"
                  "csvw:datatype" "string"
                  "csvw:name" "height"
                  "csvw:titles" "Height"}]})

(deftest round-tripping-release-schema-from-file-test
  (let [n (format "%03d" (rand-int 100))
        _ (setup-release n)
        {{:keys [GET POST]} :tpximpact.datahost.ldapi.test/http-client
         ld-api-app :tpximpact.datahost.ldapi.router/handler} @th/*system*

        rdf-base-uri (th/sys->rdf-base-uri @th/*system*)

        csvw-col (fn [typ col-name titles]
                   {"csvw:datatype" "string"
                    "csvw:name" col-name
                    "csvw:titles" titles
                    "@type" typ})]
    (testing "Creating a schema from file upload"
      (let [schema-fragment (format "my-series-%s/release/release-%s/schema" n n)
            schema-resource-path (str "/data/" schema-fragment)
            schema-uri (str rdf-base-uri schema-fragment)
            rel-schema {"@context" ["https://publishmydata.com/def/datahost/context"
                                    {"@base" (str rdf-base-uri "my-series-" n "/")}]
                        "dcterms:title" "Fun schema"
                        "dh:columns" [(csvw-col "dh:DimensionColumn" "foo_bar" ["Foo Bar"])
                                      (csvw-col "dh:MeasureColumn" "height" ["Height"])]}
            response (POST schema-resource-path (th/jsonld-body-request rel-schema))
            resp-body (json/read-str (:body response))
            expected-doc (schema-doc n rdf-base-uri)
            [missing _extra _matching] (diff expected-doc resp-body)]
        (is (= 201 (:status response)))
        (is (= nil missing))

        (testing "The release was updated with a reference to the schema"
          (let [{body :body} (GET (format "/data/my-series-%s/release/release-%s.json" n n))
                body (json/read-str body)]
            (is (= schema-uri (get body "dh:hasSchema")))))

        (testing "The schema can be retrieved"
          (let [{body :body} (GET (format "/data/my-series-%s/release/release-%s/schema" n n))
                body (json/read-str body)
                [missing _extra _matching] (diff expected-doc body)]
            (is (= nil missing))))))))

(t/deftest minimal-column-count-schema-test
  (let [n (format "%03d" (rand-int 1000))
        _ (setup-release n)
        {{:keys [GET POST]} :tpximpact.datahost.ldapi.test/http-client
         ld-api-app :tpximpact.datahost.ldapi.router/handler} @th/*system*
        schema-path (format "/data/my-series-%s/release/release-%s/schema" n n)
        schema {"dcterms:title" "Fun schema"
                "dcterms:description" "Description"
                "dh:columns" [{"@type" "dh:MeasureColumn"
                               "csvw:datatype" "string"
                               "csvw:name" "measure"
                               "csvw:titles" "Measure"}
                              {"@type" "dh:DimensionColumn"
                               "csvw:datatype" "string"
                               "csvw:name" "dim"
                               "csvw:titles" "Dimension"}]}

        _response (POST schema-path (th/jsonld-body-request schema))

        fetch-response (GET (format "/data/my-series-%s/release/release-%s/schema" n n))
        fetched-doc (json/read-str (:body fetch-response))
        [missing _ _ ] (diff schema fetched-doc)]
    (t/is (= nil missing))))
