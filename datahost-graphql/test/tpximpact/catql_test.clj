(ns tpximpact.catql-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [grafter-2.rdf4j.repository :as repo]
   [tpximpact.catql :as sut]
   [tpximpact.test-helpers :as h]))

(deftest queries-test
  (let [schema (h/catql-schema)]
    (testing "Catalog Queries"
      (testing "Basic Catalog Field Queries"
        (testing "with default endpoint"
          (is (= {:data
                  {:endpoint
                   {:endpoint_id "https://idp-beta-drafter.publishmydata.com/v1/sparql/live"
                    :catalog {:label "Datasets"}}}}
                 (h/execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog {
       label
     }
   }
}"))))
        (testing "with default catalog"
          (is (= {:data
                  {:endpoint
                   {:endpoint_id "https://idp-beta-drafter.publishmydata.com/v1/sparql/live"
                    :catalog {:id "http://gss-data.org.uk/catalog/datasets"
                              :label "Datasets"}}}}
                 (h/execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog {
       id
       label
     }
   }
}"))))
        (testing "with supplied catalog URI"
          (is (= {:data
                  {:endpoint
                   {:endpoint_id "https://idp-beta-drafter.publishmydata.com/v1/sparql/live"
                    :catalog {:id "http://gss-data.org.uk/catalog/datasets"
                              :label "Datasets"}}}}
                 (h/execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog(id: \"http://gss-data.org.uk/catalog/datasets\") {
       id
       label
     }
   }
}")))))

      (testing "Text search"
        (is (= {:data
                {:endpoint
                 {:endpoint_id "https://idp-beta-drafter.publishmydata.com/v1/sparql/live",
                  :catalog
                  {:catalog_query
                   {:datasets
                    [{:id
                      "http://gss-data.org.uk/data/gss_data/trade/dcms-sectors-economic-estimates-year-trade-in-services-catalog-entry"}
                     {:id
                      "http://gss-data.org.uk/data/gss_data/trade/ons-international-trade-in-services-catalog-entry"}
                     {:id
                      "http://gss-data.org.uk/data/gss_data/trade/ons-international-trade-in-services-by-subnational-areas-of-the-uk-catalog-entry"}]}}}}}
               (h/execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog {
       catalog_query(search_string: \"international trade services\") {
         datasets {
           id
         }
       }
     }
   }
}"))))

      (testing "Unconstrained query"
        (is (<= 100
                (count
                 (-> (h/execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog {
       catalog_query {
         datasets {
           id
         }
       }
     }
   }
}")
                     (get-in [:data :endpoint :catalog :catalog_query :datasets])))))))))
