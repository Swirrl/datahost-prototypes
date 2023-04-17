(ns tpximpact.catql-test
  (:require
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [com.walmartlabs.lacinia :as lacinia]
   [tpximpact.catql :as sut]))

(defn simplify
    "Converts all ordered maps nested within the map into standard hash
  maps, and sequences into vectors, which makes for easier constants
  in the tests, and eliminates ordering problems."
    [m]
    (walk/postwalk
     (fn [node]
       (cond
         (instance? clojure.lang.IPersistentMap node)
         (into {} node)

         (seq? node)
         (vec node)

         :else
         node))
     m))

(defn execute
  ([schema q]
   (execute schema q {} nil nil))
  ([schema q vars context]
   (execute schema q vars context nil))
  ([schema q vars context options]
   (-> (lacinia/execute schema q vars context options)
       simplify
       (dissoc :extensions))))



(deftest a-test
  (let [schema (sut/load-schema {:sdl-resource "catql/catalog.graphql"
                                 :drafter-base-uri "https://idp-beta-drafter.publishmydata.com/"})]
    (testing "Basic Queries"
      (is (= {:data
              {:endpoint
               {:endpoint_id "https://idp-beta-drafter.publishmydata.com/v1/sparql/live"
                :catalog {:label "Datasets"}}}}
             (execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog(id: \"http://gss-data.org.uk/catalog/datasets\") {
       label
     }
   }
}")))



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
             (execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog(id: \"http://gss-data.org.uk/catalog/datasets\") {
       catalog_query(search_string: \"international trade services\") {
         datasets {
           id
         }
       }
     }
   }
}")))

      (is (<= 100
              (count
               (-> (execute schema "
query testQuery {
   endpoint {
     endpoint_id
     catalog(id: \"http://gss-data.org.uk/catalog/datasets\") {
       catalog_query {
         datasets {
           id
         }
       }
     }
   }
}")
                   (get-in [:data :endpoint :catalog :catalog_query :datasets]))))))))
