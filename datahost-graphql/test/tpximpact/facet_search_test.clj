(ns tpximpact.facet-search-test
  (:require
   [clojure.test :refer :all]
   [tpximpact.test-helpers :as h]))

(deftest facet-search-test
  (let [schema (h/catql-schema)]
    (testing "Faceted queries"
      (testing "with no facet constraints applied"
        (let [result (h/execute schema "
query testQuery {
  endpoint {
    catalog {
      catalog_query {
        datasets {
          id
        }
        facets {
          themes {
            id
            enabled
          }
          creators {
            id
            enabled
          }
          publishers {
            id
            enabled
          }
        }
      }
    }
  }

}")]
          (testing "all datasets are returned"
            (is (= 137 (count (h/result-datasets result)))))

          (testing "all facets are enabled"
            (is (= 25 (h/facets-enabled result :themes)))
            (is (= 22 (h/facets-enabled result :creators)))
            (is (= 12 (h/facets-enabled result :publishers))))

          (testing "facet ids are returned"
            (is (= 25 (->> (h/facets result :themes) (map :id) count)))
            (is (= 22 (->> (h/facets result :creators) (map :id) count)))
            (is (= 12 (->> (h/facets result :publishers) (map :id) count))))))

      (testing "with one facet constrained"
        (let [result (h/execute schema "
query testQuery {
  endpoint {
    catalog {
      id
      catalog_query(themes: [\"http://gss-data.org.uk/def/gdp#trade\"]) {
        datasets {
          id
          theme
        }
        facets {
          themes {
            id
            enabled
          }
          creators {
            id
            label
          }
          publishers {
            id
            enabled
          }
        }
      }
    }
  }
}
")]
          (testing "datasets are filtered by the given facet"
            (is (= 7 (count (h/result-datasets result))))
            (is (= ["http://gss-data.org.uk/def/gdp#trade"]
                   (->> result h/result-datasets (map :theme) distinct))))

          (testing "locking a facet doesn't disable other selections within that facet"
            (is (= 25 (h/facets-enabled result :themes))))

          (testing "unconstrained facets that would expand search results are enabled"
            ;; publishers and creators who publish to the trade theme
            (is (= ["https://www.gov.uk/government/organisations/department-for-digital-culture-media-sport"
                    "https://www.gov.uk/government/organisations/hm-revenue-customs"]
                   (->> (h/facets-enabled result :publishers)
                        (map :id))))
            (is (= ["https://www.gov.uk/government/organisations/department-for-digital-culture-media-sport"
                    "https://www.gov.uk/government/organisations/hm-revenue-customs"]
                   (->> (h/facets-enabled result :creators)
                        (map :id))))))))))
