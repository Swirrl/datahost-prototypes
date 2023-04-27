(ns tpximpact.facet-search-test
  (:require
   [clojure.test :refer :all]
   [tpximpact.test-helpers :as h]))

(deftest ^{:kaocha/pending "https://github.com/Swirrl/catql-prototype/issues/8"}
  facet-search-test
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
                        (map :id)))))))

      (testing "with multiple facets constrained"
        (let [result (h/execute schema "
query testQuery {
  endpoint {
    catalog {
      id
      catalog_query(themes: [\"http://gss-data.org.uk/def/gdp#climate-change\"]
                    publishers: [\"https://www.gov.uk/government/organisations/met-office\"]) {
        datasets {
          id
          theme
          publisher
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
          (testing "datasets are filtered by both facets"
            (is (= 6 (count (h/result-datasets result))))
            (is (= ["http://gss-data.org.uk/def/gdp#climate-change"]
                   (->> result h/result-datasets (map :theme) distinct)))
            (is (= ["https://www.gov.uk/government/organisations/met-office"]
                   (->> result h/result-datasets (map :publisher) distinct))))

          (testing "other selections within one facet are constrained by the other"
            ;; themes that the met office publishes
            (is (= ["http://gss-data.org.uk/def/gdp#climate-change"
                    "https://www.ons.gov.uk/economy/environmentalaccounts"]
                   (->> (h/facets-enabled result :themes)
                        (map :id))))

            ;; publishers that publish to the climate change theme
            (is (= ["https://www.gov.uk/government/organisations/department-for-business-energy-and-industrial-strategy"
                    "https://www.gov.uk/government/organisations/department-for-environment-food-rural-affairs"
                    "https://www.gov.uk/government/organisations/met-office"
                    "https://www.gov.uk/government/organisations/ministry-of-housing-communities-and-local-government"
                    "https://www.gov.uk/government/organisations/forest-research"
                    "https://www.gov.uk/government/organisations/department-for-levelling-up-housing-and-communities"
                    "https://www.gov.uk/government/organisations/welsh-government"
                    "https://www.gov.uk/government/organisations/department-for-energy-security-and-net-zero"]
                   (->> (h/facets-enabled result :publishers)
                        (map :id))))

            ;; creators of datasets published by the met office to the climate change theme
            (is (= ["https://www.gov.uk/government/organisations/the-meteorological-office"
                    "https://www.gov.uk/government/organisations/met-office"]
                   (->> (h/facets-enabled result :creators)
                        (map :id))))))))))
