(ns tpximpact.facet-search-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [tpximpact.catql.search :as search]
   [tpximpact.test-helpers :as h]))

(use-fixtures :once h/with-system)

(def query-no-facets-constraints
  "
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
}")

(def one-facet-constraint
  "
query testQuery {
  endpoint {
    catalog {
      id
      catalog_query(themes: [\"http://gss-data.org.uk/def/gdp#trade\"]) {
        datasets {
          id
          theme
          creator
        }
        facets {
          themes {
            id
            enabled
          }
          creators {
            id
            label
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
}
")

(deftest facet-search-test
  (let [schema (h/catql-schema)]
    (testing "Faceted queries"

      (testing "with no facet constraints applied"
        (let [result (h/execute schema query-no-facets-constraints)]
          
          (testing "all datasets are returned"
            (is (= 137 (count (h/result-datasets result)))))

          (testing "all facets are enabled"
            (is (= 25 (count (h/facets-enabled result :themes))))
            (is (= 22 (count (h/facets-enabled result :creators))))
            (is (= 12 (count (h/facets-enabled result :publishers)))))

          (testing "facet ids are returned"
            (is (= 25 (->> (h/facets result :themes) (map :id) count)))
            (is (= 22 (->> (h/facets result :creators) (map :id) count)))
            (is (= 12 (->> (h/facets result :publishers) (map :id) count))))))

      (testing "with one facet constrained"
        (let [result (h/execute schema one-facet-constraint)]
          (testing "datasets are filtered by the given facet"
            (is (= 7 (count (h/result-datasets result))))
            (is (= ["http://gss-data.org.uk/def/gdp#trade"]
                   (->> result h/result-datasets (map :theme) distinct))))

          (testing "locking a facet doesn't disable other selections within that facet"
            (is (= 25 (count (h/facets-enabled result :themes)))))

          (testing "unconstrained facets that would expand search results are enabled"
            ;; publishers and creators who publish to the trade theme
            (is (= ["https://www.gov.uk/government/organisations/department-for-digital-culture-media-sport"
                    "https://www.gov.uk/government/organisations/hm-revenue-customs"]
                   (->> (h/facets-enabled result :publishers)
                        (map :id))))
            (is (= #{"https://www.gov.uk/government/organisations/department-for-digital-culture-media-sport"
                     "https://www.gov.uk/government/organisations/hm-revenue-customs"}
                   (->> (h/facets-enabled result :creators)
                        (map :id)
                        set))))))

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
}
")]
          (testing "datasets are filtered by both facets"
            (is (= 6 (count (h/result-datasets result))))
            (is (= ["http://gss-data.org.uk/def/gdp#climate-change"]
                   (->> result h/result-datasets (map :theme) distinct)))
            (is (= ["https://www.gov.uk/government/organisations/met-office"]
                   (->> result h/result-datasets (map :publisher) distinct))))

          (testing "other selections within one facet are constrained by the other"
            ;; themes with theme.publisher = "met-office"
            ;; aka themes that the met office publishes
            (is (= ["http://gss-data.org.uk/def/gdp#climate-change"
                    "https://www.ons.gov.uk/economy/environmentalaccounts"]
                   (->> (h/facets-enabled result :themes)
                        (map :id))))

            ;; publishers that publish to the climate change theme
            (is (= ["https://www.gov.uk/government/organisations/department-for-business-energy-and-industrial-strategy"
                    "https://www.gov.uk/government/organisations/department-for-energy-security-and-net-zero"
                    "https://www.gov.uk/government/organisations/department-for-environment-food-rural-affairs" 
                    "https://www.gov.uk/government/organisations/department-for-levelling-up-housing-and-communities"
                    "https://www.gov.uk/government/organisations/forest-research" 
                    "https://www.gov.uk/government/organisations/met-office"
                    "https://www.gov.uk/government/organisations/ministry-of-housing-communities-and-local-government"
                    "https://www.gov.uk/government/organisations/welsh-government"]



                   (->> (h/facets-enabled result :publishers)
                        (map :id)
                        sort)))

            ;; creators of datasets published by the met office to the climate change theme
            (is (= (sort ["https://www.gov.uk/government/organisations/the-meteorological-office"
                          "https://www.gov.uk/government/organisations/met-office"])
                   (->> (h/facets-enabled result :creators)
                        (map :id)
                        sort)))))))))


(def query-no-datasets
  "Note: Not including 'datasets' field int the query"
  "
query testQuery {
  endpoint {
    catalog {
      catalog_query {
        facets {
          themes {
            id
            enabled
          }
          creators {
            id
            enabled
          }
        }
      }
    }
  }
}")

(deftest no-datasets-query-test
  (let [schema (h/catql-schema)]
    (testing "Query without 'datasets' doesn't hang."
      (let [result (h/with-timeout 5000 (h/execute schema query-no-datasets))]
        (is (not= :timeout result))))))

(def query-all-datasets
  "
query testQuery {
  endpoint {
    catalog {
      catalog_query {
        datasets {
          id title description publisher creator theme
        }
      }
    }
  }
}")


(def query-facets-with-search-string
  "
query testQuery {
  endpoint {
    catalog {
      catalog_query(search_string: \"hmrc\") {
        datasets {
          id
          title
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
        }
      }
    }
  }
}")

(def query-facets-with-search-string+constraint
  "
{
  endpoint {
    catalog {
      catalog_query(search_string:\"hmrc\"
                    themes:[\"http://gss-data.org.uk/def/gdp#trade\"]
      ) {
        datasets {
          title
          creator
        }
        facets {
          creators {
            enabled
            id
          }
          themes {
            enabled
            id
          }
        }
      }
    }
  }
}")

(deftest facets-with-search-string-test
  (let [schema (h/catql-schema)
        all-ds (h/result-datasets (h/execute schema query-all-datasets))
        txt-filtered (->> all-ds
                          (search/filter-results
                           {:CatalogSearchResult/search-string "hmrc"})
                          set)]

    (testing "Facets query with a search string"
      (let [result (h/execute schema query-facets-with-search-string)
            by-creator (set/index txt-filtered [:creator])
            by-theme   (set/index txt-filtered [:theme])]
        (is (= 0 (count (h/facets-enabled result :publishers)))
            "we didn't request 'publishers' facet, so there should be zero.")
        (is (= (count by-creator) (count (h/facets-enabled result :creators))))
        (is (= (count by-theme) (count (h/facets-enabled result :themes))))))
    
    (testing "Facets query with a search string and theme constraint"
      (let [result (h/execute schema query-facets-with-search-string+constraint)
            theme "http://gss-data.org.uk/def/gdp#trade"]
        
        (testing "facets other than 'theme' should have theme locked"
          (let [with-locked-theme (filter #(= (:theme %) theme) txt-filtered)
                num-creators (->> with-locked-theme (group-by :creator) count)]
            (is (= num-creators (count (h/facets-enabled result :creators))))))

        (testing "no other constraints, so theme facet only needs text search match"
          (let [num-themes (->> txt-filtered (group-by :theme) count)]
            (is (= num-themes (count (h/facets-enabled result :themes))))))))))
