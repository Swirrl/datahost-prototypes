(ns tpximpact.native-datastore-test
  (:require [clojure.test :refer :all]
            [com.yetanalytics.flint :as fl]
            [tpximpact.datahost.ldapi.native-datastore :as sut]
            [tpximpact.test-helper :as th]))


(deftest native-datastore-test
  (testing "Base data is loaded on startup"
    (th/with-system sys
      (let [repo (:tpximpact.datahost.ldapi.native-datastore/repo sys)
            qry {:prefixes {:dcat "<http://www.w3.org/ns/dcat#>"
                            :rdfs "<http://www.w3.org/2000/01/rdf-schema#>"}
                 :select '[?label ?g]
                 :where [[:graph sut/background-data-graph
                          '[[?datasets a :dcat/Catalog]
                            [?datasets :rdfs/label ?label]]]]}
            results (sut/eager-query repo (fl/format-query qry :pretty? true))]
        (clojure.pprint/pprint results)
        (is (= "Datasets" (-> results first :label)))))))
