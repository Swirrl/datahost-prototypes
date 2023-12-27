(ns tpximpact.datahost.ldapi.handlers.delta-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [tablecloth.api :as tc]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.uris :as uris]
   [tpximpact.datahost.ldapi.handlers.delta :as handlers.delta]
   [tpximpact.datahost.ldapi.util.data.delta :as data.delta]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]))

(deftest diffing-datasets
  (let [row-schema (data.validation/make-row-schema-from-json
                    {"dh:columns" [{"@type" "dh:DimensionColumn"
                                    "csvw:name" "attr1"
                                    "csvw:required" true
                                    "csvw:datatype" :string
                                    "csvw:titles" "attr1"}
                                   {"@type" "dh:MeasureColumn"
                                    "csvw:name" "measure"
                                    "csvw:datatype" :double
                                    "csvw:titles" "measure"}]})
        ds1 (data.validation/as-dataset
             ^{:type :datahost.types/seq-of-maps} [{"measure" 1.0 "attr1" "a"}
                                                   {"measure" 1.0 "attr1" "b"}
                                                   {"measure" 4.5 "attr1" "d"}] {})
        ds2 (data.validation/as-dataset
             ^{:type :datahost.types/seq-of-maps} [{"measure" 1.2 "attr1" "a"}
                                                   {"measure" 3.0 "attr1" "c"}
                                                   {"measure" 4.5 "attr1" "d"}] {})
        result (data.delta/delta-dataset ds1 ds2 {:row-schema row-schema
                                                  :measure-column "measure"
                                                  :coords-columns ["attr1"]})]
    (is (= 4 (tc/row-count result)))
    (is (= 4 (count (set (tc/->array result "datahost.row/id")))))
    (is (= 1 (count (disj (set (tc/->array result "datahost.row.id/previous")) Long/MIN_VALUE))))))

