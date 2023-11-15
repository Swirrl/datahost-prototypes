(ns tpximpact.datahost.ldapi.handlers.delta-test
  (:require
   [clojure.test :refer :all]
   [tablecloth.api :as tc]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.uris :as uris]
   [tpximpact.datahost.ldapi.handlers.delta :as handlers.delta]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]))

(deftest diffing-datasets
  ;; (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
  ;;   (is false))
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
        result (handlers.delta/delta-dataset ds1 ds2 {:row-schema row-schema
                                                      :measure-column "measure"
                                                      :hashable-columns ["attr1"]})]
    (is (= 3 (tc/row-count result)))))

