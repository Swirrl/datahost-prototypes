(ns tpximpact.datahost.ldapi.handlers.delta-test
  (:require
   [clojure.test :refer :all]
   [tpximpact.test-helpers :as th]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.handlers.delta :as handlers.delta]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]))

(deftest diffing-datasets
  ;; (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
  ;;   (is false))
  (let [ds1 (data.validation/as-dataset
             ^{:type :datahost.types/seq-of-maps} [{"measure" 1.0 "attr1" "a"}
                                                   {"measure" 1.0 "attr1" "b"}
                                                   {"measure" 4.5 "attr1" "d"}] {})
        ds2 (data.validation/as-dataset
             ^{:type :datahost.types/seq-of-maps} [{"measure" 1.2 "attr1" "a"}
                                                   {"measure" 3.0 "attr1" "c"}
                                                   {"measure" 4.5 "attr1" "d"}] {})
        result (handlers.delta/delta-dataset ds1 ds2 {:measure-column "measure"
                                                     :hashable-columns ["attr1"]})]
    (is result)))

