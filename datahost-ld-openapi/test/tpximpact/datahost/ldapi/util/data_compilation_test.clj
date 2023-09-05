(ns tpximpact.datahost.ldapi.util.data-compilation-test
  (:require
   [clojure.test :as t :refer [deftest testing is]]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.util.data-compilation :as dc]
   [tpximpact.datahost.ldapi.util.data-validation
    :refer [-as-dataset]]))

(defmethod -as-dataset :datahost.types/seq-of-maps [v _opts]
    (tc/dataset v))

(def data
  (let [item (fn [n] {:x n :y n})
        ds-input (fn [r kind]
                   {:datahost.change.data/ref (with-meta (mapv item r) {:type :datahost.types/seq-of-maps})
                    :datahost.change.data/format "native"
                    :datahost.change/kind kind})]
    {1 (ds-input (range 4) :dh/ChangeKindAppend)
     2 (ds-input (range 4 10) :dh/ChangeKindAppend)
     3 (ds-input (range 8 10) :dh/ChangeKindRetract)}))

(defn data-record-count [k]
  (count (:datahost.change.data/ref (get data k))))

(deftest compilation-smoke-test
  (testing "We can create result from appends"
    (let [result (dc/compile-dataset {:changes [(get data 1)
                                                (get data 2)]})]
      (is (= (+ (data-record-count 1)
                (data-record-count 2))
             (tc/row-count result)))))

  (testing "We can handle appends and retractions"
    (let [result (dc/compile-dataset {:changes [(get data 1)
                                                (get data 2)
                                                (get data 3)]})]
      (is (= (+ (data-record-count 1)
                (data-record-count 2)
                (- (data-record-count 3)))
             (tc/row-count result))))))
