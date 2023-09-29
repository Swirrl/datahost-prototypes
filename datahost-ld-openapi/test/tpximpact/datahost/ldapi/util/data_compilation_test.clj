(ns tpximpact.datahost.ldapi.util.data-compilation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [malli.core :as m]
   [malli.error :as me]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.util.data-compilation :as dc]
   [tpximpact.datahost.ldapi.util.data-validation
    :refer [-as-dataset]
    :as data-validation]
   [tpximpact.datahost.system-uris :as system-uris]
   [tpximpact.datahost.uris :as uris])
  (:import (java.net URI)))

(defmethod -as-dataset :datahost.types/seq-of-maps [v _opts]
    (tc/dataset v))

(def data
  (let [item (fn [n] {"dim" n "n" n})
        ds-input (fn [r kind]
                   {:datahost.change.data/ref (with-meta (mapv item r) {:type :datahost.types/seq-of-maps})
                    :datahost.change.data/format "native"
                    :datahost.change/kind kind})]
    {1 (ds-input (range 4) :dh/ChangeKindAppend)
     2 (ds-input (range 4 10) :dh/ChangeKindAppend)
     3 (ds-input (range 8 10) :dh/ChangeKindRetract)
     4 (update (ds-input (range 5 8) :dh/ChangeKindCorrect)
               :datahost.change.data/ref
               (fn [records]
                 (with-meta (mapv #(update % "n" * 100) records)
                   (meta records))))
     5 (ds-input (range 100 120) :dh/ChangeKindRetract)}))

(defn data-record-count [k]
  (count (:datahost.change.data/ref (get data k))))

(deftest compilation-smoke-test
  (let [system-uris (system-uris/make-system-uris (URI. "https://example.org/data/"))
        row-schema (m/schema [:tuple
                              [:maybe {:dataset.column/name "n"
                                       :dataset.column/type (:measure uris/column-types)
                                       :dataset.column/datatype :int} :int]
                              [:int {:dataset.column/name "dim"
                                     :dataset.column/type (:dimension uris/column-types)
                                     :dataset.column/datatype :int}]])]
    (assert (#'data-validation/row-schema-valid? row-schema))

    (testing "We can create result from appends"
      (let [result (dc/compile-dataset {:changes [(get data 1)
                                                  (get data 2)]
                                        :row-schema row-schema})]
        (is (= (+ (data-record-count 1)
                  (data-record-count 2))
               (tc/row-count result)))))

    (testing "We can handle appends and retractions"
      (let [result (dc/compile-dataset {:changes [(get data 1)
                                                  (get data 2)
                                                  (get data 3)]
                                        :row-schema row-schema})]
        (is (= (+ (data-record-count 1)
                  (data-record-count 2)
                  (- (data-record-count 3)))
               (tc/row-count result)))))

    (testing "We can handle appends and corrections"
      (let [result (dc/compile-dataset {:changes [(get data 1)
                                                  (get data 2)
                                                  (get data 4)]
                                        :row-schema row-schema})]
        (is (= (+ (data-record-count 1)
                  (data-record-count 2))
               (tc/row-count result)))
        (is (= [500 600 700] (-> (tc/order-by result "dim")
                                 (tc/select-columns ["n"])
                                 (tc/select-rows (range 5 8))
                                 (tc/->array "n")
                                 (vec))))))
    (testing "No changes when correcting items not present in base dataset "
      (let [result (dc/compile-dataset {:changes [(get data 1)
                                                  (get data 2)
                                                  (get data 5)]
                                        :row-schema row-schema})]
        (is (= (+ (data-record-count 1)
                  (data-record-count 2))
               (tc/row-count result)))))))

