(ns tpximpact.datahost.ldapi.basic-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.java.shell :as shell]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.test-util.hurl :as hurl :refer [hurl]]))

(deftest smoke-tests
  (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
    (let [variables {:host_name (str "localhost:" http-port)
                     :series (str "my-series-" (random-uuid))
                     :auth_token "ignore"}]

      (let [result (hurl {:variables variables :script "bin/hurl-data/minimal.hurl"
                          :report-junit "test-results/hurl.xml"})]
        (is (= 0 (:exit result))))

      (let [result (hurl {:variables (assoc variables :series (random-uuid))
                          :script "bin/hurl-data/minimal_get.hurl"
                          :report-junit "test-results/hurl.xml"})]
        (is (not= 0 (:exit result)))))))

(deftest regression-tests
  (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
    (let [variables {:host_name (str "localhost:" http-port)
                     :series :hurl.variable.named/series
                     :release :hurl.variable.named/release
                     :auth_token "ignore"}
          result (hurl/run-directory "hurl-scripts"
                                     {:variables variables
                                      :report-junit "test-results/hurl.xml"})]
      (is (hurl/success? result)))))
