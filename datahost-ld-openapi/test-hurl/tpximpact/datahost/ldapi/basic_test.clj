(ns tpximpact.datahost.ldapi.basic-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.java.shell :as shell]
   [tpximpact.test-helpers :as th]))


(defn- variables->args
  [m]
  (for [[k v] m]
    (str (name k) "=" v)))

(defn hurl
  [{:keys [script variables junit-report]}]
  (let [variables (merge {:scheme "http"} variables)
        hurl-variables (mapcat (fn [v] ["--variable" v]) (variables->args variables))
        args (cond-> ["--test"]
               junit-report (conj "--report-junit" junit-report)
               :always (into hurl-variables))]
   (assoc (apply shell/sh "hurl" script args) :hurl/args args)))

(deftest smoke-tests
  (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
    (let [variables {:host_name (str "localhost:" http-port)
                     :series (str "my-series-" (random-uuid))
                     :auth_token "ignore"}]

      (let [result (hurl {:variables variables :script "bin/hurl-data/minimal.hurl"
                          :junit-report "test-results/hurl.xml"})]
        (is (= 0 (:exit result))))

      (let [result (hurl {:variables (assoc variables :series (random-uuid))
                          :script "bin/hurl-data/minimal_get.hurl"
                          :junit-report "test-results/hurl.xml"})]
        (is (= 0 (:exit result)))))))


