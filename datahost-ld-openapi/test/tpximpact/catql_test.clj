(ns tpximpact.catql-test
  (:require
    [clojure.test :refer :all]
    [clj-http.client :as http]
    [integrant.core :as ig]
    [tpximpact.datahost.ldapi :as sut]))

(deftest service-sanity-test
  (testing "LD API service starts and handles requests"
    (let [config (sut/load-configs ["test-base-system.edn"
                                    ;; env.edn contains environment specific
                                    ;; overrides to the base-system.edn and
                                    ;; is set on classpath depending on env.
                                    "catql/env.edn"])
          sys (sut/start-system config)]
      (try
        (let [response (http/get "http://localhost:3400/")]
          (is (= (:status response) 200))
          (is (= (:body response) "Hello World")))
        (finally
          (ig/halt! sys))))))
