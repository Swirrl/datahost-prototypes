(ns tpximpact.ldapi-test
  (:require
    [clojure.test :refer :all]
    [clj-http.client :as http]
    [tpximpact.test-helper :as th]))

(deftest service-sanity-test
  (th/with-system sys
    (testing "LD API service starts and handles requests"
      (let [response (http/get "http://localhost:3400/")]
        (is (= (:status response) 200))
        (is (= (:body response) "Hello World"))))))