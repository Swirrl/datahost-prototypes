(ns tpximpact.ldapi-test
  (:require
    [clojure.test :refer :all]
    [clj-http.client :as http]
    [tpximpact.test-helpers :as th]))

(deftest service-sanity-test
  (th/with-system {{:keys [GET]} :tpximpact.datahost.ldapi.test/http-client :as _sys}
    (testing "LD API service starts and handles query requests to the datastore"
      (let [response (GET "/triplestore-query")]
        (is (= (:status response) 200))
        (is (= (:body response) "Datasets"))))))
