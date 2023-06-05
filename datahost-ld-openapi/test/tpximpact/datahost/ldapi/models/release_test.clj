(ns tpximpact.datahost.ldapi.models.release-test
  (:require
   [clj-http.client :as http]
   [clojure.test :refer [deftest is testing]]
   [tpximpact.test-helpers :as th]))

;; (deftest   round-tripping-release-test
;;   (th/with-system-and-clean-up sys
;;     (testing "Creating a release for a series that is not found fails gracefully"
;;       (try
;;         (http/get "http://localhost:3400/data/does-not-exist/release/release-1")

;;         (catch Throwable ex
;;           (let [{:keys [status body]} (ex-data ex)]
;;             (is (= status 404))
;;             (is (= body "Not found"))))))

;;     (testing "Fetching a release for a series that does not exist returns 'not found'")

;;     (testing "Fetching a release that does not exist returns 'not found'")
;;     ;; (http/put)
;;     )
;;   ;; (th/with-system-and-clean-up sys
;;   ;;   (testing "A series that does not exist returns 'not found'"
;;   ;;     (try
;;   ;;       (http/get "http://localhost:3400/data/does-not-exist")

;;   ;;       (catch Throwable ex
;;   ;;         (let [{:keys [status body]} (ex-data ex)]
;;   ;;           (is (= status 404))
;;   ;;           (is (= body "Not found"))))))

;;   ;;   (let [incoming-jsonld-doc {"@context"
;;   ;;                              ["https://publishmydata.com/def/datahost/context"
;;   ;;                               {"@base" "https://example.org/data/"}],
;;   ;;                              "dcterms:title" "A title"}
;;   ;;         augmented-jsonld-doc (sut/normalise-series {:series-slug "new-series"}
;;   ;;                                                    incoming-jsonld-doc)]
;;   ;;     (testing "A series can be created and retrieved via the API"

;;   ;;       (let [response (http/put
;;   ;;                       "http://localhost:3400/data/new-series"
;;   ;;                       {:content-type :json
;;   ;;                        :body (json/write-str incoming-jsonld-doc)})]
;;   ;;         (is (= (:status response) 201))
;;   ;;         (is (= (json/read-str (:body response)) augmented-jsonld-doc)))

;;   ;;       (let [response (http/get "http://localhost:3400/data/new-series")]
;;   ;;         (is (= (:status response) 200))
;;   ;;         (is (= (json/read-str (:body response)) augmented-jsonld-doc))))

;;   ;;     (testing "A series can be updated via the API"
;;   ;;       (let [response (http/put "http://localhost:3400/data/new-series?title=A%20new%20title")]
;;   ;;         (is (= (:status response) 200)))

;;   ;;       (let [response (http/get "http://localhost:3400/data/new-series")]
;;   ;;         (is (= (:status response) 200))
;;   ;;         (is (= (-> response :body json/read-str (get "dcterms:title")) "A new title"))))))
;;   )
