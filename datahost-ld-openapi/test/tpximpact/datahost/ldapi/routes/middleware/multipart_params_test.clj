(ns tpximpact.datahost.ldapi.routes.middleware.multipart-params-test
  "From: ring.middleware.test.multipart-params tag 1.10.0"
  (:require [clojure.test :refer :all]
            [tpximpact.datahost.ldapi.routes.middleware.multipart-params :as mp :refer :all]
            [tpximpact.datahost.ldapi.router :refer [leave-keys-alone-muuntaja-coercer]]
            [ring.middleware.multipart-params.byte-array :refer :all]
            [ring.util.io :refer [string-input-stream]]
            [reitit.coercion.malli :as rcm]
            [muuntaja.core :as m]))

(defn wrap-multipart-params
  ([handler]
   (wrap-multipart-params handler {}))
  ([handler {:keys [parameters] :as options}]
   (let [reitit {:parameters (or parameters {:multipart {}})
                 :coercion (rcm/create)
                 :muuntaja leave-keys-alone-muuntaja-coercer}
         reitit-middleware (@#'mp/compile-multipart-middleware options)
         middleware (:wrap (reitit-middleware reitit {}))]
     (middleware handler))))

(defn string-store [item]
  (-> (select-keys item [:filename :content-type])
      (assoc :body (slurp (:stream item)))))

(deftest test-wrap-multipart-params
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"upload\"; filename=\"test.txt\"\r\n"
                       "Content-Type: text/plain\r\n\r\n"
                       "foo\r\n"
                       "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"baz\"\r\n\r\n"
                       "qux\r\n"
                       "--XXXX--")
        handler (wrap-multipart-params identity {:store string-store})
        request {:headers {"content-type" "multipart/form-data; boundary=XXXX"
                           "content-length" (str (count form-body))}
                 :params {"foo" "bar"}
                 :body (string-input-stream form-body)}
        response (handler request)]
    (is (= (get-in response [:params "foo"]) "bar"))
    (is (= (get-in response [:params :baz :body]) "qux"))
    (let [upload (get-in response [:params :upload])]
      (is (= (:filename upload)     "test.txt"))
      (is (= (:content-type upload) "text/plain"))
      (is (= (:body upload)         "foo")))))

(deftest test-multiple-params
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "bar\r\n"
                       "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "baz\r\n"
                       "--XXXX--")
        handler (wrap-multipart-params identity {:store string-store})
        request {:headers {"content-type" "multipart/form-data; boundary=XXXX"
                           "content-length" (str (count form-body))}
                 :body (string-input-stream form-body)}
        response (handler request)]
    (is (= (mapv :body (get-in response [:params :foo]))
           ["bar" "baz"]))))

(defn all-threads []
  (.keySet (Thread/getAllStackTraces)))

(deftest test-multipart-threads
  (testing "no thread leakage when handler called"
    (let [handler (wrap-multipart-params identity)]
      (dotimes [_ 200]
        (handler {}))
      (is (< (count (all-threads))
             100))))

  (testing "no thread leakage from default store"
    (let [form-body (str "--XXXX\r\n"
                         "Content-Disposition: form-data;"
                         "name=\"upload\"; filename=\"test.txt\"\r\n"
                         "Content-Type: text/plain\r\n\r\n"
                         "foo\r\n"
                         "--XXXX--")]
      (dotimes [_ 200]
        (let [handler (wrap-multipart-params identity)
              request {:headers {"content-type" "multipart/form-data; boundary=XXXX"
                                 "content-length" (str (count form-body))}
                       :body (string-input-stream form-body)}]
          (handler request))))
    (is (< (count (all-threads))
           100))))

(deftest wrap-multipart-params-cps-test
  (let [handler   (wrap-multipart-params (fn [req respond _] (respond req)))
        form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "bar\r\n"
                       "--XXXX--")
        request   {:headers {"content-type" "multipart/form-data; boundary=XXXX"}
                   :body    (string-input-stream form-body "UTF-8")}
        response  (promise)
        exception (promise)]
        (handler request response exception)
        (is (= (get-in @response [:multipart-params :foo :body])
               "bar"))
        (is (not (realized? exception)))))

(deftest multipart-params-request-test
  (is (fn? multipart-params-request)))

(deftest decode-with-utf8-by-default
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "Øæßç®£èé\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX")}
                 :body (string-input-stream form-body "UTF-8")}
        request* (multipart-params-request request)]
        (is (= (get-in request* [:multipart-params :foo :body])
               "Øæßç®£èé"))))

(deftest parts-may-have-invidual-charsets-in-content-type
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n"
                       "Content-Type: text/plain; charset=ISO-8859-15\r\n\r\n"
                       "äÄÖöÅå€\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX")}
                 :body (string-input-stream form-body "ISO-8859-15")}
        request* (multipart-params-request request)]
    (is (= (get-in request* [:multipart-params :foo :body])
           "äÄÖöÅå€"))))

(deftest charset-may-be-defined-html5-style-parameter
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "Øæßç®£èé\r\n"
                       "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"_charset_\"\r\n\r\n"
                       "UTF-8\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX; charset=US-ASCII")}
                 :body (string-input-stream form-body "UTF-8")}
        request* (multipart-params-request request)]
    (is (= (get-in request* [:multipart-params :foo :body]) "Øæßç®£èé"))))

(deftest form-field-recognition-test
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"upload\"; filename=\"test.txt\"\r\n"
                       "Content-Type: text/plain\r\n\r\n"
                       "foo\r\n"
                       "--XXXX--")
        handler (wrap-multipart-params identity {:store (byte-array-store)})
        request {:headers {"content-type" "multipart/form-data; boundary=XXXX"
                           "content-length" (str (count form-body))}
                 :body (string-input-stream form-body)}
        response (handler request)]
    (let [upload (get-in response [:multipart-params :upload])]
      (is (java.util.Arrays/equals (:bytes upload) (.getBytes "foo"))))))

(deftest forced-encoding-option-works
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n"
                       "Content-Type: application/json; charset=UTF-8\r\n\r\n"
                       "{\"åå\":\"ÄÖ\"}\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX; charset=US-ASCII")}
                 :body (string-input-stream form-body "ISO-8859-1")}
        request* (multipart-params-request request {:encoding "ISO-8859-1"})]
    (is (= (get-in request* [:multipart-params :foo :body])
           "{\"åå\":\"ÄÖ\"}"))))

(deftest fallback-encoding-option-works
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"foo\"\r\n\r\n"
                       "äÄÖöÅå€\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX; charset=US-ASCII")}
                 :body (string-input-stream form-body "ISO-8859-15")}
        request* (multipart-params-request request {:fallback-encoding "ISO-8859-15"})]
    (is (= (get-in request* [:multipart-params :foo :body]) "äÄÖöÅå€"))))

(deftest test-enforced-limits
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"upload\"; filename=\"test1.txt\"\r\n"
                       "Content-Type: text/plain\r\n\r\n"
                       "foobarbaz\r\n"
                       "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"upload\"; filename=\"test2.txt\"\r\n"
                       "Content-Type: text/plain\r\n\r\n"
                       "foobar\r\n"
                       "--XXXX--")
        headers       {"content-type" (str "multipart/form-data; boundary=XXXX")}
        handler       (constantly {:status 200, :headers {}, :body "OK"})
        handler-async (fn [_ respond _]
                        (respond {:status 200, :headers {}, :body "OK"}))]
    (is (thrown? org.apache.commons.fileupload.FileUploadBase$FileUploadIOException
                 (multipart-params-request
                  {:headers headers, :body (string-input-stream form-body)}
                  {:max-file-size 6})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (multipart-params-request
                  {:headers headers, :body (string-input-stream form-body)}
                  {:max-file-count 1})))
    (let [response ((wrap-multipart-params handler {:max-file-size 9})
                    {:headers headers, :body (string-input-stream form-body)})]
      (is (= 200 (:status response))))
    (let [response ((wrap-multipart-params handler {:max-file-count 2})
                    {:headers headers, :body (string-input-stream form-body)})]
      (is (= 200 (:status response))))))

(deftest blob-json-parsing
  (let [form-body (str "--XXXX\r\n"
                       "Content-Disposition: form-data;"
                       "name=\"json\"; filename=\"blob\"\r\n"
                       "Content-Type: application/json;\r\n\r\n"
                       "{}\r\n"
                       "--XXXX--")
        request {:headers {"content-type"
                           (str "multipart/form-data; boundary=XXXX")}
                 :body (string-input-stream form-body "UTF-8")}
        handler (wrap-multipart-params identity {:parameters {:multipart {:json [:any]}}})
        request* (handler request)]
    (is (= (get-in request* [:multipart-params :json]) {}))
    (is (= (get-in request* [:parameters :multipart :json]) {}))))
