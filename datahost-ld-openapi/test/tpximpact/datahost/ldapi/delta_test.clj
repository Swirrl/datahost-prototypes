(ns tpximpact.datahost.ldapi.delta-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [tpximpact.test-helpers :as th])
  (:import (java.io BufferedReader StringReader)))


(comment deftest delta-route-test
  (th/with-system-and-clean-up
   {{:keys [GET POST PUT] :as http-client} :tpximpact.datahost.ldapi.test/http-client :as sys}
   (testing "Single measure CSV files produce Reified TX formatted delta log"
     (let [base-csv-file (io/file (io/resource "test-inputs/delta/orig.csv"))
           delta-csv-file (io/file (io/resource "test-inputs/delta/new.csv"))
           delta-api-response (POST "/delta"
                                     {:multipart [(th/multipart "base-csv" base-csv-file "text/csv")
                                                  (th/multipart "delta-csv" delta-csv-file "text/csv")]})
           ;; using line seqs overcomes any line-ending differences
           expected-delta-seq (line-seq (io/reader (io/resource "test-inputs/delta/delta-output.csv")))
           delta-resp-body-seq (line-seq (BufferedReader. (StringReader. (:body delta-api-response))))]
       (is (= 200 (:status delta-api-response)))
       (is (= expected-delta-seq delta-resp-body-seq))))))
