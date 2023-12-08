(ns tests_and_reports
  (:require [get_schema_and_csv :as gen]))


;; takes a few minutes to create data for 10mil, let it run or the csv will be incomplete.

(gen/get-data 15 10000000 "datagen/hurl_tests/tenmil")
;; hurl test10mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 - Error1.txt

(gen/get-split-data 15 10000000 "datagen/hurl_tests/tenmil" 10)
;; hurl test10mil10.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;;Consecutive Appends
;; Error 500 on 3rd Append ~ Error1.txt

(gen/get-split-data 15 10000000 "datagen/hurl_tests/tenmil" 100)
;; hurl test10mil100.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Consecutive Appends
;; Error 500 on 37th Append ~ Error1.txt

(gen/get-data 15 5000000 "datagen/hurl_tests/fivemil")
;; hurl test5mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 ~ Error1.txt

(gen/get-data 15 4000000 "datagen/hurl_tests/fourmil")
;; hurl test4mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 ~ Error1.txt

(gen/get-data 15 3000000 "datagen/hurl_tests/threemil")
;; hurl test3mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append
;; Successful

;; test3milAD.hurl
;; Append Delete cycle
;; Error 500 on the 1st delete ~ Error1.txt

(gen/get-data 15 2000000 "datagen/hurl_tests/twomil")
;; hurl test2mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append
;; Successful

;; hurl test2milAD.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append Delete cycle
;; Inconsistent resutls - sometimes 500 on the 1st or 4th delete
;; other times it can do 4 + cycles successfully ~ Error1.txt

(gen/get-data 15 1000000 "datagen/hurl_tests/onemil")
;; hurl test1milAD.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append Delete cycle
;; Successful for 10 cycles (takes a long while)