(ns tpximpact.datahost.ldapi.datagen.tests_and_reports
  ;;Test dataset generation for load/performace experiments and testing. 
  ;; Example usage-  Creating a dataset of 100,000 rows and 15 columns:
  ;; (gen/get-data 15 100000 "test/tpximpact/datahost/ldapi/datagen/hurl_tests/" "FILENAME")
  (:require [tpximpact.datahost.ldapi.datagen.get_schema_and_csv :as gen]))


(def path "test/tpximpact/datahost/ldapi/datagen/hurl_tests/")
(def seed (System/currentTimeMillis))
;; Documentation
;;  usage-  Creating a dataset of 100,000 rows and 15 columns:
;;     (gen/get-data 15 100000 path "FILENAME")
;; This will create FILENAME-schema.json and FILENAME.csv in datagen/hurl_tests.

;;  usage-  Creating 10 datasets of 10,000 rows each and 15 columns:
;;     (gen/get-split-data 15 100000 path "FILENAME" 10)
;; This will create FILENAME-schema.json
;; and FILENAME0.csv, FILENAME1.csv ... FILENAME9.csv in datagen/hurl_tests.

;; testing -
;; Currently the hurl scripts are not auto generated. 
;; There are three different types of test in datagen/hurl_tests
;; All the tests create a new series, release, revision and add the schema.
;; Then, they either: 
;; 1. Single append of a single csv.
;; 2. Append a csv and then delete it, then repeat that cycle a number of times.
;; 3. Consecutive appends of multiple different csv. (for use with get-split-data)
;; To create a hurl test - copy and paste an existing test to a new hurl file
;; and change the file names to match yours. gen_big_hurl.clj can be used
;; when there are a large number of requests to edit.

;; NOTE - It takes a few minutes to create data for 10mil, let it run or the csv will be incomplete.

;; report on some basic cases -

(gen/get-data 15 10000000 path "tenmil" seed)
;; hurl issue-310-test10mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 - Error1.txt

(gen/get-split-data 15 10000000 path "tenmil" 10 seed)
;; hurl issue-310-test10mil10.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;;Consecutive Appends
;; Error 500 on 3rd Append ~ Error 1.

(gen/get-split-data 15 10000000 path "tenmil" 100 seed)
;; hurl issue-310-test10mil100.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Consecutive Appends
;; Error 500 on 37th Append ~ Error 1.

(gen/get-data 15 5000000 path "fivemil" seed)
;; hurl issue-310-test5mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 ~ Error 1.

(gen/get-data 15 4000000 path "fourmil" seed)
;; hurl issue-310-test4mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append 
;; Error 500 ~ Error 1.

(gen/get-data 15 3000000 path "threemil" seed)
;; hurl issue-310-test3mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append
;; Successful

;; issue-310-test3milAD.hurl
;; Append Delete cycle
;; Error 500 on the 1st delete ~ Error 1.

(gen/get-data 15 2000000 path "twomil" seed)
;; hurl issue-310-test2mil.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append
;; Successful

;; hurl issue-310-test2milAD.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append Delete cycle
;; Inconsistent resutls - sometimes 500 on the 1st or 4th delete
;; other times it can do 4 + cycles successfully ~ Error 1.

(gen/get-data 15 1000000 path "onemil" seed)
;; hurl issue-310-test1milAD.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token="string" --variable series="dummy$(date +%s)"
;; Append Delete cycle
;; Successful for 10 cycles (takes a long while)


;;Errors:

;;1. java.lang.OutOfMemoryError: Java heap space
;;  	at java.base/java.util.Arrays.copyOf(Arrays.java:3585)
;;  	at ham_fisted.ArrayLists$IntArrayList.ensureCapacity(ArrayLists.java:966)
;;  	at ham_fisted.ArrayLists$IntArrayList.addLong(ArrayLists.java:973)
;;  	at ham_fisted.LongMutList$2.invokePrim(LongMutList.java:72)
;;  	at ham_fisted.IFnDef$OLO.invoke(IFnDef.java:565)
;;  	at clojure.core.protocols$naive_seq_reduce.invokeStatic(protocols.clj:62)
;;  	at clojure.core.protocols$interface_or_naive_reduce.invokeStatic(protocols.clj:72)
;;  	at clojure.core.protocols$fn__8249.invokeStatic(protocols.clj:169)
;;  	at clojure.core.protocols$fn__8249.invoke(protocols.clj:124)
;;  	at clojure.core.protocols$fn__8204$G__8199__8213.invoke(protocols.clj:19)
;;  	at clojure.core.protocols$seq_reduce.invokeStatic(protocols.clj:31)
;;  	at clojure.core.protocols$fn__8236.invokeStatic(protocols.clj:75)
;;  	at clojure.core.protocols$fn__8236.invoke(protocols.clj:75)
;;  	at clojure.core.protocols$fn__8178$G__8173__8191.invoke(protocols.clj:13)
;;  	at ham_fisted.Reductions.serialRe
;;  duction(Reductions.java:84)
;;  	at ham_fisted.LongMutList.addAllReducible(LongMutList.java:70)
;;  	at ham_fisted.ArrayLists$IntArrayList.addAllReducible(ArrayLists.java:999)
;;  	at tech.v3.datatype.array_buffer$array_sub_list.invokeStatic(array_buffer.clj:652)
;;  	at tech.v3.datatype.array_buffer$array_sub_list.invoke(array_buffer.clj:628)
;;  	at tech.v3.datatype.copy_make_container$eval45276$fn__45277.invoke(copy_make_container.clj:38)
;;  	at clojure.lang.MultiFn.invoke(MultiFn.java:244)
;;  	at tech.v3.datatype.copy_make_container$make_container.invokeStatic(copy_make_container.clj:105)
;;  	at tech.v3.datatype.copy_make_container$make_container.invoke(copy_make_container.clj:96)
;;  	at tech.v3.datatype.copy_make_container$__GT_array.invokeStatic(copy_make_container.clj:176)
;;  	at tech.v3.datatype.copy_make_container$__GT_array.invoke(copy_make_container.clj:158)
;;  	at tech.v3.datatype.copy_make_container$__GT_int_array.invokeStatic(copy_make_container.clj:212)
;;  	at tech.v3.datatype.copy_make_container$__GT_int_array.invoke(copy_make_contai
