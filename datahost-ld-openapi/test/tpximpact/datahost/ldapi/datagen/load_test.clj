(ns tpximpact.datahost.ldapi.datagen.load-test
    (:require [tpximpact.datahost.ldapi.datagen.gen-schema-and-csv :as datagen])
  (:require [tpximpact.datahost.ldapi.datagen.gen-hurl :as hurlgen]))


(def path "test/tpximpact/datahost/ldapi/datagen/issue-310/")
(def seed (System/currentTimeMillis))
;; Documentation

;; GENERATE DATA
;; - (generate x y repeats filename seed)
;; x: int number of desired columns
;; y: int number of desired rows
;; repeats: int (only relevant for AD)
;; filename: string with no spaces
;; seed: int used for regenerating the same dataset again

;; This will generate a schema and corresponding csv in datagen/issue-310
;; It will also generate two hurl scripts:
;; A: Append, tests a single append
;; AD: Append Delete, appends and deletes the csv for as many repeats as specified

;; GENERATE SPLIT DATA
;; - (generate-split x y splits filename seed repeats)
;; splits: int how many csv's the data should be divided into

;; This will generate a schema and a number corresponding csv's in datagen/issue-310
;; It will also generate a hurl script:
;; CA: Consecutive Appends, posts all the split csvs one after the other

;; NOTES
;; - The command for running the hurl scripts can be found at the top of each one
;; - It takes a few minutes to create data for > 5 mil, let it run or the csv will be incomplete.

(defn generate [x y repeats filename seed]
  (datagen/get-data x y path filename seed)
  (hurlgen/gen-hurl path filename "A" repeats)
  (hurlgen/gen-hurl path filename "AD" repeats)
)

(defn generate-split [x y splits filename seed]
  (datagen/get-split-data x y path (str filename "-split") splits seed)
  (hurlgen/gen-hurl path (str filename "-split") "CA" splits))
