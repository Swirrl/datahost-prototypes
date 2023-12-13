(ns tpximpact.datahost.ldapi.datagen.load-test
    (:require [tpximpact.datahost.ldapi.datagen.gen-schema-and-csv :as datagen])
  (:require [tpximpact.datahost.ldapi.datagen.gen-hurl :as hurlgen]))


(def path "test/tpximpact/datahost/ldapi/datagen/issue-310/")

(defn generate [x y repeats filename seed]
  (datagen/get-data x y path filename seed)
  (hurlgen/gen-hurl path filename "A" repeats)
  (hurlgen/gen-hurl path filename "AD" repeats)
)

(defn generate-split [x y splits filename seed]
  (datagen/get-split-data x y path (str filename "-split") splits seed)
  (hurlgen/gen-hurl path (str filename "-split") "CA" splits))
