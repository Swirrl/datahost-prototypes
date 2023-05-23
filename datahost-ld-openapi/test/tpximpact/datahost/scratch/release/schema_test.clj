(ns tpximpact.datahost.scratch.release.schema-test
  (:require [tpximpact.datahost.scratch.release.schema :as sut]
            [clojure.test :as t]
            [clojure.set :as set]
            [malli.core :as m]
            [malli.transform :as mt]))

;; TODO test defaults
(m/validate sut/dh:ColumnSpec

              (m/decode sut/dh:ColumnSpec
                        {"csvw:datatype" "string"
                         "csvw:name" "foo_bar"
                         "csvw:titles" ["Foo Bar"]
                         "@type" "dh:DimensionColumn"}
                        mt/default-value-transformer)

              )
