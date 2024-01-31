(ns tpximpact.datahost.uris
  (:require [clojure.set :as set])
  (:import (java.net URI)))

(defrecord ColumnTypes [measure dimension attribute])

(def ^:private col-types-base "https://publishmydata.com/def/datahost")

(def column-types (->ColumnTypes (URI. (str col-types-base "/MeasureColumn"))
                                 (URI. (str col-types-base "/DimensionColumn"))
                                 (URI. (str col-types-base "/AttributeColumn"))))

(def json-column-types
  (set/rename-keys column-types {:dimension "dh:DimensionColumn"
                                 :attribute "dh:AttributeColumn"
                                 :measure "dh:MeasureColumn"}))

