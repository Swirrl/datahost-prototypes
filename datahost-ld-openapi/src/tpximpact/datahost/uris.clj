(ns tpximpact.datahost.uris
  (:import (java.net URI)))

(defrecord ColumnTypes [measure dimension attribute])

(def ^:private col-types-base "https://publishmydata.com/def/datahost")

(def column-types (->ColumnTypes (URI. (str col-types-base "/MeasureColumn"))
                                 (URI. (str col-types-base "/DimensionColumn"))
                                 (URI. (str col-types-base "/AttributeColumn"))))
