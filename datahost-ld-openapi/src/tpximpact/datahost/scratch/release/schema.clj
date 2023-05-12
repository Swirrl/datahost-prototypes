(ns tpximpact.datahost.scratch.release.schema
  (:require [clojure.set :as set]))

;; TODO


(let [coercion-properties #{"csvw:null" "csvw:default" "csvw:separator" "csvw:ordered"}
      transformation-properties #{"csvw:aboutUrl" "csvw:propertyUrl" "csvw:valueUrl" "csvw:virtual" "csvw:suppressOutput"}]


  (def reserved-schema-properties
    "These csvw properties are currently intentional unsupported, but some may
  be supported in the future.

  For example we're hoping to automatically define and generate
  aboutUrl's from the CSV when we know the type of all columns in the
  cube."
    (set/union coercion-properties transformation-properties)))

(defn normalise-schema [release schema]
  {"@type" #{"dh:TableSchema" "dh:UserSchema"} ; | dh:RevisionSchema
   "@context" ["https://publishmydata.com/def/datahost/context"
               {"@base" "https://example.org/data/my-dataset-series/2018/schema/"}]
   "@id" "2018"
   "dh:columns" [{"csvw:datatype" "string" ;; should support all/most csvw datatype definitions
                  "csvw:name" "sex"
                  "csvw:title" "Sex"
                  "@type" "dh:DimensionColumn" ;; | "dh:MeasureColumn" | "dh:AttributeColumn"
                  ;;"csvw:ordered" false
                  ;;"csvw:virtual" true

                  ;;"csvw:aboutUrl" "uri-template"
                  ;;"csvw:propertyUrl" "uri-template"
                  ;;"csvw:valueUrl" "uri-template"

                  ;;"csvw:required" true
                  ;;"csvw:separator" ";"
                  ;;"csvw:suppressOutput" false
                  ;;"csvw:textDirection" "ltr"
                  ;;"csvw:transformations" []
                  ;;
                  ;;"csvw:default" "default-value"
                  ;;"csvw:null" "n/a"


                  ;;"csvw:lang" "en"


                  }]}
  )

(defn derive-revision-schema
  "Derive a dh:RevisionSchema from a dh:CubeSchema"
  [cube-schema]
  )
