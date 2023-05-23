(ns tpximpact.datahost.scratch.release.schema
  (:require [clojure.set :as set]
            [malli.core :as m]
            [malli.transform :as mt]))

;; TODO

(def ^{:spec "https://w3c.github.io/csvw/metadata/#column-name"}
  uritemplate:variable
  ;; not a perfect implementation but should be good enough for prototype
  [:and
   [:string {:min 1}]
   [:re #"^[a-z,A-Z,\\%,0-9,_]+$"]])

(def ^{:spec "https://w3c.github.io/csvw/metadata/#built-in-datatypes"}
  supported-datatypes
  "We currently just support a subset of CSVW here. Commented out ones
  are expected on some future timeframe."
  ["string" "float" "boolean" "integer" "double"
   #_"anyURI" #_"date" #_"dateTime" #_"dateTimeStamp"
   #_"duration" #_"dayTimeDuration" #_"yearMonthDuration"
   #_"time" #_"gDay" #_"gMonthDay" #_"gMonth"
   #_"gYear" #_"gYearMonth"])

(def dh:ColumnSpec [:map
                    ["@type" [:enum {:optional true} "dh:DimensionColumn" "dh:MeasureColumn" "dh:AttributeColumn"
                              #_"dh:RequiredAttribute" ;; might want to distinguish this too as it's part of QB.  If set it would imply csvw:required.
                              ;; dh:DimensionColumn and dh:MeasureColumn should also imply csvw:required.
                              ]]
                    ["csvw:datatype" [:enum "string"]]
                    ["csvw:name" uritemplate:variable]
                    ["csvw:titles" [:or :string [:sequential :string]]]
                    ["csvw:required" [:boolean {:default false}]]])

;;(m/coerce :int "52" mt/string-transformer)

(def dh:TableSchemaSchema [:map
                           ["@type" [:enum "dh:TableSchema"]]
                           ["dh:columns" [:sequential dh:ColumnSpec]]
                           ["csvw:dialect" [:map {:default {"header" true}}
                                            ["header" {:default true} :boolean]]]])

(comment
  ;; expand defaults
  (m/decode dh:TableSchemaSchema {} mt/default-value-transformer))

(comment

  (def example-schema {"@type" "dh:TableSchema"
                       "dh:columns" [{"csvw:datatype" "string"
                                      "csvw:name" "foo_bar"
                                      "csvw:titles" ["Foo Bar"]
                                      "@type" "dh:DimensionColumn"}]})

  (m/validate dh:TableSchemaSchema example-schema)

  )

(def csvw-types->malli-types
  {
   "string" :string
   "float" :double ;; Alias to double for now
   "double" :double
   "integer" :int ;; Malli :int is actually anything in the range of a Java Long
   })

(defn build-malli-cell-schema [colspec]
  (get csvw-types->malli-types (colspec "csvw:datatype") :any))

(defn build-malli-table-schema [dh-schema]
  (let [colspecs (get-in dh-schema ["dh:columns"])
        row-schema (->> colspecs
                        (map build-malli-cell-schema)
                        (cons :tuple)
                        (vec))]

    [:sequential row-schema]))

(let [coercion-properties #{"csvw:null" "csvw:default" "csvw:separator" "csvw:ordered"}
      transformation-properties #{"csvw:aboutUrl" "csvw:propertyUrl" "csvw:valueUrl" "csvw:virtual" "csvw:suppressOutput"}]

  (def reserved-schema-properties
    "These csvw properties are currently intentional unsupported, but
  some may be supported in the future.

  For example we're hoping to automatically define and generate
  aboutUrl's from the CSV when we know the type of all columns in the
  cube.

  We also currently exclude any CSVW properties to do with coercion
  because the semantics of coercion are a little fuzzy when a schema
  is used for both input and output purposes... i.e. are the
  expectations that we apply the coercion to the input data at
  ingestion, or leave the data uncoerced and rely on users applying
  them from the CSVW? The later adds a burden to users, whilst the
  former means these properties are then redundant at output time.

  For now it seems simpler to avoid the issues and assume we don't do
  coercion with the schemas as these coercions can also in principle
  be done upstream.

  "
    (set/union coercion-properties transformation-properties)))

(defn normalise-schema [release schema]
  {"@type" #{"dh:TableSchema" "dh:UserSchema"} ; | dh:RevisionSchema
   "@context" ["https://publishmydata.com/def/datahost/context"
               {"@base" "https://example.org/data/my-dataset-series/2018/schema/"}]
   "@id" "2018"
   "dh:columns" [{"csvw:datatype" "string" ;; should support all/most csvw datatype definitions
                  "csvw:name" "sex"
                  "csvw:titles" ["Sex"]
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
                  }]})

(defn derive-revision-schema
  "Derive a dh:RevisionSchema from a dh:CubeSchema"
  [cube-schema]
  )
