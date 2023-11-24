(ns tpximpact.datahost.ldapi.util.data.internal
  "Internal namespace: Do not use outside of *.ldapi.util.data.*"
  (:refer-clojure :exclude [hash])
  (:require
   [malli.core :as m]
   [tablecloth.api :as tc]
   [tpximpact.datahost.uris :as uris])
  (:import
   (net.openhft.hashing LongHashFunction)))


(def hash-column-name
  "Name of the column holding the hash of the row (excluding the measurement itself.)"
  "datahost.row/coords")

(def op-column-name
  "Operation: APPEND|RETRACT|CORRECT"
  "dh/op")
(def parent-column-name "dh/parent")
(def tx-column-name "dh/tx")
(def id-column-name hash-column-name)   ;TODO: remove?

(defn r-joinable-name
  "Returns (right) column name suitable for use in joins.

  Motivation: we need to take dataset name into account."
  [ds-name col-name]
  (if (= "_unnamed" ds-name)
    (str "right." col-name)
    (str ds-name "." col-name)))


;;; ---- COLUMN HASH/ID

(def ^:private hash-fn (LongHashFunction/xx128low))

(defn make-hasher-input
  "Returns a StringBuilder"
  [^StringBuilder sb columns]
  (.append sb "|")
  (loop [columns columns]
    (if-not (seq columns)
      sb
      (do
        (.append sb (first columns))
        (.append sb "|")
        (recur (next columns))))))

(defn hash [^StringBuilder sb]
  (.hashChars ^LongHashFunction hash-fn sb))

(defn make-columnwise-hasher
  "Returns a function seq -> long"
  []
  (fn hasher* [& columns]
    (let [^StringBuilder sb (StringBuilder.)
          h ^LongHashFunction hash-fn]
      (.hashChars h (make-hasher-input sb columns)))))

(def ^:private measure-type-uri (:measure uris/column-types))

(defn- make-column-info-xform
  [filter-fn]
  (comp (map m/properties)
        (filter filter-fn)
        (map #(get % :dataset.column/name))))

(def ^:private tuple-schema-component-names-xform
  "Transducer transforming elements of [[m/children]] output into strings."
  (make-column-info-xform #(not= measure-type-uri (get % :dataset.column/type))))

(def ^:private tuple-schema-measure-names-xform
  "Transducer transforming elements of [[mu/children]] output into strings."
  (make-column-info-xform #(= measure-type-uri (get % :dataset.column/type))))

(defn check-dataset-has-columns
  [dataset column-names]
  (when-not (every? #(tc/has-column? dataset %) column-names)
    (throw (ex-info "Not every required column is in the dataset"
                    {:dataset-columns (tc/column-names dataset)
                     :schema-columns column-names}))))

(defn- row-schema--extract-component-column-names
  "Returns a seq of column names. Throws when any of the component
  columns extracted from release-schema are not present in the
  dataset."
  [row-schema]
  (sort (sequence tuple-schema-component-names-xform
                  (m/children row-schema))))

(defn- row-schema--extract-measure-column-name
  [row-schema]
  {:post [some?]}
  (first (sort (sequence tuple-schema-measure-names-xform
                         (m/children row-schema)))))

(defn add-id-column
  "Adds a column column containing unique id of the measurment
  (based on component names from row-schema)."
  [dataset row-schema]
  (assert row-schema)
  (let [col-names (row-schema--extract-component-column-names row-schema)
        hasher (make-columnwise-hasher)]
    (check-dataset-has-columns dataset col-names)
    (-> dataset
        (tc/map-columns hash-column-name
                        :long
                        col-names
                        hasher)
        (vary-meta assoc ::hash-column hash-column-name))))

(defn ensure-id-column
  [dataset row-schema]
  (if (tc/has-column? dataset hash-column-name)
    dataset
    (add-id-column dataset row-schema)))

;;; ---- ROW SCHEMA UTILS

(defn make-schema-context
  "Returns {:hashable-columns seq<COL-NAME> :measure-column COL-NAME}.

  Motivation: get metadata necessary do diff two datasets (like name
  of the measure column or attribute, and dimension columns)."
  [row-schema]
  {:row-schema row-schema
   :hashable-columns (row-schema--extract-component-column-names row-schema)
   :measure-column (row-schema--extract-measure-column-name row-schema)})

(defn make-change-context
  [change-kind row-schema]
  {:row-schema row-schema
   :datahost.change/kind change-kind
   :hashable-columns (row-schema--extract-component-column-names row-schema)
   :measure-column (row-schema--extract-measure-column-name row-schema)})
