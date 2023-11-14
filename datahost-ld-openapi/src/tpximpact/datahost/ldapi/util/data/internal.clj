(ns tpximpact.datahost.ldapi.util.data.internal
  "Internal namespace: Do not use outside of *.ldapi.util.data.*"
  )


(def hash-column-name
  "Name of the column holding the hash of the row (excluding the measurement itself.)"
  "datahost.row/hash")

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
