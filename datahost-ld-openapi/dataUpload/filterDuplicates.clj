(ns filterDuplicates
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :refer [difference]]))

(defn read-csv [file-path]
  (with-open [reader (io/reader file-path)]
    (doall (csv/read-csv reader))))

(defn write-csv [file-path data]
  (with-open [writer (io/writer file-path)]
    (csv/write-csv writer data)))

(defn filter-duplicates [data]
  (let [dimension-columns (rest (map first data)) ; Assuming the first column is 'measure'
        data-set (set data)
        grouped-data (group-by (fn [row] (map (partial select-keys row) dimension-columns)) data-set)
        non-duplicate-rows (apply concat (vals (difference data-set (set (map set (vals grouped-data))))))]
    non-duplicate-rows))

(defn main []
  (let [csv-file-path "dataUpload/Permanent-dwellings-completed-England-District-By-Tenure.csv"
        output-file-path "filtered_output.csv"]
    (let [csv-data (read-csv csv-file-path)
          filtered-data (filter-duplicates csv-data)]
      (write-csv output-file-path filtered-data))))

(main)
