(ns get_schema_and_csv
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.string :as str]))


;;SCHEMA
(defn random-column-name []
  (let [prefixes ["Measure" "Statistical" "Economic" "Age" "Unit" "Upper" "Lower" "Observation" "Quality" "Survey" "Population" "Age" "Income" "Education" "Health" "Employment" "Housing" "Climate" "Environmental" "Transportation"]
        suffixes ["type" "Geography" "Amount" "Max" "Measure" "Confidence" "Status" "Distribution" "Rate" "Index" "Score" "Level" "Ratio" "Value" "Average" "Percentage" "Change" "Trend" "Variation" "Impact"]]
    (str/join " " [(rand-nth prefixes) (rand-nth suffixes)])))


(defn generate-random-schema [num-columns]
  (let [data-types (cycle ["string" "integer" "double"])
        random-index (rand-int num-columns)]
    {"@type" "dh:TableSchema"
     "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180"
     "dcterms:title" (str "Randomly generated w/" num-columns " columns")
     "dh:columns" (->> (map-indexed
                        (fn [index _]
                          (let [name (loop [name (random-column-name) seen #{}]
                                       (if (contains? seen name)
                                         (recur (random-column-name) seen)
                                         name))]
                            {"@type" (if (= index random-index) "dh:MeasureColumn" "dh:DimensionColumn")
                             "csvw:datatype" (if (= index random-index) "integer" (nth data-types index))
                             "csvw:name" name
                             "csvw:titles" name}))
                        (range num-columns))
                       (shuffle))}))


;; (def generated-random-schema (generate-random-schema input-amount))
;; (pprint generated-random-schema)

;; (let [output-path (str "datagen/datasets/rand-schema-" input-amount "-col.json")]
;;   (with-open [writer (io/writer output-path)]
;;     (json/write generated-random-schema writer)))

(defn get-rand-schema
  [output-name x]
  (let [generated-schema (generate-random-schema x)
        output-path (str output-name "-schema.json")]
    (with-open [writer (io/writer output-path)]
      (json/write generated-schema writer))
    generated-schema))



;;CSV
(defn random-rounded [decimal-places]
  (let [random-number (rand)]
    (->> (format (str "%." decimal-places "f") random-number)
         (Double/parseDouble))))

(defn generate-random-data [datatype]
  (case datatype
    "string" (str/join "" (repeatedly (+ (rand-int 23) 3)
                                      #(rand-nth "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789! ?")))
    "integer" (rand-int 10000)
    "double" (str/join "" [(rand-int 10) ;; Integer part
                           "."
                           (str/join "" (repeatedly 4 #(rand-int 10)))])))


(defn generate-column-data [column num-rows]
  (let [title (get column "csvw:titles")
        datatype (get column "csvw:datatype")]
    (cons title (repeatedly num-rows #(generate-random-data datatype)))))

(defn generate-csv [schema filename num-rows]
  (let [columns (get schema "dh:columns")
        data (map #(generate-column-data % num-rows) columns)
        data-transposed (apply map vector data)]
    (with-open [writer (io/writer filename)]
      (csv/write-csv writer data-transposed))))

(defn get-csv [schema output-name y]
  (generate-csv schema (str output-name ".csv") y))



(defn get-data [x y output-name] 
  (def generated-schema (get-rand-schema output-name x))
  (get-csv generated-schema (str output-name) y) 
  )

(defn get-split-data [x y output-name dividedby] 
  (def generated-schema (get-rand-schema output-name x))
  (dotimes [i dividedby]
   (let [csv-output-name (str output-name i)]
     (get-csv generated-schema csv-output-name (/ y dividedby)))))

(get-data 15 10000000 "datagen/tenmil")

(get-split-data 15 10000000 "datagen/tenmil" 10)

;;takes a few minutes for 10mil, let it run or the csv will be incomplete.
;;After they're done you can run the hurl tests in datagen (commands at the top of each)
