(ns tpximpact.datahost.ldapi.datagen.gen-schema-and-csv
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.string :as str]))

(def ^:dynamic *rand* clojure.core/rand)

(defn rand-1
  ([]
   (*rand* 1))
  ([n]
   (*rand* n)))

(defn rand-2
  "Generate a random integer between 0 (inclusive) and n (exclusive) or 1 if no argument is provided."
  ([] (int (rand-1)))
  ([n] (int (rand-1 n))))


(defmacro with-rand-seed
  "Sets seed for calls to random in body. Beware of lazy seqs!"
  [seed & body]
  `(let [g# (java.util.Random. ~seed)]
     (binding [*rand* #(* % (.nextFloat g#))]
       (with-redefs [rand rand-1 rand-int rand-2]
         ~@body))))

(defn create-directory-if-not-exists [directory-path]
  (let [dir (io/file directory-path)]
    (when-not (.exists dir)
      (.mkdirs dir))))

;;SCHEMA
(defn random-column-name []
  (let [prefixes ["Measure" "Statistical" "Economic" "Age" "Unit" "Upper" "Lower" "Observation" "Quality" "Survey" "Population" "Age" "Income" "Education" "Health" "Employment" "Housing" "Climate" "Environmental" "Transportation"]
        suffixes ["type" "Geography" "Amount" "Max" "Measure" "Confidence" "Status" "Distribution" "Rate" "Index" "Score" "Level" "Ratio" "Value" "Average" "Percentage" "Change" "Trend" "Variation" "Impact"]]
    (str/join " " [(nth prefixes
                        (rand-int (count prefixes))) (nth suffixes
                                                          (rand-int (count suffixes)))])))

(defn generate-random-schema [num-columns seed]
  (let [data-types (cycle ["string" "integer" "double"])
        random-index (rand-int num-columns)]
    {"@type" "dh:TableSchema"
     "appropriate-csvw:modeling-of-dialect" "UTF-8,RFC4180"
     "dcterms:title" (str "Randomly generated w/ seed " seed " and " num-columns " columns")
     "dh:columns" (let [seen (atom #{})]  ; Use an atom to allow modification across loop iterations
                    (->> (map-indexed
                          (fn [index _]
                            (let [name (loop [name (random-column-name) seen seen]
                                         (if (contains? @seen name)
                                           (recur (random-column-name) seen)
                                           (do
                                             (swap! seen conj name)
                                             name)))]
                              {"@type" (if (= index random-index) "dh:MeasureColumn" "dh:DimensionColumn")
                               "csvw:datatype" (if (= index random-index) "integer" (nth data-types index))
                               "csvw:name" name
                               "csvw:titles" name}))
                          (range num-columns))
                         ))}))

(defn get-rand-schema
  [path-name output-name x seed]
  (let [generated-schema ( generate-random-schema x seed)
        output-path (str path-name "schema-" output-name ".json")]
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
                                      #(nth "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789! ?" (rand-int 64))))
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

(defn get-csv [schema path-name output-name y]
  (generate-csv schema (str path-name output-name ".csv") y))

(defn get-data [x y path-name output-name seed]
  (create-directory-if-not-exists path-name)
  (let [generated-schema (with-rand-seed seed (get-rand-schema path-name output-name x seed))]
    (with-rand-seed seed (get-csv generated-schema path-name (str output-name) y))))

(defn get-split-data-seeded [x y path-name output-name dividedby seed]
  (let [generated-schema (get-rand-schema path-name output-name x seed)]
    (dotimes [i dividedby]
      (let [csv-output-name (str output-name i)] 
        (get-csv generated-schema path-name csv-output-name (/ y dividedby))))))

(defn get-split-data [x y path-name output-name dividedby seed]
  (create-directory-if-not-exists path-name)
  (with-rand-seed seed (get-split-data-seeded x y path-name output-name dividedby seed)))
