(ns tpximpact.datahost.scratch.delta-tool
  (:require
   [tablecloth.api :as tc]))

;; A dataset -- the current version with correct data
(def original-ds (tc/dataset
         {"Area" ["W06000022" "W06000022" "W06000022" "W06000022"]
          "Period" ["2004-01-01T00:00:00/P3Y"
                    "2004-01-01T00:00:00/P3Y"
                    "2005-01-01T00:00:00/P3Y"
                    "2005-01-01T00:00:00/P3Y"]
          "Sex" ["Female" "Male" "Female" "Male"]
          "Life Expectancy" [80.7 77.1 80.9 72.9]}))

;; The new dataset -- one correction, one append, one delete
;; note the updated life expectancy for the last row (72.9 -> 73.9),
;; deleted first row, and new second row
(def new-ds (tc/dataset
                  {"Area" ["W06000022" "W06000022" "W06000022" "W06000022"]
                   "Period" ["2006-01-01T00:00:00/P3Y"
                             "2004-01-01T00:00:00/P3Y"
                             "2005-01-01T00:00:00/P3Y"
                             "2005-01-01T00:00:00/P3Y"]
                   "Sex" ["Female" "Male" "Female" "Male"]
                   "Life Expectancy" [81.7 77.1 80.9 73.9]}))

(def default-schema
  {:columns [{:name "Area"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? (fn [v] (= 9 (count v)))}
             {:name "Period"
              :datatype :period
              :coltype :qb/dimension
              :valid-value? (constantly true)}
             {:name "Sex"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? #{"Male" "Female" "Other"}}
             {:name "Life Expectancy"
              :datatype :double
              :coltype :qb/measure
              :valid-value? double?}]})

;; Find dimension columns, i.e. ones that need to not change for
;; a row to be considered a correction and not an append

(def id-columns
  (->> default-schema
       :columns
       (filter (comp #{:qb/dimension :qb/attribute} :coltype))
       (map :name)))

(defn hash-fn [& dims]
  ;; use a real hash fn here not just this one..
  (hash dims))

(defn add-ids [ds]
  (-> ds
      ;; adds a new column named "ID" by applying `hash-fn` to the
      ;; vals in the selected columns
      (tc/map-columns "ID" id-columns hash-fn)))

(defn get-measure-column-name [schema]
  (->> schema
       :columns
       (some #(when ((comp #{:qb/measure} :coltype) %) %))
       :name))

;; Different approaches to figuring out which rows are new/deleted/corrected:

;; 1. Join up datasets by ID and label each resulting row as either new/deleted/corrected,
;;  then we could pull out whatever ones we want for different purposes..

(let [old (add-ids original-ds)
      new (add-ids new-ds)
      measure-column-name (get-measure-column-name default-schema)
      right-measure-column-name (str "right." measure-column-name)
      column-names (tc/column-names original-ds)
      joined-ds (tc/full-join old new "ID")
      labelled-ds (-> joined-ds
                      (tc/map-columns :status [measure-column-name right-measure-column-name]
                                      (fn [old-measure-val new-measure-val]
                                        (cond
                                          (nil? old-measure-val) :new
                                          (nil? new-measure-val) :deleted
                                          (not= old-measure-val new-measure-val) :corrected))))]
  ;; now we can pull out new/deleted/corrected easily
  {:new-rows (-> labelled-ds
                 (tc/select-rows (comp #(= :new %) :status))
                 (tc/select-columns (map #(str "right." %) column-names)))
   :deleted-rows (-> labelled-ds
                     (tc/select-rows (comp #(= :deleted %) :status))
                     (tc/select-columns column-names))
   :corrected-rows (-> labelled-ds
                       (tc/select-rows (comp #(= :corrected %) :status))
                       (tc/select-columns (conj column-names (str "right." measure-column-name)))
                       (tc/drop-columns measure-column-name))})
;; note columns need renamed etc but this is the idea

;;
;;
;;
;;
;;

;; 2. Find new/deleted/corrected ids by comparing old and new datasets

(defn- get-new-rows [old new]
  (tc/anti-join new old "ID"))

(defn- get-deleted-rows [old new]
  (tc/anti-join old new "ID"))

(defn- get-corrected-rows [old new]
  (let [measure-column-name (get-measure-column-name default-schema)
        right-measure-column-name (str "right." measure-column-name)
        column-names (tc/column-names new)]
    (-> (tc/inner-join old new "ID")
        (tc/select-rows (fn [row]
                          (not= (get row measure-column-name)
                                (get row right-measure-column-name))))
        (tc/select-columns (conj column-names (str "right." measure-column-name)))
        (tc/drop-columns measure-column-name)
        (tc/rename-columns {right-measure-column-name measure-column-name}))))

;; return new rows, deleted rows, and corrected rows:

(let [old (add-ids original-ds)
      new (add-ids new-ds)
      new-rows (get-new-rows old new)
      deleted-rows (get-deleted-rows old new)]
  {:new-rows new-rows
   :deleted-rows deleted-rows
   :corrected-rows (get-corrected-rows old new)})
