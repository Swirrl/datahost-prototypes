(ns tpximpact.datahost.scratch.corrections
  (:require [tablecloth.api :as tc]))

;; A dataset -- the current version with correct data
(def original-ds (tc/dataset
         {"Area" ["W06000022" "W06000022" "W06000022" "W06000022"]
          "Period" ["2004-01-01T00:00:00/P3Y"
                    "2004-01-01T00:00:00/P3Y"
                    "2005-01-01T00:00:00/P3Y"
                    "2005-01-01T00:00:00/P3Y"]
          "Sex" ["Female" "Male" "Female" "Male"]
          "Life Expectancy" [80.7 77.1 80.9 72.9]}))

;; The new dataset -- a new version with one correction, not the updated
;; life expectancy for the last row (72.9 -> 73.9)
(def new-ds (tc/dataset
                  {"Area" ["W06000022" "W06000022" "W06000022" "W06000022"]
                   "Period" ["2004-01-01T00:00:00/P3Y"
                             "2004-01-01T00:00:00/P3Y"
                             "2005-01-01T00:00:00/P3Y"
                             "2005-01-01T00:00:00/P3Y"]
                   "Sex" ["Female" "Male" "Female" "Male"]
                   "Life Expectancy" [80.7 77.1 80.9 73.9]}))

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


;; To see what changed between the original dataset and the new one,
;; see which IDs are in both datasets but have different values. There are
;; multiple ways to do this:

;; 1. Join the two datasets by ID and collapse the dataset back down
;; to the new values only

(let [original-ds-with-ids (add-ids original-ds)
      new-ds-with-ids (add-ids new-ds)]
  (tc/semi-join new-ds-with-ids original-ds-with-ids "ID"))

;; 2. Find the set difference between old and new datasets then use
;; the id column to match up the rows to update the one that changed,
;; then drop and replace the updated row (note the ID stays the same)

(let [original-ds-with-ids (add-ids original-ds)
      new-ds-with-ids (add-ids new-ds)
      updated-row-ids (-> (tc/difference original-ds-with-ids new-ds-with-ids)
                          (get "ID")
                          set)

      new-rows (-> new-ds-with-ids
                   (tc/select-rows #(updated-row-ids (get % "ID"))))]
  (-> original-ds-with-ids
      (tc/drop-rows #(updated-row-ids (get % "ID")))
      (tc/concat new-rows)))

;; outputs:
;; |      Area |                  Period |    Sex | Life Expectancy |          ID |
;; |-----------|-------------------------|--------|----------------:|------------:|
;; | W06000022 | 2004-01-01T00:00:00/P3Y | Female |            80.7 | -1897080923 |
;; | W06000022 | 2004-01-01T00:00:00/P3Y |   Male |            77.1 |  1964733781 |
;; | W06000022 | 2005-01-01T00:00:00/P3Y | Female |            80.9 |  -533211380 |
;; | W06000022 | 2005-01-01T00:00:00/P3Y |   Male |            73.9 |  -165980273 |
