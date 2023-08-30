(ns tpximpact.datahost.scratch.corrections
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

;; The new dataset -- one correction, note the updated
;; life expectancy for the last row (72.9 -> 73.9)
(def new-ds (tc/dataset
                  {"Area" ["W06000022"]
                   "Period" ["2005-01-01T00:00:00/P3Y"]
                   "Sex" ["Male"]
                   "Life Expectancy" [73.9]}))

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

;; To see what changed between the original dataset and the new one,
;; see which IDs are in both datasets but have different values. There are
;; multiple ways to do this:

;; 1. Join the two datasets by ID and collapse the dataset back down
;; to the new values where they exist

(defn- select-newest-value [joined-dataset measure-column-name]
  (let [right-measure-column-name (str "right." measure-column-name)]
    (tc/map-columns joined-dataset
                    measure-column-name
                    [measure-column-name right-measure-column-name]
                    (fn [old new]
                      (if new new old)))))

(let [original-ds-with-ids (add-ids original-ds)
      new-ds-with-ids (add-ids new-ds)
      measure-column-name (get-measure-column-name default-schema)
      column-names (tc/column-names original-ds)]
  (-> original-ds-with-ids
      (tc/left-join new-ds-with-ids "ID")
      (select-newest-value measure-column-name)
      (tc/select-columns column-names)))

;; note this changes the row order, if that matters we can preserve the original
;; sorting by ID

;; 2. Find the set difference between old and new datasets then use
;; the id column to match up the rows to update the one that changed,
;; then drop and replace the updated row (note the ID stays the same)

(let [original-ds-with-ids (add-ids original-ds)
      new-ds-with-ids (add-ids new-ds)
      ;; in this example unnecessary, all new rows are updated since we are trusting
      ;; user to tell us these are corrections
      updated-row-ids (-> (tc/intersect (tc/select-columns original-ds-with-ids "ID")
                                        (tc/select-columns new-ds-with-ids "ID"))
                          (get "ID")
                          set)

      ;; in the corrections case all rows will be new, this step is unnecessary
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
