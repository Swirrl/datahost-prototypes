(ns tpximpact.datahost.ldapi.util.data.delta
  (:require
   [tpximpact.datahost.ldapi.util.data.internal :as data.internal]
   [clojure.string :as str]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.util.data.compilation :as data.compilation]
   [tpximpact.datahost.ldapi.util.data.internal :as data.internal]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]))

(def ^:private tag=modify 3)

(defn- tag-change
  [old-measure-val new-measure-val]
  (cond
    (nil? old-measure-val) 1
    (nil? new-measure-val) 2
    (not= old-measure-val new-measure-val) tag=modify))

(defn- row-changed? [row] (some? (get row data.internal/op-column-name)))

(defn- add-tx-columns
  [{:keys [row-schema hashable-columns] measure-column-name :measure-column :as _ctx}
   row]
  (let [sb (data.internal/make-hasher-input (StringBuilder.) (map #(get row %) hashable-columns))
        coords (data.internal/hash sb)
        id (data.internal/hash (.append sb (get row measure-column-name)))]
    (assoc row "datahost.row/id" id data.internal/hash-column-name coords)))

(defn- make-column-names
  "Returns TODO"
  [right-ds-name measure-column-name coord-column-names]
  (let [right-measure-column-name (data.internal/r-joinable-name right-ds-name measure-column-name)
        right-coord-column-names (map #(data.internal/r-joinable-name right-ds-name %)
                                      coord-column-names)]
    {:coords {:left coord-column-names
              :right right-coord-column-names}
     :measure {:left measure-column-name
               :right right-measure-column-name}}))

(defn- make-diff-column-pairs
  "Returns a seq of pairs: [left-col-name right-col-name]"
  [right-ds-name {{measure-l :left measure-r :right} :measure
                  {coords-l :left coords-r :right} :coords}]
  (map vector
       (conj (conj (vec coords-l)
                   measure-l
                   "datahost.row/id"
                   "datahost.row/coords"))
       (conj (vec coords-r)
             measure-r
             (data.internal/r-joinable-name right-ds-name
                                            "datahost.row/id")
             (data.internal/r-joinable-name right-ds-name
                                            "datahost.row/coords"))))

(defn- augment-append-row
  "Copy values from the \"right\" dataset into the \"left\" one."
  [new-ds-name col-names row]
  (let [append? (fn [row] (= 1 (get row "dh/op")))]
    (if-not (append? row)
      row
      (reduce (fn [row [l-col r-col]]
                (assoc row l-col (get row r-col)))
              row
              (make-diff-column-pairs new-ds-name col-names)))))

(defn- corrections->retractions+appends
  "Takes dataset joined & tagged with \"dh/op\" and returns a dataset
  where each correction is turned into a pair of append+retraction
  rows. The two rows will be linked via the value in \"datahost.row.id/ref\" column
  of the append row."
  [diff {measure-l :left measure-r :right :as measure}]
  (let [correction? (fn [row] (= tag=modify (get row "dh/op")))
        corrections (tc/map-rows (tc/select-rows diff correction?)
                                 (fn [row] (assoc row "datahost.row.id/ref" (get row "datahost.row/id"))))]
    (tc/union (tc/map-rows corrections (fn [row]
                                         (assoc row "dh/op" 2 "datahost.row.id/ref" nil)))
              (tc/map-rows corrections (fn [row] (assoc row "dh/op" 1 measure-l (get row measure-r)))))))

(defn delta-dataset
  "Returns a dataset with extra columns: TODO(finalise names)

  Value of the operation column can be 1=append | 2=retract.

  Unchanged rows are not present in the returned delta dataset.

  Assumptions:

  - rows of both datasets conform to row-schema"
  [base-ds new-ds {:keys [row-schema hashable-columns] measure-column-name :measure-column :as ctx}]
  (let [new-ds-name (tc/dataset-name new-ds)
        {{right-coord-column-names :right} :coords
         {right-measure-column-name :right} :measure
         :as col-names} (make-column-names new-ds-name
                                           measure-column-name
                                           hashable-columns)

        [base-ds new-ds] (let [add-cols (partial add-tx-columns ctx)]
                           [(tc/map-rows base-ds add-cols)
                            (tc/map-rows new-ds add-cols)])

        ;; we do a full join and choose only appeds/retractions/corrections
        joined (-> (tc/full-join base-ds new-ds data.internal/hash-column-name
                                 {:operation-space :int64})
                   (tc/map-columns data.internal/op-column-name
                                   [measure-column-name right-measure-column-name]
                                   tag-change)
                   (tc/select-rows row-changed?))
        ;; we turn correctionsn into pairs of retraction+append rows
        correction? (fn [row] (= tag=modify (get row "dh/op")))
        corrections (corrections->retractions+appends joined (:measure col-names))

        diff (tc/map-rows (tc/select-rows joined (complement correction?))
                          (partial augment-append-row new-ds-name col-names))]
    (tc/select-columns (tc/concat diff corrections)
                       (conj (vec hashable-columns)
                             measure-column-name
                             "dh/op"
                             "datahost.row/id"
                             "datahost.row.id/ref"))))
