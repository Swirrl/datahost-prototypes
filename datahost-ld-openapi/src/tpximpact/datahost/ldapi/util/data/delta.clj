(ns tpximpact.datahost.ldapi.util.data.delta
  (:require
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.util.data.internal :as data.internal]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]))

(def ^:private tag=modify "correct")

(defn- tag-change
  [old-measure-val new-measure-val]
  (cond
    (nil? old-measure-val) "append"
    (nil? new-measure-val) "retract"
    (not= old-measure-val new-measure-val) tag=modify))

(defn- row-changed? [row] (some? (get row data.internal/op-column-name)))

(defn- add-tx-columns
  "Adds \"datahost.row/id\" and  `data.internal/coords-column-name`"
  [{:keys [coords-columns] measure-column-name :measure-column :as _ctx}
   row]
  (let [sb (data.internal/make-hasher-input (StringBuilder.) (map #(get row %) coords-columns))
        coords (data.internal/hash sb)
        id (data.internal/hash (.append sb (get row measure-column-name)))]
    (assoc row "datahost.row/id" id data.internal/coords-column-name coords)))

(defn- make-column-names
  "Returns a map of {:coords {:left ... :right ...} :measure {:left ... right ...} :id RIGHT-ID-COL-NAME}"
  [right-ds-name measure-column-name coord-column-names]
  (let [right-measure-column-name (data.internal/r-joinable-name right-ds-name measure-column-name)
        right-coord-column-names (map #(data.internal/r-joinable-name right-ds-name %)
                                      coord-column-names)]
    {:id (data.internal/r-joinable-name right-ds-name "datahost.row/id")
     :coords {:left coord-column-names
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
  "Given a row of a joined dataset, copy values from the \"right\"
  dataset into the \"left\" one."
  [new-ds-name col-names row]
  (let [append? (fn [row] (= "append" (get row "dh/op")))]
    (if-not (append? row)
      row
      (reduce (fn [row [l-col r-col]]
                (assoc row l-col (get row r-col)))
              row
              (make-diff-column-pairs new-ds-name col-names)))))

(defn- correction-row? [row] (= tag=modify (get row data.internal/op-column-name)))

(defn- corrections->retractions+appends
  "Takes dataset joined & tagged with \"dh/op\" and returns a dataset
  where each correction is turned into a pair of append+retraction
  rows. The two rows will be linked via the value in \"datahost.row.id/previous\" column
  of the append row."
  [joined-ds {{measure-l :left measure-r :right} :measure right-id :id}]
  (let [corrections (tc/map-rows (tc/select-rows joined-ds correction-row?)
                                 (fn [row] (assoc row "datahost.row.id/previous" (get row "datahost.row/id"))))]
    (tc/union (tc/map-rows corrections (fn [row] (assoc row data.internal/op-column-name "retract" "datahost.row.id/previous" nil)))
              (tc/map-rows corrections (fn [row] (assoc row
                                                        data.internal/op-column-name "append"
                                                        measure-l (get row measure-r)
                                                        "datahost.row/id" (get row right-id)))))))

(defn delta-dataset
  "Returns a dataset with extra columns:

  - \"dh/op\" - append | retract
  - \"datahost.row/id\" - hash of coords+measure value
  - \"datahost.row.id/previous\" - see \"datahost.row/id\"

  Unchanged rows are not present in the returned delta dataset.

  Assumptions:

  - rows of both datasets conform to row-schema
  - coords uniqueness (but validation of new-ds still performed)."
  [base-ds new-ds {:keys [row-schema]}]
  (let [new-ds-name (tc/dataset-name new-ds)
        {:keys [coords-columns]
         measure-column-name :measure-column :as ctx} (data.internal/make-schema-context row-schema)
        {{right-measure-column-name :right} :measure
         :as col-names} (make-column-names new-ds-name
                                           measure-column-name
                                           coords-columns)

        [base-ds new-ds] (let [add-cols (partial add-tx-columns ctx)]
                           [(tc/map-rows base-ds add-cols)
                            (tc/map-rows new-ds add-cols)])
        _ (data.validation/validate-row-coords-uniqueness new-ds row-schema)
        ;; we do a full join and choose only appends/retractions/corrections
        joined (-> (tc/full-join base-ds new-ds data.internal/coords-column-name
                                 {:operation-space :int64})
                   (tc/map-columns data.internal/op-column-name
                                   [measure-column-name right-measure-column-name]
                                   tag-change)
                   (tc/select-rows row-changed?))
        ;; we turn corrections into pairs of retraction+append rows
        corrections (corrections->retractions+appends joined col-names)
        appends+retractions (tc/map-rows (tc/select-rows joined (complement correction-row?))
                                         (partial augment-append-row new-ds-name col-names))]
    (tc/select-columns (tc/concat appends+retractions corrections)
                       (conj (vec coords-columns)
                             measure-column-name
                             data.internal/op-column-name
                             "datahost.row/id"
                             "datahost.row.id/previous"))))
