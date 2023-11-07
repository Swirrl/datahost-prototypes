(ns tcwork.diff 
  (:require [tablecloth.api :as tc] [tech.v3.datatype.functional :as dfn]))

(require '[tablecloth.api :as tc] '[tech.v3.datatype.functional :as dfn])


(def DS (tc/dataset "tcwork/2021.csv"))
(def DS2 (tc/dataset "tcwork/2021-final.csv"))

(def DSh (tc/join-columns DS :hash (complement #{"Aged 16 to 64 years level 3 or above qualifications"}) {:result-type hash :drop-columns? false} ))
(def DSh2 (tc/join-columns DS2 :hash (complement #{"Aged 16 to 64 years level 3 or above qualifications"}) {:result-type hash }))
(println DSh DSh2)
(def DSh2full (tc/join-columns DS2 :hash (complement #{"Aged 16 to 64 years level 3 or above qualifications"}) {:result-type hash :drop-columns? false}))


;CORRECTIONS
(def join (tc/inner-join DSh DSh2 [:hash]))
(println join)

(def trimmedjoin (tc/select-rows
  join
  (fn [row]
    (and
      (not= (get row "Aged 16 to 64 years level 3 or above qualifications")
         (get row "tcwork/2021-final.csv.Aged 16 to 64 years level 3 or above qualifications"))))))

(def corrections 
  (tc/add-column 
   (tc/rename-columns trimmedjoin 
                      {"tcwork/2021-final.csv.Aged 16 to 64 years level 3 or above qualifications" "new value"}) 
  :op "correction"))

(println corrections)

;DELETES
(def deletedrows
  (tc/select-rows
   DSh
   (fn [row1]
     (not (contains? (set (tc/column DSh2 :hash)) (:hash row1)) ))))
    
(def retractions (tc/add-columns deletedrows {:op "deletions"
                                              "new value" nil}))

(println retractions)

;ADDITIONS

(def addedrows
  (tc/select-rows
   DSh2full
   (fn [row1]
     (not (contains? (set (tc/column DSh :hash)) (:hash row1))))))


(def additions (tc/add-columns addedrows {:op "additions"
                                              "new value" nil}))

(println additions)

(def changes (tc/concat additions retractions corrections))

(println changes)