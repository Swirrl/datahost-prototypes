(ns delta.diff 
  (:require [tablecloth.api :as tc]))

(require '[tablecloth.api :as tc] '[tech.v3.datatype.functional :as dfn])


(def DS (tc/dataset "delta/2021.csv"))
(def DS2 (tc/dataset "delta/2021-final.csv"))

(def DSh (tc/join-columns DS :hash (complement #{"Aged 16 to 64 years level 3 or above qualifications"}) {:result-type hash}))
(def DSh2 (tc/join-columns DS2 :hash (complement #{"Aged 16 to 64 years level 3 or above qualifications"}) {:result-type hash}))

(def join (tc/inner-join DSh DSh2 [:hash "Aged 16 to 64 years level 3 or above qualifications"]))

(join (tc/select-rows (fn [row]
                       (not= ("Aged 16 to 64 years level 3 or above qualifications" row) 
                             ("delta/2021-final.csv.Aged 16 to 64 years level 3 or above qualifications" row)
                             )
                       )
                ))

(println join)
(println corrections)