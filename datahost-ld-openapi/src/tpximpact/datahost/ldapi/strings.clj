(ns tpximpact.datahost.ldapi.strings
  (:require [camel-snake-kebab.core :refer [->kebab-case-string]]
            [clojure.string :as s]))

(def ^:private diacritical-marks-map
  (zipmap "ąàáäâãåæăćčĉęèéëêĝĥìíïîĵłľńňòóöőôõðøśșšŝťțŭùúüűûñÿýçżźž"
          "aaaaaaaaaccceeeeeghiiiijllnnoooooooossssttuuuuuunyyczzz"))

(defn slugify [s]
  (let [s (str s)]
    (if-not (s/blank? s)
      (-> s
          (s/escape (assoc diacritical-marks-map
                      \£ "pound"
                      \% "percent"
                      \. "-"))
          (s/replace #"[^\w\s-]+" "")
          (->kebab-case-string))
      s)))