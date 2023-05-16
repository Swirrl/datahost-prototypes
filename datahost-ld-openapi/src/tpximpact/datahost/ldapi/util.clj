(ns tpximpact.datahost.ldapi.util)

(defn dissoc-by-key [m pred]
  (reduce (fn [acc k]
            (if (pred k)
              (dissoc acc k)
              acc))
          m (keys m)))
