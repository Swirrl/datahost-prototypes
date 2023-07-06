(ns tpximpact.datahost.ldapi.util.collections
  "Utilities for dealing with collections.")

(defn dissoc-by-key [m pred]
  (reduce (fn [acc k]
            (if (pred k)
              (dissoc acc k)
              acc))
          m (keys m)))
