(ns tpximpact.sys
  (:require [clojure.java.io :as io]
            [meta-merge.core :as mm]
            [integrant.core :as ig]))

(defn load-system-config [config]
  (if config
    (-> config
        slurp
        ig/read-string)
    {}))

(defn load-configs [configs]
  (->> configs
       (map (comp load-system-config io/resource))
       (apply mm/meta-merge)))

(defn prep-config
  "Load, build and prep a configuration into an Integrant
  configuration that's ready to be initiated."
  [config]
  (-> config
      (doto ig/load-namespaces)
      (ig/prep)))
