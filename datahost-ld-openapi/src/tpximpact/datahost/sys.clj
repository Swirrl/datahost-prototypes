(ns tpximpact.datahost.sys
  (:require [clojure.java.io :as io]
            [meta-merge.core :as mm]
            [integrant.core :as ig])
  (:import (java.net URI)))

(defn get-env [s]
  (System/getenv (str s)))

(def readers
  {'env get-env
   ;; naive `or`; only takes 2 args
   'or (fn [pair]
         (apply #(or %1 %2) pair))
   'uri (fn [v] (URI. v))
   'int (fn [v]
          (Integer/parseInt v))
   'resource (fn [v] (io/resource v))
   'regex (fn [r] (re-pattern r))})

(defn load-system-config [config]
  (if config
    (->> config
         slurp
         (ig/read-string {:readers readers}))
    {}))

(defn load-configs [configs]
  (->> configs
       (map (comp load-system-config io/resource))
       (apply mm/meta-merge)))

(defn prep-config
  "Load, build and prep a configuration of modules into an Integrant
  configuration that's ready to be initiated."
  [config]
  (-> config
      (doto ig/load-namespaces)
      (ig/prep)))
