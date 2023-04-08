(ns dev
  (:require
   [clojure.java.io :as io]
   [integrant.core :as ig]
   [meta-merge.core :as mm]))

;; require scope capture as a side effect
(require 'sc.api)

(defn load-system-config [config]
  (-> config
      slurp
      ig/read-string))

(defn load-configs [configs]
  (->> configs
       (map (comp load-system-config io/resource))
       (apply mm/meta-merge)))

(defn start-system [config]
  (-> config
      (doto
       (ig/load-namespaces))
      ig/init))

(defn start! []
  (def sys (start-system (load-configs ["catql/base-system.edn"])))
  :ready)

(defn reset! []
  (ig/halt! sys)
  (def sys (start-system (load-configs ["catql/base-system.edn"]))))
