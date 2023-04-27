(ns dev
  (:require
   [integrant.core :as ig]
   [tpximpact.datahost.ldapi :as main]))

;; require scope capture as a side effect
(require 'sc.api)

(defn start! []
  (def sys (main/start-system (main/load-configs ["ldapi/base-system.edn"])))
  :ready)

(defn reset! []
  (ig/halt! sys)
  (def sys (main/start-system (main/load-configs ["ldapi/base-system.edn"]))))
