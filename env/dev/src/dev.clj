(ns dev
  (:require
   [tpximpact.catql :as main]))

;; require scope capture as a side effect
(require 'sc.api)



(defn start! []
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn"])))
  :ready)

(defn reset! []
  (ig/halt! sys)
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn"]))))
