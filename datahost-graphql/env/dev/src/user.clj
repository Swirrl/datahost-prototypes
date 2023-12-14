(ns user)

(defn help []
  (println)
  (println "Welcome to Catql")
  (println "****************")
  (println)
  (println "Available commands are:")
  (println)
  (println "(start) or (go)          ;; Start the system")
  (println "(reset)                  ;; Reset the system"))

(defn dev
  "Load and switch to the 'dev' namespace."
  []
  (require 'dev)
  (help)
  (in-ns 'dev)
  :loaded)
