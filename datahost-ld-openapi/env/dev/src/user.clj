(ns user)

(defn help []
  (println "Welcome to ldapi")
  (println)
  (println "Available commands are:")
  (println)
  (println "(start!)                  ;; Start the system")
  (println "(reset!)                  ;; Reset the system"))

(defn dev
  "Load and switch to the 'dev' namespace."
  []
  (require 'dev)
  (help)
  (in-ns 'dev)
  :loaded)
