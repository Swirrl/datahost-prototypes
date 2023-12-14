(ns tpximpact.catql
  (:require
   [integrant.core :as ig]
   [tpximpact.sys :as sys])
  (:gen-class))

(def system nil)

(defn stop-system! []
  (println "Stopping Service")
  (when system
    (ig/halt! system)))

(defn add-shutdown-hook!
  "Register a shutdown hook with the JVM.  This is not guaranteed to
  be called in all circumstances, but should be called upon receipt of
  a SIGTERM (a normal Unix kill command)."
  []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-system!)))

(defn start-system [configs]
  (let [initialised-sys (-> configs
                            (sys/load-configs)
                            (sys/prep-config)
                            (ig/init))]
    (alter-var-root #'system (constantly initialised-sys))
    initialised-sys))

(defn -main [& _args]
  (println "Starting Service...")
  (add-shutdown-hook!)
  (start-system ["catql/base-system.edn"
                 ;; env.edn contains environment specific
                 ;; overrides to the base-system.edn and
                 ;; is set on classpath depending on env.
                 "catql/env.edn"]))
