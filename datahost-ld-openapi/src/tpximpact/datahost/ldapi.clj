(ns tpximpact.datahost.ldapi
  (:require
    [clojure.spec.alpha :as s]
    [integrant.core :as ig]
    [tpximpact.datahost.sys :as sys])
  (:import
    [java.net URI])
  (:gen-class))

(def default-catalog (URI. "http://gss-data.org.uk/catalog/datasets"))

(s/def ::sdl-resource string?)

(defmethod ig/pre-init-spec ::schema [_k]
  (s/keys :req-un [::sdl-resource]))

(defmethod ig/init-key ::const [_ v] v)

(defmethod ig/pre-init-spec ::drafter-base-uri [_]
  string?)

(derive ::drafter-base-uri ::const)

;; The application base path or mount point
(derive ::base-path ::const)

(defmethod ig/pre-init-spec ::default-catalog-id [_]
  string?)

(derive ::default-catalog-id ::const)

(derive :tpximpact.datahost.ldapi.jetty/http-port ::const)

(derive :tpximpact.datahost.ldapi.native-datastore.repo/data-directory ::const)

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
  (try
   (let [initialised-sys (-> configs
                             (sys/load-configs)
                             (sys/prep-config)
                             (ig/init))]
     (println "Service initialised")
     (alter-var-root #'system (constantly initialised-sys))
     initialised-sys)
   (catch Exception ex
     (println "ERROR: " (ex-message ex))
     (throw ex))))

(defn -main [& _args]
  (println (format "Starting Service... CI='%s'" (System/getenv "CI")))
  (add-shutdown-hook!)
  (start-system ["ldapi/base-system.edn"
                 ;; env.edn contains environment specific
                 ;; overrides to the base-system.edn and
                 ;; is set on classpath depending on env.
                 "ldapi/env.edn"]))
