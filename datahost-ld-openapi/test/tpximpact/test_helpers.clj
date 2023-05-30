(ns tpximpact.test-helpers
  (:require [clojure.test :refer :all]
            [integrant.core :as ig]
            [tpximpact.datahost.sys :as sys]))

(defn clean-up-database! [system]
  (when-let [db (get system :tpximpact.datahost.ldapi.db/db)]
    (reset! db {})))

(defn start-system [configs]
  (-> configs
      (sys/load-configs)
      (sys/prep-config)
      (ig/init)))

(defmacro with-system [system-binding & body]
  `(let [sys# (start-system ["ldapi/base-system.edn"
                             ;; TODO - nuke test-system & move contents to env.edn
                             "test-system.edn"
                             ;; env.edn contains environment specific
                             ;; overrides to the base-system.edn and
                             ;; is set on classpath depending on env.
                             "ldapi/env.edn"])
         ~system-binding sys#]
     (try
       ~@body
       (finally
         (clean-up-database! sys#)
         (ig/halt! sys#)))))
