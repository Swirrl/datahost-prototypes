(ns tpximpact.test-helpers
  (:require [clojure.test :refer :all]
            [integrant.core :as ig]
            [tpximpact.datahost.ldapi :as ldapi-system]))

(defn clean-up-database! [system]
  (let [db (get system :tpximpact.datahost.ldapi.db/db)]
    (reset! db {})))

(defmacro with-system [system-binding & body]
  `(let [config# (ldapi-system/load-configs ["ldapi/base-system.edn"
                                             "test-system.edn"
                                             ;; env.edn contains environment specific
                                             ;; overrides to the base-system.edn and
                                             ;; is set on classpath depending on env.
                                             "ldapi/env.edn"])
         sys# (ldapi-system/start-system config#)
         ~system-binding sys#]
     (try
       ~@body
       (finally
         (clean-up-database! sys#)
         (ig/halt! sys#)))))
