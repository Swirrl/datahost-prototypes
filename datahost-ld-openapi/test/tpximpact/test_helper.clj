(ns tpximpact.test-helper
  (:require [clojure.test :refer :all]
            [integrant.core :as ig]
            [tpximpact.datahost.ldapi :as ldapi-system]))


(defmacro with-system [system-binding & body]
  `(let [config# (ldapi-system/load-configs ["test-base-system.edn"
                                             ;; env.edn contains environment specific
                                             ;; overrides to the base-system.edn and
                                             ;; is set on classpath depending on env.
                                             "ldapi/env.edn"])
         sys# (ldapi-system/start-system config#)
         ~system-binding sys#]
     (try
      ~@body
      (finally
        (ig/halt! sys#)))))
