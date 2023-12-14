(ns tpximpact.datahost.ldapi.jetty
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [ring.adapter.jetty :as rj])
  (:import (org.eclipse.jetty.server Server)))

(def server-instance (atom nil))

(defmethod ig/init-key ::runnable-service [_ {:keys [handler] :as opts}]
  (let [handler (atom (delay handler))
        options (-> opts
                    (dissoc :handler)
                    (assoc :join? false))
        service {:handler handler}]
    (log/info ::starting-server (select-keys opts [:port]))
    (if-let [svr @server-instance]
      (assoc service :server svr)
      (assoc service :server (let [server (rj/run-jetty (fn [req] (@@handler req)) options)]
                               (reset! server-instance server)
                               server)))))

(defmethod ig/halt-key! ::runnable-service [_ {:keys [server]}]
  (log/info ::stopping-server)
  (.stop ^Server server)
  (reset! server-instance nil))