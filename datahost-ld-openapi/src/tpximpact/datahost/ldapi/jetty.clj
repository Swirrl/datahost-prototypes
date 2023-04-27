(ns tpximpact.datahost.ldapi.jetty
  (:require [integrant.core :as ig]
            [ring.adapter.jetty :as rj])
  (:import (org.eclipse.jetty.server Server)))

(defmethod ig/init-key ::runnable-service [_ opts]
  (let [handler (atom (delay (:handler opts)))
        options (-> opts (dissoc :handler) (assoc :join? false))]
    {:handler handler
     :server  (rj/run-jetty (fn [req] (@@handler req)) options)}))

(defmethod ig/halt-key! ::runnable-service [_ {:keys [^Server server]}]
  (.stop server))

(defmethod ig/suspend-key! ::runnable-service [_ {:keys [handler]}]
  (reset! handler (promise)))

(defmethod ig/resume-key ::runnable-service [key opts old-opts old-impl]
  (if (= (dissoc opts :handler) (dissoc old-opts :handler))
    (do (deliver @(:handler old-impl) (:handler opts))
        old-impl)
    (do (ig/halt-key! key old-impl)
        (ig/init-key key opts))))

(defmethod ig/resolve-key :adapter/jetty [_ {:keys [server]}]
  server)