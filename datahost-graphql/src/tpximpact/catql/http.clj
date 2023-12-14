(ns tpximpact.catql.http
  (:require [clojure.tools.logging :as log]
            [com.walmartlabs.lacinia.pedestal2 :as lp]
            [integrant.core :as ig]
            [io.pedestal.http :as http]))

(def server-instance (atom nil))

(def cors-config
  {:allowed-origins (constantly true)
   :creds false
   :max-age (* 60 60 2)         ;; 2 hours
   :methods "GET, POST, OPTIONS"})

(defmethod ig/init-key :tpximpact.catql.http/service [_ {:keys [schema] :as opts}]
  (lp/default-service schema opts))

(defmethod ig/init-key :tpximpact.catql.http/runnable-service [_ {:keys [service]}]
  (if-let [svr @server-instance]
    {:server svr}
    (when service
      (let [{:io.pedestal.http/keys [host port]} service
            server (-> service
                       (assoc ::http/allowed-origins cors-config)
                       http/create-server
                       http/start)]
        (reset! server-instance server)
        (log/info (str "CatQL running: http://" host ":" port "/"))
        {:server server}))))

(defmethod ig/halt-key! :tpximpact.catql.http/runnable-service [_ {:keys [server]}]
  (log/info ::stopping-server)
  (when server
    (http/stop server))
  (reset! server-instance nil))
