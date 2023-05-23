(ns tpximpact.catql
  (:require
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [com.walmartlabs.lacinia.pedestal2 :as lp]
   [integrant.core :as ig]
   [io.pedestal.http :as http]
   [meta-merge.core :as mm])
  (:gen-class))

(defmethod ig/init-key ::service [_ {:keys [schema] :as opts}]
  (lp/default-service schema opts))

(def cors-config
  {:allowed-origins (constantly true)
   :creds false
   :max-age (* 60 60 2)         ;; 2 hours
   :methods "GET, POST, OPTIONS"})

;; This is an adapted service map, that can be started and stopped.
;; From the REPL you can call http/start and http/stop on this service:
(defmethod ig/init-key ::runnable-service [_ {:keys [service]}]
  (let [{:io.pedestal.http/keys [host port]} service
        server (-> service
                   (assoc ::http/allowed-origins cors-config)
                   http/create-server
                   http/start)]
    (log/info (str "CatQL running: http://" host ":" port "/"))
    server))

(defmethod ig/halt-key! ::runnable-service [_ server]
  (http/stop server))

(defn load-system-config [config]
  (if config
    (-> config
        slurp
        ig/read-string)
    {}))

(defn load-configs [configs]
  (->> configs
       (map (comp load-system-config io/resource))
       (apply mm/meta-merge)))

(defn start-system [config]
  (let [sys (-> config
                (doto
                  (ig/load-namespaces))
                ig/init)]
    sys))

(defn -main [& args]
  (let [config (load-configs ["catql/base-system.edn"
                              ;; env.edn contains environment specific
                              ;; overrides to the base-system.edn and
                              ;; is set on classpath depending on env.
                              "catql/env.edn"])
        sys (start-system config)]
    (log/info "System started")))
