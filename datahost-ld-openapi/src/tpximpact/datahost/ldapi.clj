(ns tpximpact.datahost.ldapi
  (:require
    [clojure.java.io :as io]
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [ring.adapter.jetty :as rj]
    [integrant.core :as ig]
    [meta-merge.core :as mm])
  (:import
    [java.net URI])
  (:gen-class))

(def default-catalog (URI. "http://gss-data.org.uk/catalog/datasets"))

(s/def ::sdl-resource string?)

(defmethod ig/pre-init-spec ::schema [k]
  (s/keys :req-un [::sdl-resource]))

(defmethod ig/init-key ::const [_ v] v)

(defmethod ig/pre-init-spec ::drafter-base-uri [_]
  string?)

(derive ::drafter-base-uri ::const)

(defmethod ig/pre-init-spec ::default-catalog-id [_]
  string?)

(derive ::default-catalog-id ::const)

;; This is an adapted service map, that can be started and stopped.
;; From the REPL you can call http/start and http/stop on this service:
(defmethod ig/init-key ::runnable-service [_ {:keys [host port handler]}]
  (future
    (let [http-svr (rj/run-jetty handler {:port port :host host})]
      (try
        (log/info (str "LD API running: http://" host ":" port "/"))
        (catch InterruptedException _iex
          (.stop http-svr))
        (finally (log/info "LD API was shutdown"))))))

(defmethod ig/halt-key! ::runnable-service [_ server-future]
  (future-cancel server-future))

(defmethod ig/init-key ::api-handler [_ _opts]
  (fn handler [request]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body "Hello World"}))

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
  (-> config
      (doto
        (ig/load-namespaces))
      ig/init))

(defn -main [& _args]
  (let [config (load-configs ["catql/base-system.edn"
                              ;; env.edn contains environment specific
                              ;; overrides to the base-system.edn and
                              ;; is set on classpath depending on env.
                              "catql/env.edn"])
        sys (start-system config)]
    (log/info "System started")))
