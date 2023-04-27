(ns tpximpact.datahost.ldapi
  (:require
    [clojure.java.io :as io]
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [integrant.core :as ig]
    [meta-merge.core :as mm])
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

(defmethod ig/pre-init-spec ::default-catalog-id [_]
  string?)

(derive ::default-catalog-id ::const)

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
  (let [config (load-configs ["ldapi/base-system.edn"
                              ;; env.edn contains environment specific
                              ;; overrides to the base-system.edn and
                              ;; is set on classpath depending on env.
                              "ldapi/env.edn"])
        sys (start-system config)]
    (log/info "System started")))
