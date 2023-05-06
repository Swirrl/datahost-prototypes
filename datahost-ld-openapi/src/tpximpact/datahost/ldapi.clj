(ns tpximpact.datahost.ldapi
  (:require
    [clojure.java.io :as io]
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [integrant.core :as ig]
    [meta-merge.core :as mm]
    [com.yetanalytics.flint :as fl]
    [tpximpact.datahost.ldapi.native-datastore :as datastore])
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

(defmethod ig/init-key ::api-handler [_ {:keys [triplestore] :as _opts}]
  (fn handler [request]
    ;; temporary code to facilitate end-to-end service wire up
    (let [qry {:prefixes {:dcat "<http://www.w3.org/ns/dcat#>"
                          :rdfs "<http://www.w3.org/2000/01/rdf-schema#>"}
               :select '[?label ?g]
               :where [[:graph datastore/background-data-graph
                        '[[?datasets a :dcat/Catalog]
                          [?datasets :rdfs/label ?label]]]]}

          results (datastore/eager-query triplestore (fl/format-query qry :pretty? true))]
      {:status 200
       :headers {"Content-Type" "text/plain"}
       :body (-> results first :label)})))


(defmethod ig/init-key ::at-context [_ _]
  (keyword "@context"))


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
