(ns tpximpact.test-helpers
  (:require
    [clojure.java.io :as io]
    [clojure.walk :as walk]
    [com.walmartlabs.lacinia :as lacinia]
    [grafter-2.rdf4j.repository :as repo]
    [integrant.core :as ig]
    [tpximpact.catql.schema :as schema]
    [tpximpact.sys :as sys])
  (:import (clojure.lang IPersistentMap)))

(defn simplify
    "Converts all ordered maps nested within the map into standard hash
  maps, and sequences into vectors, which makes for easier constants
  in the tests, and eliminates ordering problems."
    [m]
    (walk/postwalk
     (fn [node]
       (cond
         (instance? IPersistentMap node)
         (into {} node)

         (seq? node)
         (vec node)

         :else
         node))
     m))

(defn execute
  ([schema q]
   (execute schema q {} nil nil))
  ([schema q vars context]
   (execute schema q vars context nil))
  ([schema q vars context options]
   (-> (lacinia/execute schema q vars context options)
       simplify
       (dissoc :extensions))))

(defn catql-schema []
  (schema/load-schema {:sdl-resource "catql/catalog.graphql"
                       :drafter-base-uri "https://idp-beta-drafter.publishmydata.com/"
                       :default-catalog-id "http://gss-data.org.uk/catalog/datasets"
                       :repo-constructor (constantly
                                           (repo/fixture-repo (io/resource "fixture-data.ttl")))}))

(defn result-datasets [result]
  (-> result :data :endpoint :catalog :catalog_query :datasets))

(defn facets [result facet-key]
  (->> result :data :endpoint :catalog :catalog_query :facets facet-key))

(defn facets-enabled [result facet-key]
  (->> (facets result facet-key) (filter :enabled)))

(defmacro with-timeout
  "Tries to return value of expr in within specified timeout (in ms),
  otherwise returns sentinel `:timeout` value."
  [timeout-ms expr]
  `(let [f# (future ~expr)]
     (deref f# ~timeout-ms :timeout)))

(defn load-configs
  [configs]
  (-> configs
      (sys/load-configs)
      (dissoc :tpximpact.catql/service :tpximpact.catql/runnable-service)))

(defn- start-system
  [config]
  (-> config
      (doto (ig/load-namespaces))
      ig/init))

(def ^:dynamic *system* (atom nil))

(defn start-test-system
  "Use this fixture for graphql tests."
  ([]
   (start-test-system ["catql/base-system.edn"]))
  ([configs]
   (let [sys (-> configs
                 (load-configs)
                 (start-system))]
     (reset! *system* sys))))

(defn stop-test-system []
  (let [sys @*system*]
    (assert sys)
    (ig/halt! sys)))

(defn with-system
  "Test fixture for starting / stopping the system."
  [test-fn]
  (start-test-system)
  (test-fn)
  (stop-test-system))
