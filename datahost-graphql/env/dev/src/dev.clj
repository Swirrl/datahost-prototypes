(ns dev
  (:refer-clojure :exclude [reset!])
  (:require
   [clojure.java.io :as io]
   [grafter-2.rdf4j.repository :as repo]
   [integrant.core :as ig]
   [tpximpact.catql :as main]))

;; require scope capture as a side effect
(require 'sc.api)

(def fixture-repo (repo/fixture-repo (io/resource "fixture-data.ttl")))

(defmethod ig/init-key :dev/fixture-repo [_ _]
  (constantly fixture-repo))

(defn start! []
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn" "dev-fixtures.edn"])))
  :ready)

(defn reset! []
  (ig/halt! sys)
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn" "dev-fixtures.edn"]))))

(defn start-live! []
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn"])))
  :ready)

(defn reset-live! []
  (ig/halt! sys)
  (def sys (main/start-system (main/load-configs ["catql/base-system.edn"]))))

(comment

  (with-open [conn (repo/->connection fixture-repo)]
    (into [] (repo/query conn "select * where { ?s ?p ?o } limit 10")))

  )
