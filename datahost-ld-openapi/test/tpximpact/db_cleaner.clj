(ns tpximpact.db-cleaner
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo]))

(defn drop-all! [repo]
  (log/debug "Truncating database...")
  (with-open [conn (repo/->connection repo)]
    (pr/update! conn "DROP ALL ;")))

(defn clean-db [sys]
  (when-let [repo (:tpximpact.datahost.ldapi.native-datastore/repo sys)]
    (drop-all! repo)))