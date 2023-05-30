(ns tpximpact.datahost.ldapi.native-datastore
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [medley.core :as med]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as rio])
  (:import (java.io File)
           (java.net URI)))

(def repo-instance (atom nil))

(def background-data-graph (URI. "http://www.example.org/graphs/background-data"))

(defn eager-query
  "Executes a SPARQL query which returns a sequence of results and
  ensures it is eagerly consumed before being returned. The underlying
  TCP connection is not released until all results have been iterated
  over so this prevents holding connections open longer than
  necessary."
  ([repo query-str]
   (eager-query repo query-str {}))
  ([repo query-str opts]
   (with-open [conn (repo/->connection repo)]
     (let [res (med/mapply repo/query conn query-str opts)]
       (if (seq? res)
         (doall res)
         res)))))

(defn load-repo [{:keys [data-directory]}]
  (let [r (-> (File. ^String data-directory)
              (repo/native-store)
              (repo/sail-repo))]
    (with-open [c (repo/->connection r)]
      (pr/add c (rio/statements (io/resource "ldapi/base-data.trig"))))
    r))

(defmethod ig/init-key ::repo [_ {:keys [data-directory] :as opts}]
  (if-let [repo @repo-instance]
    repo
    (when data-directory
      (let [repo (load-repo opts)]
        (reset! repo-instance repo)
        repo))))

(defmethod ig/halt-key! ::repo [_ repo]
  (when repo
    (repo/shutdown repo))
  (reset! repo-instance nil))
