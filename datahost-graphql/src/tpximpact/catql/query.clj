(ns tpximpact.catql.query
  (:require [clojure.tools.logging :as log]
            [com.yetanalytics.flint :as f]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig])
  (:import (java.net URI)))

(def default-prefixes {:dcat (URI. "http://www.w3.org/ns/dcat#")
                       :dcterms (URI. "http://purl.org/dc/terms/")
                       :owl (URI. "http://www.w3.org/2002/07/owl#")
                       :qb (URI. "http://purl.org/linked-data/cube#")
                       :rdf (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                       :rdfs (URI. "http://www.w3.org/2000/01/rdf-schema#")
                       :skos (URI. "http://www.w3.org/2004/02/skos/core#")
                       :void (URI. "http://rdfs.org/ns/void#")
                       :xsd (URI. "http://www.w3.org/2001/XMLSchema#")
                       :foaf (URI. "http://xmlns.com/foaf/0.1/")})

(defmethod ig/init-key ::sparql-repo [_ {:keys [endpoint]}]
  (repo/sparql-repo endpoint))

(defn query
  "Returns a vector result of a query specified by query-data
  argument (a SPARQL query as clojure data)."
  [repo query-data]
  (with-open [conn (repo/->connection repo)]
    (let [sparql (f/format-query query-data :pretty? true)]
      (log/info sparql)
      (into [] (repo/query conn sparql)))))

(defn constrain-by-pred
  "Returns a SPARQL query fragment in clojure data form.

  If values seq is empty, the var will be unconstrained."
  [pred values]
  (let [facet-var (gensym "?")]
    (if (seq values)
      [[:values {facet-var values}]
       ['?id pred facet-var]]
      [['?id pred facet-var]])))

