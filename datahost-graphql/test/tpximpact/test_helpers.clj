(ns tpximpact.test-helpers
  (:require
    [clojure.java.io :as io]
    [clojure.walk :as walk]
    [com.walmartlabs.lacinia :as lacinia]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.catql.schema :as schema]))

(defn simplify
    "Converts all ordered maps nested within the map into standard hash
  maps, and sequences into vectors, which makes for easier constants
  in the tests, and eliminates ordering problems."
    [m]
    (walk/postwalk
     (fn [node]
       (cond
         (instance? clojure.lang.IPersistentMap node)
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
