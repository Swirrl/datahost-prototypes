(ns tpximpact.datahost.ldapi.resource
  (:require
    [clojure.java.io :as io]
    [grafter-2.rdf.protocols :as rdf]
    [grafter-2.rdf4j.io :as gio]
    [grafter-2.rdf.protocols :as pr]
    [clojure.data.json :as json]
    [clojure.set :as set]
    [tpximpact.datahost.ldapi.compact :as compact])
  (:import [java.io StringReader StringWriter]
           [org.eclipse.rdf4j.rio RioSetting]
           [org.eclipse.rdf4j.rio.helpers JSONLDMode JSONLDSettings]))

(def id-key (keyword "@id"))

(defn merge-properties [resource properties]
  (merge-with set/union resource properties))

(defn set-properties [resource properties]
  (merge resource properties))

(defn create-properties [] {})

(defn create [id]
  {id-key id})

(defn id [resource]
  (get resource id-key))

(defn add-property [resource p o]
  (update resource p (fnil conj #{}) o))

(defn set-property1 [resource p o]
  (assoc resource p #{o}))

(defn add-statement [resource statement]
  (if (= (id resource) (pr/subject statement))
    (add-property resource (pr/predicate statement) (pr/object statement))
    (throw (ex-info "Statement subject must match resource @id" {:id (id resource)
                                                                 :statement statement}))))

(defn ->statements [resource]
  (let [subject (id resource)
        properties (dissoc resource id-key)]
    (mapcat (fn [[p os]]
              (map (fn [o] (pr/->Triple subject p o)) os))
            properties)))

(defn from-statements [statements]
  (if-let [s (first statements)]
    (reduce add-statement
            (create (pr/subject s))
            statements)
    (throw (ex-info "At least one statement required for resource" {}))))

(defn- statements->json-ld
  ([statements] (statements->json-ld statements {}))
  ([statements prefixes]
   (let [prefixes (update-vals prefixes str)
         sw (StringWriter.)
         w (gio/rdf-writer sw :format :jsonld :prefixes prefixes)]
     (.. w (getWriterConfig) (set JSONLDSettings/JSONLD_MODE JSONLDMode/COMPACT))
     (when (seq statements)
       (pr/add w statements))
     (.flush sw)
     (.toString sw))))

(defn ->json-ld
  ([resource] (->json-ld resource {}))
  ([resource prefixes]
   ;; NOTE: When converting resource back to jsonld, we don't want type information
   ;; associated with each object
   ;; rdf:type uris are left so they can be compacted and converted to @type fields in the output
   (let [statements (->statements resource)
         rdf:type (compact/expand :rdf/type)
         simplified (map (fn [s] (if (= rdf:type (rdf/predicate s))
                                   s
                                   (update s :o str)))
                         statements)]
     (statements->json-ld simplified prefixes))))

(defn- json-ld-str->statements [json-ld-str]
  (with-open [r (StringReader. json-ld-str)]
    (vec (gio/statements r :format :jsonld))))

(defn- json-ld-doc->statements [json-ld-doc]
  (-> json-ld-doc json/write-str json-ld-str->statements))

(defn from-json-ld-doc [json-ld-doc]
  (-> json-ld-doc json-ld-doc->statements from-statements))

