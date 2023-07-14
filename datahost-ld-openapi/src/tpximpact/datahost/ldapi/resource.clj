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

(defn create [id]
  {id-key id
   :subjects {}})

(defn id [resource]
  (get resource id-key))

(defn set-properties [resource properties]
  (update-in resource [:subjects (id resource)] merge properties))

(defn add-property [resource p o]
  (update-in resource [:subjects (id resource) p] (fnil conj #{}) o))

(defn get-property [resource p]
  (get-in resource [:subjects (id resource) p]))

(defn get-property1 [resource p]
  (first (get-property resource p)))

(defn get-properties [resource ps]
  (let [subject (get-in resource [:subjects (id resource)])]
    (select-keys subject ps)))

(defn set-property1 [resource p o]
  (assoc-in resource [:subjects (id resource) p] #{o}))

(defn add-statement [resource statement]
  (update-in resource [:subjects (pr/subject statement) (pr/predicate statement)] (fnil conj #{}) (pr/object statement)))

(defn add-statements [resource statements]
  (reduce add-statement resource statements))

(defn ->statements [resource]
  (mapcat (fn [[s ps]]
            (mapcat (fn [[p os]]
                      (map (fn [o] (pr/->Triple s p o)) os))
                    ps))
          (:subjects resource)))

(defn- find-root [statements]
  (let [{:keys [subjects objects]} (reduce (fn [acc s]
                                             (-> acc
                                                 (update :subjects conj (pr/subject s))
                                                 (update :objects conj (pr/object s))))
                                           {:subjects #{} :objects #{}}
                                           statements)]
    (if (= 1 (count subjects))
      (first subjects)
      (let [roots (set/difference subjects objects)]
        ;; root subject is the single subject which does not appear in object position
        (case (count roots)
          0 (throw (ex-info "No candidate root object" {}))
          1 (first roots)
          2 (throw (ex-info "Multiple candidate root objects" {:candidates roots})))))))

(defn from-statements [statements]
  (let [root (find-root statements)]
    (add-statements (create root) statements)))

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

(defn empty-properties []
  {})

(defn add-properties-property [properties p o]
  (update properties p (fnil conj #{}) o))