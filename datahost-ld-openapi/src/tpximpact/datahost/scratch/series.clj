(ns tpximpact.datahost.scratch.series
  (:require [clojure.java.io :as io]
            [grafter-2.rdf4j.io :as rio]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import [java.net URI]
           [com.github.jsonldjava.core JsonLdProcessor RDFDatasetUtils JsonLdTripleCallback]
           [com.github.jsonldjava.utils JsonUtils]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it.  It should have a trailing slash."
  (URI. "https://example.org/data/"))


(def default-catalog-uri (URI. "https://example.org/data/catalog"))

(def file-store (io/file "./tmp"))


(defn load-jsonld [jsonld-file]
  (JsonUtils/fromReader (io/reader jsonld-file)))

(defn ednld->rdf
  "Takes a JSON-LD map as an EDN datastructure and converts it into RDF
  triples.

  NOTE: the implementation is the easiest but worst way to do, but
  should at least be correct.

  TODO: At some point we should change this to a more direct/better
  implementation."
  [edn-ld]
  (JsonLdProcessor/toRDF edn-ld
                         (reify
                           JsonLdTripleCallback
                           (call [_ dataset]
                             (let [nquad-string (let [sb (java.lang.StringBuilder.)]
                                                  (RDFDatasetUtils/toNQuads dataset sb)
                                                  (str sb))]

                               (tap> nquad-string)

                               (rio/statements (java.io.StringReader. nquad-string) :format :nq))))))

(defn dissoc-by-key [m pred]
  (reduce (fn [acc k]
               (if (pred k)
                 (dissoc acc k)
                 acc))
             m (keys m)))

(defn valid-slug? [slug]
  ;; TODO add more slug rules around valid character ranges e.g.
  ;; exclude _ but allow -'s etc...
  (or (str/starts-with? slug "/")
      (str/ends-with? slug "/")))

(defn calculate-base-entity
  "Calculate the base-entity for the release (and elements beneath it in
  the data model).

  NOTE: this is subtly different to the @base IRI for the series
  document which here is set to the ld-root with a trailing '/'."
  [slug]
  (when (valid-slug? slug)
    (throw (ex-info "slug must not start or end in a '/'" {:type :invalid-arguments})))
  (str ld-root slug "/"))

(defn normalise-context [ednld]
  (let [normalised-context ["https://publishmydata.com/def/datahost/context"
                            {"@base" (str ld-root)}]]
    (if-let [context (get ednld "@context")]
      (cond
        (= context "https://publishmydata.com/def/datahost/context") normalised-context
        (= context normalised-context) normalised-context

        :else (throw (ex-info "Invalid @context" {:supplied-context context
                                                  :valid-context normalised-context})))
      normalised-context)))

(defn normalise-series
  "Takes api params and an optional json-ld document of metadata, and
  returns a normalised EDN form of the JSON-LD, with the API
  parameters applied, validated and some structures like the context
  normalised."
  ([api-params]
   (normalise-series api-params nil))
  ([{:keys [slug] :as api-params} doc]
   (let [jsonld-doc (into {} (load-jsonld doc))
         merged-doc (merge (set/rename-keys api-params
                                            {:title "dcterms:title"
                                             :description "dcterms:description"})
                           jsonld-doc)
         cleaned-doc (-> merged-doc
                         (dissoc-by-key keyword?)
                         normalise-context)

         validated-doc (let [id-in-doc (get cleaned-doc "@id")]
                         (cond
                           (nil? id-in-doc) cleaned-doc

                           (= slug id-in-doc) cleaned-doc

                           :else (throw
                                  (ex-info "@id should for now be expressed as a slugged style suffix" {:supplied-id id-in-doc
                                                                                                        :expected-id slug}))))

         final-doc (assoc validated-doc
                          "@id" slug
                          "dh:base-entity" (str ld-root slug "/") ;; coin base-entity to serve as the @base for nested resources
                          )]
     final-doc)))


(comment

  (require '[dev.nu.morse :as morse])

  (load-jsonld (io/resource "./test-inputs/series/empty-1.json"))

  (rio/statements (assoc (into {} (load-jsonld (io/file "./resources/example-series.json")))
                         "dcterms:foo" "bla")
                  :format :jsonld)

  (def data (assoc (into {}
                         (load-jsonld (io/file "./resources/example-series.json")))
                   "@base" (str ld-root)
                   "@id" "my-dataset-series"
                   "dcterms:foo" "bla"))

  (ednld->rdf data)

  (create-series {:slug "my-dataset-series"
                  :title "new title"
                  :description "blah blah"}
                 (io/file "./resources/example-series.json"))

  )
