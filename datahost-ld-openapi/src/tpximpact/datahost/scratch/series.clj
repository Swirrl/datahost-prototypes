(ns tpximpact.datahost.scratch.series
  (:require [clojure.java.io :as io]
            [grafter-2.rdf4j.io :as rio]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]
            [duratom.core :as db])
  (:import [java.net URI]
           [com.github.jsonldjava.core JsonLdProcessor RDFDatasetUtils JsonLdTripleCallback]
           [com.github.jsonldjava.utils JsonUtils]))

(def default-catalog-uri (URI. "https://example.org/data/catalog"))

(def file-store (io/file "./tmp"))

(defn- ->edn-data
  "Convert mutable java data, as returned by the JSON-LD parser into
  immutable clojure data structures, so we can work with it without
  surprises."
  [java-data]
  (walk/prewalk (fn [f]
                  (cond
                     (instance? java.util.HashMap f) (into {} f)
                     (instance? java.util.ArrayList f) (into [] f)
                     :else f))
                 java-data))

(defn load-jsonld [jsonld-file]
  (->edn-data (JsonUtils/fromReader (io/reader jsonld-file))))

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



(defn valid-slug? [slug]
  ;; TODO add more slug rules around valid character ranges e.g.
  ;; exclude _ but allow -'s etc...
  (not (or (str/starts-with? slug "/")
           (str/ends-with? slug "/"))))

(defn calculate-base-entity
  "Calculate the base-entity for the release (and elements beneath it in
  the data model).

  NOTE: this is subtly different to the @base IRI for the series
  document which here is set to the ld-root with a trailing '/'."
  [slug]
  (when-not (valid-slug? slug)
    (throw (ex-info "slug must not start or end in a '/'" {:type :invalid-arguments})))
  (str ld-root slug "/"))







(def SeriesJsonLdInput [:map
                        ["@id" :series-slug-string]
                        ["dh:baseEntity" :url-string]])

(comment
  (m/validate SeriesJsonLdInput {"@id" "foo-bar"
                                 "dh:baseEntity" "http://foo"}
              {:registry registry})

  (m/validate SeriesApiParams
              {:series-slug "my-dataset-series" :title "foo"}
              {:registry registry})

  (me/humanize (m/explain SeriesApiParams
                          {:series-slug "foo-bar" :title "foo"}
                          {:registry registry}))
  )







;; PUT /data/:series-slug








(comment
  (let [db (atom {})]

    (swap! db upsert-series {:api-params {:series-slug "my-dataset-series"}
                             :jsonld-doc {}})

    (swap! db upsert-series {:api-params {:series-slug "my-other-series"} :jsonld-doc {}})
    (swap! db upsert-series {:api-params {:series-slug "my-other-series"}
                             :jsonld-doc {"dcterms:title" "New title"}})


    @db)

  )



;; POST /data/:series-slug/:release-slug

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

  (create-series {:series-slug "my-dataset-series"
                  :title "new title"
                  :description "blah blah"}
                 (io/file "./resources/example-series.json"))

  )
