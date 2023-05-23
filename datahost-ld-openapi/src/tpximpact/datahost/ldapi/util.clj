(ns tpximpact.datahost.ldapi.util
  (:require
   [grafter-2.rdf4j.io :as rio])
  (:import [com.github.jsonldjava.core JsonLdProcessor RDFDatasetUtils JsonLdTripleCallback]))

(defn dissoc-by-key [m pred]
  (reduce (fn [acc k]
            (if (pred k)
              (dissoc acc k)
              acc))
          m (keys m)))

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
