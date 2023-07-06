(ns tpximpact.datahost.ldapi.util.rdf
  (:require
   [grafter-2.rdf4j.io :as rio])
  (:import [com.github.jsonldjava.core
            JsonLdProcessor
            JsonLdTripleCallback
            RDFDatasetUtils]))

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

                               (rio/statements (java.io.StringReader. nquad-string) :format :nq))))))
