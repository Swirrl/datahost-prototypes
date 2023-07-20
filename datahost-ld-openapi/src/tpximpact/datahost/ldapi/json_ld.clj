(ns tpximpact.datahost.ldapi.json-ld
  (:require [jsonista.core :as jsonista])
  (:import (com.apicatalog.jsonld JsonLd)
           (com.apicatalog.jsonld.document JsonDocument)
           (java.io StringReader)))

(def simple-context {:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     :dh "https://publishmydata.com/def/datahost/"
                     :dcterms "http://purl.org/dc/terms/"})

(defn ->json-document
  ^JsonDocument [edn]
  (-> edn
      (jsonista/write-value-as-string)
      (StringReader.)
      (JsonDocument/of)))

(defn compact
  "Takes an EDN JSON-LD data structure and converts them to a compacted
  jakarta.json.JsonObject, which can be further manipulated or written
  to a string via .toString"
  [json-ld context]
  (-> (->json-document json-ld)
      (JsonLd/compact (->json-document context))
      (.compactArrays true)
      (.compactToRelative true)
      (.get)))


