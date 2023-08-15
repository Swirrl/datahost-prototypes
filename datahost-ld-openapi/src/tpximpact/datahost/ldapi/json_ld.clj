(ns tpximpact.datahost.ldapi.json-ld
  (:require [jsonista.core :as jsonista]
            [tpximpact.datahost.ldapi.models.shared :as m.shared])
  (:import (com.apicatalog.jsonld JsonLd)
           (com.apicatalog.jsonld.document JsonDocument)
           (java.io StringReader)))

(def simple-context {:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     :dh "https://publishmydata.com/def/datahost/"
                     :dcat "http://www.w3.org/ns/dcat#"
                     :dcterms "http://purl.org/dc/terms/"
                     :csvw "http://www.w3.org/ns/csvw#"
                     :appropriate-csvw "https://publishmydata.com/def/appropriate-csvw/"
                     "@base" m.shared/ld-root})

(def simple-collection-context
  "Use this context when the top level payload will be a collection e.g. Series list"
  (merge simple-context
         {:contents {"@id" "dh:collection-contents",
                     "@container" "@set"}}))

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