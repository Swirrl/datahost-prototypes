(ns tpximpact.datahost.ldapi.json-ld
  (:require [jsonista.core :as jsonista]
            [tpximpact.datahost.system-uris :as su])
  (:import (com.apicatalog.jsonld JsonLd)
           (com.apicatalog.jsonld.document JsonDocument)
           (java.io StringReader)))

(defn simple-context [system-uris]
  {:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   :rdfs "http://www.w3.org/2000/01/rdf-schema#"
   :dh "https://publishmydata.com/def/datahost/"
   :dcat "http://www.w3.org/ns/dcat#"
   :dcterms "http://purl.org/dc/terms/"
   :csvw "http://www.w3.org/ns/csvw#"
   :appropriate-csvw "https://publishmydata.com/def/appropriate-csvw/"
   "@base" (su/rdf-base-uri system-uris)})

(defn simple-collection-context
  "Use this context when the top level payload will be a collection e.g. Series list"
  [system-uris]
  (merge (simple-context system-uris)
         {:contents {"@id" "dh:collection-contents",
                     "@container" "@set"}}))

(defn dcat-distribution-context
  "Use this context when the top level payload will be a collection e.g. Series list"
  [system-uris]
  (assoc (simple-context system-uris)
         :dcat:distribution {"@id" "http://www.w3.org/ns/dcat#distribution"
                             "@container" "@index"}))

(defn dcat-distribution-collection-context
  "Use this context when the top level payload will be a collection e.g. Series list"
  [system-uris]
  (assoc (simple-collection-context system-uris)
         :dcat:distribution {"@id" "http://www.w3.org/ns/dcat#distribution"
                             "@container" "@index"}))

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
