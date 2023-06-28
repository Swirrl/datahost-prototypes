(ns tpximpact.datahost.ldapi.routes.shared
  (:require [malli.core :as m]
            [malli.util :as mu]))


(def JsonLdBase
  "Common entries in JSON-LD documents."
  [:map {:closed false}
   ["dcterms:title" {:optional true} string?]
   ["dcterms:description" {:optional true} string?]
   ["@type" {:optional true} string?]
   ["@id" {:optional true} string?]
   ["@context" [:or :string
                [:tuple :string [:map
                                 ["@base" string?]]]]]])

(def JsonLdSchema
  "Datahost specific JSON-LD documents"
  (:maybe
   (mu/merge
    JsonLdBase
    [:map
     ["dh:baseEntity" {:optional true} string?]])))

(def LdSchemaInput
  "Schema for new schema documents"
  (mu/merge
   JsonLdBase
   [:map {:closed false}
    ["dh:columns" [:repeat {:min 1} 
                   [:map 
                    ["csvw:datatype" :string]
                    ["csvw:name" :string]
                    ["csvw:titles" [:sequential :string]]]]]]))
