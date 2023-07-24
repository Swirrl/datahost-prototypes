(ns tpximpact.datahost.ldapi.routes.shared
  (:require [malli.core :as m]
            [malli.util :as mu]))


(def JsonLdBase
  "Common entries in JSON-LD documents.

  Note: we don't require users to pass @context, since we accept a
  relaxed JSON-LD subset."
  [:map {:closed false}
   ["dcterms:title" {:optional true} string?]
   ["dcterms:description" {:optional true} string?]
   ["@type" {:optional true} string?]
   ["@id" {:optional true} string?]
   ;; ["@context" [:or :string [:tuple :string [:map ["@base" string?]]]]]
   ])

(def JsonLdSchema
  "Datahost specific JSON-LD documents"
  (:maybe
   (mu/merge
    JsonLdBase 
    [:map
     ["dh:baseEntity" {:optional true} string?]])))

(def LdSchemaInputColumn
  [:map 
   ["csvw:datatype" [:or
                     [:enum :integer :string :double]
                     ;; [:map]
                     ]]
   ["csvw:name" :string]
   ["csvw:titles" [:or
                   :string
                   [:sequential :string]]]])

(def LdSchemaInput
  "Schema for new schema documents"
  (mu/merge
   JsonLdBase
   [:map {:closed false}
    ["dh:columns" [:repeat {:min 1} LdSchemaInputColumn]]]))

;; TODO: create better resource representation
(def ResourceSchema
  [:string])
