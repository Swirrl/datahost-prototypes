(ns tpximpact.datahost.ldapi.routes.shared)

(def JsonLdSchema
  [:maybe
   [:map {:closed false}
    ["dcterms:title" {:optional true} string?]
    ["dcterms:description" {:optional true} string?]
    ["@type" {:optional true} string?]
    ["@id" {:optional true} string?]
    ["dh:baseEntity" {:optional true} string?]
    ["@context" {:optional true} [:or :string
                                  [:tuple :string [:map {:closed false}
                                                   ["@base" string?]]]]]]])

;; TODO: create better resource representation
(def ResourceSchema
  [:string])
