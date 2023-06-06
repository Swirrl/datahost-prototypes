(ns tpximpact.datahost.ldapi.routes.shared)

(def JsonLdSchema
  [:maybe
   [:map {:closed false}
    ["dcterms:title" {:optional true} string?]
    ["dcterms:description" {:optional true} string?]
    ["@type" {:optional true} string?]
    ["@id" {:optional true} string?]
    ["dh:baseEntity" {:optional true} string?]
    ["@context" [:or :string
                 [:tuple :string [:map
                                  ["@base" string?]]]]]]])
