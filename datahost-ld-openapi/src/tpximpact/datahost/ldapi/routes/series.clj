(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]))

(def JsonLdSchema
  [:maybe
   [:map
    ["dcterms:title" {:optional true} string?]
    ["dcterms:description" {:optional true} string?]
    ["@context" [:or :string
                 [:tuple :string [:map
                                  ["@base" string?]]]]]]])

(defn get-series-route-config [db]
  {:summary copy/get-series-summary
   :description copy/get-series-description
   :handler (partial handlers/get-dataset-series db)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:body JsonLdSchema}
               404 {:body [:enum "Not found"]}}})

(defn put-series-route-config [db]
  {:summary copy/put-series-summary
   :handler (partial handlers/put-dataset-series db)
   :parameters {:body JsonLdSchema
                :path {:series-slug string?}
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of dataset"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of dataset"
                                       :optional true} string?]]}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :body map?}
               201 {:description "Series did not exist previously and was successfully created"
                    :body map?}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
