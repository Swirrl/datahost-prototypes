(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]))

(def JsonLdSchema
  [:maybe
   [:map
    ["dcterms:title" {:optional true} string?]
    ["dcterms:description" {:optional true} string?]
    ["@context" [:or :string
                 [:tuple :string [:map
                                  ["@base" string?]]]]]]])

(defn get-series-route-config [db]
  {:summary "Retrieve metadata for an existing dataset-series"
   :description "blah blah blah. [a link](http://foo.com/)
* bulleted
* list
* here"
   :parameters {:path {:series-slug string?}}
   :responses {200 {:body JsonLdSchema}
               404 {:body [:enum "Not found"]}}
   :handler (partial handlers/get-dataset-series db)})

(defn put-series-route-config [db]
 {:summary "Create or update metadata on a dataset-series"
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
                                     [:message string?]]}}
             :handler (partial handlers/put-dataset-series db)} )
