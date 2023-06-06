(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-series-route-config [db]
  {:summary copy/get-series-summary
   :description copy/get-series-description
   :handler (partial handlers/get-dataset-series db)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:body routes-shared/JsonLdSchema}
               404 {:body [:re "Not found"]}}})

(defn put-series-route-config [db]
  {:summary copy/put-series-summary
   :handler (partial handlers/put-dataset-series db)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?}
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of dataset series"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of dataset series"
                                       :optional true} string?]]}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :body routes-shared/JsonLdSchema}
               201 {:description "Series did not exist previously and was successfully created"
                    :body routes-shared/JsonLdSchema}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
