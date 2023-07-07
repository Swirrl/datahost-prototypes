(ns tpximpact.datahost.ldapi.routes.revision
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-revision-route-config [triplestore]
  {:summary "Retrieve metadata for an existing revision"
   :handler (partial handlers/get-revision triplestore)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {200 {:body map?}
               404 {:body [:re "Not found"]}}})

(defn post-revision-route-config [triplestore]
  {:summary "Create metadata for a revision"
   :handler (partial handlers/post-revision triplestore)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?
                       :release-slug string?}
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of revision"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of revision"
                                       :optional true} string?]]}
   :responses {201 {:description "Revision was successfully created"
                    :body string?}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
