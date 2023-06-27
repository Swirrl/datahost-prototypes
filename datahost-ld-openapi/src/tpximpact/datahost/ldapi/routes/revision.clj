(ns tpximpact.datahost.ldapi.routes.revision
  (:require
    [reitit.ring.malli]
    [tpximpact.datahost.ldapi.handlers :as handlers]
    [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-revision-route-config [db]
  {:summary "Retrieve metadata for an existing revision"
   :handler (partial handlers/get-revision db)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {200 {:body map?}
               404 {:body [:re "Not found"]}}})

(defn post-revision-route-config [db]
  {:summary (str "Create metadata for a revision. The successfully created resource "
                 "path will be returned in the `Location` header")
   :handler (partial handlers/post-revision db)
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
                    :body map?
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn post-revision-changes-route-config [db]
  {:summary "Add changes to a Revision via a CSV file."
   :handler (partial handlers/post-revision-changes db)
   :parameters {:multipart [:map [:appends reitit.ring.malli/temp-file-part]]
                :path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {201 {:description "Changes were added to a Revision"
                    :body map?
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
