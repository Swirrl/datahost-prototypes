(ns tpximpact.datahost.ldapi.routes.revision
  (:require
    [reitit.ring.malli]
    [reitit.coercion.malli :as rcm]
    [tpximpact.datahost.ldapi.handlers :as handlers]
    [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-revision-route-config [db]
  {:summary "Retrieve metadata or CSV contents for an existing revision"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-revision db)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json" {:body map?}}}
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

(defn get-revision-changes-route-config [db]
  {:summary "Retrieve CSV contents for an existing change"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-change db)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?
                       :change-id int?}}
   :responses {200 {:content
                    {"text/csv" any?}}
               404 {:body [:re "Not found"]}}})
