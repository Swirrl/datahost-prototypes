(ns tpximpact.datahost.ldapi.routes.revision
  (:require
   [reitit.ring.malli]
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [clojure.data.json :as json]))

(defn get-revision-route-config [triplestore change-store system-uris]
  {:summary "Retrieve metadata or CSV contents for an existing revision"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-revision triplestore change-store system-uris)
   :middleware [[(partial middleware/csvm-request-response triplestore system-uris) :csvm-response]
                [(partial middleware/entity-or-not-found triplestore system-uris :dh/Revision)
                 :entity-or-not-found]
                [(partial middleware/entity-uris-from-path system-uris #{:dh/Release}) :entity-uris]]
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json+ld" {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn get-release-list-route-config [triplestore system-uris]
  {:summary "All releases metadata in the given series"
   :handler (partial handlers/get-release-list triplestore system-uris)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:content {"application/json+ld"
                              {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn get-revision-list-route-config [triplestore system-uris]
  {:summary "All revisions metadata in the given release"
   :handler (partial handlers/get-revision-list triplestore system-uris)
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:content {"application/json+ld"
                              {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn post-revision-route-config [triplestore system-uris]
  {:summary (str "Create metadata for a revision. The successfully created resource "
                 "path will be returned in the `Location` header")
   :handler (partial handlers/post-revision triplestore system-uris)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore system-uris
                          :dh/Revision ::revision) :resource-exists?]
                [(partial middleware/validate-creation-body+query-params
                          {:resource-id ::revision
                           :body-explainer (get-in routes-shared/explainers [:post-revision :body])
                           :query-explainer (get-in routes-shared/explainers [:post-revision :query])})
                 :validate-body+query]]
   :parameters {:body [:any]
                :path {:series-slug string?
                       :release-slug string?}
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of revision"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of revision"
                                       :optional true} string?]]}
   :openapi {:security [{"basic" []}]}
   :responses {201 {:description "Revision was successfully created"
                    :content {"application/json+ld"
                              {:body string?}}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn changes-route-base [triplestore change-store system-uris change-kind]
  {:handler (partial handlers/post-change triplestore change-store system-uris change-kind)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/entity-uris-from-path system-uris #{:dh/Release :dh/Revision}) :entity-uris]
                [(partial middleware/resource-exist? triplestore system-uris :dh/Revision) :resource-exists?]
                [(partial middleware/resource-already-created?
                          triplestore system-uris
                          {:resource :dh/Change :missing-params {:change-id 1}} )
                 :resource-already-created?]]
   :parameters {:multipart [:map
                            [:jsonld-doc routes-shared/CreateChangeInput]
                            [:appends reitit.ring.malli/temp-file-part]]
                :path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :openapi {:security [{"basic" []}]}
   :responses {201 {:description "Changes were added to a Revision"
                    :content {"application/json+ld"
                              {:body string?}}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn post-revision-appends-changes-route-config [triplestore change-store system-uris]
  (merge (changes-route-base triplestore change-store system-uris :dh/ChangeKindAppend)
         {:summary "Add appends changes to a Revision via a CSV file."}))

(defn post-revision-deletes-changes-route-config [triplestore change-store system-uris]
  (merge (changes-route-base triplestore change-store system-uris :dh/ChangeKindRetract)
         {:summary "Add deletes changes to a Revision via a CSV file."}))

(defn get-revision-changes-route-config [triplestore change-store system-uris]
  {:summary "Retrieve CSV contents for an existing change"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-change triplestore change-store system-uris)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?
                       :change-id int?}}
   :responses {200 {:content
                    {"text/csv" any?}}
               404 {:body [:re "Not found"]}}})
