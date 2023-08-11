(ns tpximpact.datahost.ldapi.routes.revision
  (:require
    [reitit.ring.malli]
    [reitit.coercion.malli :as rcm]
    [tpximpact.datahost.ldapi.handlers :as handlers]
    [tpximpact.datahost.ldapi.routes.middleware :as middleware]
    [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-revision-route-config [triplestore change-store]
  {:summary "Retrieve metadata or CSV contents for an existing revision"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-revision triplestore change-store)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json+ld" {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn get-release-list-route-config [triplestore]
  {:summary "All releases metadata in the given series"
   :handler (partial handlers/get-release-list triplestore)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:content {"application/json+ld"
                              {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn get-revision-list-route-config [triplestore]
  {:summary "All revisions metadata in the given release"
   :handler (partial handlers/get-revision-list triplestore)
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:content {"application/json+ld"
                              {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn post-revision-route-config [triplestore]
  {:summary (str "Create metadata for a revision. The successfully created resource "
                 "path will be returned in the `Location` header")
   :handler (partial handlers/post-revision triplestore)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore
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

(defn changes-route-base [triplestore change-store change-kind]
  {:handler (partial handlers/post-change triplestore change-store change-kind)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore
                          :dh/Revision ::revision) :resource-exists?]
                [(partial middleware/validate-creation-body+query-params
                          {:resource-id ::revision
                           :body-explainer (get-in routes-shared/explainers [:post-revision-change :body])
                           :query-explainer (get-in routes-shared/explainers [:put-revision-change :query])})
                 :validate-body+query]]
   :parameters {:multipart [:map [:appends reitit.ring.malli/temp-file-part]]
                :body [:any]
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

(defn post-revision-appends-changes-route-config [triplestore change-store]
  (merge (changes-route-base triplestore change-store :dh/ChangeKindAppend)
         {:summary "Add appends changes to a Revision via a CSV file."}))

(defn post-revision-deletes-changes-route-config [triplestore change-store]
  (merge (changes-route-base triplestore change-store :dh/ChangeKindRetract)
         {:summary "Add deletes changes to a Revision via a CSV file."}))

(defn get-revision-changes-route-config [triplestore change-store]
  {:summary "Retrieve CSV contents for an existing change"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-change triplestore change-store)
   :parameters {:path {:series-slug string?
                       :release-slug string?
                       :revision-id int?
                       :change-id int?}}
   :responses {200 {:content
                    {"text/csv" any?}}
               404 {:body [:re "Not found"]}}})
