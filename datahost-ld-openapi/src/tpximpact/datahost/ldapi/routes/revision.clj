(ns tpximpact.datahost.ldapi.routes.revision
  (:require
   [reitit.ring.malli]
   [reitit.coercion :as rc]
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-revision-route-config [triplestore change-store system-uris]
  {:summary "Retrieve metadata or CSV contents for an existing revision"
   :description "A revision represents a named set of updates to a
 dataset release.

A revision is essentially a pointer to the head of a sequence of
changes or commits, that when materialised result in a dataset
conforming to the release's stated schema.

See the [description of
Release](#/Consumer%20API/get_data__series_slug__releases__release_slug_)
for a summary of the versioning model.
"
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-revision triplestore change-store system-uris)
   :middleware [[(partial middleware/csvm-request-response triplestore system-uris) :csvm-response]
                [(partial middleware/entity-or-not-found triplestore system-uris :dh/Revision)
                 :entity-or-not-found]
                [(partial middleware/entity-uris-from-path system-uris #{:dh/Release}) :entity-uris]]
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec
                       routes-shared/revision-id-param-spec]}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn get-revision-list-route-config [triplestore system-uris]
  {:summary "List all revisions metadata in the given release"
   :description "List all revisions metadata in the given release."
   :handler (partial handlers/get-revision-list triplestore system-uris)
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]}
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn post-revision-route-config [triplestore system-uris]
  {:summary (str "Create metadata for a revision. The successfully created resource "
                 "path will be returned in the `Location` header")

   :description "Create a new revision with the given metadata.

You should first create a revision to before submitting the changes
that make up that revision.

*NOTE: It is considered an error to commit data into a revision that
 has been succeeded by another.*
"

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
                :path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of revision"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of revision"
                                       :optional true} string?]]}
   :openapi {:security [{"basic" []}]}
   :responses {201 {:description "Revision was successfully created"
                    :content {"application/ld+json" string?}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}
   :tags ["Publisher API"]})

(defn changes-route-base [triplestore change-store system-uris change-kind]
  {:handler (partial handlers/post-change triplestore change-store system-uris change-kind)
   :middleware [[(partial middleware/entity-uris-from-path system-uris #{:dh/Release :dh/Revision}) :entity-uris]
                [(partial middleware/resource-exist? triplestore system-uris :dh/Revision) :resource-exists?]
                ;; don't allow changes to revision N when revision N+1 already exists
                [(partial middleware/resource-already-created?
                          triplestore system-uris
                          {:resource :dh/Revision
                           :param-fn (fn [{:keys [revision-id]}]
                                       {:revision-id (inc (Integer/parseInt  revision-id))})})
                 :resource-already-created?]
                [(partial middleware/validate-headers
                          (malli.core/validator routes-shared/CreateChangeHeaders)
                          (malli.core/explainer routes-shared/CreateChangeHeaders))
                 :validate-change-request-header]]
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec
                       routes-shared/revision-id-param-spec]
                :query routes-shared/CreateChangeInputQueryParams}
   :openapi {:security [{"basic" []}]
             :requestBody {:content {"text/csv" {:schema {:type "string" :format "binary"}}}}}
   ::rc/parameter-coercion {:query (rc/->ParameterCoercion :query-params :string false true)}
   :responses {201 {:description "Changes were added to a Revision"
                    :content {"application/ld+json" string?}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn post-revision-appends-changes-route-config [triplestore change-store system-uris]
  (merge (changes-route-base triplestore change-store system-uris :dh/ChangeKindAppend)
         {:summary "Add appends changes to a Revision via a CSV file."
          :description "Upload a delta of rows to append to a dataset as a CSV file."
          :tags ["Publisher API"]}))

(defn post-revision-retractions-changes-route-config [triplestore change-store system-uris]
  (merge (changes-route-base triplestore change-store system-uris :dh/ChangeKindRetract)
         {:summary "Add retractions changes to a Revision via a CSV file."
          :description "Upload a delta of rows to retract from a dataset as a CSV file."
          :tags ["Publisher API"]}))

(defn post-revision-corrections-changes-route-config [triplestore change-store system-uris]
  (merge (changes-route-base triplestore change-store system-uris :dh/ChangeKindCorrect)
         {:summary "Add corrections to a Revision via a CSV file."
          :description "Upload a file of rows containing corrected
 'measures' as a CSV file. Datahost will attempt to detect which
 observations have been corrected, and will maintain a sequence of
 previous values, for users."

          :tags ["Publisher API"]}))

(defn get-revision-changes-route-config [triplestore change-store system-uris]
  {:summary "Retrieve CSV contents for an existing change"
   :description "Return the data as a CSV file for the specified change.  This returns individual deltas."
   :coercion (rcm/create {:transformers {}, :validate false})
   :handler (partial handlers/get-change triplestore change-store system-uris)
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec
                       routes-shared/revision-id-param-spec
                       routes-shared/change-id-param-spec]}
   :responses {200 {:content
                    {"text/csv" any?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})
