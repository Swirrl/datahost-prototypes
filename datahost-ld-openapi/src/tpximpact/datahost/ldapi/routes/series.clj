(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-series-list-route-config [triplestore system-uris]
  {:summary "List all series metadata in the database"
   :description "Lists all dataset-series stored in the database."
   :handler (partial handlers/get-series-list triplestore system-uris)
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn get-series-route-config [triplestore system-uris]
  {:summary "Retrieve metadata for an existing dataset-series"
   :description "A dataset-series refers to a dataset over time irrespective of any
potential schema or methodology changes which may occur throughout
its life time.

For example \"The Census\" is a dataset-series, which has had
many releases and revisions over its long life.

By convention a series should not contain any notion of a specific
release or revision as part of its name."
   :handler (partial handlers/get-dataset-series triplestore system-uris)
   :parameters {:path [:map routes-shared/series-slug-param-spec]}
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn put-series-route-config [clock triplestore system-uris]
  {:summary "Create or update metadata on a dataset-series"
   :description "Create or update metadata on a dataset-series.

If the release did not previously exist and the data
is valid a new release will be created in the specified
dataset-series.

If the release previously did exist the metadata document associated
with it will be updated.

NOTE: some fields of metadata are considered \"managed\" and will be
managed by the Datahost platform; for example Datahost will add and
set `dcterms:modified` times for you."
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore system-uris
                          :dh/DatasetSeries ::series)
                 :resource-exists?]
                [(partial middleware/validate-creation-body+query-params
                          {:resource-id ::series
                           :body-explainer (get-in routes-shared/explainers [:put-series :body])
                           :query-explainer (get-in routes-shared/explainers [:put-series :query])})
                 :validate-body+query]]
   :handler (partial handlers/put-dataset-series clock triplestore system-uris)
   :parameters {:body [:any]         ; validation logic via middleware
                :path [:map routes-shared/series-slug-param-spec]
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of dataset series.

_Note: Setting this parameter will override the value of `dcterms:title` in the JSON/LD metadata document in the request body._"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of dataset series.

_Note: Setting this parameter will override the value of `dcterms:description` in the JSON/LD metadata document in the request body._"
                                       :optional true} string?]]}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :content {"application/ld+json" routes-shared/ResourceSchema}}
               201 {:description "Series did not exist previously and was successfully created"
                    :content {"application/ld+json" routes-shared/ResourceSchema}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}
   :tags ["Publisher API"]})

(defn delete-series-route-config
  [{:keys [triplestore change-store system-uris] :as system}]
  {:summary "Delete a series and all its child resources"
   :description "Deletes the given dataset-series and all of its child resources, i.e. releases, schemas, revisions, and commits.

**WARNING: This route is highly destructive and should not be used as part of a standard workflow, as it will break commitments to dataset consumers.**"
   :handler (partial handlers/delete-dataset-series system)
   :middleware [[(partial middleware/entity-uris-from-path system-uris #{:dh/DatasetSeries}) :entity-uris]
                [(partial middleware/entity-or-not-found triplestore system-uris :dh/DatasetSeries) :entities]]
   :parameters {:path [:map routes-shared/series-slug-param-spec]}
   :responses {204 {:description "Series existed and was successfully deleted"}
               404 {:description "Series does not exist"}}
   :tags ["Publisher API"]})
