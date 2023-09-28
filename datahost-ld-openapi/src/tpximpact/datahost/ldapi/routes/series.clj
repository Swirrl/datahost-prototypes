(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.series :as schema]))

(defn get-series-list-route-config [triplestore system-uris]
  {:summary "All series metadata in the database"
   :description "Lists all dataset-series stored in the database."
   :handler (partial handlers/get-series-list triplestore system-uris)
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}})

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
   :parameters {:path {:series-slug string?}}
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}})

(defn put-series-route-config [clock triplestore system-uris]
  {:summary "Create or update metadata on a dataset-series"
   :description "Creates or updates the dataset-series with the supplied metadata.

Additionally Datahost will augment the supplied metadata with some
additional managed properties, such as `dcterms:issued` and
`dcterms:modified`.
"
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
   :parameters {:body [:any]            ; validation logic via middleware
                :path {:series-slug string?}
                :query schema/ApiQueryParams}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :content {"application/ld+json" routes-shared/ResourceSchema}}
               201 {:description "Series did not exist previously and was successfully created"
                    :content {"application/ld+json" routes-shared/ResourceSchema}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn delete-series-route-config [triplestore change-store system-uris]
  {:summary "Delete a series and all its child resources"
   :handler (partial handlers/delete-dataset-series triplestore change-store system-uris)
   :parameters {:path {:series-slug string?}}
   :responses {204 {:description "Series existed and was successfully deleted"}
               404 {:description "Series does not exist"}}})
