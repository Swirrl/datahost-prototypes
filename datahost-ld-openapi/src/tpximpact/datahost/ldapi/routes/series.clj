(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.series :as schema]))

(defn get-series-list-route-config [triplestore system-uris]
  {:summary "All series metadata in the database"
   :handler (partial handlers/get-series-list triplestore system-uris)
   :responses {200 {:content {"application/ld+json"
                              {:body string?}}}
               404 {:body routes-shared/NotFoundErrorBody}}})

(defn get-series-route-config [triplestore system-uris]
  {:summary "Retrieve metadata for an existing dataset-series"
   :description "A dataset series is a named dataset regardless of schema, methodology or compatibility changes"
   :handler (partial handlers/get-dataset-series triplestore system-uris)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:content {"application/ld+json"
                              {:body string?}}}
               404 {:body routes-shared/NotFoundErrorBody}}})

(defn put-series-route-config [clock triplestore system-uris]
  {:summary "Create or update metadata on a dataset-series"
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
                    :content {"application/ld+json"
                              {:body routes-shared/ResourceSchema}}}
               201 {:description "Series did not exist previously and was successfully created"
                    :content {"application/ld+json"
                              {:body routes-shared/ResourceSchema}}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
