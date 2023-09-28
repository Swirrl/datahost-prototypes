(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [reitit.ring.malli]
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.release :as schema]))

(def internal-server-error-desc "Internal server error")

(defn get-release-list-route-config [triplestore system-uris]
  {:summary "List all releases metadata in the given series"
   :description "List all releases metadata in the given dataset-series."
   :handler (partial handlers/get-release-list triplestore system-uris)
   :parameters {:path [:map routes-shared/series-slug-param-spec]}
   :responses {200 {:content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn get-release-route-config [triplestore change-store system-uris]
  {:summary "Retrieve metadata for an existing release"
   :description "Given a dataset-series slug and a release slug,
return data or metadata associated with a release.

The data associated with the latest release and revision can obtained
by issuing a request with an `Accept` header of `text/csv`, this will
result in a redirection to the latest revision of the data.

The metadata associated with the release can be obtained by issuing a
request with an `Accept` header of `application/ld+json`, which will
return a JSON/LD metadata document describing the release resource."
   :handler (partial handlers/get-release triplestore change-store system-uris)
   :middleware [[(partial middleware/csvm-request-response triplestore system-uris) :csvm-response]
                [(partial middleware/entity-uris-from-path system-uris #{:dh/Release}) :entity-uris]]
   :coercion (rcm/create {:transformers {}, :validate false})
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn put-release-route-config [clock triplestore system-uris]
  {:summary "Create or update metadata for a release"
   :description "Create or update the specified release in the given
dataset-series with the supplied metadata.

If the release did not previously exist and the data is valid a new
release will be created in the specified dataset-series.

If the release previously did exist the metadata document associated
with it will be updated.

NOTE: some fields of metadata are considered \"managed\" and will be
managed by the Datahost platform; for example Datahost will add and
set `dcterms:modified` times for you."
   :handler (partial handlers/put-release clock triplestore system-uris)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore system-uris
                          :dh/Release ::release) :resource-exists?]
                [(partial middleware/validate-creation-body+query-params
                          {:resource-id ::release
                           :body-explainer (get-in routes-shared/explainers [:put-release :body])
                           :query-explainer (get-in routes-shared/explainers [:put-release :query])})
                 :validate-body+query]]
   :parameters {:body [:any]
                :path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]
                :query schema/ApiQueryParams}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Release already existed and was successfully updated"
                    :content {"application/ld+json" string?}}
               201 {:description "Release did not exist previously and was successfully created"
                    :content {"application/ld+json" string?}}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}
   :tags ["Publisher API"]})

(defn get-release-ld-schema-config
  [triplestore system-uris]
  {:summary "Retrieve release schema"
   :description "Returns the Datahost TableSchema associated with the release.

NOTE: Datahost tableschemas are extended subsets of CSVW:TableSchema."
   :handler (partial handlers/get-release-schema triplestore system-uris)
   :parameters {:path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]}
   :responses {200 {:description "Release schema successfully retrieved"
                    :content {"application/ld+json" string?}}
               404 {:body routes-shared/NotFoundErrorBody}}
   :tags ["Consumer API"]})

(defn post-release-ld-schema-config
  [clock triplestore system-uris]
  {:summary "Create schema for a release"
   :description "Associates a Datahost TableSchema with the specified release.

The supplied document should conform to the Datahost TableSchema."
   :handler (partial handlers/post-release-schema clock triplestore system-uris)
   ;; NOTE: file schema JSON content string is validated within the handler itself
   :parameters {:body routes-shared/LdSchemaInput
                :path [:map
                       routes-shared/series-slug-param-spec
                       routes-shared/release-slug-param-spec]}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Schema already exists."
                    :content {"application/ld+json" string?}}
               201 {:description "Schema successfully created"
                    :content {"application/ld+json" string?}}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}
   :tags ["Publisher API"]})
