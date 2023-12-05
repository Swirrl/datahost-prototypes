(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [reitit.ring.malli :as ring.malli]
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.handlers.delta :as handlers.delta]
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
return a JSON/LD metadata document describing the release resource.

Releases themselves do not contain data, and instead defer to
revisions to describe the data at a particular point in time. This
means that all releases that contain any data MUST have at least one
revision containing at least one change/commit.

To load a release with data you must first create a revision with
it (representing the identifier for the next update), then within that
revision you create any number of changes/commits.

e.g. looking at the following diagram:

- My Release
  - Revision 1
    - Commit 1 (append 100 Rows)
  - Revision 2
    - Commit 1 (delete 1 Row)
    - Commit 2 (append 100 Rows)
    - Commit 3 (correct 2 Rows)

We can see that the Revision 1 contains the first commit of data to
the dataset.

Revision 2 however follows Revision 1, and it contains 3 further
commits (R2.1 R2.2 & R2.3), but it also follows R1.1, so the state of
the Revision when materialised contains the data supplied in
R1.1, less the delete made in R2.1.

It is currently considered an error to put data into a revision which
has been succeeded by another.
"
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

(defn post-release-delta-config [{:keys [system-uris triplestore] :as sys}]
  {:handler (partial handlers.delta/post-delta-files sys)
   :middleware [[(partial middleware/entity-uris-from-path system-uris #{:dh/Release}) :release-uri]
                [(partial middleware/resource-exist? triplestore system-uris :dh/Release) :release-exists?]]
   :parameters {}
   :openapi {:security [{"basic" []}]
             :requestBody {:content {"text/csv" {:schema {:type "string" :format "binary"}}}}}
   :responses {200 {:description "Differences between latest release and supplied dataset."
                    ;;  application/x-datahost-tx-csv
                    :content {"text/csv" any?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
