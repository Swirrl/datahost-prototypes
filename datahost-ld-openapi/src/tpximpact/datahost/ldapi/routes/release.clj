(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [reitit.ring.malli]
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.release :as schema]))

(def internal-server-error-desc "Internal server error")

(defn get-release-route-config [triplestore change-store]
  {:summary "Retrieve metadata for an existing release"
   :handler (partial handlers/get-release triplestore change-store)
   :coercion (rcm/create {:transformers {}, :validate false})
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json+ld" {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn put-release-route-config [clock triplestore]
  {:summary "Create or update metadata for a release"
   :handler (partial handlers/put-release clock triplestore)
   :middleware [[middleware/json-only :json-only]
                [(partial middleware/flag-resource-exists triplestore
                          :dh/Release ::release) :resource-exists?]
                [(partial middleware/validate-creation-body+query-params
                          {:resource-id ::release
                           :body-explainer (get-in routes-shared/explainers [:put-release :body])
                           :query-explainer (get-in routes-shared/explainers [:put-release :query])})
                 :validate-body+query]]
   :parameters {:body [:any]
                :path {:series-slug string?
                       :release-slug string?}
                :query schema/ApiQueryParams}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Release already existed and was successfully updated"
                    :content {"application/json+ld"
                              {:body string?}}}
               201 {:description "Release did not exist previously and was successfully created"
                    :content {"application/json+ld"
                              {:body string?}}}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn get-release-ld-schema-config
  [triplestore]
  {:summary "Retrieve release schema"
   :handler (partial handlers/get-release-schema triplestore)
   :parameters {:path {:series-slug :string
                       :release-slug :string}}
   :responses {200 {:description "Release schema successfully retrieved"
                    :content {"application/json+ld"
                              {:body string?}}}
               404 {:body [:re "Not found"]}}})

(defn post-release-ld-schema-config
  [clock triplestore]
  {:summary "Create schema for a release"
   :handler (partial handlers/post-release-schema clock triplestore)
   ;; NOTE: file schema JSON content string is validated within the handler itself
   :parameters {:multipart [:map [:schema-file reitit.ring.malli/temp-file-part]]
                :path {:series-slug :string
                       :release-slug :string}}
   :openapi {:security [{"basic" []}]}
   :responses {200 {:description "Schema already exists."
                    :content {"application/json+ld"
                              {:body string?}}}
               201 {:description "Schema successfully created"
                    :content {"application/json+ld"
                              {:body string?}}}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}})
