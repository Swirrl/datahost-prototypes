(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.release :as schema]))

(def internal-server-error-desc "Internal server error")

(defn get-release-route-config [db]
  {:summary "Retrieve metadata for an existing release"
   :handler (partial handlers/get-release db)
   :coercion (rcm/create {:transformers {}, :validate false})
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json" {:body map?}}}
               404 {:body [:re "Not found"]}}})

(defn put-release-route-config [db triplestore]
  {:summary "Create or update metadata for a release"
   :handler (partial handlers/put-release db triplestore)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?
                       :release-slug string?}
                :query schema/ApiQueryParams}
   :responses {200 {:description "Release already existed and was successfully updated"
                    :body map?}
               201 {:description "Release did not exist previously and was successfully created"
                    :body map?}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn get-release-ld-schema-config
  [db]
  {:summary "Retrieve release schema"
   :handler (partial handlers/get-release-schema db)
   :parameters {:path {:series-slug :string
                       :release-slug :string}}
   :responses {200 {:description "Release schema successfully retrieved"
                    :body map?}
               404 {:body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}})

(defn put-release-ld-schema-config
  [db]
  {:summary "Create schema for a release"
   :handler (partial handlers/put-release-schema db)
   :parameters {:body routes-shared/LdSchemaInput
                :path {:series-slug :string
                       :release-slug :string
                       :schema-slug :string}}
   :responses {200 {:description "Schema already exists."
                    :body map?}
               201 {:description "Schema successfully created"
                    :body map?}
               500 {:description internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}})
