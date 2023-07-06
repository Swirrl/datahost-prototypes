(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [reitit.coercion.malli :as rcm]
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.release :as schema]))

(defn get-release-route-config [db]
  {:summary copy/get-release-summary
   :handler (partial handlers/get-release db)
   :coercion (rcm/create {:transformers {}, :validate false})
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:content
                    {"text/csv" any?
                     "application/json" {:body map?}}}
               404 {:body [:re "Not found"]}}})

(defn put-release-route-config [db]
  {:summary copy/put-release-summary
   :handler (partial handlers/put-release db)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?
                       :release-slug string?}
                :query schema/ApiQueryParams}
   :responses {200 {:description copy/put-release-200-desc
                    :body map?}
               201 {:description copy/put-release-201-desc
                    :body map?}
               500 {:description copy/internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn get-release-ld-schema-config
  [db]
  {:summary copy/get-release-schema-summary
   :handler (partial handlers/get-release-schema db)
   :parameters {:path {:series-slug :string
                       :release-slug :string}}
   :responses {200 {:description copy/get-release-schema-200-desc
                    :body map?}
               404 {:body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}})

(defn put-release-ld-schema-config
  [db]
  {:summary copy/put-release-schema-summary
   :handler (partial handlers/put-release-schema db)
   :parameters {:body routes-shared/LdSchemaInput
                :path {:series-slug :string
                       :release-slug :string
                       :schema-slug :string}}
   :responses {200 {:description copy/put-release-schema-200-desc
                    :body map?}
               201 {:description copy/put-release-schema-201-desc
                    :body map?}
               500 {:description copy/internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message :string]]}}})
