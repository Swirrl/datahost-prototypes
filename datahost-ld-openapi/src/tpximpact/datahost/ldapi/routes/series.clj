(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.series :as schema]))

(defn get-series-route-config [triplestore]
  {:summary copy/get-series-summary
   :description copy/get-series-description
   :handler (partial handlers/get-dataset-series triplestore)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:body string?}
               404 {:body [:re "Not found"]}}})

(defn put-series-route-config [clock triplestore]
  {:summary copy/put-series-summary
   :handler (partial handlers/put-dataset-series clock triplestore)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?}
                :query schema/ApiQueryParams}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :body routes-shared/ResourceSchema}
               201 {:description "Series did not exist previously and was successfully created"
                    :body routes-shared/ResourceSchema}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
