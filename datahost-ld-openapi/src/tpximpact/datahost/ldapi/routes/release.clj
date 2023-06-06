(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]))

(defn get-release-route-config [db]
  {:summary copy/get-release-summary
   :handler (partial handlers/get-release db)
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:body map?}
               404 {:body [:re "Not found"]}}})

(defn put-release-route-config [db]
  {:summary copy/put-release-summary
   :handler (partial handlers/put-release db)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?
                       :release-slug string?}
                :query [:map
                        [:title {:title "Title"
                                 :description "Title of release"
                                 :optional true} string?]
                        [:description {:title "Description"
                                       :description "Description of release"
                                       :optional true} string?]]
                :responses {200 {:description copy/put-release-200-desc
                                 :body map?}
                            201 {:description copy/put-release-201-desc
                                 :body map?}
                            500 {:description copy/internal-server-error-desc
                                 :body [:map
                                        [:status [:enum "error"]]
                                        [:message string?]]}}}})
