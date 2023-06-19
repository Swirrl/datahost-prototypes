(ns tpximpact.datahost.ldapi.schemas.release
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]))

;;; ---- API SCHEMA

(def ApiPathParams
  (m/schema [:map 
             [:series-slug :datahost/slug-string]
             [:release-slug :datahost/slug-string]]
            {:registry registry}))

(def ApiQueryParams
  [:map
   [:title {:title "Title"
            :description "Title of release"
            :optional true} string?]
   [:description {:title "Description"
                  :description "Description of release"
                  :optional true} string?]])

(def ApiParams
  (mu/merge
   ApiPathParams
   ApiQueryParams))

(def api-params-valid? (m/validator ApiParams))

;;; ---- MODEL SCHEMA

(def UpsertArgs
  (let [db-schema [:map {}]
        api-params-schema (m/schema [:map
                                     [:op/timestamp :datahost/timestamp]]
                                    {:registry registry})
        input-jsonld-doc-schema [:maybe [:map {}]]]
    [:catn
     [:db db-schema]
     [:api-params api-params-schema]
     [:jsonld-doc input-jsonld-doc-schema]]))

(def upsert-args-valid? (m/validator UpsertArgs))
