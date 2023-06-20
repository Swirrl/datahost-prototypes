(ns tpximpact.datahost.ldapi.schemas.series
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]))

;;; ---- API SCHEMA

(def ApiPathParams
  (m/schema
   [:map
    [:series-slug :datahost/slug-string]]
   {:registry registry}))

(def ApiQueryParams
  "Query parameters to the series endpoint"
  [:map
   [:title {:title "Title"
            :description "Title of dataset series"
            :optional true} string?]
   [:description {:title "Description"
                  :description "Description of dataset series"
                  :optional true} string?]])

(def ApiParams
  "API params of the series endpoint."
  (mu/merge
   ApiPathParams
   ApiQueryParams))

;;; ---- MODEL SCHEMA

(def UpsertKeys
  [:map
   [:series :string]])

(def UpsertArgs
  (let [db-schema [:map {}]
        api-params-schema (mu/merge
                           ApiParams
                           (m/schema [:map  
                                      [:op/timestamp :datahost/timestamp]
                                      [:op.upsert/keys UpsertKeys]]
                                     {:registry registry}))
        input-jsonld-doc-schema [:maybe [:map {}]]]
    [:catn
     [:db db-schema]
     [:api-params api-params-schema]
     [:jsonld-doc input-jsonld-doc-schema]]))

(def upsert-args-valid? (m/validator UpsertArgs))
