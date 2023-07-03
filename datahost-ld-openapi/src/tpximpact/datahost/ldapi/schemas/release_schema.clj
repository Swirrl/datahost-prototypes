(ns tpximpact.datahost.ldapi.schemas.release-schema
  (:require
   [malli.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.schemas.release :as s.release]
   [tpximpact.datahost.ldapi.schemas.common :refer [registry]]))


;;; ---- API SCHEMA

(def ApiQueryParams
  ;; changes to schema not supported
  [:map])

(def ApiParams
  (mu/merge ApiQueryParams s.release/ApiPathParams))

(def api-params-valid? (m/validator ApiParams))

;;; ---- MODEL SCHEMA

