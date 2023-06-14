(ns tpximpact.datahost.ldapi.routes.copy)

(def root-route-summary)

(def get-series-summary "Retrieve metadata for an existing dataset-series")
(def get-series-description "A dataset series is a named dataset regardless of schema, methodology or compatibility changes")
(def put-series-summary "Create or update metadata on a dataset-series")

(def get-release-summary "Retrieve metadata for an existing release")
(def put-release-summary "Create or update metadata for a release")
(def put-release-200-desc "Release already existed and was successfully updated")
(def put-release-201-desc "Release did not exist previously and was successfully created")

(def internal-server-error-desc "Internal server error")
