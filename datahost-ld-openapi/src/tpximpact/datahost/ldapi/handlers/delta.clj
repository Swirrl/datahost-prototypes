(ns tpximpact.datahost.ldapi.handlers.delta ;TODO: move to *.handlers.delta
  "Contains functionality for diffing datasets.

  TODO: proper explanation."
  (:require [malli.error :as m.e]
            [malli.core :as m]
            [clojure.string :as str]
            [ring.util.io :as ring-io]
            [ring.util.response :as util.response]
            [reitit.ring.malli :as ring.malli]
            [tablecloth.api :as tc]
            [tpximpact.datahost.ldapi.db :as db]
            [tpximpact.datahost.ldapi.store :as store]
            [tpximpact.datahost.ldapi.handlers :refer [->byte-array-input-stream]]
            [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
            [tpximpact.datahost.ldapi.util.data.compilation :as data.compilation]
            [tpximpact.datahost.ldapi.util.data.internal :as data.internal]
            [tpximpact.datahost.ldapi.util.data.delta :as data.delta]))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn error-no-revisions
  []
  (-> (util.response/response "This release has no revisions yet")
      (util.response/status 422)
      (util.response/header "content-type" "text/plain")))

(defmulti -post-delta-files (fn [sys {{:strs [accept]} :headers}] accept))

(defmethod -post-delta-files "text/csv";; "application/x-datahost-tx-csv"
  [sys request]
  (let [{:keys [triplestore change-store]} sys
        {{{:keys [csv]} :multipart} :parameters
         {release-uri :dh/Release} :datahost.request/uris} request

        schema (db/get-release-schema triplestore release-uri)

        change-infos (db/get-changes-info triplestore release-uri)
        _ (when (empty? change-infos)
            (throw (ex-info "This release has no revisions"
                            {:type :tpximpact.datahost.ldapi.errors/exception})))
        
        {snapshot-key :snapshotKey rev-uri :rev} (last change-infos)
        _ (when (nil? snapshot-key)
            (throw (ex-info (format "Missing :snapshotKey for '%s'" rev-uri)
                            {:type :tpximpact.datahost.ldapi.errors/exception})))
        
        row-schema (data.validation/make-row-schema schema)
        opts {:store change-store :file-type :csv :enforce-schema row-schema}
        _ (assert snapshot-key)
        ds-release (data.validation/as-dataset snapshot-key opts)
        ds-input (data.validation/as-dataset (->byte-array-input-stream (:body request)) opts)
        diff-results (data.delta/delta-dataset ds-release ds-input {:row-schema row-schema})]
    {:status 200
     :body (write-dataset-to-outputstream diff-results)}))

(defn post-delta-files [sys request]    ;TODO: rename this fn
  ;; TODO: add basic validation for incoming dataset, (e.g.
  ;; `data.compilation/validate-row-uniqueness`) after ns reorg
  (-post-delta-files sys request))

; Curl command used to test the delta route:
;
; curl -X 'POST' 'http://localhost:3000/delta' -H 'accept: text/csv' \
;   -H 'Content-Type: multipart/form-data' \
;   -F 'base-csv=@./env/test/resources/test-inputs/delta/orig.csv;type=text/csv' \
;   -F 'delta-csv=@./env/test/resources/test-inputs/delta/new.csv;type=text/csv' \
;   --output ./deltas.csv
;
