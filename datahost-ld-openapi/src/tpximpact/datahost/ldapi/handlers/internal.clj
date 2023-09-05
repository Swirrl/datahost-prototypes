(ns tpximpact.datahost.ldapi.handlers.internal
  "Functions in this namespace shouldn't be used outside
  `tpximpact.datahost.ldapi.handlers` namespace."
  (:require
   [clojure.tools.logging :as log]
   [grafter.matcha.alpha :as matcha]
   [malli.core :as m]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.compact :as cmp]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.util.data-compilation :as data-compilation]
   [tpximpact.datahost.ldapi.util.triples
    :refer [triples->ld-resource]])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream]))


(defn- previous-dataset-snapshot-key
  "Returns a  [[store/ChangeStore]] key for the previous revision+change, or nil.
  
  Throws when previous change exists but does not
  contain :dh/revisionSnapshotCSV."
  [{:keys [triplestore system-uris]} params]
  (let [prev-change (db/get-previous-change triplestore system-uris
                                            (update params :revision-id #(Long/parseLong %)))
        prev-change (some-> prev-change matcha/index-triples triples->ld-resource)
        prev-ds-snapshot-key (get prev-change (cmp/expand :dh/revisionSnapshotCSV))]
    ;; we have previous change but no snapshot? how did we get into that state?
    (when (and prev-change (not prev-ds-snapshot-key))
      (throw (ex-info "Previous change has no snapshot file."
                      {:path-params params :previous-change prev-change})))
    prev-ds-snapshot-key))

(defn- ->change-info
  [data-ref kind fmt]
  (assert data-ref)
  {:datahost.change.data/ref data-ref
   :datahost.change/kind kind
   :datahost.change.data/format fmt})

(defn post-change--generate-csv-snapshot
  "Returns a map  {:new-snapshot-key ... (optional-key :previous-snapshot-key) ...}.

  Throws when we found previous change, but there's no CSV snapshot
  associated with it.

  Generates new dataset snapshot and stores it in change-store.
  
  Assumptions:
  - the incoming change's (CSV) dataset fits in memory
  - the existing dataset snapshot fits in memory
  - the new snapshot (existing snapshot + changes) fits in memory."
  [{:keys [triplestore change-store system-uris] :as sys}
   {:keys [change-id change-kind path-params] change-ds :dataset :as change-info}]
  {:pre [(m/validate s.common/ChangeKind change-kind) (contains? change-info "dcterms:format")]}
  (let [prev-ds-snapshot-key (previous-dataset-snapshot-key sys (assoc path-params :change-id change-id))
        change-ds-fmt (get change-info "dcterms:format")
        changes (if prev-ds-snapshot-key
                  [(->change-info prev-ds-snapshot-key :dh/ChangeKindAppend "text/csv")
                   (->change-info change-ds change-kind change-ds-fmt)]
                  [(->change-info change-ds change-kind change-ds-fmt)])
        os ^ByteArrayOutputStream (ByteArrayOutputStream.)
        ds (data-compilation/compile-dataset {:changes changes :store change-store})
        _ (tc/write! ds os {:file-type :csv})
        is ^ByteArrayInputStream (ByteArrayInputStream. (.toByteArray os))
        insert-request (store/make-insert-request! change-store is)]

    (store/request-data-insert change-store insert-request)
    (log/debug "post-change--generate-csv-snapshot:" path-params
               {:snapshot-rows (tc/row-count ds)
                :bytes-written (.size os)
                :snapshot-key (:key insert-request)})

    (cond-> {:new-snapshot-key (:key insert-request)}
      prev-ds-snapshot-key (assoc :previous-snapshot-key prev-ds-snapshot-key))))
