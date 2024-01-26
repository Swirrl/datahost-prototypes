(ns tpximpact.datahost.ldapi.store.sql
  (:require
   [next.jdbc :as jdbc]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.data.csv :as csv]
   [integrant.core :as ig]
   [malli.core :as m]
   [malli.error :as m.error]
   [metrics.timers :refer [time!]]
   [tpximpact.datahost.ldapi.metrics :as metrics]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.store.file :as store.file]
   [tpximpact.datahost.ldapi.models.release :as m.release]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]
   [tpximpact.datahost.ldapi.store.sql.interface :as sql.interface :refer [submit]]
   [tpximpact.datahost.ldapi.util.name-munging :as util.name-munging]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
   [tpximpact.datahost.ldapi.util.data.compilation :as data.compilation])
  (:import (java.net URI)
           (java.util.concurrent Executors ExecutorService ThreadFactory)
           (java.util.concurrent.atomic AtomicLong)))

(defrecord ObservationInsertRequest [release-uri key data op])

(defn make-observation-insert-request!
  "Analogous to `store/make-insert-request!`"
  [release-uri data change-kind]
  (let [op (case change-kind
             :dh/ChangeKindAppend (int 1)
             :dh/ChangeKindRetract (int 2)
             :dh/ChangeKindCorrect (int 3))]
    (when-not (instance? URI release-uri)
      (throw (ex-info (str "Given URI does not resolve to a valid release URI " release-uri) {:uri release-uri})))
    (->ObservationInsertRequest release-uri (store.file/file->digest data "SHA-256") data op)))

(defn relevant-indices
  [required-col-names found-col-names]
  (let [found->indices (into {} (map-indexed (fn [i col-name] [col-name i]) found-col-names))
        _ (when-not (= (count found-col-names) (count found->indices))
            (throw (ex-info "Invalid arguments. The counts of required and found columns do not match."
                            {:required required-col-names :found found-col-names})))
        indices-to-extract (map #(get found->indices %) required-col-names)]
    (assert (= (count required-col-names) (count indices-to-extract)))
    indices-to-extract))

(defn- extract-header
  "Returns a vector of seq of column names or throws."
  [s]
  (let [[[header] _seq<lines>] (split-at 1 s)
        header (first (csv/read-csv header))]
    (when-not (seq header)
      (throw (ex-info "No columns found in header" {:type :tpximpact.datahost.ldapi.errors/input-data-error})))
    header))

(defn- csv-line-coercers
  "Returns a vector of one arg. coercer fns in the same ordre as columns
  specified in row-schema. Throws when value can't be reasonably
  coerced."
  ;; TODO: find a better place to put the coercer fn. Maybe in row-schema's metadata?
  [row-schema]
  (let [coerce (fn type-coerce [{:dataset.column/keys [datatype name]}]
                      (case datatype
                        :string (fn v->string [v]
                                  (cond
                                    (string? v) v))
                        :int (fn v->int [v]
                               (cond (int? v) v
                                     (string? v) (Long/parseLong v)))
                        :double (fn v->double [v]
                                  (cond
                                    (double? v) v
                                    (string? v) (Double/parseDouble v)))))]
    (into [] (comp (map m/properties)
                   (map coerce))
          (m/children row-schema))))

(defn- safely-read-csv-line [line] (first (csv/read-csv line)))

(defn- extract-column
  "Extracts column at idx, turns empty string values into nulls."
  [coll idx]
  (let [value (nth coll idx)]
    (when-not (and (string? value) (.isBlank ^String value))
      value)))

(defn- validate-request [{:keys [release-uri] :as _store} {req-release-uri :release-uri :as request}]
  ;; TODO: make a schema
  (assert (:op request))
  (assert (:key request))
  (when-not (= req-release-uri release-uri)
    (throw (ex-info (str "Request release URI does not match the observations store release URI of:" release-uri)
                    {:request-uri req-release-uri :release-uri  release-uri}))))

(defn- insert-import-record
  "Returns {:next.jdbc/update-count int}. Update count of zero means the
  data for the given digest was already imported
  
  Parameters:

  - tx - jdbc transaction
  - insert request
  - commit-uri
  - header - seq of column names"
  [tx {:keys [release-uri key] :as _store} commit-uri header]
  (let [params {:import-uid key :release-uri release-uri}
        insert-import-result (try
                               (m.release/-insert-import tx (assoc params :import-id (m.release/import-id params)))
                               (catch Exception ex
                                 (throw (ex-info (str "Insert import failure. import_uid=" key ": " (ex-message ex))
                                                 {:import-uid key :header header :commit-uri commit-uri}
                                                 ex))))
        munged-release-id (util.name-munging/sanitize-name release-uri)]
    (log/info (format "-insert-data-with-request: commit-uri='%s', import_uid='%s', table=import_%s"
                      commit-uri key munged-release-id)
              insert-import-result)
    insert-import-result))

(defn- make-valid-observation-row
  "Returns a vector to be used as an 'insert' value>, columns are ordered
  [import-id, coords-hash, synth-hash  ...COLS-IN-SCHEMA-ORDER]

  row-prefix is a tuple of [import-id ?coords ?synt-id], wheret the ?coords and ?synth id
  are filled out by this function so the passed in values are not used."
  [row-prefix line seq<index+coercer> row-schema* validator {:keys [coords-columns hasher]}]
  (let [validator! (fn validator* [full-row]
                     (if (validator full-row)
                       full-row
                       ;;TODO: derive this error in errrors.clj
                       (throw (ex-info "Row does not conform to schema."
                                       {:type :dataset.validation/error
                                        :import-id (nth row-prefix 0)
                                        :extracted-row (drop (count row-prefix) full-row)
                                        :malli/explanation (m.error/humanize (m/explain row-schema* full-row))}))))
        row (into row-prefix
                  (map (fn extract+coerce [i+c]
                         (let [index (nth i+c 0) coercer (nth i+c 1)]
                           (coercer (extract-column (safely-read-csv-line line) index)))))
                  seq<index+coercer>)
        offset (count row-prefix)
        coords (apply hasher (map #(nth row (+ % offset)) coords-columns))
         synth-id (apply hasher (drop offset row))
        row (-> (transient row) (assoc! 1 coords 2 synth-id) persistent!)]
    (validator! row)))

(defn- row-producer
  [^java.io.Reader rdr
   ^java.util.concurrent.ArrayBlockingQueue q
   partition-size
   ^AtomicLong row-counter
   {:keys [required-columns row-schema] :as _ostore}
   {import-id :import-id header :header  munged-release-id :release-id* :as _derived}
   {:keys [op] :as request}]
  (try
    (when (not row-schema)
      (throw (ex-info "Missing row-schema" {:import-id import-id})))
    (let [indices (relevant-indices required-columns header)
          coercers (csv-line-coercers row-schema)
          _ (assert (= (count indices) (count coercers)))
          ;; we extend the 'raw' release schema with SQL table's bookkeeping columns
          schema* (into [:tuple :int :int :int] (m/children row-schema))
          validator (m/validator schema*)
          seq<index+coercer> (map (fn [i c] [i c]) indices coercers)
          {:keys [hasher]} (data.compilation/make-change-context op row-schema)
          col-indices (data.validation/row-schema-indices row-schema)]
      (doseq [lines (partition-all partition-size (line-seq rdr))]
        (let [rows ^java.util.List (java.util.ArrayList. (count lines))
              row-prefix [import-id nil nil]] ;; we fill out the nils in `make-valid-observation-row`
          (doseq [line lines]
            (.add rows (make-valid-observation-row row-prefix
                                                   line
                                                   seq<index+coercer>
                                                   schema*
                                                   validator
                                                   (assoc col-indices :hasher hasher))))
          (.addAndGet row-counter (count rows))
          (.put q rows)))
      (log/debug (format "terminating producer thread (table=%s)" munged-release-id))
      (.put q ::done))
    (catch Exception ex
      (let [msg "ObservationStore: terminating producer thread due to error"]
        (log/warn ex msg)
        (.put q (ex-info msg {:import-id import-id :request request} ex))))
    (catch AssertionError err
      (log/error "row-producer: assertion error:" (ex-message err))
      (.put q err))))


(defn- insert-data-with-request
  "Note: this function performs multiple database calls. It's on the caller to
  ensure the passed in db object is a transaction."
  [{:keys [data-source executor ^URI release-uri row-schema required-columns munged-col-names] :as ostore}
   {digest :key data :data ^URI commit-uri :commit-uri op :op :as request}]
  (assert executor)
  (assert commit-uri)
  (assert row-schema)
  (validate-request ostore request)
  (time!
      metrics/insert-observations
    (let [partition-size 2000           ;TODO(rosado): make it configurable, probably in the insert-request map
          q (java.util.concurrent.ArrayBlockingQueue. partition-size)]
      (with-open [rdr (io/reader data)]
        (let [header (extract-header (line-seq rdr))
              batch-counter ^AtomicLong (AtomicLong. 0)
              row-counter ^AtomicLong (AtomicLong. 0)
              munged-release-id (util.name-munging/sanitize-name release-uri)
              t0 (System/currentTimeMillis)
              tx (if (instance? javax.sql.DataSource data-source) ;NOTE(rosado): not happy with this conditional
                   (jdbc/get-connection data-source)
                   data-source)

              {:next.jdbc/keys [update-count]} (.get (submit executor #(insert-import-record tx (assoc request :release-uri release-uri)  commit-uri header)))]
          (if (zero? update-count)
            (log/info "insert-data-with-request: Data alread imported for data-key. Skipping"
                      {:data-key digest
                       :commit-uri commit-uri
                       :status :import.status/already-imported})
            (do
              (let [{import-id :id} (m.release/select-import tx {:import-uid digest} {:store ostore})]
                (future      ;TODO: setup a proper thread pool for IO
                  (row-producer rdr q partition-size row-counter
                                ostore
                                {:release-id* munged-release-id :header header :import-id import-id}
                                request))

                (loop [batch (.take q)]
                  (when (instance? Throwable batch)
                    (throw (ex-info "Failed to import observations"
                                    {:type :tpximpact.datahost.ldapi.errors/input-data-error
                                     :release-uri release-uri}
                                    batch)))
                  (when-not (= batch ::done)
                    (.get (submit executor
                                  #(do
                                     (m.release/import-observations tx {:release_id munged-release-id 
                                                                        :import_id (int import-id)
                                                                        :column-names munged-col-names
                                                                        :observations (seq batch)})
                                     (log/debug (format "writing observations: table=import_%s batch=%s"
                                                        munged-release-id (.incrementAndGet batch-counter))))))
                    
                    (recur (.take q)))))

              (let [seconds (/ (- (System/currentTimeMillis)  t0) 1000.0)
                    num-rows (.longValue row-counter) 
                    rows-per-sec (when (< 0.0001 seconds)
                                   (/ num-rows (float seconds)))]
                (log/info (format (str "-insert-data-with-request: done writing release-path='%s', "
                                       "num-batches=%s, batch-size=%s total-rows=%s took %.3fs, %s")
                                  (.getPath release-uri) (.longValue batch-counter) partition-size num-rows
                                  seconds
                                  (format "%.2f rows/sec" (or rows-per-sec (float num-rows))))))))
          (cond-> {:data-key digest :commit-uri commit-uri :status :import.status/success}
            (zero? update-count) (assoc :status :import.status/already-imported)))))))

(defn execute-insert-request*
  [{:keys [executor data-source release-uri] :as store} {import-uid :key :as request}]
  (validate-request store request)
  (try
    (insert-data-with-request store request)
    (catch Exception ex
      (log/warn (or (ex-cause ex) ex)
                "execute-insert-request*: Data insert failed, marking import as 'failed', import-uid: " import-uid)
      (.get (submit executor #(m.release/update-import-status (jdbc/get-connection data-source)
                                                              {:release-uri release-uri
                                                               :import-uid import-uid
                                                               :status "failed"})))
      (throw ex))))

(defprotocol ObservationRepo
  "Operations for interacting with the repository keeping the
  observation data.

  See also:
  - [[create-commit?]]"
  (-execute-insert-request [this request] "Returns a {:status STATUS-KW}"))

(defn execute-insert-request
  "Returns a map of {:status STATUS-KW ...}"
  [store request]
  (-execute-insert-request store request))

(defrecord ObservationsStore [data-source executor release-uri row-schema required-columns munged-col-names]
  ObservationRepo
  (-execute-insert-request [this request]
    (execute-insert-request* this request))

  sql.interface/SQLStoreCompatible
  (-make-select-observations-as [_ prefix]
    (assert (= (count required-columns) (count munged-col-names)))
    (let [make-mangled (if (nil? prefix)
                         identity
                         #(str prefix "." % ))]
      (map (fn [mangled demangled] [(make-mangled mangled) demangled])
           munged-col-names required-columns)))

  ;; TODO: remove impl of `store/ChangeStore` protocol.
  store/ChangeStore
  (-get-data [this data-key]
    (m.release/select-observations (jdbc/get-connection data-source)
                                   {:release-uri release-uri :import-uid data-key}
                                   {:store this}))

  (-data-key [_ data]
    (store.file/file->digest data "SHA-256")))

(defn complete-import
  "Completes the import related bookeeping, with the expectation that import-request finished
  successfully. Returns a map of {:status STATUS} or throws.

  - :import.status/already-imported means the import was completed at an earlier time (e.g. file with the same
    contents was processed already)."
  [{db :data-source db-executor :executor :as store}
   {^URI commit-uri :commit-uri digest :key op :op :as request}]
  (let [base-params {:import-uid digest :release-uri (:release-uri store)}
        update-import-status
        (-> db-executor
            (submit
             #(do
                (try
                  (log/debugf "complete-import--copy-records commit-path=%s affected: %s rows"
                              (.getPath commit-uri)
                              (m.release/complete-import--copy-records
                               db
                               (merge base-params {:insert-columns (:munged-col-names store) :op op})
                               {:store store}))
                  (m.release/complete-import--delete-import-records db base-params {:store store})
                  (m.release/update-import-status db {:status "completed"
                                                      :import-id (m.release/import-id base-params)})
                  (catch Exception ex
                    ;; log and terminate
                    (log/warn ex "Error")))))
            (.get))
        {update-status-num :next.jdbc/update-count} update-import-status]
    (log/debug (str "commit-path=" (.getPath commit-uri) " import status update: " update-status-num))
    (when (zero? update-status-num)
      (throw (ex-info "Could not complete import. Status update failed"
                      {:update-status-count update-status-num
                       :import-uid digest
                       :request request})))
    {:status :import.status/completed}))


(def ^:private import-status-h
  (-> (make-hierarchy)
      (derive :import.status/already-imported :datahost.sql.commit/create)
      (derive :import.status/success :datahost.sql.commit/create)))

(defn create-commit?
  "Should we create a 'commit' based on the import status?"
  [import-status]
  (isa? import-status-h import-status :datahost.sql.commit/create))

(defn create-commit
  [{db :data-source db-executor :executor :as _store}
   {^URI commit-uri :commit-uri ^URI release-uri :release-uri import-uid :key op :op
    :as request}]
  (let [[_ rev com] (re-find #"^.*\/revision\/(\d+)\/commit\/(\d+)$" (str commit-uri))
        rev-id (Long/parseLong rev)
        com-id (Long/parseLong com)
        update-count (-> db-executor
                         (submit #(m.release/insert-commit db {:release-uri release-uri
                                                               :uid commit-uri
                                                               :revision-id rev-id
                                                               :change-id com-id
                                                               :op op
                                                               :import_uid import-uid}))
                         (.get)
                         :next.jdbc/update-count)]
    (when-not (= 1 update-count)
      (log/debugf "create-commit/insert-failed: commit-path=%s" (.getPath commit-uri))
      (throw (ex-info (format "Could not create commit %s" commit-uri) {:insert-request request})))))

(defn make-observation-store
  [db db-executor release-uri row-schema]
  (when-not (m/validate :datahost/uri release-uri {:registry s.common/registry})
    (throw (ex-info "Ivalid release-uri" {:uri release-uri})))
  (let [{:keys [col-names original-column-names]} (util.name-munging/observation-column-defs-sql {:row-schema row-schema})]
    (->ObservationsStore db
                         db-executor
                         release-uri
                         row-schema
                         original-column-names
                         col-names)))

;;; TODO: writes in `replay-commits` should happen on the executor thread

(defn replay-commits
  [tx {:keys [store ^URI commit-uri snapshot-table]}]
  (let [opts {:store store}
        commit-ids (reverse (m.release/get-commit-ids tx {:commit-uri commit-uri :release-uri (:release-uri store)}))
        _ (m.release/create-temporary-snapshot-table tx {:table-name snapshot-table} opts)
        result (m.release/select-commit-observations-into tx {:snapshot-table snapshot-table
                                                              :commit-uri commit-uri
                                                              :commit-id (:id (first commit-ids))}
                                                          opts)
        first-ids (map :id (take 3 commit-ids))]
    (log/info (format "replay-commits/commit-path=%s found %s commits, first %s ids="
                      (.getPath commit-uri) (count commit-ids) (count first-ids))
              first-ids)
    (log/debug {:replay-commits/select-commit-obs-into result})
    (doseq [{commit-uid :uid op :op commit-id :id}  (next commit-ids)
            :let [params {:snapshot-table snapshot-table
                          :commit-id commit-id
                          :commit-uri ^java.net.URI (java.net.URI. commit-uid)}]]
      (cond
        (= op 1)
        (log/debug (format "replay-commits/commit(append): %s" (.getPath commit-uri)) (m.release/commit-op-append tx params opts))

        (= op 2)
        (log/debug (format "replay-commits/commit(retraction): %s" (.getPath commit-uri)) (m.release/commit-op-retract tx params opts))

        (= op 3)
        (let [temp-table (str "scratch_" (java.util.UUID/randomUUID))]
          (m.release/-create-corrections-scratch-table tx {:table-name temp-table} opts)
          (m.release/populate-corrections-scratch-table tx (assoc params :table-name temp-table) opts)
          (m.release/-commit-op-correct--delete-stale-records tx (assoc params :table-name temp-table))
          (let [result (m.release/commit-op-correct--insert-updated-records tx (assoc params :table-name temp-table) opts)]
            (log/debug (format "replay-commits/commit(correction): %s, insert updated records result: "
                               (.getPath commit-uri))
                       result
                       {:temp-table temp-table})))
        :else (throw (ex-info (format "Unknown 'op' value='%s'" op) {:commit-uri commit-uri :op op}))))))

(defn sqlite-table-exist?
  [conn table-name]
  (= 1 (-> conn
           (jdbc/execute-one!  ["select exists (select name from sqlite_master where type = ? and name = ?) as count"
                                "table" table-name])
           :count)))

;; (defn pg-table-exists?
;;   [conn table-name]
;;   (= 1 (-> conn
;;            (jdbc/execute-one! ["select exists ( select 1 from information_schema.tables WHERE table_name = ? ) AS count"
;;                                table-name]))))

(defmethod ig/init-key ::store-factory [_ {:keys [data-source db-executor]}]
  (log/info "making observation store factory: " data-source)
  (partial make-observation-store data-source db-executor))

(defmethod ig/halt-key! ::store-factory [_ _])

(defmethod ig/init-key ::executor [_ {_config :config}]
  (let [thread-factory (reify ThreadFactory
                         (newThread [_this runnable]
                           (let [t (Thread. runnable)]
                             (doto t
                               (.setName (str "sqlexec-" (.getId t)))))))
        exec (Executors/newSingleThreadExecutor thread-factory)]
    exec))

(defmethod ig/halt-key! ::executor [_ ^ExecutorService executor]
  (.shutdown executor))

(defmethod ig/init-key ::data-source [_ {:keys [db-config]}]
  ;; NOTE(rosado): using 'jdbc/get-datasource' here resulted no data
  ;; being written to the database. Possibly a configuration issue,
  ;; but it ate too much time, so doing it the easy way for now
  (let [{:keys [spec user password]} db-config]
   (reify javax.sql.DataSource
     (getConnection [_this]
       (jdbc/get-connection spec user password)))))

(defmethod ig/halt-key! ::data-source [_ _data-source])

