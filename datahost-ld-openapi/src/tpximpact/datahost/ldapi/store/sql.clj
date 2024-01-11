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
   [tpximpact.datahost.ldapi.store.sql.interface :as sql.interface]
   [tpximpact.datahost.ldapi.util.name-munging :as util.name-munging]))


(defrecord ObservationInsertRequest [release-uri key data op])

(defn make-observation-insert-request!
  "Analogous to `store/make-insert-request!`"
  [release-uri data op]
  (->ObservationInsertRequest release-uri (store.file/file->digest data "SHA-256") data op))

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
  (let [[[header] seq<lines>] (split-at 1 s)
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

(defn- validate-request [{:keys [release-uri] :as store} {req-release-uri :release-uri :as request}]
  ;; TODO: make a schema
  (assert (:op request))
  (assert (:key request))
  (when-not (= req-release-uri release-uri)
    (throw (ex-info (str "Request release URI does not match the observations store release URI of:" release-uri)
                    {:request-uri req-release-uri}))))

(defn- insert-import-record
  "Returns nil.
  Parameters:

  - tx - jdbc transaction
  - release-uri - URI
  - digest 
  - header - seq of column names"
  [tx release-uri digest op header]
  (assert (int? op))
  (let [insert-import-result (try
                               (m.release/-insert-import tx {:import_uid digest :op op})
                               (catch Exception ex
                                 (throw (ex-info "Insert import failure"
                                                 {:import_uid digest :header header :release-uri release-uri}
                                                 ex))))
        munged-release-id (util.name-munging/sanitize-name release-uri)]
    (log/info (format "-insert-data-with-request: release-uri='%s', import_id='%s', table=import_%s"
                      release-uri digest munged-release-id)
              insert-import-result)))

(defn- make-valid-observation-row
  "Returns a vector to be used as an 'insert' value>, columns are ordered
  [import-id, op, ...COLS-IN-SCHEMA-ORDER]

  row-prefix is a tuple of [import-id op]"
  [row-prefix line seq<index+coercer> row-schema* validator]
  (let [validator! (fn validator* [full-row]
                     (if (validator full-row)
                       full-row
                       ;;TODO: derive this error in errrors.clj
                       (throw (ex-info "Row does not conform to schema."
                                       {:type :dataset.validation/error
                                        :import-id (nth row-prefix 0)
                                        :extracted-row (drop 2 full-row)
                                        :malli/explanation (m.error/humanize (m/explain row-schema* full-row))}))))
        row (into row-prefix
                  (map (fn extract+coerce [i+c]
                         (let [index (nth i+c 0) coercer (nth i+c 1)]
                           (coercer (extract-column (safely-read-csv-line line) index)))))
                  seq<index+coercer>)]
    (validator! row)))

(defn- insert-data-with-request
  [{:keys [db release-uri row-schema required-columns munged-col-names] :as this}
   {digest :key data :data op :op :as request}]
  (validate-request this request)
  (time!
      metrics/insert-observations
    (let [partition-size 2000
          q (java.util.concurrent.ArrayBlockingQueue. partition-size)]
      (with-open [rdr (io/reader (:data request))]
        (let [header (extract-header (line-seq rdr))
              counter (java.util.concurrent.atomic.AtomicLong. 0)
              munged-release-id (util.name-munging/sanitize-name release-uri)
              t0 (System/currentTimeMillis)]
          (let [tx db] ;;;jdbc/with-transaction [tx db]
            (insert-import-record tx release-uri digest op header)
            (let [{import-id :id} (m.release/select-import tx {:import_uid digest})
                  indices (relevant-indices required-columns header)
                  coercers (csv-line-coercers row-schema)
                  
                  _ (assert (= (count indices) (count coercers)))
                  schema* (into [:tuple :int :int] (m/children row-schema))
                  validator (m/validator schema*)
                  seq<index+coercer> (map (fn [i c] [i c]) indices coercers)]
              (future             ;TODO: setup a proper thread pool
                (try
                  (doseq [lines (partition-all partition-size (line-seq rdr))]
                    (let [rows ^java.util.List (java.util.ArrayList. (count lines))
                          row-prefix [import-id op]]
                      (doseq [line lines]
                        (.add rows (make-valid-observation-row row-prefix line seq<index+coercer> schema* validator)))
                      (.put q rows)))
                  (log/debug (format "terminting producer thread (table=%s)" munged-release-id))
                  (.put q ::done)
                  (catch Exception ex
                    (log/warn ex "ObservationStore: terminating producer thread due to error")
                    (.put q ex))))

              (loop [batch (.take q)]
                (when (instance? Throwable batch)
                  (throw (ex-info "Failed to import observations"
                                  {:type :tpximpact.datahost.ldapi.errors/input-data-error
                                   :release-uri release-uri}
                                  batch)))
                (when-not (= batch ::done)
                  (m.release/import-observations tx {:release_id munged-release-id 
                                                     :import_id (int import-id)
                                                     :column-names munged-col-names
                                                     :observations (seq batch)})
                  (log/debug (format "writing observations: table=import_%s batch=%s"
                                     munged-release-id (.incrementAndGet counter)))
                  (recur (.take q))))))

          (log/debug (format "-insert-data-with-request: done writing release-path='%s', num-batches=%s, took %.3fs"
                             (.getPath release-uri) (.longValue counter) (/ (- (System/currentTimeMillis)  t0)
                                                                            1000.0)))
          {:data-key digest})))))


(defrecord ObservationsStore [db release-uri row-schema required-columns munged-col-names]
  store/ChangeStore
  (-insert-data-with-request [this request]
    (validate-request this request)
    (try
      (insert-data-with-request this request)
      (catch clojure.lang.ExceptionInfo ex
        (throw (ex-info (str "-insert-data-with-request: " (ex-message ex))
                        (assoc (ex-data ex) :release-uri release-uri)
                        ex)))))

  (-get-data [this data-key]
    (m.release/select-observations db {:release-uri release-uri :import_uid data-key} {:store this}))

  (-data-key [_ data]
    (store.file/file->digest data "SHA-256"))

  (-delete [this data-key])

  sql.interface/SQLStoreCompatible
  (-make-select-observations-as [_ prefix]
    (assert (= (count required-columns) (count munged-col-names)))
    (let [make-mangled (if (nil? prefix)
                         identity
                         #(str prefix "." % ))]
      (map (fn [mangled demangled] [(make-mangled mangled) demangled])
           munged-col-names required-columns))))

(defn complete-import
  [{:keys [db] :as store} {release-uri :release-uri digest :key op :op :as _request}]
  (m.release/complete-import--copy-records db
                                           {:release-uri release-uri
                                            :import_uid digest
                                            :insert-columns (:munged-col-names store)
                                            :op op}
                                           {:store store})
  (m.release/complete-import--delete-import-records db
                                                    {:release-uri release-uri
                                                     :import_uid digest}
                                                    {:store store}))

(defn make-observation-store
  [db-config release-uri row-schema]
  (when-not (m/validate :datahost/uri release-uri {:registry s.common/registry})
    (throw (ex-info "Ivalid release-uri" {:uri release-uri})))
  (let [{:keys [col-names]} (util.name-munging/observation-column-defs-sql {:row-schema row-schema})]    
    (->ObservationsStore db-config
                         release-uri
                         row-schema
                         (into [] (comp (map m/properties)
                                        (map :dataset.column/name))
                               (m/children row-schema))
                         col-names)))


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

(defmethod ig/init-key ::store-factory [_ {:keys [db-config connection]}]
  (partial make-observation-store (or connection db-config)))

(defmethod ig/halt-key! ::store-factory [_ _])
