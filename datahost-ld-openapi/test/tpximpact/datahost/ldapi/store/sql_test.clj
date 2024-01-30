(ns ^:sql tpximpact.datahost.ldapi.store.sql-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [clojure.string :as string]
   [malli.core :as m]
   [malli.error :as m.e]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :refer [as-unqualified-maps]]
   [next.jdbc.result-set :as jdbc.rs]
   [tpximpact.test-helpers :as th :refer [*system*]]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.store.sql :as store.sql]
   [tpximpact.datahost.ldapi.util.data.validation :as d.validation]
   [tpximpact.datahost.ldapi.models.release :as m.release]
   [tpximpact.datahost.ldapi.util.name-munging :as util.munge])
  (:import
   (java.sql Clob
             ResultSet ResultSetMetaData
             Statement
             SQLException)))


(use-fixtures :each th/with-system-fixture)

(defn make-test-db
  [data-source]
  (jdbc/get-connection data-source))

(defn- humanize [v]
  (-> (m/explain tpximpact.datahost.ldapi.routes.shared/LdSchemaInput v) m.e/humanize))

(def row-schema (d.validation/make-row-schema-from-json
                 {"dh:columns" [{"@type" "dh:DimensionColumn"
                                 "csvw:datatype" "string"
                                 "csvw:name" "some dimension!"}
                                {"@type" "dh:MeasureColumn"
                                 "csvw:datatype" "int"
                                 "csvw:name" "some measure!"}]}))


(def CSV (.getBytes "some measure!,some dimension!
1,a
2,b
3,c"))

(def CSV2 (.getBytes "some measure!,some dimension!
4,d
1,a")) ;; -> after retraction: ((b, ...),  (c, ...) )

(def CSV3 (.getBytes "some measure!,some dimension!
44,d
1,a
33,c")) ;; -> (2, 'b'), (33, 'c')

(def expected-datasets
  (let [D (fn [v] [(keyword "some dimension!") v])
        M (fn [v] [(keyword "some measure!") v])
        R (fn [m d] (into {} [(M m) (D d)]))]
    {:materialised-1 [(R 1 "a") (R 2 "b") (R 3 "c")]
     :materialised-2 [(R 1 "a") (R 2 "b") (R 3 "c") (R 4 "d")]
     :materialised-3 [(R 2 "b") (R 3 "c")]
     :materialised-4 [(R 2 "b") (R 33 "c")]}))

(defrecord MapResultSetBuilder [^ResultSet rs rsmeta cols]
  jdbc.rs/RowBuilder
  (->row [this] (transient {}))
  (column-count [this] (count cols))
  (with-column [this row i]
    (jdbc.rs/with-column-value this row (nth cols (dec i))
      (jdbc.rs/read-column-by-index (.getObject rs ^Integer i) rsmeta i)))
  (with-column-value [this row col v]
    (assoc! row (string/replace-first (name col) #"^(dim::|m::|attr::)(.*)" "$2") v))
  (row! [this row] (persistent! row))
  jdbc.rs/ResultSetBuilder
  (->rs [this] (transient []))
  (with-row [this mrs row]
    (conj! mrs row))
  (rs! [this mrs] (persistent! mrs)))

(defn commit
  [conn {:keys [^java.net.URI release-uri] :as ostore}  commit-uri kind input-stream]
  (let [insert-req (store.sql/make-observation-insert-request! release-uri input-stream kind)
        insert-req (assoc insert-req :commit-uri commit-uri)
        {import-uid :data-key status :status} (store.sql/execute-insert-request ostore insert-req)]
    (when (= status :import.status/success)
      (store.sql/complete-import ostore insert-req))
    (log/debugf "commit: commit-path=%s status=%s" (.getPath commit-uri) status)
    (assert (store.sql/create-commit? status))
    (store.sql/create-commit ostore insert-req)))

(deftest from-zero-to-snapshot
  (let [{{:keys [data-source db-executor store-factory]} :tpximpact.datahost.ldapi.test/sql-db} @*system*]
    (with-open [conn (make-test-db data-source)]
      (assert (store.sql/sqlite-table-exist? conn "imports"))
      (is (m.release/db-ok? conn) "The `imports` table should exist.")
      (jdbc/with-transaction [tx conn]
        (let [conn tx
              release-uri (java.net.URI. "http://localhost/data/series1/release/REL1")
              parent-commit nil
              commit-uri (java.net.URI. "http://localhost/data/series1/release/REL1/revision/1/commit/1")
              input-data (java.io.ByteArrayInputStream. CSV)
              _ (m.release/create-release-tables conn {:row-schema row-schema :release-uri release-uri} {:quoting :ansi})

              ;;conn' conn
              ;;conn (jdbc/with-logging conn (fn [s v] (log/debug "SQL::" s  "\n" v)))
              
              insert-req (store.sql/make-observation-insert-request! release-uri input-data :dh/ChangeKindAppend)
              insert-req (assoc insert-req :commit-uri commit-uri)
              ostore (store.sql/make-observation-store conn db-executor release-uri row-schema)
              {import-uid :data-key status :status} (store.sql/execute-insert-request ostore insert-req)
              _ (is (= :import.status/success status))
              imports (m.release/select-imported-observations conn {:import-uid import-uid} {:store ostore})
              _ (store.sql/complete-import ostore insert-req)
              _ (store.sql/create-commit ostore insert-req)
              
              observations (m.release/select-observations conn {:import-uid import-uid} {:quoting :ansi :store ostore})
              imports' (m.release/select-imported-observations conn {:import-uid import-uid} {:store ostore})
              _ (is (empty? imports') "The imports::... table should be empty after completed import")
              commit-ids (m.release/get-commit-ids conn {:commit-uri commit-uri :release-uri release-uri})

              _ (is (= 1 (count (jdbc/execute! conn ["select * from commits"]))))
              _ (store.sql/replay-commits conn {:store ostore
                                                :snapshot-table "snapshot_1"
                                                :commit-uri commit-uri})

              commit-uri-2 (java.net.URI. "http://localhost/data/series1/release/REL1/revision/1/commit/2")
              _ (commit conn ostore commit-uri-2 :dh/ChangeKindAppend (java.io.ByteArrayInputStream. CSV2))
              _ (is (= 2 (count (jdbc/execute! conn ["select * from commits"]))))

              commit-uri-3 (java.net.URI. "http://localhost/data/series1/release/REL1/revision/1/commit/3")
              _ (commit conn ostore commit-uri-3 :dh/ChangeKindRetract (java.io.ByteArrayInputStream. CSV2))

              commit-uri-4 (java.net.URI. "http://localhost/data/series1/release/REL1/revision/1/commit/4")
              _ (commit conn ostore commit-uri-4 :dh/ChangeKindCorrect (java.io.ByteArrayInputStream. CSV3))
              
              materialise-opts {:store ostore :builder-fn as-unqualified-maps}]

          ;; materialise the dataset snapshots to different tables
          (store.sql/replay-commits conn {:store ostore :commit-uri commit-uri :snapshot-table "snapshot_1_again"})
          (store.sql/replay-commits conn {:store ostore :commit-uri commit-uri-2 :snapshot-table "snapshot_2"})
          (store.sql/replay-commits conn {:store ostore :commit-uri commit-uri-3 :snapshot-table "snapshot_3"})
          (store.sql/replay-commits conn {:store ostore :commit-uri commit-uri-4 :snapshot-table "snapshot_4"})

          (let [m2 (m.release/materialize-snapshot conn
                                                   {:release-uri release-uri
                                                    :snapshot-table "snapshot_2"}
                                                   materialise-opts)
                m3 (m.release/materialize-snapshot conn
                                                   {:release-uri release-uri
                                                    :snapshot-table "snapshot_3"}
                                                   materialise-opts)
                m4 (m.release/materialize-snapshot conn
                                                   {:release-uri release-uri
                                                    :snapshot-table "snapshot_4"}
                                                   materialise-opts)]
            (is (= (:materialised-2 expected-datasets) m2))
            (is (= (:materialised-3 expected-datasets) m3))
            (is (= (:materialised-4 expected-datasets) m4)))
          
          (is (not (empty? imports)))
          (is (not (empty? observations)))
          (is (empty? imports')))))))

