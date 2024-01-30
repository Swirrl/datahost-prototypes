(ns tpximpact.test-helpers
  (:require [clojure.test :refer :all]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [tpximpact.datahost.system-uris :as su]
            [tpximpact.db-cleaner :as dc]
            [tpximpact.datahost.sys :as sys]
            [tpximpact.datahost.ldapi.test-util.http-client :as http-client]
            [clojure.data.json :as json]
            [clojure.java.io :as io]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.test/http-client [_ config]
  (http-client/make-client config))

(defmethod ig/init-key :tpximpact.datahost.ldapi.test/sql-db [_ m]
  m)

(defmethod ig/init-key :tpximpact.datahost.ldapi.test/sqlite-connection [_ {{:keys [spec user password]} :db-config}]
  (jdbc/get-connection spec user password))

(defmethod ig/halt-key! :tpximpact.datahost.ldapi.test/sqlite-connection [_ connection]
  (.close connection))

(defmethod ig/init-key :tpximpact.datahost.ldapi.test/data-source [_ {:keys [connection]}]
  (reify javax.sql.DataSource
    (getConnection [this]
      connection)))

(defmethod ig/halt-key! :tpximpact.datahost.ldapi.test/data-source [_ _])

(defmethod ig/halt-key! :tpximpact.datahost.ldapi.test/sql-db [_ {{:keys [spec user password]} :db-config conn :connection}]
  (with-open [conn (jdbc/get-connection spec user password)]
    (try 
      ;;(.execute (.createStatement conn) "SHUTDOWN")
      (catch RuntimeException ex
        (when-not (isa? (type (ex-cause ex)) java.sql.SQLException)
          (throw ex)))))) 

(defn start-system [configs]
  (-> configs
      (sys/load-configs)
      (sys/prep-config)
      (ig/init)))

(def ^:dynamic *system* (atom nil))

(defn start-system
  ([]
   (start-system ["ldapi/base-system.edn"
                  ;; TODO - nuke test-system & move contents to env.edn
                  "test-system.edn"
                  ;; env.edn contains environment specific
                  ;; overrides to the base-system.edn and
                  ;; is set on classpath depending on env.
                  "ldapi/env.edn"]))
  ([configs]
   (let [sys (-> configs
                 (sys/load-configs)
                 (sys/prep-config)
                 (ig/init))]
     (reset! *system* sys))))

(defn stop-system
  []
  (let [sys @*system*]
    (reset! *system* nil)
    (ig/halt! sys)))

(defn with-system-fixture [test-fn]
  (start-system)
  (test-fn)
  (stop-system))

(defmacro with-system* [{:keys [on-halt on-init] :as _opts} system-binding system-loader & body]
  `(let [sys# (or ~system-loader (start-system))
         ~system-binding sys#]
     (try
       (doseq [f!# ~on-init]
         (f!# sys#))
       ~@body
       (finally
         (doseq [f!# ~on-halt]
           (f!# sys#))))))

(defmacro with-system [system-bindings system-loader & body]
  `(with-system* {:on-halt [ig/halt!]}
                 ~system-bindings ~system-loader
                 ~@body))


(defmacro with-system-and-clean-up [system-binding & body]
  `(with-system* {:on-init [dc/clean-db]
                  :on-halt [ig/halt!]}
                 ~system-binding
                 nil
                 ~@body))

(defn sys->rdf-base-uri [sys]
  (-> sys :tpximpact.datahost.system-uris/uris (su/rdf-base-uri)))

(defn multipart [name content content-type]
  (cond-> {:name name
           :content content
           :mime-type content-type
           :encoding "UTF-8"}
    (string? content)
    (assoc :size (.length content))
    (instance? java.io.File content)
    (assoc :filename (.getName content)
           :size (.length content))
    (instance? java.io.InputStream content)
    (assoc :size (.size content))))

(defn build-csv-multipart [csv-path]
  (let [appends-file (io/file (io/resource csv-path))]
    (multipart "appends" appends-file "text/csv")))

(defn file-upload [resource-path]
  (io/file (io/resource resource-path)))

(defn jsonld-multipart [name jsonld]
  (multipart name (.getBytes (json/write-str jsonld) "UTF-8") "application/ld+json"))

(defn jsonld-body-request [jsonld]
  {:headers {"content-type" "application/ld+json"}
   :body (.getBytes (json/write-str jsonld) "UTF-8")})
