(ns tpximpact.datahost.ldapi.util.name-munging
  "Utilites for turning arbitrary strings into identifiers valid in SQL"
  (:require
   [clojure.string :as string]
   [malli.core :as m]
   [tpximpact.datahost.uris :as uris]))

(defn uri->series+release [^java.net.URI uri]
  (let [[_ series-name release-name] (re-find  #"^\/\w+\/(.*)\/release\/([\w-_\.^\/]+)[\/]{0,1}.*" (.getPath uri))]
    (when release-name
      [series-name release-name])))

(defprotocol NameSanitizer
  (-sanitize-name [this] "Returns a string"))

(defprotocol NameAdapter
  (-sanitize [this v] "Returns a string")
  (-observation-column-name [this v] "Returns a string with a prefix appropiate for the column type"))

(defrecord PrefixSQLNameAdapter []
  NameSanitizer
  (-sanitize [this s]
    s)

  NameAdapter
  (-observation-column-name [this props]
    (str (condp = (:dataset.column/type props)
           (:measure uris/column-types) "m::"
           (:dimension uris/column-types) "dim::"
           (:attribute uris/column-types) "attr::")
         (-sanitize this (:dataset.column/name props)))))

(extend-protocol NameSanitizer          ;TODO: improve naming. not specific enough
  java.net.URI
  (-sanitize-name [this]
    (let [[series-name release-name] (uri->series+release this)]
      (when-not (and series-name release-name)
        (throw (ex-info "URI should identify a release." {:this this})))
      (string/replace (str series-name "/" release-name) #"\." "_"))))

(defn sanitize-name
  [v]
  (-sanitize-name v))

(defn make-name-adapter
  []
  (->PrefixSQLNameAdapter))

(defn demunge-name
  "Returns the demunged name or nil."
  [s]
  (when-let [m (re-find #"^(m::|attr::|dim::)(.*)$" s)]
    (nth m 2)))

(defn observation-column-defs-sql
  "Returns a map of {:original-column-names vector<String>, :col-names vector<munged-string>, :col-spec seq<[name munged-name sql-string]>}"
  [{:keys [row-schema]}]
  (let [adapter (make-name-adapter)
        seq<col-name+munged+col-sql>
        (sequence (comp (map m/properties)
                        (map (fn to-tuples [props]
                               (let [munged-name (-observation-column-name adapter props)
                                     dt (:dataset.column/datatype props)]
                                 [(:dataset.column/name props)
                                  munged-name
                                  ;; TODO: find better place for the below, probabaly a builder fn
                                  ;; should passed in
                                  (case dt
                                    :string "varchar(128)"
                                    :int "integer"
                                    :double "double")]))))
                  (m/children row-schema))]
    {:original-column-names (mapv #(nth % 0) seq<col-name+munged+col-sql>)
     :col-names (mapv #(nth % 1) seq<col-name+munged+col-sql>)
     :col-spec seq<col-name+munged+col-sql>}))
