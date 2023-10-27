(ns tpximpact.datahost.ldapi.test-util.hurl
  "Utilities for running Hurl scripts from Clojure.

  Hurl: https://hurl.dev"
  (:require
   [clojure.java.shell :as shell]
   [clojure.java.io :as io])
  (:import
   [java.nio.file Path Files]))

(defn- variables->args
  [m]
  (for [[k v] m]
    (str (name k) "=" v)))

(defn hurl
  [{:keys [script variables report-junit report-html file-root]}]
  (let [variables (merge {:scheme "http"} variables)
        hurl-variables (mapcat (fn [v] ["--variable" v]) (variables->args variables))
        args (cond-> ["--test"]
               report-junit (conj "--report-junit" report-junit)
               report-html (conj "--report-html" report-html)
               file-root (conj "--file-root" (str file-root))
               :always (into hurl-variables))]
    (assoc (apply shell/sh "hurl" script args) :hurl/args args)))

(defn- to-path
  [dir-path]
  (cond
    (string? dir-path) (.toPath (io/file dir-path))
    (instance? java.io.File dir-path) (.toPath dir-path)
    (instance? Path dir-path) dir-path
    :else (IllegalArgumentException. (str "Illegal argument type: " (type dir-path)))))

(defn hurl-files
  "Returns a seq<Path>."
  [dir-path]
  (with-open [stream<path> (Files/newDirectoryStream (to-path dir-path) "*.hurl")]
    (into [] stream<path>)))

(defmulti -instantiate-value (fn [kw] (namespace kw)))

(defmethod -instantiate-value "hurl.variable.named" [kw]
  (str (name kw) "-" (random-uuid)))

(defn instantiate-variables
  [variables]
  (->> variables
       (reduce-kv (fn [m k v]
                    (assoc! m k (if (and (keyword? v) (namespace v))
                                 (-instantiate-value v)
                                 v)))
                  (transient {}))
       (persistent!)))

(defn run-directory
  "Runs all hurl scripts in the given directory.
  Returns a seq of maps (return values of [[shell/sh]].

  When each scripts requires different values for the same
  variable, use keyword values (see [[-instantiate-value]]).
  For example:
  
  ```
  {:release :hurl.variable.named/release, ...}
  ```
  
  Parameters:

  - dir-path - path to a direcotry (string | File| Path)
  - options
    - variables - map in which values can be strings or
      keywords for which `-instantiate-value` implementation
      exists.
    - report-junit - report file path
    - report-html - report directory path"
  [dir-path opts]
  (let [path (to-path dir-path)
        paths (hurl-files path)]
    (doall
     (for [p ^Path paths]
       (hurl (-> (select-keys opts [:report-junit :report-html :variables])
                 (update :variables instantiate-variables)
                 (assoc :script (str p) :file-root (str path))))))))

(defn success?
  "Did all scripts execute succesfully?

  Expects a seq of values returned by [[shell/sh]]"
  [results]
  (every? zero? (map :exit results)))

