(ns tpximpact.datahost.ldapi.test-util.hurl
  "Utilities for running Hurl scripts from Clojure.

  Hurl: https://hurl.dev"
  (:require
   [clojure.java.shell :as shell]
   [clojure.java.io :as io]
   [babashka.fs :as fs])
  (:import
   [java.nio.file Path Files]))

(defn- variables->args
  [m]
  (for [[k v] m]
    (str (name k) "=" v)))

(defn hurl
  [{:keys [script variables report-junit report-html file-root]}]
  (let [variables (merge {"scheme" "http"} variables)
        hurl-variables (mapcat (fn [v] ["--variable" v])
                               (variables->args variables))
        args (cond-> ["--test"]
               report-junit (conj "--report-junit" report-junit)
               report-html (conj "--report-html" report-html)
               file-root (conj "--file-root" (str file-root))
               :always (into hurl-variables))]
    (assoc (apply shell/sh "hurl" (str script) args)
           :hurl/args args
           :script (str script))))

(defmulti -instantiate-value (fn [kw] (namespace kw)))

(defmethod -instantiate-value "hurl.variable.named" [kw]
  (str (name kw) "-" (random-uuid)))

(defn instantiate-variables
  "Takes a map of {KEYWORD (KEYWORD | STRING)

  Where STRING or KEYWORD can be any string or
  special value: :hurl.variable.named/NAME or \"hurl.variable.named/NAME\"
  which will be turned into \"NAME-UUID\""
  [variables]
  (->> variables
       (reduce-kv (fn [m k v]
                    (assoc! m k (cond
                                  (and (keyword? v) (namespace v))
                                  (-instantiate-value v)

                                  (and (string? v) (.startsWith v "hurl.variable.named/"))
                                  (let [a (.split v "/")]
                                    (-instantiate-value (keyword (aget a 0) (aget a 1))))

                                  :else
                                  v)))
                  (transient {}))
       (persistent!)))

(defn- run-test-setup
  [file-root ^Path p  opts]
  (let [options (-> (select-keys opts [:variables])
                    (update :variables instantiate-variables)
                    (assoc :script (str p) :file-root (str file-root)))
        setup-script (fs/path p "setup.hurl")
        setup-ref (fs/path p "setup.ref")]
    (cond
      (fs/exists? setup-script)
      (do
        (println "\tsetup:" (str setup-script))
        (hurl (assoc options :script (str setup-script))))
      
      (fs/exists? setup-ref)
      (let [path (fs/path file-root (-> setup-ref fs/file slurp (.trim)))]
        (println "\tsetup:" (str setup-ref) "-->" (str path))
        (hurl (assoc options :script (str path))))

      :else nil)))

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
  (let [file-root (fs/path dir-path)
        paths (fs/match dir-path "regex:(issue-.*|pr-.*|int-.*)"
                        {:max-depth 1 :recursive false})]
    (doall
     (for [p ^Path paths
           :when (not (clojure.string/ends-with? (str p) ".disabled"))]
       (let [options (-> (select-keys opts [:variables])
                         (update :variables instantiate-variables)
                         (assoc :file-root (str file-root)))]
         (when (fs/directory? p)
           (println "executing:" (str p))
           (let [{:keys [exit err]} (run-test-setup file-root p options)]
             (when (and (some? exit) (not= 0 exit))
               (throw (ex-info (format "Test setup for '%s' failed." p)
                               {:test-location p
                                :file-root file-root
                                :exit-code exit
                                :err err})))))
         (when (and (fs/directory? p)
                    (not (fs/exists? (fs/path p (str (fs/file-name p) ".hurl")))))
           (throw (ex-info (format "No test script in '%s'" p)
                           {:file-root file-root
                            :test-location p})))
         (hurl (-> options
                   (merge (select-keys opts [:report-junit :report-html]))
                   (assoc :script (if (fs/directory? p)
                                    (fs/path p (str (fs/file-name p) ".hurl"))
                                    p)))))))))

(defn success?
  "Did all scripts execute succesfully?

  Expects a seq of values returned by [[shell/sh]]"
  [results]
  (every? zero? (map :exit results)))
