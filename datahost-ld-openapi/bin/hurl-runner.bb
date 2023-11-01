(ns hurl-runner
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [babashka.fs :as fs]
            [babashka.cli :as cli]
            [tpximpact.datahost.ldapi.test-util.hurl :refer [run-directory]])
  (:import [java.nio.file Path Files]))

(def cli-options {:dir {:coerce :string}
                  :variable {:coerce []}
                  :report-junit {:coerce :string}
                  :report-html {:coerce :string}})

(defn- transform-cli-variables
  [vars]
  (into {} (map #(vec (.split % "="))) vars))

(defn main [cmd-line-args]
  (let [options {:spec cli-options :require [:dir]
                 :error-fn (fn [{:keys [spec type cause msg option] :as data}]
                             (if (= :org.babashka/cli type)
                               (case cause
                                 :require
                                 (println
                                  (format "Missing required argument:\n%s\n"
                                          (cli/format-opts {:spec (select-keys cli-options [option])})))
                                 (println msg))
                               (throw (ex-info msg data)))
                             (System/exit 1))}
        _ (cli/parse-args cmd-line-args options)
        {:keys [variable] :as opts} (cli/parse-opts cmd-line-args
                                                    {:spec cli-options
                                                     :require [:dir]})]
    (run-directory (:dir opts)
                   (-> opts
                       (dissoc :variable)
                       (assoc :variables (transform-cli-variables variable))))))

(binding [pp/*print-right-margin* 200]
 (pp/pprint (main *command-line-args*)))
