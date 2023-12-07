(ns tpximpact.datahost.ldapi.metrics
  (:require
   [integrant.core :as ig]
   [metrics.core :as metrics :refer [new-registry]]
   [metrics.timers :refer [deftimer]]))

(def reg (new-registry))


(defn- make-reporter
  []
  (.. (com.codahale.metrics.Slf4jReporter/forRegistry reg)
      (outputTo (org.slf4j.LoggerFactory/getLogger "datahost.ldapi.metrics"))
      (convertRatesTo java.util.concurrent.TimeUnit/SECONDS)
      (convertDurationsTo java.util.concurrent.TimeUnit/MILLISECONDS)
      (build)))

(defn- as-time-unit
  [time-unit]
  (case time-unit
    :seconds java.util.concurrent.TimeUnit/SECONDS
    :minutes java.util.concurrent.TimeUnit/MINUTES))

(defmethod ig/init-key ::reporter [_ {:keys [enabled? period time-unit]
                                      :or {period 30 time-unit :seconds} :as _opts}]
  (let [reporter (make-reporter)]
    (when enabled?
      (.start reporter period (as-time-unit time-unit)))
    reporter))

(defmethod ig/halt-key! ::reporter [_ reporter]
  (.stop reporter))

(deftimer reg ["db" "read" "get-release-by-uri"])
(deftimer reg ["db" "read" "get-dataset-series"])
(deftimer reg ["db" "write" "insert-series"])
(deftimer reg ["db" "write" "insert-change!"])
(deftimer reg ["db" "read" "get-all-series"])
(deftimer reg ["db" "read" "get-changes-info"])
(deftimer reg ["db" "write" "delete-series!"])

;; 77 get-dataset-series - read
;; 112 get-release-schema-statements - read
;; 133 get-revision - read
;; 171 get-revisions - read
;; 188 get-releases - read
;; 209 get-change - read
;; 316 update-series - write
;; 319 update-release - write
;; 310 update-resource-title-description-modified - write
;; 408 upsert-series! - write
;; 498 upsert-release! - write
;; 539 get-release-snapshot-info - read
;; 635 insert-revision! - write
;; 643 insert-change! - write
;; 762 upsert-release-schema! - write
