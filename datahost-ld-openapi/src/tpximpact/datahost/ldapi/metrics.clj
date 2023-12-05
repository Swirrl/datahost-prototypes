(ns tpximpact.datahost.ldapi.metrics)
(require '[metrics.core :refer [new-registry]])
(require '[metrics.timers :refer [timer]])
(require '[metrics.timers :as tmr])
(require '[metrics.reporters.console :as console])
(require '[metrics.counters :refer [counter inc!]])

;;test by running from hurlscripts dir: hurl int-030.hurl --variable scheme=http --variable host_name=localhost:3000 --variable auth_token= "string" --variable series="$date(.+)"

(def reg (new-registry))
(def db-query-timer
  (timer reg ["db" "read" "execution-time"]))


(defn start-db-query-timer []
  (tmr/start db-query-timer))
(defn stop-db-query-timer [timer-id]
  (tmr/stop timer-id))


(def db-execution-counter (counter reg ["db" "read" "times-called"]))


(defn inc-counter []
(inc! db-execution-counter))


(def CR (console/reporter reg {}))
(console/start CR 30)

;; 46 get-release-by-uri - read
;; 77 get-dataset-series - read
;; 112 get-release-schema-statements - read
;; 133 get-revision - read
;; 156 get-all-series - read
;; 171 get-revisions - read
;; 188 get-releases - read
;; 209 get-change - read
;; 240 get-changes-info - read
;; 316 update-series - write
;; 319 update-release - write
;; 310 update-resource-title-description-modified - write
;; 408 upsert-series! - write
;; 425 delete-series! - write
;; 498 upsert-release! - write
;; 539 get-release-snapshot-info - read
;; 635 insert-revision! - write
;; 643 insert-change! - write
;; 762 upsert-release-schema! - write