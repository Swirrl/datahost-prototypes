(ns tpximpact.datahost.time
  (:require [integrant.core :as ig])
  (:import [java.time OffsetDateTime]))

(defprotocol Clock
  "Represents a source for the current time"
  (now [this]
    "Return the current time according to this clock"))

(defn- make-system-clock
  "Returns a Clock implementation which returns the system UTC time"
  []
  (let [jclock (java.time.Clock/systemUTC)]
    (reify Clock
      (now [_] (OffsetDateTime/now jclock)))))

(def system-clock (make-system-clock))

(defn system-now
  "Returns the current time according to the system clock"
  []
  (now system-clock))

(extend-protocol Clock
  OffsetDateTime
  (now [dt] dt))

(defn parse
  "Parses a string representation of the time into the system representation"
  [s]
  (OffsetDateTime/parse s))

(defrecord ManualClock [current]
  Clock
  (now [_this] (or @current (system-now))))

(defn manual-clock
  "Creates a clock that continually returns the given time until updated with
   the set-now and advance-by functions. If the current time is nil, delegates
   to the system clock."
  [initial-time]
  (->ManualClock (atom initial-time)))

(defn set-now
  "Sets the time on the manual clock to the given instant"
  [{:keys [current] :as manual-clock} now]
  (reset! current now)
  nil)

(defmethod ig/init-key ::system-clock [_ _opts]
  system-clock)