(ns tpximpact.datahost.ldapi.generators
  (:require
    [clojure.string :as string]
    [com.gfredericks.test.chuck.generators :as cgen]
    [clojure.test.check.generators :as gen])
  (:import [java.time Duration Instant OffsetDateTime ZoneOffset ZonedDateTime]))

(def title-gen gen/string-alphanumeric)
(def description-gen gen/string-alphanumeric)

(def slug-gen (cgen/string-from-regex #"\w(-?\w{0,5}){0,2}"))

(def series-gen
  (gen/hash-map :type (gen/return :series)
                :slug slug-gen
                "dcterms:title" title-gen
                "dcterms:description" description-gen))

(def release-gen
  (gen/hash-map :type (gen/return :release)
                :slug slug-gen
                "dcterms:title" title-gen
                "dcterms:description" description-gen))

(def release-deps-gen
  (gen/fmap (fn [[series release]]
              (assoc release :parent series)) (gen/tuple series-gen release-gen)))

(defn- update-key-gen [resource k value-gen]
  (let [existing-value (get resource k)
        new-value-gen (gen/such-that (fn [v] (not= v existing-value)) value-gen)]
    (gen/fmap (fn [new-value]
                (assoc resource k new-value))
              new-value-gen)))

(defn update-title-gen [resource]
  (update-key-gen resource "dcterms:title" title-gen))

(defn update-description-gen [resource]
  (update-key-gen resource "dcterms:description" description-gen))

(defn mutate-title-description-gen [resource]
  (let [mutators [update-title-gen
                  update-description-gen]]
    (gen/bind (gen/elements mutators) (fn [mf] (mf resource)))))

(defn maybe-add-mutation-gen [versions mutator decision-gen]
  {:pre [(seq versions)]}
  (let [last-version (last versions)]
    (gen/bind decision-gen (fn [mutate?]
                             (if mutate?
                               (gen/bind (mutator last-version) (fn [version] (maybe-add-mutation-gen (conj versions version) mutator decision-gen)))
                               (gen/return versions))))))
(defn mutations-gen [initial mutator]
  (letfn [(gen-remaining [versions previous decision-gen]
            (gen/bind decision-gen (fn [add?]
                                     (if add?
                                       (gen/bind (mutator previous) (fn [next]
                                                                      (gen-remaining (conj versions next) next decision-gen)))
                                       (gen/return versions)))))]
    (gen/bind (mutator initial) (fn [first] (gen-remaining [first] first gen/boolean)))))

(defn series-updates-gen [series]
  (mutations-gen series mutate-title-description-gen))

(defn instant-gen
  ([]
   (let [local-now (ZonedDateTime/now)
         min-date (.minusWeeks local-now 1)
         max-date (.plusWeeks local-now 1)]
     (instant-gen (Instant/from min-date) (Instant/from max-date))))
  ([^Instant min  ^Instant max]
   (gen/fmap (fn [secs] (Instant/ofEpochSecond secs)) (gen/choose (.getEpochSecond min) (.getEpochSecond max)))))

(defn- instant->utc-offset-datetime [^Instant i]
  (OffsetDateTime/ofInstant i ZoneOffset/UTC))

(defn datetime-gen
  ([] (gen/fmap instant->utc-offset-datetime (instant-gen)))
  ([^OffsetDateTime min ^OffsetDateTime max]
   (gen/fmap instant->utc-offset-datetime (instant-gen (.toInstant min) (.toInstant max)))))

(defn duration-gen
  ([] (duration-gen 0 (.getSeconds (Duration/ofDays 1))))
  ([min-seconds max-seconds]
   (gen/fmap (fn [secs] (Duration/ofSeconds secs)) (gen/choose min-seconds max-seconds))))

(defn tick-gen [^OffsetDateTime current]
  (gen/fmap (fn [^Duration d] (.plus current d)) (duration-gen)))

(defn n-mutations-gen [initial mutator n]
  (letfn [(gen-remaining [versions n]
            (if (pos? n)
              (gen/bind (mutator (last versions)) (fn [next]
                                                    (gen-remaining (conj versions next) (dec n))))
              (gen/return versions)))]
    (gen/bind (mutator initial) (fn [first] (gen-remaining [first] (dec n))))))

(def word-gen (cgen/string-from-regex #"[A-Za-z]+"))
(def schema-column-name-gen word-gen)
(def schema-column-title-gen
  (gen/fmap (fn [words] (string/join " " words)) (gen/vector word-gen 1 4)))

(def schema-column-gen
  (gen/hash-map "csvw:datatype" (gen/return "string")
                "csvw:name" schema-column-name-gen
                "csvw:titles" (gen/vector schema-column-title-gen 1 3)))
(def schema-gen
  (gen/hash-map
    :type (gen/return :schema)
    :slug slug-gen
    "dcterms:title" title-gen
    "dcterms:description" description-gen
    "dh:columns" (gen/vector schema-column-gen 1 5)))

(def schema-deps-gen
  (gen/fmap (fn [[release schema]]
              (assoc schema :parent release))
            (gen/tuple release-deps-gen schema-gen)))

(def revision-gen
  (gen/hash-map
    :type (gen/return :revision)
    "dcterms:title" title-gen
    "dcterms:description" description-gen))

(def revision-deps-gen
  (gen/fmap (fn [[release revision]]
              (assoc revision :parent release))
            (gen/tuple release-deps-gen revision-gen)))

