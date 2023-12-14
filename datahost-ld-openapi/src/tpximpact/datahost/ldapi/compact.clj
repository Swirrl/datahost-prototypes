(ns tpximpact.datahost.ldapi.compact
  (:require [grafter-2.rdf4j.io :as gio])
  (:import [java.net URI]))

(def default-context (atom {}))

(defn sub-context [prefixes]
  (select-keys @default-context prefixes))

(defn add-prefix [prefix uri]
  (swap! default-context assoc (name prefix) uri))

(defn- add-grafter-prefixes []
  (doseq [[prefix uri-str] gio/default-prefixes]
    (add-prefix prefix (URI. uri-str))))

(defn expand [compact-uri]
  (let [prefix-key (-> compact-uri namespace)
        prefixes @default-context]
    (if-let [^URI prefix (get prefixes prefix-key)]
      (URI. (str prefix (name compact-uri)))
      (throw (ex-info (format "Unknown prefix '%s'" (name prefix-key)) {:prefixes prefixes})))))

(defn as-flint-prefixes []
  (into {} (map (fn [[prefix uri]] [(keyword prefix) (format "<%s>" (str uri))]) @default-context)))

(add-prefix :dh (URI. "https://publishmydata.com/def/datahost/"))
(add-prefix :appropriate-csvw (URI. "https://publishmydata.com/def/appropriate-csvw/"))
(add-prefix :csvw (URI. "http://www.w3.org/ns/csvw#"))
(add-prefix :rdfs (URI. "http://www.w3.org/2000/01/rdf-schema#"))
(add-grafter-prefixes)
