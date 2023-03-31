(ns tpximpact.rdf
  (:import [java.net URI]))

(def default-prefixes {:owl "http://www.w3.org/2002/07/owl#"
                       :qb "http://purl.org/linked-data/cube#"
                       :rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                       :rdfs "http://www.w3.org/2000/01/rdf-schema#"
                       :sdmx "http://purl.org/linked-data/sdmx#"
                       :sdmxa "http://purl.org/linked-data/sdmx/2009/attribute#"
                       :sdmxd "http://purl.org/linked-data/sdmx/2009/dimension#"
                       :sdmxm "http://purl.org/linked-data/sdmx/2009/measure#"
                       :skos "http://www.w3.org/2004/02/skos/core#"
                       :time "http://www.w3.org/2006/time#"
                       :xsd "http://www.w3.org/2001/XMLSchema#"
                       :csvw "http://www.w3.org/ns/csvw#"})

;; hacky -- maybe more correct to use safe-curis; though this is
;; probably good enough 99.99% of the time.
(defn curi->uri [prefix-map curi-or-uri]
  (if (string? curi-or-uri)
    (let [[_ prefix suffix] (re-find #"(.*):(.*)" curi-or-uri)]
      (if (#{"http" "urn" "https"} prefix)
        (URI. curi-or-uri)
        (if-let [base (get prefix-map (keyword prefix))]
          (let [resolved-val (str base suffix)]
            (URI. resolved-val))
          (throw (ex-info (str "No such prefix '" prefix "'") {::prefixes prefix-map})))))
    curi-or-uri))

(comment

  (curi->uri default-prefixes "qb:Datset")
  (curi->uri default-prefixes "sdmxd:refPeriod")
  (curi->uri default-prefixes "nosuch:refPeriod")
  (curi->uri default-prefixes "http://foo.bar/blah"))


(defprotocol CuriToURI
  (->uri [t context]))

(extend-protocol CuriToURI
  clojure.lang.Keyword
  (->uri [_ context]
    :id))

(deftype CuriOrURI [v]
  CuriToURI
  (->uri [_ context]
    (curi->uri context v))
  Object
  (toString [_]
    v))
