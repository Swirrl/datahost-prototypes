(ns tpximpact.catql

  (:require [grafter-2.rdf4j.repository :as repo]
            [com.yetanalytics.flint :as f]

            [com.walmartlabs.lacinia :as lacinia]
            [com.walmartlabs.lacinia.parser.schema :as parser]
            [com.walmartlabs.lacinia.executor :as executor]
            [com.walmartlabs.lacinia.resolve :as resolve]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.util :as util]
            [com.walmartlabs.lacinia.pedestal2 :as lp]

            [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [meta-merge.core :as mm]

            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.set :as set]
            [tpximpact.rdf :as cqlrdf]
            [stemmer.snowball :as stem]
            [io.pedestal.http :as http])
  (:import [java.net URI])
  (:gen-class))

(def default-prefixes {:dcat (URI. "http://www.w3.org/ns/dcat#")
                       :dcterms (URI. "http://purl.org/dc/terms/")
                       :owl (URI. "http://www.w3.org/2002/07/owl#")
                       :qb (URI. "http://purl.org/linked-data/cube#")
                       :rdf (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                       :rdfs (URI. "http://www.w3.org/2000/01/rdf-schema#")
                       :skos (URI. "http://www.w3.org/2004/02/skos/core#")
                       :void (URI. "http://rdfs.org/ns/void#")
                       :xsd (URI. "http://www.w3.org/2001/XMLSchema#")
                       :foaf (URI. "http://xmlns.com/foaf/0.1/")})

(def default-catalog (URI. "http://gss-data.org.uk/catalog/datasets"))

(def facet-k->pred {:publishers :dcterms/publisher
                    :creators :dcterms/creator
                    :themes :dcat/theme})

(defn constrain-ds-by-pred [pred values]
  (let [facet-var (symbol (str "?" (name pred)))]
    (if (seq values)
      [[:values {facet-var values}]
       ['?id pred facet-var]]

      [['?id pred facet-var]])))

(defn -all-datasets [{:keys [:CatalogSearchResult/themes
                             :CatalogSearchResult/creators
                             :CatalogSearchResult/publishers]} catalog-uri]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[~catalog-uri ~'(cat :dcat/record :foaf/primaryTopic) ?id]
            ~@(constrain-ds-by-pred :dcterms/publisher publishers)
            ~@(constrain-ds-by-pred :dcterms/creator creators)
            ~@(constrain-ds-by-pred :dcat/theme themes)
            {?id {:dcterms/title #{?title}
                  :rdfs/label #{?label}
                  :dcterms/modified #{?modified}}}
            [:optional [[~'?id :dcterms/description ~'?description]]]
            [:optional [[~'?id :rdfs/comment ~'?comment]]]
            [:optional [{?id {:dcterms/publisher #{?publisher}}}]]
            [:optional [{?id {:dcat/theme #{?theme}}}]]
            [:optional [{?id {:dcterms/creator #{?creator}}}]]

            [:optional [{?id {:dcterms/issued #{?issued}}}]]]})

(comment

  (f/format-query (-all-datasets {:CatalogSearchResult/publishers [(URI. "http://publisher")]} default-catalog)
                  :pretty? true)

  :end)

(defn query [repo query-data]
  (with-open [conn (repo/->connection repo)]
    (let [sparql (f/format-query query-data
                                 :pretty? true)]
      (println sparql)
      (into [] (repo/query conn sparql)))))




(comment
  (def repo (repo/sparql-repo "https://beta.gss-data.org.uk/sparql"))

  (query repo (-all-datasets {} default-catalog))

  :end)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GraphQL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(s/def ::sdl-resource string?)

(defmethod ig/pre-init-spec ::schema [k]
  (println "checking " k)
  (s/keys :req-un [::sdl-resource]))


(defmethod ig/init-key ::service [_ {:keys [schema] :as opts}]
  (lp/default-service schema opts))

;; This is an adapted service map, that can be started and stopped.
;; From the REPL you can call http/start and http/stop on this service:
(defmethod ig/init-key ::runnable-service [_ {:keys [service]}]
  (let [{:io.pedestal.http/keys [host port]} service
        server (-> service
                   http/create-server
                   http/start)]
    (println (str "CatQL running: http://" host ":" port "/"))
    server))

(defmethod ig/halt-key! ::runnable-service [_ server]
  (http/stop server))


(defn load-system-config [config]
  (if config
    (-> config
        slurp
        ig/read-string)
    {}))

(defn load-configs [configs]
  (->> configs
       (map (comp load-system-config io/resource))
       (apply mm/meta-merge)))

(defn start-system [config]
  (-> config
      (doto
       (ig/load-namespaces))
      ig/init))

(defn- make-prefix-map [custom-prefixes]
  (let [custom-prefixes (zipmap (map :prefix custom-prefixes) (map (comp str :base_uri) custom-prefixes))]
    (merge cqlrdf/default-prefixes custom-prefixes)))

(def valid-repo? #{"https://beta.gss-data.org.uk/sparql"})


(defn endpoint-resolver [context {:keys [endpoint_id prefixes] :as args} value]
  (let [endpoint {:endpoint_id endpoint_id}
        prefix-map (make-prefix-map prefixes)]

    (if (valid-repo? endpoint_id)
      (resolve/with-context endpoint {::repo (repo/sparql-repo endpoint_id)
                                      ::prefixes prefix-map})
      (resolve/with-error {} {:message (str "Invalid endpoint '" endpoint_id "'")}))
    ;; TODO endpoint_id becomes the authenticated repo/endpoint we're working against
    ))


(defprotocol CuriToURI
  (->uri [t context]))

(extend-protocol CuriToURI
  clojure.lang.Keyword
  (->uri [_ context]
    :id))

(deftype CuriOrURI [v]
  CuriToURI
  (->uri [_ context]
    (cqlrdf/curi->uri context v))
  Object
  (toString [_]
    v))

(defn catalog-query* [catalog]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[:bind [~catalog ?id]]
            [:optional [{~catalog {:dcterms/title #{?title}}}]]
            [:optional [{~catalog {:rdfs/label #{?label}}}]]]})

(defn catalog-resolver [{:keys [::repo ::prefixes] :as _context} {:keys [id] :as _args} _value]
  (let [catalog (->uri id prefixes)] ;; TODO fix prefixes here to use flint format
    (first (query repo (catalog-query* catalog)))))

(def snowball (stem/stemmer :english))

(defn clean-chars [s]
  (str/replace s #"[.-:(),&\-£'`“”]" ""))

(def stopwords (-> (->> "./catql/stopwords-en.txt"
                        io/resource
                        io/reader
                        line-seq
                        (map clean-chars)
                        set)))

(defn tokenise
  "Crude tokeniser to tokenise a string into snowball stems.

  Always returns a set of tokens, or the empty set."
  [s]
  (let [lower-case (fnil str/lower-case "")]
    (-> (->> (-> s
                 lower-case
                 (str/split #"\p{Space}+"))
             (map clean-chars)
             (remove stopwords)
             (map snowball)
             set)
        (disj ""))))

(defn filter-results [{:keys [:CatalogSearchResult/search-string] :as context} results]
  (let [search-tokens (tokenise search-string)]
    (->> results
         (filter (fn [{:keys [title description]}]
                   (let [data-tokens (tokenise (str title " " description))]
                     ;; or
                     #_(boolean (seq (set/intersection data-tokens search-tokens)))

                     ;; and
                     (= search-tokens
                        (set/intersection data-tokens search-tokens))))))))



(defn datasets-resolver [{:keys [::repo :CatalogSearchResult/search-string] :as context} _args {catalog-uri :id :as _value}]
  (let [results (query repo (-all-datasets context catalog-uri))]
    ;;(sc.api/spy)
    (filter-results context results)))



(def facet-k->facet-type {:publishers "PublisherFacet"
                          :creators "CreatorFacet"
                          :themes "ThemeFacet"})

(defn constrain-by-facet [context facet-k values]
  (let [facet-var (symbol (str "?" (name facet-k) "_constraint"))
        facet-label-var (symbol (str "?" (name facet-k) "_label"))
        pred (get facet-k->pred facet-k)]
    (-> [[:values {facet-var (if (seq values)
                               values
                               [nil])}]
         ['?id pred facet-var]]
        #_(cond->
            (executor/selects-field? context
                                  (keyword (facet-k->facet-type facet-k)
                                           "label"))
          (conj [:optional [[facet-var :rdfs/label facet-label-var]]])))))

(defn -all-facets [context catalog-uri {:keys [:CatalogSearchResult/datasets
                                               :CatalogSearchResult/search-string
                                               :CatalogSearchResult/publishers
                                               :CatalogSearchResult/themes
                                               :CatalogSearchResult/creators] :as constraints}]
  `{:prefixes ~default-prefixes
    :select-distinct ~['?id '?publisher '?publisher_label '?creator '?creator_label '?theme '?theme_label]
    :where [[~catalog-uri ~'(cat :dcat/record :foaf/primaryTopic) ~'?id]
            #_~@(when (seq datasets)
                  [[:values {'?id datasets}]])

            ~@(constrain-by-facet context :themes themes)
            ~@(constrain-by-facet context :publishers publishers)
            ~@(constrain-by-facet context :creators creators)

            [:optional [[~'?id :dcterms/publisher ~'?publisher]
                        [~'?publisher :rdfs/label ~'?publisher_label]]]
            [:optional [[~'?id :dcat/theme ~'?theme]
                        [~'?theme :rdfs/label ~'?theme_label]]]

            [:optional [[~'?id :dcterms/creator ~'?theme]
                        [~'?creator :rdfs/label ~'?creator_label]]]]})

(defn make-facet [facet-k results]
  (->> results
       (group-by facet-k)
       (map (fn [[k col]]
              (let [v (first col)
                    label (get v (keyword (str (name facet-k) "_label")))
                    facet-type (str (str/capitalize (subs (name facet-k) 0 (dec (count (name facet-k))))) "Facet")]

                (schema/tag-with-type
                                   {:id k
                                    :label label
                                    :count -10 ;; TODO count facet
                                    }
                                   facet-type))))))

(defn facets-resolver [{:keys [::repo
                               :CatalogSearchResult/search-string
                               :CatalogSearchResult/themes
                               :CatalogSearchResult/creators
                               :CatalogSearchResult/publishers] :as context}
                       _args
                       {catalog-uri :id :as _value}]


  (let [results (query repo
                       (-all-facets context default-catalog (select-keys context [:CatalogSearchResult/publishers
                                                                                  :CatalogSearchResult/themes
                                                                                  :CatalogSearchResult/creators
                                                                                  #_:CatalogSearchResult/search-string])))]


    {:creators (make-facet :creators results)
     :publishers (make-facet :publishers results)
     :themes (make-facet :themes results)}))

(defn catalog-query-resolver [{:keys [::repo ::prefixes] :as context} {search-string :search_string
                                                                       themes :themes
                                                                       creators :creators
                                                                       publishers :publishers}
                              {:keys [id] :as catalog-value}]
  (let [coerce-uri #(->uri % prefixes)]
    (resolve/with-context {:id id} {:CatalogSearchResult/search-string search-string
                                    :CatalogSearchResult/themes (map coerce-uri themes)
                                    :CatalogSearchResult/creators (map coerce-uri creators)
                                    :CatalogSearchResult/publishers (map coerce-uri publishers)})))

(defn load-schema [sdl-resource]
    (-> (parser/parse-schema (slurp (io/resource sdl-resource)))
        (util/inject-scalar-transformers {:URL {:parse #(java.net.URI. %)
                                                :serialize str}

                                          ;; These can be CURI's or URI's however we can't handle them here
                                          ;; because we need access to the context.
                                          :ID {:parse #(CuriOrURI. %)
                                               :serialize str}

                                          :LangTag {:parse identity
                                                    :serialize identity}

                                          :DateTime {:parse str
                                                     :serialize str}})

        (util/inject-resolvers {:Query/endpoint endpoint-resolver

                                :DataEndpoint/catalog catalog-resolver
                                :Catalog/catalog_query catalog-query-resolver

                                :CatalogSearchResult/datasets datasets-resolver
                                :CatalogSearchResult/facets facets-resolver

                                })
        (schema/compile {:default-field-resolver schema/default-field-resolver
                         :apply-field-directives (fn [field-def resolver-f]
                                                   ;;(sc.api/spy)
                                                   nil)})))

(defmethod ig/init-key ::schema [_ {:keys [sdl-resource]}]
  (load-schema sdl-resource))

(defn -main [& args]
  (println "Starting")
  (let [config (load-configs ["catql/base-system.edn"
                              ;; env.edn contains environment specific
                              ;; overrides to the base-system.edn and
                              ;; is set on classpath depending on env.
                              "catql/env.edn"
                              ])

        sys (start-system config)]

    (println "System started")))





(comment


  ;; Eval this form to start at a REPL
  (do

    (ig/halt! sys)

    (def sys (start-system
              (load-configs ["catql/base-system.edn"])))
    )


  :end)
