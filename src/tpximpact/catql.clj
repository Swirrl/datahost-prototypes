(ns tpximpact.catql
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.walmartlabs.lacinia.parser.schema :as parser]
   [com.walmartlabs.lacinia.pedestal2 :as lp]
   [com.walmartlabs.lacinia.resolve :as resolve]
   [com.walmartlabs.lacinia.schema :as schema]
   [com.walmartlabs.lacinia.util :as util]
   [com.yetanalytics.flint :as f]
   [grafter-2.rdf4j.repository :as repo]
   [integrant.core :as ig]
   [io.pedestal.http :as http]
   [meta-merge.core :as mm]
   [tpximpact.rdf :as cqlrdf]
   [tpximpact.catql.search :as search])
  (:import
   [java.net URI]
   [tpximpact.rdf CuriOrURI])
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

(defn query [repo query-data]
  (with-open [conn (repo/->connection repo)]
    (let [sparql (f/format-query query-data :pretty? true)]
      (log/info sparql)
      (into [] (repo/query conn sparql)))))

(defn constrain-by-pred [pred values]
  (let [facet-var (gensym "?")]
    (if (seq values)
      [[:values {facet-var values}]
       ['?id pred facet-var]]
      [['?id pred facet-var]])))

(defn -all-datasets [{:keys [:CatalogSearchResult/themes
                             :CatalogSearchResult/creators
                             :CatalogSearchResult/publishers]} catalog-uri]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[~catalog-uri ~'(cat :dcat/record :foaf/primaryTopic) ~'?id]
            ~@(constrain-by-pred :dcterms/publisher publishers)
            ~@(constrain-by-pred :dcterms/creator creators)
            ~@(constrain-by-pred :dcat/theme themes)
            {~'?id {:dcterms/title #{~'?title}
                    :rdfs/label #{~'?label}
                    :dcterms/modified #{~'?modified}}}
            [:optional [[~'?id :dcterms/description ~'?description]]]
            [:optional [[~'?id :rdfs/comment ~'?comment]]]
            [:optional [{~'?id {:dcterms/publisher #{~'?publisher}}}]]
            [:optional [{~'?id {:dcat/theme #{~'?theme}}}]]
            [:optional [{~'?id {:dcterms/creator #{~'?creator}}}]]
            [:optional [{~'?id {:dcterms/issued #{~'?issued}}}]]]})

(defn datasets-resolver [{:keys [::repo] :as context} _args {catalog-uri :id :as _value}]
  (let [results (query repo (-all-datasets context catalog-uri))]
    (search/filter-results context results)))


(defn -all-facets [{:keys [:CatalogSearchResult/search-string
                           :CatalogSearchResult/publishers
                           :CatalogSearchResult/themes
                           :CatalogSearchResult/creators] :as context} catalog-uri]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[~catalog-uri ~'(cat :dcat/record :foaf/primaryTopic) ~'?id]
            {~'?id {:dcterms/title #{~'?title}
                    :rdfs/label #{~'?label}
                    :dcterms/modified #{~'?modified}}}
            ;; ~@(constrain-by-pred :dcterms/publisher publishers)
            ;; ~@(constrain-by-pred :dcterms/creator creators)
            ;; ~@(constrain-by-pred :dcat/theme themes)

            [:optional [[~'?id :dcterms/description ~'?description]]]
            [:optional [{~'?id {:dcterms/publisher #{~'?publishers}}}]]
            [:optional [{~'?id {:dcat/theme #{~'?themes}}}]]
            [:optional [{~'?id {:dcterms/creator #{~'?creators}}}]]

            ;; [:optional [[~'?id :dcterms/publisher ~'?publisher]
            ;;             ;; [~'?publisher :rdfs/label ~'?publisher_label]
            ;;             ]]
            ;; [:optional [[~'?id :dcat/theme ~'?theme]
            ;;             ;; [~'?theme :rdfs/label ~'?theme_label]
            ;;             ]]
            ;; [:optional [[~'?id :dcterms/creator ~'?theme]
            ;;             ;; [~'?creator :rdfs/label ~'?creator_label]
            ;;             ]]
            ]})

(defn apply-facet-filter? [facet-id constraint-values context col]
  (boolean
   (and (seq (search/filter-results context col))
        (constraint-values facet-id))))

(defn make-facet [context constraint results]
  (let [facet-type (-> constraint name drop-last str/join str/capitalize (str "Facet"))
        constraint-values (set (constraint context))
        result-key (-> constraint name keyword)]
    (->> results
         (group-by result-key)
         (map (fn [[facet-id col]]
                (when facet-id
                  (schema/tag-with-type
                   {:id facet-id
                    :label "TODO: fetch labels"
                    :enabled (apply-facet-filter? facet-id constraint-values context col)}
                   facet-type))))
         (remove nil?))))

(defn facets-resolver [{:keys [::repo] :as context}
                       _args
                       {catalog-uri :id :as _value}]
  (let [results (query repo (-all-facets context catalog-uri))]
    {:creators (make-facet context :CatalogSearchResult/creators results)
     :publishers (make-facet context :CatalogSearchResult/publishers results)
     :themes (make-facet context :CatalogSearchResult/themes results)}))

(comment
  (f/format-query (-all-datasets {:CatalogSearchResult/publishers [(URI. "http://publisher")]} default-catalog)
                  :pretty? true)
  :end)

(comment
  (def repo (repo/sparql-repo "https://beta.gss-data.org.uk/sparql"))
  (query repo (-all-datasets {} default-catalog))
  :end)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GraphQL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sdl-resource string?)

(defmethod ig/pre-init-spec ::schema [k]
  (s/keys :req-un [::sdl-resource]))

(defmethod ig/init-key ::service [_ {:keys [schema] :as opts}]
  (lp/default-service schema opts))

(defmethod ig/pre-init-spec ::drafter-base-uri [_]
  string?)

(defmethod ig/init-key ::drafter-base-uri [_ val]
  val)

;; This is an adapted service map, that can be started and stopped.
;; From the REPL you can call http/start and http/stop on this service:
(defmethod ig/init-key ::runnable-service [_ {:keys [service]}]
  (let [{:io.pedestal.http/keys [host port]} service
        server (-> service
                   http/create-server
                   http/start)]
    (log/info (str "CatQL running: http://" host ":" port "/"))
    server))

(defmethod ig/halt-key! ::runnable-service [_ server]
  (http/stop server))

(defn- make-prefix-map [custom-prefixes]
  (let [custom-prefixes (zipmap (map :prefix custom-prefixes) (map (comp str :base_uri) custom-prefixes))]
    (merge cqlrdf/default-prefixes custom-prefixes)))

(defn endpoint-resolver [{:keys [drafter-base-uri] :as _context} {:keys [draftset_id prefixes] :as _args} _value]
    ;; TODO endpoint_id becomes the authenticated repo/endpoint we're working against
  (let [endpoint_id (if draftset_id
                      (throw (ex-info "The draftset_id parameter is not supported yet" {:type ::unsupported-parameter}))
                      (str drafter-base-uri "v1/sparql/live"))
        endpoint {:endpoint_id endpoint_id}
        prefix-map (make-prefix-map prefixes)]
    (resolve/with-context endpoint {::repo (repo/sparql-repo endpoint_id)
                                    ::prefixes prefix-map})))

(defn catalog-query* [catalog]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[:bind [~catalog ?id]]
            [:optional [{~catalog {:dcterms/title #{?title}}}]]
            [:optional [{~catalog {:rdfs/label #{?label}}}]]]})

(defn catalog-resolver [{:keys [::repo ::prefixes] :as _context} {:keys [id] :as _args} _value]
  (let [catalog (cqlrdf/->uri id prefixes)] ;; TODO fix prefixes here to use flint format
    (first (query repo (catalog-query* catalog)))))

(defn catalog-query-resolver [{:keys [::repo ::prefixes] :as context} {search-string :search_string
                                                                       themes :themes
                                                                       creators :creators
                                                                       publishers :publishers}
                              {:keys [id] :as catalog-value}]
  (let [coerce-uri #(cqlrdf/->uri % prefixes)]
    (resolve/with-context {:id id} {:CatalogSearchResult/search-string search-string
                                    :CatalogSearchResult/themes (map coerce-uri themes)
                                    :CatalogSearchResult/creators (map coerce-uri creators)
                                    :CatalogSearchResult/publishers (map coerce-uri publishers)})))

(defn load-schema [{:keys [sdl-resource drafter-base-uri]}]
  (-> (parser/parse-schema (slurp (io/resource sdl-resource)))
      (util/inject-scalar-transformers {:URL {:parse #(java.net.URI. %)
                                              :serialize str}

                                          ;; These can be CURI's or URI's however we can't handle them here
                                          ;; because we need access to the context.
                                        :ID {:parse #(cqlrdf/CuriOrURI. %)
                                             :serialize str}

                                        :LangTag {:parse identity
                                                  :serialize identity}

                                        :DateTime {:parse str
                                                   :serialize str}})

      (util/inject-resolvers {:Query/endpoint (fn [context args value]
                                                (endpoint-resolver (assoc context
                                                                          :drafter-base-uri drafter-base-uri)
                                                                   args
                                                                   value))

                              :DataEndpoint/catalog catalog-resolver
                              :Catalog/catalog_query catalog-query-resolver

                              :CatalogSearchResult/datasets datasets-resolver
                              :CatalogSearchResult/facets facets-resolver})

      (schema/compile {:default-field-resolver schema/default-field-resolver
                       :apply-field-directives (fn [field-def resolver-f]
                                                   ;;(sc.api/spy)
                                                 nil)})))

(defmethod ig/init-key ::schema [_ opts]
  (load-schema opts))

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

(defn -main [& args]
  (let [config (load-configs ["catql/base-system.edn"
                                 ;; env.edn contains environment specific
                                 ;; overrides to the base-system.edn and
                                 ;; is set on classpath depending on env.
                                 "catql/env.edn"
                                 ])

        sys (start-system config)]

    (log/info "System started")))
