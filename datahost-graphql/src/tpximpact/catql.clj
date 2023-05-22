(ns tpximpact.catql
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
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

(defonce facet-labels-store (atom nil))

(defn query
  "Returns a vector result of a query specified by query-data
  argument (a SPARQL query as clojure data)."
  [repo query-data]
  (with-open [conn (repo/->connection repo)]
    (let [sparql (f/format-query query-data :pretty? true)]
      (log/info sparql)
      (into [] (repo/query conn sparql)))))

(defn constrain-by-pred
  "Returns a SPARQL query fragment in clojure data form.

  If values seq is empty, the var will be unconstrained."
  [pred values]
  (let [facet-var (gensym "?")]
    (if (seq values)
      [[:values {facet-var values}]
       ['?id pred facet-var]]
      [['?id pred facet-var]])))

(defn -all-datasets 
  "Returns a SPARQL query as clojure form."
  [{:CatalogSearchResult/keys [themes creators publishers]} catalog-uri]
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

(defn datasets-resolver 
  [{:keys [::repo ::promise<datasets>] :as context} _args {catalog-uri :id :as _value}]
  (let [results (query repo (-all-datasets context catalog-uri))
        filtered (search/filter-results context results)]
    (resolve/deliver! promise<datasets> filtered)))


(defn -all-facets-query [{:keys [:CatalogSearchResult/search-string
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

(defn apply-facet-filter?               ;TODO: remove?
  "Should the facet filter be applied?
  
  Arguments:
  - facet-id - an URI
  - constraint-values - set of URIs (values passed to the query)
  - context - {:CatalogSearchresult/search-string ...}
  - col - seq of {:title String, :description String} "
  [facet-id constraint-values context col]
  (boolean
   (and (seq (search/filter-results context col))
        (constraint-values facet-id))))

(defn- constraint-type
  "Returns a string."
  [constraint-kw]
  {:pre [(keyword? constraint-kw)]}
  (let [constraint-name (name constraint-kw)]
    (-> constraint-name 
        (subs 0 (dec (count constraint-name)))
        str/capitalize 
        (str "Facet"))))

(def ^:private constraint->pred
  {:themes :dcat/theme
   :creator :dcterms/creator
   :publishers :dcterms/publisher})

(defn- lookup-facet-label [facet-constraint facet-id]
  (get (@facet-labels-store (constraint->pred facet-constraint))
       facet-id))

(defn make-facet
  "Returns a facet map.
  
  Arguments:
  - context - application context (as in Lacinia)
  - constraint - keyword 
  - available-ids - set of URIs occurring in the returned datasets.
  - indexes - a map of indexes for each facet
            {:publishers {{:publishers URI} seq<FACET>, ...}
            as crated by [[clojure.set/index]]."
  [context constraint available-ids indexes]
  {:pre [(contains? #{:publishers :creators :themes} constraint)]}
  (let [other-constraints (dissoc (get-in context [::args :facets]) constraint)]
    (for [index-entry (dissoc (get indexes constraint) {})
          :let [facet-id (get (key index-entry) constraint)]]
      (schema/tag-with-type
       {:id facet-id
        :label (lookup-facet-label constraint facet-id)
        :enabled (or ;;(seq (get-in context [::args :facets] constraint))
                  (contains? available-ids facet-id)
                  (boolean (every? seq
                                   (for [[other-constraint-key values] other-constraints
                                         constraint-value values]
                                     (sequence (comp (map other-constraint-key)
                                                     (filter #(= constraint-value %)))
                                               (val index-entry))))))}
       (constraint-type constraint)))))

(defn- datasets-resolver-completion 
  "Completion fn for datasets promise. To be used from within the facets
  resolver. Takes a promise to which the value should be delivered. The
  function returns the same promise."
  [context promise<facets> facets-results datasets]
  (let [reducer (fn reducer [triple ds]
                  (let [extract ((juxt :publisher :creator :theme) ds)
                        safely-conj (fn [s item]
                                      (cond-> s
                                        item (conj item)))]
                    [(safely-conj (nth triple 0) (nth extract 0))
                     (safely-conj (nth triple 1) (nth extract 1))
                     (safely-conj (nth triple 2) (nth extract 2))]))
        [publishers-ids creators-ids themes-ids] (reduce reducer
                                                         [#{} #{} #{}]
                                                         datasets)
        set<facet> (set facets-results)
        indexes (into {} (for [facet-g [:publishers :creators :themes]]
                           [facet-g (set/index set<facet> [facet-g])]))
        make-facet* (fn make-facet* [kw ids]
                      (make-facet context kw ids indexes))
        facets {:publishers (make-facet* :publishers publishers-ids)
                :creators (make-facet* :creators creators-ids)
                :themes (make-facet* :themes themes-ids)}]
    (resolve/deliver! promise<facets> facets)))

(defn facets-resolver
  [{:keys [::repo ::promise<datasets>] :as context}
   _args
   {catalog-uri :id :as _value}]
  (let [results (query repo (-all-facets-query context catalog-uri))
        promise<facets> (resolve/resolve-promise)]
    (resolve/on-deliver! promise<datasets>
                         (partial datasets-resolver-completion
                                  context
                                  promise<facets>
                                  results))
    promise<facets>))

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

(defmethod ig/init-key ::const [_ v] v)

(defmethod ig/pre-init-spec ::drafter-base-uri [_]
  string?)

(derive ::drafter-base-uri ::const)

(defmethod ig/pre-init-spec ::default-catalog-id [_]
  string?)

(derive ::default-catalog-id ::const)

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
  (let [custom-prefixes (zipmap (map :prefix custom-prefixes) 
                                (map (comp str :base_uri) custom-prefixes))]
    (merge cqlrdf/default-prefixes custom-prefixes)))

(defn endpoint-resolver
  [{:keys [drafter-base-uri repo-constructor] :as _context} 
   {:keys [draftset_id prefixes] :as _args} _value]
    ;; TODO endpoint_id becomes the authenticated repo/endpoint we're working against
  (let [endpoint_id (if draftset_id
                      (throw (ex-info "The draftset_id parameter is not supported yet" {:type ::unsupported-parameter}))
                      (str drafter-base-uri "v1/sparql/live"))
        endpoint {:endpoint_id endpoint_id}
        prefix-map (make-prefix-map prefixes)]
    (resolve/with-context endpoint {::repo (repo-constructor endpoint_id)
                                    ::prefixes prefix-map})))

(defn catalog-query* [catalog]
  `{:prefixes ~default-prefixes
    :select :*
    :where [[:bind [~catalog ?id]]
            [:optional [{~catalog {:dcterms/title #{?title}}}]]
            [:optional [{~catalog {:rdfs/label #{?label}}}]]]})

(defn catalog-resolver [{:keys [::repo ::prefixes ::default-catalog-id] :as _context}
                        {:keys [id] :as _args} _value]
  ;; TODO fix prefixes here to use flint format
  (let [catalog (cqlrdf/->uri (or id default-catalog-id) prefixes)]
    (first (query repo (catalog-query* catalog)))))

(defn catalog-query-resolver
  [{:keys [::repo ::prefixes] :as context} 
   {search-string :search_string
    themes :themes
    creators :creators
    publishers :publishers}
   {:keys [id] :as catalog-value}]
  (let [coerce-uri #(cqlrdf/->uri % prefixes)
        [arg-publishers arg-creators arg-themes] (map #(map coerce-uri  %) [publishers creators themes])]
    (resolve/with-context {:id id} {::promise<datasets> (resolve/resolve-promise)
                                    ::args {:facets {:publishers arg-publishers 
                                                     :themes arg-themes
                                                     :creators arg-creators}}
                                    :CatalogSearchResult/search-string search-string
                                    :CatalogSearchResult/themes arg-themes
                                    :CatalogSearchResult/creators arg-creators
                                    :CatalogSearchResult/publishers arg-publishers})))

(defn load-schema
  [{:keys [sdl-resource drafter-base-uri repo-constructor default-catalog-id]
    :or {repo-constructor repo/sparql-repo}}]
  (-> (parser/parse-schema (slurp (io/resource sdl-resource)))
      (util/inject-scalar-transformers {:URL {:parse #(URI. %)
                                              :serialize str}

                                          ;; These can be CURI's or URI's however we can't handle them here
                                          ;; because we need access to the context.
                                        :ID {:parse #(CuriOrURI. %)
                                             :serialize str}

                                        :LangTag {:parse identity
                                                  :serialize identity}

                                        :DateTime {:parse str
                                                   :serialize str}})

      (util/inject-resolvers {:Query/endpoint (fn [context args value]
                                                (endpoint-resolver (assoc context
                                                                          :drafter-base-uri drafter-base-uri
                                                                          :repo-constructor repo-constructor)
                                                                   args
                                                                   value))

                              :DataEndpoint/catalog (fn [context args value]
                                                      (catalog-resolver (assoc context
                                                                               ::default-catalog-id default-catalog-id)
                                                                        args
                                                                        value))
                              :Catalog/catalog_query catalog-query-resolver

                              :CatalogSearchResult/datasets datasets-resolver
                              :CatalogSearchResult/facets facets-resolver})

      (schema/compile {:default-field-resolver schema/default-field-resolver
                       :apply-field-directives (fn [field-def resolver-f]
                                                 nil)})))

(defmethod ig/init-key ::schema [_ opts]
  (load-schema opts))

(defn- facet-label-query [pred]
  {:prefixes default-prefixes
   :select-distinct '[?facet ?facet_label]
   :where ['[?catalog_uri (cat :dcat/record :foaf/primaryTopic) ?id]
           [:optional
            [['?id pred '?facet]
             '[?facet :rdfs/label ?facet_label]]]]})

(defn- load-facet-labels! [repo]
  (let [facet-preds [:dcat/theme :dcterms/publisher :dcterms/creator]]
    (->> (for [pred facet-preds]
           [pred (->> (query repo (facet-label-query pred))
                      (remove empty?)
                      (map (juxt :facet :facet_label))
                      (into {}))])
         (into {}))))

(defmethod ig/init-key ::sparql-repo [_ {:keys [endpoint]}]
  (repo/sparql-repo endpoint))

(defmethod ig/init-key ::facet-labels [_ {:keys [sparql-repo]}]
  (reset! facet-labels-store
          (try (load-facet-labels! sparql-repo)
               (catch Exception ex
                 (throw (ex-info "Could not load facet labels" {} ex))
                 {}))))

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
  (let [sys (-> config
                (doto
                  (ig/load-namespaces))
                ig/init)]
    sys))

(defn -main [& args]
  (let [config (load-configs ["catql/base-system.edn"
                              ;; env.edn contains environment specific
                              ;; overrides to the base-system.edn and
                              ;; is set on classpath depending on env.
                              "catql/env.edn"])
        sys (start-system config)]
    (log/info "System started")))
