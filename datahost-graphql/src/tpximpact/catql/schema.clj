(ns tpximpact.catql.schema
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [com.walmartlabs.lacinia.executor :as executor]
            [com.walmartlabs.lacinia.util :as util]
            [com.walmartlabs.lacinia.resolve :as resolve]
            [com.walmartlabs.lacinia.schema :as schema]
            [com.walmartlabs.lacinia.parser.schema :as parser]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [tpximpact.rdf :as cqlrdf]
            [tpximpact.catql.query :as q]
            [tpximpact.catql.search :as search])
  (:import (java.net URI)
           (tpximpact.rdf CuriOrURI)))

(def default-catalog (URI. "http://gss-data.org.uk/catalog/datasets"))

(defonce facet-labels-store (atom nil))

(defn -all-datasets
  "Returns a SPARQL query as clojure form."
  [{:CatalogSearchResult/keys [themes creators publishers]} catalog-uri]
  `{:prefixes ~q/default-prefixes
    :select :*
    :where [[~catalog-uri ~'(cat :dcat/record :foaf/primaryTopic) ~'?id]
            ~@(q/constrain-by-pred :dcterms/publisher publishers)
            ~@(q/constrain-by-pred :dcterms/creator creators)
            ~@(q/constrain-by-pred :dcat/theme themes)
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
  (let [results (q/query repo (-all-datasets context catalog-uri))
        filtered (search/filter-results context results)]
    (resolve/deliver! promise<datasets> filtered)))

(defn -all-facets-query [{:keys [:CatalogSearchResult/search-string
                                 :CatalogSearchResult/publishers
                                 :CatalogSearchResult/themes
                                 :CatalogSearchResult/creators] :as context} catalog-uri]
  `{:prefixes ~q/default-prefixes
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

(s/def ::constraint-key #{:publishers :creators :themes})

(defn- constraint-type
  "Returns a string."
  [constraint-kw]
  {:pre [(s/valid? ::constraint-key constraint-kw)]}
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

(s/def :tpximpact.catql.schema.make-facet/available-ids set?)

(s/def :tpximpact.catql.schema/other-constraints
  (s/and (s/keys :opt-un [:tpximpact.catql.schema.other-constraints/publishers
                          :tpximpact.catql.schema.other-constraints/creators
                          :tpximpact.catql.schema.other-constraints/themes])
         #(= 2 (count %))))

(s/def :tpximpact.catql.schema/facet-by-id
  (s/keys :req-un [:tpximpact.catql.schema.facet-by-id/id]
          :opt-un [:tpximpact.catql.schema.facet-by-id/title
                   :tpximpact.catql.schema.facet-by-id/description]))

(defn -facet-enabled?
  "Returns whether the facet

  - text-search-fn - fn taking a collection of records and returning a
    boolean
    "
  [context constraint [facet-id grouped] available-ids text-search-fn]
  {:pre [(s/valid? ::constraint-key constraint)
         (s/valid? :tpximpact.catql.schema.make-facet/available-ids available-ids)]}
  (let [other-constraints (dissoc (get-in context [::args :facets]) constraint)]
    (boolean
     (or
      (contains? available-ids facet-id)
      ;; Let's say we are looking at a particular publisher.
      ;; for every *other* constraint: is there at least one match?
      ;; AND (when search-string was passed)
      ;; do we have a text match for a record grouped under this publisher
      (and (every? seq
                   (for [[other-constraint-key values] other-constraints
                         constraint-value values]
                     (sequence (comp (map other-constraint-key)
                                     (filter #(= constraint-value %)))
                               grouped)))
           (text-search-fn grouped))))))

(s/def :CatalogSearchResult/search-string (s/nilable string?))

(s/def :tpximpact.catql.schema.make-facet.context.args/facets
  (s/keys :req-un [::publishers ::creators ::themes]))

(s/def ::args (s/keys :un-req [:tpximpact.catql.schema.make-facet.context.args/facets]))

(s/def :tpximpact.catql.schema.make-facet/context
  (s/keys :req [::args]
          :opt [:CatalogSearchResult/search-string]))

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
  {:pre [(s/valid? ::constraint-key constraint)]}
  (let [text-search-fn (if-some [search-string (:CatalogSearchResult/search-string context)]
                         (fn text-search [ms]
                           (->> (search/filter-results context ms)
                                (some some?)))
                         (fn [_] true))]
    (for [index-entry (dissoc (get indexes constraint) {})
          :let [facet-id (get (key index-entry) constraint)]]
      (schema/tag-with-type
       {:id facet-id
        :label (lookup-facet-label constraint facet-id)
        ;; Let's say we are looking at a particular publisher.
        ;; we set 'enabled=true' when:
        ;; - if this publisher is in the set of all publishers extracted from our datasets
        ;; OR
        ;; - if any of the items published by this particular publisher
        ;;   match *all* other constraints (e.g. particular creators, themes, 
        ;;   and search string, as passed in query arguments)
        :enabled (-facet-enabled? context
                                  constraint
                                  index-entry
                                  available-ids
                                  text-search-fn)}
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
  (let [results (q/query repo (-all-facets-query context catalog-uri))
        promise<facets> (resolve/resolve-promise)]
    (resolve/on-deliver! promise<datasets>
                         (partial datasets-resolver-completion
                                  context
                                  promise<facets>
                                  results))
    promise<facets>))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GraphQL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sdl-resource string?)

;(defmethod ig/pre-init-spec :tpximpact.catql.schema/schema [k]
;  (s/keys :req-un [::sdl-resource]))

(defmethod ig/init-key :tpximpact.catql.schema/const [_ v] v)

(defmethod ig/pre-init-spec :tpximpact.catql.schema/drafter-base-uri [_]
  string?)

(derive :tpximpact.catql.schema/drafter-base-uri :tpximpact.catql.schema/const)

(defmethod ig/pre-init-spec :tpximpact.catql.schema/default-catalog-id [_]
  string?)

(derive :tpximpact.catql.schema/default-catalog-id :tpximpact.catql.schema/const)

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
  `{:prefixes ~q/default-prefixes
    :select :*
    :where [[:bind [~catalog ?id]]
            [:optional [{~catalog {:dcterms/title #{?title}}}]]
            [:optional [{~catalog {:rdfs/label #{?label}}}]]]})

(defn catalog-resolver [{:keys [::repo ::prefixes ::default-catalog-id] :as _context}
                        {:keys [id] :as _args} _value]
  ;; TODO fix prefixes here to use flint format
  (let [catalog (cqlrdf/->uri (or id default-catalog-id) prefixes)]
    (first (q/query repo (catalog-query* catalog)))))

(defn catalog-query-resolver
  [{:keys [::repo ::prefixes] :as context}
   {search-string :search_string
    themes :themes
    creators :creators
    publishers :publishers}
   {:keys [id] :as catalog-value}]
  (let [coerce-uri #(cqlrdf/->uri % prefixes)
        [arg-publishers arg-creators arg-themes] (map #(map coerce-uri  %) [publishers creators themes])
        promise<datasets> (cond-> (resolve/resolve-promise)
                            (not (executor/selects-field? context :CatalogSearchResult/datasets))
                            (resolve/deliver! []))]
    (resolve/with-context {:id id} {::promise<datasets> promise<datasets>
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

(defmethod ig/init-key :tpximpact.catql.schema/schema [_ opts]
  ;; init can be called multiple times and doesn't always have its values filled out
  (when (:sdl-resource opts)
    (load-schema opts)))

(defn- facet-label-query [pred]
  {:prefixes q/default-prefixes
   :select-distinct '[?facet ?facet_label]
   :where ['[?catalog_uri (cat :dcat/record :foaf/primaryTopic) ?id]
           [:optional
            [['?id pred '?facet]
             '[?facet :rdfs/label ?facet_label]]]]})

(defn- load-facet-labels! [repo]
  (let [facet-preds [:dcat/theme :dcterms/publisher :dcterms/creator]]
    (->> (for [pred facet-preds]
           [pred (->> (q/query repo (facet-label-query pred))
                      (remove empty?)
                      (map (juxt :facet :facet_label))
                      (into {}))])
         (into {}))))

(defmethod ig/init-key :tpximpact.catql.schema/facet-labels [_ {:keys [sparql-repo]}]
  (when sparql-repo
    (reset! facet-labels-store
            (try (load-facet-labels! sparql-repo)
                 (catch Exception ex
                   (throw (ex-info "Could not load facet labels" {} ex))
                   {}))))
  (when @facet-labels-store :initialised))
