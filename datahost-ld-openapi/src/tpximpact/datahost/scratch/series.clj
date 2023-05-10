(ns tpximpact.datahost.scratch.series
  (:require [clojure.java.io :as io]
            [grafter-2.rdf4j.io :as rio]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]
            [duratom.core :as db])
  (:import [java.net URI]
           [com.github.jsonldjava.core JsonLdProcessor RDFDatasetUtils JsonLdTripleCallback]
           [com.github.jsonldjava.utils JsonUtils]))

(def ld-root
  "For the prototype this item will come from config or be derived from
  it.  It should have a trailing slash."
  (URI. "https://example.org/data/"))

(def default-catalog-uri (URI. "https://example.org/data/catalog"))

(def file-store (io/file "./tmp"))

(defn- ->edn-data
  "Convert mutable java data, as returned by the JSON-LD parser into
  immutable clojure data structures, so we can work with it without
  surprises."
  [java-data]
  (walk/prewalk (fn [f]
                  (cond
                     (instance? java.util.HashMap f) (into {} f)
                     (instance? java.util.ArrayList f) (into [] f)
                     :else f))
                 java-data))

(defn load-jsonld [jsonld-file]
  (->edn-data (JsonUtils/fromReader (io/reader jsonld-file))))

(defn ednld->rdf
  "Takes a JSON-LD map as an EDN datastructure and converts it into RDF
  triples.

  NOTE: the implementation is the easiest but worst way to do, but
  should at least be correct.

  TODO: At some point we should change this to a more direct/better
  implementation."
  [edn-ld]
  (JsonLdProcessor/toRDF edn-ld
                         (reify
                           JsonLdTripleCallback
                           (call [_ dataset]
                             (let [nquad-string (let [sb (java.lang.StringBuilder.)]
                                                  (RDFDatasetUtils/toNQuads dataset sb)
                                                  (str sb))]

                               (tap> nquad-string)

                               (rio/statements (java.io.StringReader. nquad-string) :format :nq))))))

(defn dissoc-by-key [m pred]
  (reduce (fn [acc k]
               (if (pred k)
                 (dissoc acc k)
                 acc))
             m (keys m)))

(defn valid-slug? [slug]
  ;; TODO add more slug rules around valid character ranges e.g.
  ;; exclude _ but allow -'s etc...
  (not (or (str/starts-with? slug "/")
           (str/ends-with? slug "/"))))

(defn calculate-base-entity
  "Calculate the base-entity for the release (and elements beneath it in
  the data model).

  NOTE: this is subtly different to the @base IRI for the series
  document which here is set to the ld-root with a trailing '/'."
  [slug]
  (when-not (valid-slug? slug)
    (throw (ex-info "slug must not start or end in a '/'" {:type :invalid-arguments})))
  (str ld-root slug "/"))

(defn normalise-context [ednld]
  (let [normalised-context ["https://publishmydata.com/def/datahost/context"
                            {"@base" (str ld-root)}]]
    (if-let [context (get ednld "@context")]
      (cond
        (= context "https://publishmydata.com/def/datahost/context") normalised-context
        (= context normalised-context) normalised-context

        :else (throw (ex-info "Invalid @context" {:supplied-context context
                                                  :valid-context normalised-context})))

      ;; return normalised-context if none provided
      normalised-context)))

(def registry
  (merge
   (m/class-schemas)
   (m/comparator-schemas)
   (m/base-schemas)
   (m/type-schemas)
   {:series-slug-string [:and :string [:re {:error/message "should contain alpha numeric characters and hyphens only."}
                                #"^[a-z,A-Z,\-,0-9]+$"]]
    :url-string (m/-simple-schema {:type :url-string :pred
                                   (fn [x]
                                     (and (string? x)
                                          (try (URI. x)
                                               true
                                               (catch Exception ex
                                                 false))))})}))

(def SeriesApiParams [:map
                      [:series-slug :series-slug-string]
                      [:title {:optional true} :string]
                      [:description {:optional true} :string]])

(def SeriesJsonLdInput [:map
                        ["@id" :series-slug-string]
                        ["dh:base-entity" :url-string]])

(comment
  (m/validate SeriesJsonLdInput {"@id" "foo-bar"
                                 "dh:base-entity" "http://foo"}
              {:registry registry})

  (m/validate SeriesApiParams
              {:series-slug "my-dataset-series" :title "foo"}
              {:registry registry})

  (me/humanize (m/explain SeriesApiParams
                          {:series-slug "foo-bar" :title "foo"}
                          {:registry registry}))
  )

(defn validate-id [{:keys [series-slug] :as _api-params} cleaned-doc]
  (let [id-in-doc (get cleaned-doc "@id")]
    (cond
      (nil? id-in-doc) cleaned-doc

      (= series-slug id-in-doc) cleaned-doc

      :else (throw
             (ex-info "@id should for now be expressed as a slugged style suffix, and if present match that supplied as the API slug."
                      {:supplied-id id-in-doc
                       :expected-id series-slug})))))

(defn validate-series-context [ednld]
  (if-let [base-in-doc (get-in ednld ["@context" 1 "@base"])]
    (if (= (str ld-root) base-in-doc)
      (update ednld "@context" normalise-context)
      (throw (ex-info
              (str "@base for the dataset-series must currently be set to the linked-data root '" ld-root "'")
              {:type :validation-error
               :expected-value (str ld-root)
               :actual-value base-in-doc})))
    (update ednld "@context" normalise-context)))

(defn merge-params-with-doc [{:keys [series-slug] :as api-params} jsonld-doc]
  (let [merged-doc (merge (set/rename-keys api-params
                                           {:title "dcterms:title"
                                            :description "dcterms:description"})
                          jsonld-doc)
        cleaned-doc (-> merged-doc
                        (dissoc-by-key keyword?)
                        #_(update "@context" normalise-context))

        ]
    cleaned-doc))

;; PUT /data/:series-slug
(defn normalise-series
  "Takes api params and an optional json-ld document of metadata, and
  returns a normalised EDN form of the JSON-LD, with the API
  parameters applied, validated and some structures like the context
  normalised."
  ([api-params]
   (normalise-series api-params nil))
  ([{:keys [series-slug] :as api-params} doc]
   (let [jsonld-doc doc #_(into {} (load-jsonld doc))]

     (when-not (m/validate SeriesApiParams api-params {:registry registry})
       (throw (ex-info "Invalid API parameters"
                       {:type :validation-error
                        :validation-error (-> (m/explain SeriesApiParams api-params {:registry registry})
                                              (me/humanize))})))

     (let [cleaned-doc (merge-params-with-doc api-params doc)

           validated-doc (-> (validate-id api-params cleaned-doc)
                             (validate-series-context))

           final-doc (assoc validated-doc
                            ;; add any managed params
                            "@id" series-slug
                            "dh:base-entity" (str ld-root series-slug "/") ;; coin base-entity to serve as the @base for nested resources
                            )]
       final-doc))))

(comment
  (def db (db/duratom :local-file
                      :file-path ".datahostdb.edn"
                      :commit-mode :sync
                      :init {}))

  (defn update-series [old-series {:keys [api-params jsonld-doc] :as _new-series}]
    (log/info "Updating series " (:series-slug api-params))
    (normalise-series api-params jsonld-doc))

  (defn create-series [api-params jsonld-doc]
    (log/info "Updating series " (:series-slug api-params))
    (normalise-series api-params jsonld-doc))

  (defn put-db [db {:keys [api-params jsonld-doc] :as new-series}]
    (let [k (str (.getPath ld-root) (:series-slug api-params))]
      (if-let [old-series (get db k)]
        (update db k update-series new-series)
        (assoc db k (normalise-series api-params jsonld-doc)))))

  (let [db (atom {})]

    (swap! db put-db {:api-params {:series-slug "my-dataset-series"} :jsonld-doc (io/resource "./test-inputs/series/empty-1.json")})

    (swap! db put-db {:api-params {:series-slug "my-other-series"} :jsonld-doc (io/resource "./test-inputs/series/empty-1.json")})
    (swap! db put-db {:api-params {:series-slug "my-other-series" :title "my title"} :jsonld-doc (io/resource "./test-inputs/series/empty-1.json")})
    #_(swap! db put-db {:api-params {:series-slug "my-dataset-series"} :jsonld-doc (io/resource "./test-inputs/series/empty-1.json")})

    db)

  )



(comment



  (swap! db assoc "some-other-path" {"some" "jsonld"})

  (db/destroy db)

  )


;; POST /data/:series-slug/:release-slug

(comment

  (require '[dev.nu.morse :as morse])

  (load-jsonld (io/resource "./test-inputs/series/empty-1.json"))

  (rio/statements (assoc (into {} (load-jsonld (io/file "./resources/example-series.json")))
                         "dcterms:foo" "bla")
                  :format :jsonld)

  (def data (assoc (into {}
                         (load-jsonld (io/file "./resources/example-series.json")))
                   "@base" (str ld-root)
                   "@id" "my-dataset-series"
                   "dcterms:foo" "bla"))

  (ednld->rdf data)

  (create-series {:series-slug "my-dataset-series"
                  :title "new title"
                  :description "blah blah"}
                 (io/file "./resources/example-series.json"))

  )
