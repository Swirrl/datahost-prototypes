(ns tpximpact.datahost.ldapi.router
  (:require
   [buddy.auth :refer [authenticated?]]
   [buddy.auth.backends.httpbasic :as http-basic]
   [buddy.auth.middleware :as buddy]
   [buddy.hashers :as hashers]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [integrant.core :as ig]
   [malli.util :as mu]
   [muuntaja.core :as m]
   [reitit.core :as r]
   [reitit.coercion.malli :as rcm]
   [reitit.dev.pretty :as pretty]
   [reitit.interceptor.sieppari :as sieppari]
   [reitit.openapi :as openapi]
   [reitit.ring :as ring]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.parameters :as parameters]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.middleware.cors :as cors]
   [ring.util.request :as r.u.request]
   [tpximpact.datahost.ldapi.errors :as ldapi-errors]
   [tpximpact.datahost.ldapi.routes.middleware :as middleware]
   [tpximpact.datahost.ldapi.routes.release :as routes.rel]
   [tpximpact.datahost.ldapi.routes.revision :as routes.rev]
   [tpximpact.datahost.ldapi.routes.series :as routes.s]
   [muuntaja.format.json :as json-format]
   [clojure.data.json :as json]
   [clojure.java.io :as io]))

(defn jsonld-options [options]
  (assoc-in options
            [:formats "application/ld+json"]
            (-> json-format/format
                (assoc :name "application/ld+json"
                       :decoder-opts {:decode-key-fn identity}
                       :encoder-opts {:encode-key-fn identity}))))

(defn leave-json-keys-alone [options]
  (-> options
      (assoc-in [:formats "application/json" :decoder-opts] {:decode-key-fn identity})
      (assoc-in [:formats "application/json" :encoder-opts] {:encode-key-fn identity})))

(def default-muuntaja-coercer
  (-> m/default-options
      jsonld-options
      m/create))

(def leave-keys-alone-muuntaja-coercer
  (-> m/default-options
      jsonld-options
      leave-json-keys-alone
      m/create))

(defn authenticate-user [users authdata]
  (let [username (:username authdata)
        password (:password authdata)]
    (when-let [passhash (get users username)]
      (:valid (hashers/verify password passhash)))))

(defn basic-auth-backend [context]
  (http-basic/http-basic-backend
   {:realm  (:realm context)
    :authfn (fn [_request auth-data]
              (authenticate-user (:users context) auth-data))}))

(def authenticated-methods #{:delete :patch :post :put})

(defn basic-auth-middleware [auth]
  (let [backend (basic-auth-backend auth)]
    (fn [handler]
      (fn [request]
        (if (contains? authenticated-methods (:request-method request))
          (let [request (buddy/authentication-request request backend)]
            (if (authenticated? request)
              (handler request)
              {:status 401
               :headers {"Content-Type" "text/plain"}
               :body "Unauthorized"}))
          (handler request))))))

(defn- read-request? [request]
  (#{:get :head} (:request-method request)))


(defn- browser-html-request? [request]
  (when-let [accept-header (get-in request [:headers "accept"])]
    (and (str/includes? accept-header "text/html")
         (not (str/includes? accept-header "application/json"))
         (not (str/includes? accept-header "application/ld+json")))))


(def browser-render-convenience-middleware
  "This is an affordance that attempts to detect an in-browser GET request. If
  detected, the application/ld+json content-type from API response will be overridden
  so that the browser renders the response as plain JSON and does not attempt to download
  the unrecognized application/ld+json as a file."
  (fn [handler]
    (fn [request]
      (if (and (read-request? request)
               (browser-html-request? request))
        (let [response (handler request)]
          (if (= (get-in response [:headers "content-type"]) "application/ld+json")
            ;; replace content-type for raw browser request
            (assoc-in response [:headers "content-type"] "application/json")
            response))
        (handler request)))))

(defn wrap-context-middleware
  "Associate a :context and :path-info with the request. The request URI must be
  a sub-path of the supplied context."
  [base-path]
  (when-not (str/blank? base-path)
    (println (str "\nApp `base-path` will be set to: `" base-path "`\n")))
  (fn wrap-context [handler]
    (fn [request]
      (if (str/blank? base-path)
        (handler request)
        (handler
         (r.u.request/set-context request base-path))))))

(defn wrap-request-base-uri
  "Reitit does not directly acknowledge or act on the Ring request :context key, so
  we must manually intercept request URIs here and remove the :context value (base path)
  from the :uri so reitit routes are matched"
  [handler]
  (fn [request]
    (handler (update request :uri #(str/replace-first % (:context request "") "")))))

(def cors-middleware
  "Defines a CORS middleware for a route"
  {:name ::cors
   :compile (fn [_route _opts]
              (fn [handler]
                ;; apply CORS to each router
                ;; NOTE: in future we might want to make this opt-in for specific routes
                (cors/wrap-cors handler
                                :access-control-allow-origin (constantly true)
                                :access-control-allow-methods [:get :post :put])))})

(def ^:private api-description
  "Datahost is a prototype service aiming to simplify the publication
and consumption of statistical datasets on the web, whilst following
Linked Data Principles, best practices and web standards, without
assuming detailed knowledge of them.

It has been developed by TPXImpact in partnership with the Office of
National Statistics.

More details of the project can be found on the projects [GitHub
repository](https://github.com/Swirrl/datahost-prototypes/tree/main/datahost-ld-openapi).

Datahost's API is broadly divided to support two distinct audiences:

1. Data users and consumers via the [Consumer API](#/Consumer%20API).
2. Data publishers, who use both the [Publisher API](#/Publisher%20API) and [Consumer API](#/Consumer%20API).

# Data Model

## Overview

Datahost aims to give improved guarantees to data consumers, by
requiring publishers make commitments to consumers about the
management and shape of the data. These commitments govern how the
data will be managed over time, and come in the form of a versioning
model enforced by Datahost and dataset schemas.

With datahost publishers are encouraged to declare their datasets
schema in advance by associating releases of their dataset with a
schema. Datahost ensures that all revisions of a dataset/release must
then conform to that releases declared schema.

To understand the Datahost data model, we first need to understand
some datahost terminology:

- Dataset-Series - In Datahost a dataset-series is a name and global
  identifier representing a dataset over its whole lifetime. It can
  cover all historic and future releases of that Dataset. A good
  example would be \"[The Census](https://www.ons.gov.uk/census)\",
  which has been assembled every ten years since 1801. Throughout this
  time the methodology and shape/schema of the data has changed many
  times, but its purpose to measure the population has remained. -

- Release - In Datahost a release is how we identify a dataset with a
  specific schema and methodology. Releases may be updated over time,
  but they make the commitment to consumers that any changes to them
  will be explicitly captured in change logs, and that all such
  revisions will conform to the schema for the release.

  This allows publishers to make improvements to a release after
  publication, for example correcting mistakes in the figures whilst
  providing transparency to consumers that changes were made, and that
  those changes will always be compatible with the releases published
  schema.

  A release has a name e.g. \"2021\" which when combined with the
  dataset-series name forms the full name of the dataset release e.g.
  \"The Census 2021\". Users will usually want the latest revision of
  a release, and knowing the combined series and release name will let
  them do that.

- Schema - In Datahost we define a simple schema language which is a
  valid subset of CSVW, but introduces a few extra properties to help
  support versioning. We surface these schemas as datahost/CSVW
  schemas to consumers.

- Revision - In Datahost a revision represents a named published set
  of changes to a dataset release. All changes within a revision
  *MUST* conform to the Release's pre-published schema. Revisions
  contain a sequence of changes or commits.

- Change/Commit - In Datahost there are 3 types of change that can occur.

  - Appends - Represent the addition of more data to a release. Adding
    more data providing it conforms to the pre-published schema should
    never be a breaking change to consumers. Datahost will enforce
    this guarantee.

  - Retractions - We don't encourage publishers removing data from
    releases, however from time to time there is a need for publishers
    to do it. Datahost will support publishers in removing the
    information as a soft-delete, which retains the previous revision
    containing the deletion and also a retraction commit stating
    explicitly what data was removed. This is useful information for
    consumers, and provides a method to safely stay up to date.

  - Corrections - Are a special composite form of a retraction and an
    append that represent an amended figure in the data. In addition
    corrections contain a backlink to the previous figure, allowing
    users and publishers an easy way to explore figure changes.

  All commits contain a message in the form of a description.

## Data and Metadata

The Datahost data model follows linked data principles and web best
practices to ensure as much as possible a resource oriented approach
to our API.

In particular resource's may contain many representations, and those
representations may partially represent that entity.

Broadly speaking Datahost assumes that when we're speaking about the
data behind a resource we will refer it to with a mimetype of
`text/csv`, whilst if we're speaking about the metadata we'll refer to
it as `application/json` (or `application/ld+json`). Typically this is
done by setting the `Accept` and `Content-Type` HTTP headers as
appropriate.

For example a revision to a dataset-series's release is represented by it's URI e.g.

`/data/census/releases/2021/revisions/1`

If we want to know what all the data in this revision looks like, we
can request it by setting the `Accept` header to `text/csv`, which
will return all the data in that revision. However if we want to know
what metadata there is about that revision, we will request it by
setting the `Accept` header to `application/json`.

In Datahost it's possible for some resources to only have metadata or
data representations available. This should be documented in the API
specifications for each route.")

(defn router [{:keys [clock triplestore change-store auth system-uris]}]
  (ring/router
   [["/openapi.json"
     {:get {:no-doc true
            :openapi {:openapi "3.0.0"
                      :info {:title "Datahost Prototype API"
                             :description api-description
                             :version "0.1.0"}
                      :components {:securitySchemes {"basic" {:type "http" :scheme "basic"}}}
                      :tags [{:name "Consumer API"
                              :description "Operations for data consumers (including publishers)"}
                             {:name "Publisher API"
                              :description "Operations for data publishers"}]}
            :handler (openapi/create-openapi-handler)}}]

    ["/.well-known"
     ["/csvm" {:no-doc true
               :get {:handler (constantly
                               {:status 200
                                :headers {"content-type" "text/plain"}
                                :body "{+url}-metadata.json\nmetadata.json"})}}]]

    ["/data" {:muuntaja leave-keys-alone-muuntaja-coercer
              ;; Routes below /data should have json keys left alone, not
              ;; coerced.
              ;; Routes above, E.G., swagger routes should have their keys
              ;; coerced, especially on the way out as the response. Otherwise
              ;; keywords will remain ":keyword" and swagger.json will be
              ;; invalid
              }
     [""
      {:get (routes.s/get-series-list-route-config triplestore system-uris)}]

     ["/:series-slug"
      {:get (routes.s/get-series-route-config triplestore system-uris)
       :put (routes.s/put-series-route-config clock triplestore system-uris)
       :delete (routes.s/delete-series-route-config triplestore change-store system-uris)}]

     ["/:series-slug/releases"
      {:get (routes.rel/get-release-list-route-config triplestore system-uris)}]

     ["/:series-slug/release"
      ["/{release-slug}.{extension}"
       {:get (routes.rel/get-release-route-config triplestore change-store system-uris)}]

      ["/{release-slug}"
       {:put (routes.rel/put-release-route-config clock triplestore system-uris)
        :post (routes.rel/post-release-delta-config {:triplestore triplestore
                                                     :change-store change-store
                                                     :clock clock
                                                     :system-uris system-uris})
        :get (routes.rel/get-accept-release-route-config)}]

      ["/{release-slug}/schema"
       {:get (routes.rel/get-release-ld-schema-config triplestore system-uris)
        :post (routes.rel/post-release-ld-schema-config clock triplestore system-uris)}]

      ["/{release-slug}/revisions"
       {:post (routes.rev/post-revision-route-config triplestore system-uris)
        :get (routes.rev/get-revision-list-route-config triplestore system-uris)}]

      ["/:release-slug/revision"
       ["/:revision-id"
        {:name ::revision
         :get (routes.rev/get-revision-route-config triplestore change-store system-uris)
         :post (routes.rev/post-revision-delta-config {:triplestore triplestore
                                                       :change-store change-store
                                                       :clock clock
                                                       :system-uris system-uris})}]

       ["/:revision-id/commit/:commit-id"
        {:name ::commit
         :get (routes.rev/get-revision-commit-route-config triplestore change-store system-uris)}]

       ["/:revision-id/appends"
        {:post (routes.rev/post-revision-appends-route-config triplestore change-store system-uris)}]

       ["/:revision-id/retractions"
        {:post (routes.rev/post-revision-retractions-route-config triplestore change-store system-uris)}]

       ["/:revision-id/corrections"
        {:post (routes.rev/post-revision-corrections-route-config triplestore change-store system-uris)}]]]]

    ["/doc" {:muuntaja leave-keys-alone-muuntaja-coercer}
     ["/:series-slug/release"

      ;; /doc/dataset-series/release/:release-id.csv-metadata.json
      ["/{release-slug}.csv-metadata.{extension}"
       {:get (routes.rel/get-release-csvw-metadata-config triplestore system-uris)}]

      ["/{release-slug}.{extension}"
       {:get (routes.rel/get-release-route-config triplestore change-store system-uris)}]]]]

   {;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
    ;;:validate spec/validate ;; enable spec validation for route data
    ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
    :router r/linear-router
    :exception pretty/exception
    :data {:coercion (rcm/create
                      {;; set of keys to include in error messages
                       :error-keys #{#_:type :coercion :in :schema :value :errors :humanized #_:transformed}
                       ;; schema identity function (default: close all map schemas)
                       :compile mu/closed-schema
                       ;; strip-extra-keys (effects only predefined transformers)
                       :strip-extra-keys true
                       ;; add/set default values
                       :default-values true
                       ;; malli options
                       :options nil})

           :multipart-opts {:formats {"application/json" (comp json/read io/reader)}}
           :muuntaja default-muuntaja-coercer
           :middleware [cors-middleware
                        ;; swagger & openapi
                        swagger/swagger-feature
                        openapi/openapi-feature
                        ;; query-params & form-params
                        parameters/parameters-middleware
                        ;; content-negotiation
                        muuntaja/format-negotiate-middleware

                        ;; encoding response body
                        muuntaja/format-response-middleware
                        ;; exception handling
                        ldapi-errors/exception-middleware
                        ;; decoding request body
                        muuntaja/format-request-middleware
                        ;; coercing response bodies
                        coercion/coerce-response-middleware
                        ;; coercing request parameters
                        ;; coercion/coerce-request-middleware
                        middleware/configurable-coerce-request-middleware
                        ;; multipart
                        middleware/multipart-middleware

                        (if (and auth (not (Boolean/valueOf (System/getenv "CI"))))
                          (basic-auth-middleware auth)
                          identity)

                        browser-render-convenience-middleware]}}))

(defn handler [opts]
  {:pre [(:clock opts) (:triplestore opts) (:change-store opts)]}
  (ring/ring-handler
   (router opts)
   (ring/routes
    (swagger-ui/create-swagger-ui-handler
     {:path "/"
      :config {:validatorUrl nil
               :urls [{:name "swagger", :url "swagger.json"}
                      {:name "openapi", :url "openapi.json"}]
               :urls.primaryName "openapi"
               :operationsSorter "alpha"}})
    (ring/create-default-handler))
   (cond-> {:executor sieppari/executor}

           ;; This middleware is inserted here because it needs to run _before_ route matching
           (not (str/blank? (:base-path opts)))
           (assoc :middleware [(wrap-context-middleware (:base-path opts))
                               wrap-request-base-uri]))))

(defmethod ig/pre-init-spec :tpximpact.datahost.ldapi.router/handler [_]
  (s/keys :req-un [::clock ::triplestore ::change-store ::system-uris ::base-path]
          :opt-un [::auth]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.router/handler [_ opts]
  (handler opts))
