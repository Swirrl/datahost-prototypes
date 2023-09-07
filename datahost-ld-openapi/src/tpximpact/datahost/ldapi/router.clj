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
   [muuntaja.format.core :as fc]
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
   [clojure.java.io :as io])
  (:import
   (java.io InputStream InputStreamReader OutputStream)))

(defn decode-str [_options]
  (reify
    fc/Decode
    (decode [_ data charset]
      (slurp (InputStreamReader. ^InputStream data ^String charset)))))

(defn encode-str [_options]
  (reify
    fc/EncodeToBytes
    (encode-to-bytes [_ data charset]
      (.getBytes data ^String charset))
    fc/EncodeToOutputStream
    (encode-to-output-stream [_ data charset]
      (fn [^OutputStream output-stream]
        (.write output-stream
                (.getBytes data ^String charset))))))

(def csv-format
  (fc/map->Format
    {:name "text/csv"
     :decoder [decode-str]
     :encoder [encode-str]}))

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

(def ^:private api-github-link
  (str "Source viewable in GitHub "
       "[here](https://github.com/Swirrl/datahost-prototypes/tree/main/datahost-ld-openapi)."))

(defn router [{:keys [clock triplestore change-store auth system-uris base-path]}]
  (ring/router
   [["/openapi.json"
     {:get {:no-doc true
            :openapi {:openapi "3.0.0"
                      :info {:title "Prototype OpenData API"
                             :description api-github-link
                             :version "0.1.0"}
                      :components {:securitySchemes {"basic" {:type "http" :scheme "basic"}}}}
            :handler (openapi/create-openapi-handler)}}]
    ["/.well-known"
     ["/csvm" {:no-doc true
               :get {:handler (constantly
                               {:status 200
                                :headers {"content-type" "text/plain"}
                                :body "{+url}-metadata.json\nmetadata.json"})}}]]

    ["/data" {:tags ["linked data api"]}
     [""
      {:get (routes.s/get-series-list-route-config triplestore system-uris)}]

     ["/:series-slug"
      {:get (routes.s/get-series-route-config triplestore system-uris)
       :put (routes.s/put-series-route-config clock triplestore system-uris)}]

     ["/:series-slug/releases"
      ["" {:get (routes.rev/get-release-list-route-config triplestore system-uris)}]

      ["/:release-slug"
       {:get (routes.rel/get-release-route-config triplestore change-store system-uris)
        :put (routes.rel/put-release-route-config clock triplestore system-uris)}]

      ["/:release-slug/schema"
       {:get (routes.rel/get-release-ld-schema-config triplestore system-uris)
        :post (routes.rel/post-release-ld-schema-config clock triplestore system-uris)}]

      ["/:release-slug/revisions"
       ["" {:post (routes.rev/post-revision-route-config triplestore system-uris)
            :get (routes.rev/get-revision-list-route-config triplestore system-uris)}]

       ["/:revision-id"
        {:get (routes.rev/get-revision-route-config triplestore change-store system-uris)}]

       ["/:revision-id/changes"
        ["/:change-id"
         {:get (routes.rev/get-revision-changes-route-config triplestore change-store system-uris)}]]

       ["/:revision-id/appends"
        ["" {:post (routes.rev/post-revision-appends-changes-route-config triplestore change-store system-uris)}]]

       ["/:revision-id/retractions"
        {:post (routes.rev/post-revision-deletes-changes-route-config triplestore change-store system-uris)}]]]]]

   {;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
    ;;:validate spec/validate ;; enable spec validation for route data
    ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
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
           :muuntaja leave-keys-alone-muuntaja-coercer
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

                        (if auth
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
