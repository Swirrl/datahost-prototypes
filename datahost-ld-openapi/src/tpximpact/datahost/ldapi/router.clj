(ns tpximpact.datahost.ldapi.router
  (:require
   [buddy.auth.backends.httpbasic :as http-basic]
   [buddy.auth.middleware :as buddy]
   [buddy.auth :refer [authenticated?]]
   [buddy.hashers :as hashers]
   [clojure.string :as str]
   [integrant.core :as ig]
   [reitit.dev.pretty :as pretty]
   [reitit.interceptor.sieppari :as sieppari]
   [reitit.openapi :as openapi]
   [reitit.ring :as ring]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.parameters :as parameters]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.coercion.malli :as rcm]
   [muuntaja.core :as m]
   [muuntaja.format.core :as fc]
   [malli.util :as mu]
   [ring.util.request :as r.u.request]
   [tpximpact.datahost.ldapi.routes.series :as routes.s]
   [tpximpact.datahost.ldapi.routes.release :as routes.rel]
   [tpximpact.datahost.ldapi.routes.revision :as routes.rev]
   [tpximpact.datahost.ldapi.errors :as ldapi-errors]
   [ring.middleware.cors :as cors])
  (:import (java.io InputStream InputStreamReader OutputStream)))

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

(def muuntaja-custom-instance
  (m/create
   (-> (assoc-in
        m/default-options
        [:formats "text/csv"] csv-format)
       (update :formats (fn [formats]
                          (let [json-format (get formats "application/json")]
                            (assoc formats "application/ld+json"
                                   (assoc json-format :name "application/ld+json"))))))))

(def leave-keys-alone-muuntaja-coercer
  (m/create
   (-> m/default-options
       (assoc-in [:formats "application/json" :decoder-opts] {:decode-key-fn identity})
       (assoc-in [:formats "application/json" :encoder-opts] {:encode-key-fn identity}))))

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
         (not (str/includes? accept-header "application/json+ld")))))


(def browser-render-convenience-middleware
  "This is an affordance that attempts to detect an in-browser GET request. If
  detected, the application/json+ld content-type from API response will be overridden
  so that the browser renders the response as plain JSON and does not attempt to download
  the unrecognized application/json+ld as a file."
  (fn [handler]
    (fn [request]
      (if (and (read-request? request)
               (browser-html-request? request))
        (let [response (handler request)]
          (if (= (get-in response [:headers "content-type"]) "application/json+ld")
            ;; replace content-type for raw browser request
            (assoc-in response [:headers "content-type"] "application/json")
            response))
        (handler request)))))

(defn wrap-context-middleware [base-path]
  (when-not (str/blank? base-path)
    (println (str "\nApp `base-path` will be set to: `" base-path "`\n")))
  (fn wrap-context [handler]
    (fn [request]
      (if (str/blank? base-path)
        (handler request)
        (handler
         (r.u.request/set-context request base-path))))))

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

    ["/data" {:muuntaja leave-keys-alone-muuntaja-coercer
              :tags ["linked data api"]}
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
        ["" {:post (routes.rev/post-revision-appends-changes-route-config triplestore change-store system-uris)}]
        ["/:change-id"
         {:get (routes.rev/get-revision-changes-route-config triplestore change-store system-uris)}]]

       ["/:revision-id/deletes"
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
           :muuntaja muuntaja-custom-instance
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
                        ;; decoding request body
                        muuntaja/format-request-middleware
                        ;; coercing response bodies
                        coercion/coerce-response-middleware
                        ;; coercing request parameters
                        coercion/coerce-request-middleware
                        ;; multipart
                        multipart/multipart-middleware
                        ;; exception handling
                        ldapi-errors/exception-middleware

                        (if auth
                          (basic-auth-middleware auth)
                          identity)

                        browser-render-convenience-middleware

                        (wrap-context-middleware base-path)]}}))

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
   {:executor sieppari/executor}))

;(defmethod ig/pre-init-spec :tpximpact.datahost.ldapi.router/handler [_]
;  (s/keys :req-un [::clock ::triplestore ::change-store ::system-uris ::rdf-base-uri]
;          :opt-un [::auth]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.router/handler [_ opts]
  (handler opts))
