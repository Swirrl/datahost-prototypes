(ns tpximpact.datahost.ldapi.routes.middleware
  (:require
   [clojure.data.json :as json]
   [malli.core :as m]
   [malli.error :as me]
   [reitit.coercion :as coercion]
   [reitit.ring.middleware.multipart :as multipart]
   [ring.middleware.multipart-params :as multipart-params]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.system-uris :refer [resource-uri] :as su]
   [tpximpact.datahost.ldapi.routes.shared :as shared]
   [tpximpact.datahost.ldapi.schemas.common :as s.common]))

(def not-found-response
  {:status 404
   :body "Not found"})

(defn json-only
  "Middleware that requires the request to pass 'Content-Type: application/json'.
  Returns 406 when content type does not match."
  [handler _id]
  (fn json-only-middleware* [request]
    (let [content-type (get-in request [:muuntaja/request :raw-format] "")]
      (if (or (= "application/json" content-type)
              (= "application/ld+json" content-type))
        (handler request)
        {:status 406
         :body "Not acceptable. Only Content-Type: application/json is accepted"}))))

(defn entity-uris-from-path
  [system-uris entities handler _id]
  {:pre [(m/validate [:set s.common/EntityType] entities)]}
  (fn entity-uris [request]
    (let [uris (reduce (fn [r entity]
                         (assoc! r entity (su/resource-uri entity system-uris (:path-params request))))
                       (transient {})
                       entities)]
     (handler (assoc request :datahost.request/uris (persistent! uris))))))

(defn entity-or-not-found
  "If found, puts the entity under [:datahost.request/entities entity-kw],
  short-circuits with 404 response otherwise."
  [triplestore system-uris entity-kw handler _id]
  {:pre [(m/validate s.common/EntityType entity-kw)]}
  (fn inner [{:keys [path-params]:as request}]
    (let [{:keys [series-slug release-slug revision-id change-id]} path-params
          uri (resource-uri entity-kw system-uris path-params)
          entity (case entity-kw
                   :dh/DatasetSeries (db/get-dataset-series triplestore uri)
                   :dh/Release (db/get-release-by-uri triplestore uri)
                   :dh/Revision (db/get-revision triplestore uri)
                   :dh/Change (db/get-change triplestore uri))]
      (if entity
        (handler (assoc-in request [:datahost.request/entities entity-kw] entity))
        not-found-response))))

(defn resource-exist?
  "Checks whether resource exists and short-circuits with 404 response if
  not.

  Relies on [[resource-uri]] to create URI of the resource."
  [triplestore system-uris resource handler _id]
  {:pre [(m/validate [:enum :dh/DatasetSeries :dh/Release :dh/Revision] resource)]}
  (fn [{:keys [path-params] :as request}]
    (if (not (db/resource-exists? triplestore (resource-uri resource system-uris path-params)))
      {:status 404 :body "not found"}
      (handler request))))

(defn resource-already-created?
  "Checks whether resource already exists and short-circuits with 422
  response if it does.

  Options:

  - :resource - :dh/DataSeries etc
  - :missing-params - map of params possibly missing from path
  params (e.g. when the request generates an ID)"
  [triplestore system-uris {:keys [resource missing-params]} handler _id]
  {:pre [(m/validate s.common/EntityType resource)]}
  (fn created? [{:keys [path-params] :as request}]
    (if (db/resource-exists? triplestore (resource-uri resource system-uris (merge path-params missing-params)))
      {:status 422 :body "Resource already exists"}
      (handler request))))

(defn flag-resource-exists
  "Adds a boolean flag to the request under resource-id, when
  the given resource exists.

  - resource: [:enum :dh/DatasetSeries :dh/Release :dh/Revision :dh/Change]"
  [triplestore system-uris resource resource-id handler _id]
  {:pre [(m/validate s.common/EntityType resource)]}
  (fn [{:keys [path-params] :as request}]
    (handler (cond-> request
               (db/resource-exists? triplestore (resource-uri resource system-uris path-params))
               (assoc resource-id true)))))

(defn validate-creation-body+query-params
  "Tries to distinguish between update and creation of a resource
  and validates the parameters accordingly based on that.

  Explainers will be used to return the error message in the
  response (see [[malli.core/explainer]]).
  
  Motivation: on creation we usually require different set of
  parameters to be in the request, while updates can supply only a
  subset (e.g. only the title).

  See [[flag-resource-exists]] middleware."
  [{:keys [resource-id body-explainer query-explainer]} handler _id]
  (fn validate* [{:keys [query-params]
                  {:keys [body]} :parameters
                  req-body :body
                  :as request}]
    (let [body (or body (when (map? req-body) req-body))
          exists? (get request resource-id)]
      (cond
        exists? ;; it's an update
        (handler request)

        (some? body)
        (let [body-errors (me/humanize (body-explainer body))]
          (if body-errors
            {:status 400
             :body {:body  body-errors}}
            (handler request)))

        :else ;; create with query params only
        (let [query-errors (me/humanize (query-explainer query-params))]
          (if (some? query-errors)
            {:status 400
             :body {:query-params query-errors}}
            (handler request)))))))

(defn- match-content-type [content-type formats]
  (or (get formats content-type)
      (let [[_ ns type] (re-find #"([^/]+)/(?:[^\+]+\+)?([^\+].*)" content-type)]
        (get formats (str ns \/ type)))))

(defn- parse-multipart-params [request {:keys [formats] :as options}]
  (letfn [(parse-part [[k {:keys [content-type tempfile] :as p}]]
            [(keyword k)
             (if-let  [parse-fn (match-content-type content-type formats)]
               (parse-fn tempfile)
               p)])]
    (update request :multipart-params #(->> % (map parse-part) (into {})))))

(defn- coerced-request [request coercers]
  (if-let [coerced (if coercers (coercion/coerce-request coercers request))]
    (update request :parameters merge coerced)
    request))

(defn- compile-multipart-middleware [options]
  (fn [{:keys [parameters coercion multipart-opts]} opts]
    (if-let [multipart (:multipart parameters)]
      (let [parameter-coercion {:multipart (coercion/->ParameterCoercion
                                            :multipart-params :string false true)}
            opts (assoc opts ::coercion/parameter-coercion parameter-coercion)
            coercers (if multipart (coercion/request-coercers coercion parameters opts))]
        {:data {:swagger {:consumes ^:replace #{"multipart/form-data"}}}
         :wrap (fn [handler]
                 (fn
                   ([request]
                    (-> request
                        (multipart-params/multipart-params-request options)
                        (parse-multipart-params multipart-opts)
                        (coerced-request coercers)
                        (handler)))
                   ([request respond raise]
                    (-> request
                        (multipart-params/multipart-params-request options)
                        (parse-multipart-params multipart-opts)
                        (coerced-request coercers)
                        (handler respond raise)))))}))))

(def multipart-middleware
  "Modified reitit.ring.middleware.multipart/multipart-middleware that parses
  multipart \"parts\" according to their content-type/configuration."
  {:name ::multipart
   :compile (compile-multipart-middleware nil)})

(defn csvm-request-response
  [triplestore system-uris handler _id]
  (fn [request]
    (if (shared/csvm-request? request)
      (shared/csvm-request {:triplestore triplestore
                            :system-uris system-uris
                            :request request})
      (handler request))))
