(ns tpximpact.datahost.ldapi.routes.middleware
  (:require
   [malli.core :as m]
   [malli.error :as me]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.models.shared
    :as models.shared
    :refer [resource-uri]]))

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
         :body "not acceptable"}))))

(defn resource-exist?
  [triplestore resource handler _id]
  {:pre [(m/validate [:enum :dh/DatasetSeries :dh/Release :dh/Revision] resource)]}
  (fn [{:keys [path-params] :as request}]
    (if (not (db/resource-exists? triplestore (resource-uri resource path-params)))
      {:status 404 :body "not found"}
      (handler request))))

(defn flag-resource-exists
  "Adds a boolean flag to the request under resource-id, when
  the given resource exists.

  - resource: [:enum :dh/DatasetSeries :dh/Release :dh/Revision :dh/Change]"
  [triplestore resource resource-id handler _id]
  {:pre [(m/validate [:enum :dh/DatasetSeries :dh/Release :dh/Revision :dh/Change] resource)]}
  (fn [{:keys [path-params] :as request}]
    (handler (cond-> request
               (db/resource-exists? triplestore (resource-uri resource path-params))
               (assoc resource-id true)))))

(defn validate-creation-body+query-params
  "Tries to distinguish between update and creation of a resource
  and validates the parameters accordingly based on that.

  Explainers will be used to return the error message in the
  respons (see [[malli.core/explainer]]).
  
  Motivation: on creation we usually require diffrent set of
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

