(ns tpximpact.datahost.ldapi.routes.middleware.multipart-params
  "Adapted from ring.middleware.multipart-params, but modified for :field?
  multipart types to not throw away the content-type multipart header.

  ...

  Middleware that parses multipart request bodies into parameters.

  This middleware is necessary to handle file uploads from web browsers.

  Ring comes with two different multipart storage engines included:

    ring.middleware.multipart-params.byte-array/byte-array-store
    ring.middleware.multipart-params.temp-file/temp-file-store"
  (:require [ring.middleware.multipart-params.temp-file :as tf]
            [ring.util.codec :refer [assoc-conj]]
            [ring.util.request :as req]
            [ring.util.parsing :as parsing]
            [reitit.coercion :as coercion]
            [muuntaja.core :as m])
  (:import [org.apache.commons.fileupload
            UploadContext
            FileItemStream
            FileUpload
            FileUploadBase$FileUploadIOException
            ProgressListener]
           [org.apache.commons.io IOUtils]))

(defn- progress-listener [request progress-fn]
  (reify ProgressListener
    (update [_ bytes-read content-length item-count]
      (progress-fn request bytes-read content-length item-count))))

(defn- set-progress-listener [^FileUpload upload request progress-fn]
  (when progress-fn
    (.setProgressListener upload (progress-listener request progress-fn))))

(defn- file-upload [request {:keys [progress-fn max-file-size]}]
  (doto (FileUpload.)
    (.setFileSizeMax (or max-file-size -1))
    (set-progress-listener request progress-fn)))

(defn- multipart-form? [request]
  (= (req/content-type request) "multipart/form-data"))

(defn- request-context ^UploadContext [request encoding]
  (reify UploadContext
    (getContentType [_]       (get-in request [:headers "content-type"]))
    (getContentLength [_]     (or (req/content-length request) -1))
    (contentLength [_]        (or (req/content-length request) -1))
    (getCharacterEncoding [_] encoding)
    (getInputStream [_]       (:body request))))

(defn- file-item-iterable [^FileUpload upload context]
  (reify Iterable
    (iterator [_]
      (let [it (.getItemIterator upload context)]
        (reify java.util.Iterator
          (hasNext [_] (.hasNext it))
          (next [_] (.next it)))))))


(defn- parse-content-type-subtype [content-type]
  (re-find #"^[^;]+" content-type))

(defn- parse-content-type-charset [^FileItemStream item]
  (some->> (.getContentType item) parsing/find-content-type-charset))

(defn- parse-file-item [^FileItemStream item store]
  (let [field? (.isFormField item)
        content-type (.getContentType item)]
    {:field? field?
     :name   (.getFieldName item)
     :value  (if field?
               (cond-> {:bytes    (IOUtils/toByteArray (.openStream item))
                        :encoding (parse-content-type-charset item)}
                 content-type
                 (assoc :content-type content-type))
               (store {:filename     (.getName item)
                       :content-type content-type
                       :stream       (.openStream item)}))}))

(defn- find-param [params name]
  (first (filter #(= name (:name %)) params)))

(defn- parse-html5-charset [params]
  (when-let [charset (some-> params (find-param "_charset_") :value :bytes)]
    (String. ^bytes charset "US-ASCII")))

(defn- decode-field
  [{:keys [bytes encoding] :as field} forced-encoding fallback-encoding]
  (let [encoding (str (or forced-encoding encoding fallback-encoding))]
    (-> field
        (dissoc :bytes)
        (assoc :body (String. ^bytes bytes encoding)
               :encoding encoding))))

(defn- build-param-map [encoding fallback-encoding params]
  (let [enc (or encoding (parse-html5-charset params))]
    (reduce (fn [m {:keys [name value field?]}]
              (assoc-conj m
                          (keyword name)
                          (if field?
                                   (decode-field value enc fallback-encoding)
                                   value)))
            {}
            params)))

(def ^:private default-store (delay (tf/temp-file-store)))

(defn- parse-multipart-params
  [request {:keys [encoding fallback-encoding store max-file-count]
            :as options}]
  (let [store             (or store @default-store)
        fallback-encoding (or encoding
                              fallback-encoding
                              (req/character-encoding request)
                              "UTF-8")]
    (->> (request-context request fallback-encoding)
         (file-item-iterable (file-upload request options))
         (sequence
          (map-indexed (fn [i item]
                         (if (and max-file-count (>= i max-file-count))
                           (throw (ex-info "Max file count exceeded"
                                           {:max-file-count max-file-count}))
                           (parse-file-item item store)))))
         (build-param-map encoding fallback-encoding))))

(defn multipart-params-request
  "Adds :multipart-params and :params keys to request.
  See: wrap-multipart-params."
  {:added "1.2"}
  ([request]
   (multipart-params-request request {}))
  ([request options]
   (let [params (if (multipart-form? request)
                  (parse-multipart-params request options)
                  {})]
     (merge-with merge request
                 {:multipart-params params}
                 {:params params}))))

(defn- decode-multipart-params [request muuntaja]
  (letfn [(decode-part [[k {:keys [content-type body tempfile encoding] :as p}]]
            [k (if-let  [decode (some->> content-type
                                         (parse-content-type-subtype)
                                         (m/decoder muuntaja))]
                 (decode (or body tempfile) encoding)
                 p)])]
    (update request :multipart-params #(->> % (map decode-part) (into {})))))

(defn- coerced-request [request coercers]
  (if-let [coerced (if coercers (coercion/coerce-request coercers request))]
    (update request :parameters merge coerced)
    request))

(defn- compile-multipart-middleware [options]
  (fn [{:keys [parameters coercion muuntaja]} opts]
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
                        (multipart-params-request options)
                        (decode-multipart-params muuntaja)
                        (coerced-request coercers)
                        (handler)))
                   ([request respond raise]
                    (-> request
                        (multipart-params-request options)
                        (decode-multipart-params muuntaja)
                        (coerced-request coercers)
                        (handler respond raise)))))}))))

(def multipart-middleware
  "Modified reitit.ring.middleware.multipart/multipart-middleware that parses
  multipart \"parts\" according to their content-type/configuration."
  {:name ::multipart
   :compile (compile-multipart-middleware nil)})
