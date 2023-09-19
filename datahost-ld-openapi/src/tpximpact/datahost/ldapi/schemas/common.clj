(ns tpximpact.datahost.ldapi.schemas.common
  "Shared schema for data handled by datahost."
  (:require
   [malli.core :as m])
  (:import
   [java.net URI URISyntaxException]
   [java.time ZoneId ZonedDateTime]))

(def ^:private custom-registry-keys
  (let [slug-error-msg "should contain alpha numeric characters and hyphens only."]
    {:datahost/slug-string [:and 
                            :string 
                            [:re {:error/message slug-error-msg}
                             #"^[a-z,A-Z,\-,0-9]+$"]]
     :datahost/url-string (m/-simple-schema
                           {:type :url-string
                            :pred (fn url-string-pred [x]
                                    (and (string? x)
                                         (try (URI. x)
                                              true
                                              (catch URISyntaxException _ex
                                                false))))})
     :datahost/timestamp (let [utc-tz (ZoneId/of "UTC")]
                           (m/-simple-schema
                            {:type :datahost/timestamp
                             :pred (fn timestamp-pred [ts]
                                     (and (instance? ZonedDateTime ts)
                                          (= (.getZone ^ZonedDateTime ts) utc-tz)))}))
     
     ;; swagger-ui complains when namespaced keys are used,
     ;; so using non-namespaced
     :title-string (let [regex (str #"^\w[\w\s\D]+$")]
                     [:re {:error/message (format "Should match regex: %s" (str regex))} regex])
     ;; description allows newlines
     :description-string (let [regex #"^\w[\w\s\S\D]+$"]
                           [:re {:error/message (format "Should match regex: %s" (str regex))} regex])
     :datahost/uri (m/-simple-schema
                    {:type :uri
                     :pred #(instance? URI %)})}))

(def registry
  (merge
   (m/class-schemas)
   (m/comparator-schemas)
   (m/base-schemas)
   (m/type-schemas)
   custom-registry-keys))

(def EntityType [:enum :dh/DatasetSeries :dh/Release :dh/Revision :dh/Change])

(def ChangeKind [:enum :dh/ChangeKindAppend :dh/ChangeKindRetract :dh/ChangeKindCorrect])
