(ns tpximpact.datahost.ldapi.schemas.release)

(def ApiQueryParams
  [:map
   [:title {:title "Title"
            :description "Title of release"
            :optional true} string?]
   [:description {:title "Description"
                  :description "Description of release"
                  :optional true} string?]])
