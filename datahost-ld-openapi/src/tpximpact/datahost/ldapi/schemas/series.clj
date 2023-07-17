(ns tpximpact.datahost.ldapi.schemas.series)

(def ApiQueryParams
  "Query parameters to the series endpoint"
  [:map
   [:title {:title "Title"
            :description "Title of dataset series"
            :optional true} string?]
   [:description {:title "Description"
                  :description "Description of dataset series"
                  :optional true} string?]])


