(ns tpximpact.catql.search
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [stemmer.snowball :as stem]))

(def snowball (stem/stemmer :english))

(defn clean-chars [s]
  (str/replace s #"[.-:(),&\-£'`“”]" ""))

(def stopwords (-> (->> "./catql/stopwords-en.txt"
                        io/resource
                        io/reader
                        line-seq
                        (map clean-chars)
                        set)))

(defn tokenise
  "Crude tokeniser to tokenise a string into snowball stems.

  Always returns a set of tokens, or the empty set."
  [s]
  (let [lower-case (fnil str/lower-case "")]
    (-> (->> (-> s
                 lower-case
                 (str/split #"\p{Space}+"))
             (map clean-chars)
             (remove stopwords)
             (map snowball)
             set)
        (disj ""))))

(defn check-result [search-tokens {:keys [title description]}]
  (let [data-tokens (tokenise (str title " " description))]
                     ;; or
    #_(boolean (seq (set/intersection data-tokens search-tokens)))

                     ;; and
    (= search-tokens
       (set/intersection data-tokens search-tokens))))

(defn filter-results [{:keys [:CatalogSearchResult/search-string]} results]
  (let [search-tokens (tokenise search-string)]
    (->> results
         (filter (partial check-result search-tokens)))))
