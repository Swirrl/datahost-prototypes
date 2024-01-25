(ns tpximpact.datahost.ldapi.util.triples
  (:require
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.rdf :as vocab.rdf]
   [tpximpact.datahost.ldapi.compact :as compact]))

(defn triples->ld-resource
  "Given triples returned from a DB query, transform them into a single resource
   map (e.g. Release or Revision) that's ready for JSON serialization"
  ([matcha-db]
   (-> (matcha/build-1 [(keyword "@id") ?s]
                       {?p ?o
                        (keyword "@type") ?t}
                       [[?s ?p ?o]
                        (matcha/optional [[?s vocab.rdf/rdf:a ?t]])]
                       matcha-db)
       (dissoc vocab.rdf/rdf:a)))
  ([matcha-db subject]
   (-> (matcha/build-1 [(keyword "@id") ?s]
                       {?p ?o
                        (keyword "@type") ?t}
                       [[?s ?p ?o]
                        (matcha/optional [[?s vocab.rdf/rdf:a ?t]])
                        (matcha/values ?s [subject])]
                       matcha-db)
       (dissoc vocab.rdf/rdf:a))))

(defn triples->ld-resource-collection
  "Given triples returned from a DB query, transform them into a collection of
  resource maps (e.g. seq of Revisions) that are ready for JSON serialization"
  ([matcha-db]
   (->> (matcha/build [(keyword "@id") ?s]
                      {?p ?o
                       (keyword "@type") ?t}
                      [[?s ?p ?o]
                       (matcha/optional [[?s vocab.rdf/rdf:a ?t]])]
                      matcha-db)
        (map #(dissoc % vocab.rdf/rdf:a))))
  ([matcha-db subject]
   (->> (matcha/build [(keyword "@id") ?s]
                      {?p ?o
                       (keyword "@type") ?t}
                      [(matcha/values ?s [subject])
                       [?s ?p ?o]
                       (matcha/optional [[?s vocab.rdf/rdf:a ?t]])]
                      matcha-db)
        (map #(dissoc % vocab.rdf/rdf:a)))))

(defn- dissoc-non-csvw-uris [resource-map]
  (dissoc resource-map
          vocab.rdf/rdf:a
          vocab.rdf/rdf:type
          :grafter.rdf/uri
          (compact/expand :dh/appliesToRelease)
          (compact/expand :appropriate-csvw/modeling-of-dialect)))

(defn triples->csvw-resource
  "Given triples returned from a DB query, transform them into a single resource
   map (e.g. Release or Revision) that's ready for CSVW JSON metadata serialization"
  ([matcha-db]
   (-> (matcha/build-1 ?s
                       {?p ?o}
                       [[?s ?p ?o]]
                       matcha-db)
       (dissoc-non-csvw-uris)))
  ([matcha-db subject]
   (-> (matcha/build-1 ?s
                       {?p ?o}
                       [[?s ?p ?o]
                        (matcha/values ?s [subject])]
                       matcha-db)
       (dissoc-non-csvw-uris))))
