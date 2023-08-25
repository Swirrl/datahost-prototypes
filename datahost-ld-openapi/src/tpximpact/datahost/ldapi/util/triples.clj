(ns tpximpact.datahost.ldapi.util.triples
  (:require
   [grafter.matcha.alpha :as matcha]
   [grafter.vocabularies.rdf :as vocab.rdf]))

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
