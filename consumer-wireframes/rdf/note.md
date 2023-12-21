# Notes on RDF

## Publishers
```
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX gss: <http://gss-data.org.uk/catalog/>
PREFIX pmd: <http://publishmydata.com/pmdcat#>


CONSTRUCT {?creator rdfs:label ?o .
?creator <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://www.w3.org/ns/org#Organization> .}
WHERE {
  {  SELECT DISTINCT ?creator ?o 
     WHERE {
        ?creator <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://www.w3.org/ns/org#Organization> .
	?creator <http://www.w3.org/2000/01/rdf-schema#label> ?o .
     } LIMIT 500
  }
  ?s ?p ?o .
}
```

## Themes
```
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX gss: <http://gss-data.org.uk/catalog/>
PREFIX pmd: <http://publishmydata.com/pmdcat#>


CONSTRUCT {?theme rdfs:label ?o .
?theme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://www.w3.org/2004/02/skos/core#Concept> .
?theme <http://www.w3.org/2004/02/skos/core#inScheme> <http://gss-data.org.uk/def/concept-scheme/ons-topics> .}
WHERE {
  {  SELECT DISTINCT ?theme ?o 
     WHERE {
        ?theme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2004/02/skos/core#Concept> .
?theme <http://www.w3.org/2004/02/skos/core#inScheme> <http://gss-data.org.uk/def/concept-scheme/ons-topics> .
		?theme <http://www.w3.org/2000/01/rdf-schema#label> ?o .
     } LIMIT 500
  }
  ?s ?p ?o .
}
```
