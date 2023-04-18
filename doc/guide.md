# GraphQL Prototype Guide

This is a guide and introduction to querying the
[`beta.gss-data.org.uk`](https://beta.gss-data.org.uk/) site with a
[new prototype GraphQL service](http://graphql-prototype.gss-data.org.uk/ide).

## Rationale

This proof of concept prototype exists primarily to prove the
following concepts:

1. That GraphQL can serve as an easier and more familiar way for
   developers to access some of the data we hold. For targetted use
   cases.
2. That GraphQL schema can serve as a better interface to the data
   than SPARQL, which is more aligned and optimised towards user and
   platform needs. SPARQL is an incredibly flexible query language,
   but that flexibility brings a performance cost, as SPARQL has to
   target all possible use cases, where as a given GraphQL schema can
   target and optimise performance for specific needs. We can hide
   optimised services behind GraphQL schema/resolvers.
3. That we can seamlessly bridge the divide between Linked Data and
   GraphQL.

This prototype does not replace SPARQL, internally it dynamically
generates SPARQL queries to perform catalog and ultimately provide an
API for faceted search.

## Motivating Use Case: Catalog Search

We developed a prototype schema which we believe with some further
refinement could support the proposed designs for our faceted catalog
search.

We also believe the schema can be evolved in the future to support
other needs such as querying and filtering datasets. For the time
being these use cases are left out.

## Using GraphiQL to build queries

We provide a GraphiQL environment to assist in the creation of GraphQL
queries, conforming to our schema at the following address:

[`http://graphql-prototype.gss-data.org.uk/ide`](http://graphql-prototype.gss-data.org.uk/ide)

This interface is wired up to query the public `beta.gss-data.org.uk`
service.

The GraphiQL interface provides automated tab-completion based upon
our GraphQL schema, which means that most queries can be written by
auto-complete, and selecting the appropriate field or argument from
the drop down.

This is one of the main reasons GraphQL is easier for users to write
than SPARQL.

### Querying the catalog

The simplest 'useful' query we currently support is:

```graphql
{
  endpoint {
    catalog {
      id
      label
    }
  }
}
```
[Load Query into GraphiQL](http://graphql-prototype.gss-data.org.uk/ide?query=%7B%0A%20%20endpoint%20%7B%0A%20%20%20%20catalog%20%7B%0A%20%20%20%20%20%20id%0A%20%20%20%20%20%20label%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)

This returns some basic metadata about the default catalog, if you're
not familiar with GraphQL you will notice that the shape of the query
directly mirrors the shape of the results:

```json
{
  "data": {
    "endpoint": {
      "catalog": {
        "id": "http://gss-data.org.uk/catalog/datasets",
        "label": "Datasets"
      }
    }
  }
}
```

What is `endpoint` and why is it in the schema? `endpoint` represents
the endpoint we're currently connected to, this exists to allow
appropriately authenticated users to query things out of specific
draftsets, e.g.

We plan to eventually support queries like this, which will ask the
same question of the appropriate draftset, at which point you will
also be able to access appropriate metadata from the draft.

Though be aware that we may wish to make some breaking schema changes
around this
([#18](https://github.com/Swirrl/catql-prototype/issues/18)).

```graphql
{
  endpoint(draftset_id: "3703f62a-7c33-4d89-be60-d47aedbb9d1f") {
    catalog {
      id
      label
    }
  }
}
```

### Listing all datasets

The following query will list all datasets in the catalog and return
their `id`/`uri` and `title`:

```graphql
{
  endpoint {
    catalog {
      catalog_query {
        datasets {
          id
          title
          # <-- Put cursor to the left of this and press `CTRL-<space>` to auto complete additional metadata fields
        }
      }
    }
  }
}
```
[Load Query into GraphIQL](http://graphql-prototype.gss-data.org.uk/ide?query=%7B%0A%20%20endpoint%20%7B%0A%20%20%20%20catalog%20%7B%0A%20%20%20%20%20%20catalog_query%20%7B%0A%20%20%20%20%20%20%20%20datasets%20%7B%0A%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20title%0A%20%20%20%20%20%20%20%20%20%20%23%20%3C--%20Put%20cursor%20to%20the%20left%20of%20this%20and%20press%20%60CTRL-%3Cspace%3E%60%20to%20auto%20complete%20additional%20metadata%20fields%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)

Be sure to note that if in GraphiQL you put the cursor at the
appropriate point, you can complete additional metadata fields such as
`publisher`, `creator`, `theme` and `modified`.

It's worth mentioning we have only added a subset of the possible ones
to the prototypes graphql schema and can introduce more as needed.

We also [have a
proposal](https://github.com/Swirrl/catql-prototype/issues/16) for
providing access to arbitrary RDF predicates without modifying the
GraphQL schema should it be required for us to support arbitrary
extensibility.

### Finding datasets by keyword search

This is easily done by providing a `search_string` argument to the
`catalog_query` field:

```graphql
{
  endpoint {
    catalog {
      catalog_query(search_string: "climate change") {
        datasets {
          id
          title
        }
      }
    }
  }
}
```
[Load Query into GraphIQL](http://graphql-prototype.gss-data.org.uk/ide?query=%7B%0A%20%20endpoint%20%7B%0A%20%20%20%20catalog%20%7B%0A%20%20%20%20%20%20catalog_query(search_string%3A%20%22climate%20change%22)%20%7B%0A%20%20%20%20%20%20%20%20datasets%20%7B%0A%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20title%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)

The prototype currently implements this with a basic snowball token
stemmer over the `title` and `description` fields, there is currently
no attempt to order results by relevance through a vector space
algorithm such as
[tf/idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf). Switching to a
[lucene](https://lucene.apache.org/) based index would be an obvious
way to provide this.

### Faceted Search Queries

_NOTE: We plan to offer a faceted search query interface via the GraphQL API.
We have a basic schema already, which should support this, though the
implementation is not yet finished/correct_.

We have designed the schema for this feature, and describe it here. It
is based upon Gareth's initial wireframes for what this might look
like in the UI:

```graphql
{
  endpoint {
    catalog {
      catalog_query(search_string:"climate change") {
        datasets {
          id
          title
          publisher
          creator
          theme
        }
        facets {
          themes {
            id
            label
            enabled
          }
          publishers {
            id
            label
            enabled
          }
          creators {
            id
            label
            enabled
          }
        }
      }
    }
  }
}
```
[Load Query into GraphIQL](http://graphql-prototype.gss-data.org.uk/ide?query=%7B%0A%20%20endpoint%20%7B%0A%20%20%20%20catalog%20%7B%0A%20%20%20%20%20%20catalog_query(search_string%3A%22climate%20change%22)%20%7B%0A%20%20%20%20%20%20%20%20datasets%20%7B%0A%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20title%0A%20%20%20%20%20%20%20%20%20%20publisher%0A%20%20%20%20%20%20%20%20%20%20creator%0A%20%20%20%20%20%20%20%20%20%20theme%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20facets%20%7B%0A%20%20%20%20%20%20%20%20%20%20themes%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20%20%20label%0A%20%20%20%20%20%20%20%20%20%20%20%20enabled%0A%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%20%20publishers%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20%20%20label%0A%20%20%20%20%20%20%20%20%20%20%20%20enabled%0A%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%20%20creators%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20%20%20label%0A%20%20%20%20%20%20%20%20%20%20%20%20enabled%0A%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)

The `facets` fields, `themes`, `publishers`, `creators`, will each
return information on the `enabled` state of all available facets,
given the current query constraints.

Each facet has an `id` (a URI), a `label` and an `enabled` state. A
facet will have `enabled` set to `true` if selecting that facet would
return results. The purpose of this is to assist users in making
selections that only result in data. For example given a query on
`climate change` you would likely not want to let users see or select
the `balance of payments` as selecting it in combination with a
`climate change` search string would, (for the purposes of this
example at least) be a dead end selection with no results.

A query for "climate change" within the energy theme facet would then look like this:

```graphql
{
  endpoint {
    catalog {
      catalog_query(search_string:"climate change"
      						  themes:["http://gss-data.org.uk/def/gdp#energy"]) {
        datasets {
          id
          title
          publisher
          creator
          theme
        }
        facets {
          themes {
            id
            label
            enabled
          }
          publishers {
            id
            label
            enabled
          }
          creators {
            id
            label
            enabled
          }
        }
      }
    }
  }
}
```


### Querying programmatically

Below is an example query posted to the `/api` endpoint.

GraphQL queries must be wrapped into a JSON object conforming to the
[GraphQL over HTTP](https://graphql.github.io/graphql-over-http/draft/)
specification.

Essentially they must be wrapped into a JSON object with at a minimum
a `query` key:

```
curl 'http://graphql-prototype.gss-data.org.uk/api' \
  -X POST \
  -H 'content-type: application/json' \
  --data '{"query": "{
  endpoint {
    catalog(id:\"http://gss-data.org.uk/catalog/datasets\") {
      catalog_query(search_string: \"climate change\") {
        datasets {
          id
          title
        }
      }
    }
  }
}"}'
```

### Parameterised Queries

A common use case is to have a static shape of query, which needs
parameterised by one or more query variables. This can be done by
supplying additional query variables as JSON.

Firstly we need to name the query and provide the parameters it takes.
We use define the `$query_string` variable with the type `String`, and
we bind this to the appropriate part of our schema:

```graphql
query textQuery($query_string: String) {
  endpoint {
    catalog(id: "http://gss-data.org.uk/catalog/datasets") {
      id
      catalog_query(search_string: $query_string) {
        datasets {
          id
          label
          publisher
        }
      }
    }
  }
}
```

We can then supply this parameter in an accompanying JSON map

```json
{"query_string": "rain"}
```

In the GraphQL over HTTP specification, this map is then supplied
under the top level `variables` key:

```
curl 'http://graphql-prototype.gss-data.org.uk/api' \
  -X POST \
  -H 'content-type: application/json' \
  --data '{"query": "{
  endpoint {
    catalog(id:\"http://gss-data.org.uk/catalog/datasets\") {
      catalog_query(search_string: \"climate change\") {
        datasets {
          id
          title
        }
      }
    }
  }
}"}'
```

For more information on [parameterised queries see the GraphQL
tutorial](https://graphql.org/learn/queries/#variables).


### Unsupported Features

The following features are currently unsupported, but could be
supported in future iterations of the prototype or a production
implementation of it:

- Faceted search result sparseness detection
- Sorting results
  - by relevance
  - by modified time
  - by title
- Paginated results
- [Global Object Identification](https://graphql.org/learn/global-object-identification/)
- [Querying arbitrary RDF metadata](https://github.com/Swirrl/catql-prototype/issues/16)
- Dynamically building a JSON-LD context for GraphQL results
