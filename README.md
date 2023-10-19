# Datahost

## Prototypes

This monorepo contains two prototypes for the datahost platform.

The two prototypes are not yet integrated, but together are intended to illustrate the broader datahost concept.

The main prototype is the [Datahost Core Data Layer](#datahost-core-data-layer), the GraphQL concept is currently lagging behind the [Core Data Layer](#datahost-graphql-service).

### Datahost Core Data Layer

The [Datahost Core Data Layer](/datahost-ld-openapi/README.md) (aka The LD API) is the most advanced, and is intended to demonstrate the core data model and how it applies to statistical data publication and consumption.

The Core Data Layer provides a Restful and Resourceful ingestion and consumption layer for Linked Data in our targeted data model, with an OpenAPI v3 schema.  It is intended to be compatible with Linked Data concepts such as dereferencing, and enforce linked data practices, without requiring publishers or consumers to be aware of those technologies at the outset.  It primarily offers consumers access to a collection of versioned datasets, whilst providing consumers guarantees around schema compatability over time.

The core data layer vision is to provide solutions to the following problems:

- Revision model
 - Keeping promises to consumers
 - lock to a specific version or track head
 - Validations / spotting errors (including code sets / lookup tables for labels).
 - providing just the diffs to people
 - Supports reproducibility for users who download the data (even just via CSV download)

- Pluggable / extendable
 - Draft aware core API, including access to the downloads.
 - Web native / Russian doll caching
 - Can build our own UIs / APIs for catalog or other archetypes
 - Support other applications by supporting a reliable and efficient data syncing mechanism

- Cloud portable
 - Abstracted auth and buckets (so we can switch between provider)
 - Not using queues (dogfood our own draft-aware API)


### Datahost GraphQL Service

The [Datahost GraphQL Service](/datahost-graphql/README.md) is intended to show how GraphQL can be used to map the Datahost Linked Data into a GraphQL API, which can provide access to specialised features such as search, (and in the future cube diving).

The prototype currently uses search as an example, and demonstrates how GraphQL can provide faceted search features.  The bulk of the work in this prototype has been on developing the GraphQL schema (the consumer interface), whilst largely ignoring implementation.  In a future phase we hope to reimplement the backend on top of a specialised ElasticSearch index and synchronise with the Core Data Layer.

This prototype currently works off a SPARQL endpoint, which is not yet integrated with the Datahost Core Data Layer.

### Other Prototypes

- The [QB Update Process Prototype](https://github.com/Swirrl/qb-update-process-prototype/) is an early demonstrator to the ideas behind our update model, including the role of schemas, commits, and computing precise deltas in the presence of a "drop and replace" workflow.

## License

Copyright © 2023 TPXimpact Data Ltd

Distributed under the Eclipse Public License version 1.0.
