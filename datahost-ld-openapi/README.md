# Prototype Datahost Publication Service

_Formerly referred to as the Prototype Datahost LD API Service_.

# Rationale

The goals of Datahost Publication Service are to assist the publication of official statistics (and eventually other data) through a standards based data model that supports the following high level vision:

1. Support the publication of statistical datasets with a [versioning model](/doc/data-model.md) that communicates changing schemas, and accurately describes how the data has been changed through synchronisable transaction logs.
2. Dataset commitments.  The platform should guarantee consumers
3. A headless / API & Data first model, separated from front end UI concerns.
4. Less effort linked data foundations.  Data should still be unambiguously published with URIs as unique identifiers, however we focus efforts on the low hanging fruit of tidy-data CSV(W), with annotations only required to support user facing features.


are to support the following requirements:

- Provide a dataset catalogue derived from [DCAT](https://www.w3.org/TR/vocab-dcat-3/)
- Support appropriate web standards and linked data principles for describing data (e.g. REST, OpenAPI, DCAT, JSON/LD, CSVW)
- Allow publishers to make and keep promises about datasets' structure
  - When a datasets structure/schema needs to change, make this clear to consumers
- Provide a detailed version history of data over time, where data can be added, corrected or retracted, and where details of what's changed are available as  deltas
- Help support data publishers in using this model correctly
- Reduce barriers to publication over previous approaches.  Support producers with lightweight ways of describing their data with RDF (e.g. JSON/LD)
- Reduce barriers to consumption: a REST based API and JSON/LD with CSV(W) downloads mean that users don't need to be RDF experts to programmatically access the data. CSV downloads will be quick to download at a single, easy to find URL.
- Establish a scalable architecture, where we optimise for the simple and most common cases (e.g. for consumers: finding and downloading a dataset, or for publishers: publishing datasets and making them available at a known time), while providing a foundation to add more complex features later.

## Background

Definitions of the terminology used to describe the resources available through this API are available in the [Data Model doc here](https://github.com/Swirrl/datahost-prototypes/blob/main/datahost-ld-openapi/doc/data-model.md).

The resources available are structured as a tree, with dataset series at the top. Each dataset series can have one or more releases, which each have exactly one schema. Each release can have one or more revisions, which each have a series of commits that describe the contents of the revision. A schematic of this resource tree can be seen [in this diagram](https://github.com/Swirrl/datahost-prototypes/blob/main/doc/data-model.md).

# Usage

The API is deployed, documented, and available for use at https://ldapi-prototype.gss-data.org.uk/index.html. Documentation is available inline in the swagger spec there or in the [doc folder for this project](https://github.com/Swirrl/datahost-prototypes/tree/main/datahost-ld-openapi/doc).

## Publisher Endpoints

Briefly, the API supports creating and retrieving all types of resources (dataset series, releases, schemas, revisions, and commits).

Publishers need to provide:

- What series the data belongs to
- What releases exist in a series
- What type of data is in each column for a release
  -e.g. strings, ints, dates, etc. Full list of potential supportable datatypes can be found here
- Which type of datacube component property the column represents.
- Schemas will be validated as well and must include exactly one measure, at least one dimension, and zero or many attributes
- What changed since the previous revision of the dataset?
  - We require that changes be made explicit by uploading only the contents that changed, i.e. we are asking publishers to provide us with the specific rows that are being added, dropped or corrected. We realise this is not how many datasets are currently published and have a proposed solution. See details under Delta tool for data below.
- Optionally, which codelist represent the contents of a given column
  - If we know which codelist a column is using we can validate the data in it for free, and this will also enable consumer-side features like connecting datasets and browsing by codelist

The information will be provided by posting JSON-LD objects and CSV files to a REST API.

## CSV(W) Downloads
Downloads of dataset contents (and changes) can be requested by dereferencing a URI and asking for CSV.

A revision can be requested as either JSON (which will return the revision’s metadata) or as CSV (which will return the contents of the revision snapshot) by requesting different media types in the accept headers. See the [W3C draft on Content Negotiation by Profile](https://www.w3.org/TR/dx-prof-conneg/) for a more in depth rationale and precedent for this approach.

Note that requesting CSV for a release will redirect to the latest revision for that release, and then return it.

## CWVW JSON-LD metadata

We will also provide a JSON-LD metadata sidecar and support the CSVW conventions (paths and link headers etc) so that all downloads are valid CSVW. In the private beta phase, the CSVW metadata supplied will not be sufficient to generate a full cube’s RDF through csv2rdf.  But we leave the possibility open for the future if more information is provided.

Initially the CSVW metadata will include:
- the table schema
- how to generate observation identifiers from the contents of the CSV (see section below on observation identifiers).
- catalogue metadata

# Builds

[Our CircleCI instance](https://app.circleci.com/pipelines/github/Swirrl/datahost-prototypes) runs the tests and deploys docker containers to our public [GCP hosted container registry](https://console.cloud.google.com/artifacts/docker/swirrl-devops-infrastructure-1/europe-west2/public/datahost-ld-openapi):

If the tests pass images are built and tagged with the following tags:

- The name of the branch (branch name tags will be mutated to track passing CI builds from the branch)
- The full commit sha of the passing build
- The names of any commit tags (or a unique abbreviated commit sha if there isn't one)

This means that the latest `main` version can be found at:

`europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-ld-openapi:main`


# Prerequisites

- JDK 17+
- [hurl](https://hurl.dev/) for HTTP tests

# Dev

Start a REPL and from the user namespace first run:

```clojure
(dev)
```

Then use `(start)` and `(reset)` to start and restart the service from the REPL.

`reset` will also reload changed namespaces.

To run all the tests, from this directory run:

```
$ ./bin/kaocha
```

To exclude the hurl integration tests you can run:

```
$ ./bin/kaocha --skip-meta hurl
```


## Basic Auth

To turn on basic authentication in any environment, include the `:auth/basic`
profile, E.G.,

``` shell
$ clojure -X:auth/basic:dev
```

## License

Copyright © 2023 TPXimpact Ltd

Distributed under the Eclipse Public License version 1.0.
