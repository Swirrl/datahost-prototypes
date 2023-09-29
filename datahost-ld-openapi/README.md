# Datahost LD API Service

# Rationale

The goals of Datahost are to support the following requirements:

- Provide a dataset catalogue
- Provide CSVW downloads of datasets
- Allow publishers to make and keep promises about datasets' structure
- When structure needs to change, make this clear to consumers
- Provide a version history of data over time, where data can be added, corrected or retracted, with details of what's changed
- Help support publishers in using this model correctly
- Reduce barriers to publication over the previous PMD4 approach: publishers don't need to generate and supply RDF
- Reduce barriers to consumption: a REST based API and CSV(W) downloads mean that users don't need to be RDF experts to programmatically access the data. CSV downloads will be quick to download at a single, easy to find URL.
- Establish a scalable architecture, where we optimise for the simple and most common cases (e.g. for consumers: finding and downloading a dataset, or for publishers: publishing datasets and making them available at a known time), while providing a foundation to add more complex features later.






## Builds

[Our CircleCI instance](https://app.circleci.com/pipelines/github/Swirrl/datahost-prototypes) runs the tests and deploys docker containers to our public [GCP hosted container registry](https://console.cloud.google.com/artifacts/docker/swirrl-devops-infrastructure-1/europe-west2/public/datahost-ld-openapi):

If the tests pass images are built and tagged with the following tags:

- The name of the branch (branch name tags will be mutated to track passing CI builds from the branch)
- The full commit sha of the passing build
- The names of any commit tags (or a unique abbreviated commit sha if there isn't one)

This means that the latest `main` version can be found at:

`europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-ld-openapi:main`

## Dev

Start a REPL and from the user namespace first run:

```clojure
(dev)
```

Then use `(start)` and `(reset)` to start and restart the service from the REPL.

`reset` will also reload changed namespaces.

To run tests, from this directory run:

```
$ clojure -X:dev:test
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
