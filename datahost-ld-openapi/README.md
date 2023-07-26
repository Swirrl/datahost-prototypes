# Datahost LD API Service



## Builds

[Our CircleCI instance](https://app.circleci.com/pipelines/github/Swirrl/datahost-prototypes) runs the tests and deploys docker containers to our public [GCP hosted container registry](https://console.cloud.google.com/artifacts/docker/swirrl-devops-infrastructure-1/europe-west2/public/datahost-ld-openapi):

If the tests pass images are built and tagged with the following tags:

- The name of the branch (branch name tags will be mutated to track passing CI builds from the branch)
- The full commit sha of the passing build
- The names of any commit tags (or a unique abreviated commit sha if there isn't one)

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
$ clojure -X:auth/basic:cider:dev
```

## License

Copyright © 2023 TPXimpact Ltd

Distributed under the Eclipse Public License version 1.0.
