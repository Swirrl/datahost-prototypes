# Datahost GraphQL

Prototype of a basic GraphQL catalog service.

Please see our [usage guide](../doc/guide.md) for information on the
rationale and how to query through this proof of concept service.

## Builds

[Our CircleCI instance](https://app.circleci.com/pipelines/github/Swirrl/catql-prototype) runs the tests and deploys docker containers to our public [GCP hosted container registry](https://console.cloud.google.com/artifacts/docker/swirrl-devops-infrastructure-1/europe-west2/public/datahost-graphql):

If the tests pass images are built and tagged with the following tags:

- The name of the branch (branch name tags will be mutated to track passing CI builds from the branch)
- The full commit sha of the passing build
- The names of any commit tags (or a unique abreviated commit sha if there isn't one)

This means that the latest `main` version can be found at:

`europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/catql:main`

## Deployment

We currently deploy the CI built containers to [`idp-graphql-prototype`](https://console.cloud.google.com/compute/instancesDetail/zones/europe-west2-c/instances/idp-graphql-prototype?project=ons-pilot) a GCP instance via a [GCP container optimised image](https://cloud.google.com/container-optimized-os/docs).  This means we only need to ship containers and can avoid managing the O/S layer.

The deployed system currently uses the [ONS beta's SPARQL endpoint](https://beta.gss-data.org.uk/sparql).

## Usage

To start the app locally:

```
clojure -M:run
```

Then visit `http://localhost:8888/ide` and enter a [GraphQL query](http://localhost:8888/ide?query=%23%20Welcome%20to%20GraphiQL%0A%23%0A%23%20GraphiQL%20is%20an%20in-browser%20tool%20for%20writing%2C%20validating%2C%20and%0A%23%20testing%20GraphQL%20queries.%0A%23%0A%23%20Type%20queries%20into%20this%20side%20of%20the%20screen%2C%20and%20you%20will%20see%20intelligent%0A%23%20typeaheads%20aware%20of%20the%20current%20GraphQL%20type%20schema%20and%20live%20syntax%20and%0A%23%20validation%20errors%20highlighted%20within%20the%20text.%0A%23%0A%23%20GraphQL%20queries%20typically%20start%20with%20a%20%22%7B%22%20character.%20Lines%20that%20start%0A%23%20with%20a%20%23%20are%20ignored.%0A%23%0A%23%20An%20example%20GraphQL%20query%20might%20look%20like%3A%0A%23%0A%23%20%20%20%20%20%7B%0A%23%20%20%20%20%20%20%20field(arg%3A%20%22value%22)%20%7B%0A%23%20%20%20%20%20%20%20%20%20subField%0A%23%20%20%20%20%20%20%20%7D%0A%23%20%20%20%20%20%7D%0A%23%0A%23%20Keyboard%20shortcuts%3A%0A%23%0A%23%20%20Prettify%20Query%3A%20%20Shift-Ctrl-P%20(or%20press%20the%20prettify%20button%20above)%0A%23%0A%23%20%20%20%20%20Merge%20Query%3A%20%20Shift-Ctrl-M%20(or%20press%20the%20merge%20button%20above)%0A%23%0A%23%20%20%20%20%20%20%20Run%20Query%3A%20%20Ctrl-Enter%20(or%20press%20the%20play%20button%20above)%0A%23%0A%23%20%20%20Auto%20Complete%3A%20%20Ctrl-Space%20(or%20just%20start%20typing)%0A%23%0A%0A%7B%0A%20%20endpoint(draftset_id%3A%22https%3A%2F%2Fbeta.gss-data.org.uk%2Fsparql%22)%20%7B%0A%20%20%20%20catalog(id%3A%22http%3A%2F%2Fgss-data.org.uk%2Fcatalog%2Fdatasets%22)%20%7B%0A%20%20%20%20%20%20id%0A%20%20%20%20%20%20catalog_query(search_string%3A%22trade%20services%20time%22)%20%7B%0A%09%09%09%09%20datasets%20%7B%0A%20%20%20%20%20%20%20%20%20%20id%0A%20%20%20%20%20%20%20%20%20%20label%0A%20%20%20%20%20%20%20%20%20%20publisher%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%0A%0A%20%20%20%20%20%20%20%20%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)

e.g.

```graphql
{
  endpoint(draftset_id: "https://beta.gss-data.org.uk/sparql") {
    catalog(id: "http://gss-data.org.uk/catalog/datasets") {
      id
      catalog_query(search_string: "trade services time") {
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

## Dev

Start a REPL and from the user namespace first run:

```clojure
(dev)
```

Then use `(start!)` and `(reset!)` to start and restart the service against a local copy of static fixture data from the REPL.

If you want to run against the live / beta site you can run `(start-live!)` and `(reset-live!)` respectively.

To run tests run:

```
$ clojure -X:test
```

## License

Copyright © 2023 TPXimpact Ltd

Distributed under the Eclipse Public License version 1.0.
