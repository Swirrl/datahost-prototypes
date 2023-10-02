# Standards Compliance

Our philosophy is that we'll take the best parts of RDF, Linked Data, CSVW, Linked Data, as well as some of the terms and principles of dcat and datacubes. We'll still aim to be "standards compliant" but in a targetted way, as required to meet the user needs.  Some of the features in our feature set have no established standards, so they will necessarily be bespoke to the implementation.  The standards and principles we aim to adopt are:

- RDF.  Our core data model is described in RDF, but this does not imply that all data will be available as RDF in a centralised SPARQL endpoint, for example  some data (e.g. the statistical tables) will only be available through the API and CSV(W) layer.
- Restful Linked Data API & Resource Orientation.  We will follow practices such that resources (URIs) may have many representations, and that those representations can be accessed via content negotiation.
- OpenAPI v3 The Restful/Linked Data API shall be expressed as an OpenAPI v3 schema, JSON-schema may also be used as a more accessible schema format for users.
- JSON-LD shall be used on the API and as an interchange format to describe the RDF metadata model.
- CSVW - has some problematic limitations for data publication so we will define and use our own table schema format which can trivially be converted into CSVW.
- GraphQL - We have a GraphQL prototype that shows how the data model we are proposing may be a better fit than SPARQL for querying across datasets defined in this application profile, for querying the versioning model, and ultimately for querying inside datasets/cubes
- RDF Datacube.  The RDF data cube informs much of our underlying model of statistics, and Datahost aims to establish a lower barrier of defaults for entry, which will support the growth towards “full cube” compliance (and beyond), whilst minimising the need to undo/retract already published data.  The data model still assumes cube shaped data inputs as tidy CSV, however annotating this data as an RDF datacube is not exposed as part of the model, though our foundation and approach is designed as much as possible to not preclude supporting this if required in the future.

At each stage we will only request from publishers (and serve to consumers) information that is stable and we have a concrete use for. This allows us to remain backward compatible, but leave the door open for additional features.  Not stating and requiring compliance earlier than we need too is a key part of the approach to ensuring that the commitments we make are minimal, and well supported.

In the private beta phase we're focusing on the consumer-side, but still putting in place the basics to support publication (and create valid test data etc). More advanced publisher-side features such as drafting/previewing workflow and user management will come later.

## Linked Data in Datahost

Datahost is inspired heavily influenced by the principles of linked data.

A key feature of the datahost API is that the URLs for routes in the REST API are one and the same as the URIs that are used in the underlying RDF model.

In the consumer API, the GET endpoints the return individual (and collections of) catalogue resources will return JSON-LD that return valid RDF representations of the resources.

The base URI in datahost will be configurable per deployment, such that URIs served by a given datahost instance will resolve on that server.

In all these ways, we're supporting Linked Data dereferencing and simultaneously supporting JSON and RDF APIs (although not a SPARQL endpoint in this phase).

This requires that we make consistent use of the correct base URIs, both in the context of the API (i.e. anything referenced on and by our own server), but also anything external to it, like downloads. The base URI must be set properly in all RDF representations of the data, including any CSVW that we publish and JSON-LD endpoints.
