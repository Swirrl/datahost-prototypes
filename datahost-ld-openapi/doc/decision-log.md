# Data Host Decision Log

This document is a running log of decisions/conclusions we've come to in meetings as a group. It will change over time but serves as a reference point for all developers at a given point in time as the design of the datahost API evolves.

## July 20, 2023

**Re: Order of appends/deletes/corrections**
- We will not worry about how appends and deletes intersect for now. We'll enforce a consistent order for every revision and if users append and delete a row in the same revision those rows will effectively just be ignored.

**Re: Basic auth**
- The point right now is just to prevent random users and bots on the internet from discovering the write API and adding data. We do not need proper auth at the moment.
- A single user/pass is fine for now. The only user will be Rob and possibly someone at ONS who we will know.
- We're undecided for now whether the app or a proxy is the right place to implement the basic auth. Decision is pending another decision about which proxy we will use ([issue #167 here](https://github.com/Swirrl/datahost-prototypes/issues/167)**

## July 21, 2023

**Re: JSON vs JSON-ld**
- Right now we'll treat the write API as a JSON API, and not require or accept contexts because there is only one correct context anyway and we know what it is. In the future we may want to allow users to supply JSON-LD but we can handle this via content negotiation and not mix JSON and JSON-LD endpoints, because validating arbitrary JSON as RDF opens some questions that we haven’t answered yet, like exactly what schema to validate the JSON-LD against and what prefixes and base URLs to accept.

## July 24, 2023

**Re: Fuseki**

- We'll stick with the RDF4J native store mounted on its own volume as the triplestore for now
- We realized fuseki/JENA likely won't be the correct solution for the full production system anyway, and the RDF4J native store can meet our needs for this phase, so there is no point in adding more complexity to our deployment and development by adding a separate db right now
