# Data Host Decision Log

This document is a running log of decisions/conclusions we've come to in meetings as a group. It will change over time but serves as a reference point for all developers at a given point in time as the design of the datahost API evolves.

## July 20, 2023

**Re: Order of appends/deletes/corrections**
- We will not worry about how appends and deletes intersect for now. We'll enforce a consistent order for every revision and if users append and delete a row in the same revision those rows will effectively just be ignored.

**Re: Basic auth**
- The point right now is just to prevent random users and bots on the internet from discovering the write API and adding data. We do not need proper auth at the moment.
- A single user/pass is fine for now. The only user will be Rob and possibly someone at ONS who we will know.
- We're undecided for now whether the app or a proxy is the right place to implement the basic auth. Decision is pending another decision about which proxy we will use ([issue #167 here](https://github.com/Swirrl/datahost-prototypes/issues/167))
