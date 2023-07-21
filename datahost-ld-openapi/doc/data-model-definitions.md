# Data Model

## Purpose of this document

Some terms have different meanings in different domains (e.g. "release" as used by some statisticians, and as typically used in software development). This document aims to clarify terms used across the codebase and in discussions with stakeholders. It should also aid in defining our data model, the _verbs_ and the _nouns_ of the built software.

## Definitions

[Releases](#release) and [revisions](#revision) etc are all public concepts.

### Series

A series is a collection of related statistical [releases](#release).

### Release

"Release" (in this context) is a term from ONS / stats publishing world. A 'statistical release' represents some data being released to the outside world. But unlike a book or a software release, there's no finality in a statistical release: the data associated with a 'statistical release' can be revised or changed orver time. 

The release itself doesn't contain any data. Users add/update/delete data via [revisions](#revision). 

When users ask for the data in a ‘statistical release’ it’s equivalent to  downloading the dataset’s contents (in old PMD parlance) - full replay of the revision history up to the latest [revision](#revision) for that statistical release.

Each release can have a schema associated with it. That schema is immutable. If the fundamental structure of the data changes so that data can’t be compared any more, statisticians make a new statistical ‘release.’

### Revision

Revisions are like ‘versions’ of the dataset (from a users point of view): snapshots in particular point in time, a close analogy would be a git commit. Each revision specifies updates to previous snapshot by including what data records to append, delete, or correct. 

While revisions specify only the data updates to _previous_ revision, the user should be able to fetch the full dataset snapshot (at this particular revision). This snapshot is a dataset accumulated by replyaing all revisions in order.
