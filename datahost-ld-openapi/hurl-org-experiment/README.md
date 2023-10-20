# Hurl experiment

This is an experiment / idea in structuring hurl.

The hurl scripts are contained in the `/hurl` subtree.

The dataset specific hurl scripts and files are stored under `/data/<dataset>/`

The file `load-data` is a shell script to load the data.  Here we demonstrate loading just one dataset, so could move it into the datasets folder, or just load them all in one file from the top.

Other tips/ideas `load-data` accepts hurl flags, so you can pass e.g. `--ignore-asserts` or override specific variables e.g. `--variable series=different-series-123` etc...

It would be nice to potentially output two variants of the concattenated script

- One with the variables left in
- One with the variables and supporting files inlined into the hurl file prior to execution, that way the script is a single file which can trivially be run to reproduce issues elsewhere.
