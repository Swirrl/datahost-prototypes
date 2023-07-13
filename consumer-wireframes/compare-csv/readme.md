
Prototype for comparing 2 csvs in js.

Currently will identify, given 2 files with the same headings (in the same order) which rows in the 2nd csv do not exactly match a row in the first csv.

`node index.js {original file path} {current file path}`

e.g.

`node index.js data/test-original.csv data/test-new.csv`
`node index.js data/original.csv data/new.csv`
`node index.js data/data-2019.csv data/data-2020.csv`


