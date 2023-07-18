
Prototype for comparing 2 csvs in js.

Currently will identify, given 2 files with the same headings (in the same order) which rows in the 2nd csv are observation corrections or appends (new combinations of dimensions) in the first CSV and output a csv for each.

`node index.js {original file path} {current file path} {output-name-prefix} {observation column name} {test}`

`{test}` is optional and will just store in the test location

Test data

`node index.js test-data/append-correction/test-original.csv test-data/append-correction/test-new.csv append-correction 'Aged 16 to 64 years level 3 or above qualifications' test`

`node index.js test-data/just-append/test-original.csv test-data/just-append/test-new.csv just-append 'Aged 16 to 64 years level 3 or above qualifications' test`

`node index.js test-data/just-correction/test-original.csv test-data/just-correction/test-new.csv just-correction 'Aged 16 to 64 years level 3 or above qualifications' test`

`node index.js test-data/no-change/test-original.csv test-data/no-change/test-new.csv no-change 'Aged 16 to 64 years level 3 or above qualifications' test`

