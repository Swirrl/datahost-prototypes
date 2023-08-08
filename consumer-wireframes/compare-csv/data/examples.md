## Annual GDP for England, Wales and the English regions
https://www.ons.gov.uk/datasets/regional-gdp-by-year/editions/time-series/versions
`node index.js data/regional-gdp-by-year-time-series-v4.csv data/regional-gdp-by-year-time-series-v5.csv rgdp-5 'v4_1'`
`node index.js data/regional-gdp-by-year-time-series-v5.csv data/regional-gdp-by-year-time-series-v6.csv rgdp-6 'v4_1'`

## Sexual orientation by age and sex
https://www.ons.gov.uk/datasets/sexual-orientation-by-age-and-sex/editions/time-series/versions
`node index.js data/sexual-orientation-by-age-and-sex-time-series-v1.csv data/sexual-orientation-by-age-and-sex-time-series-v2.csv sex-orientation 'v4_2'`
`node delete.js data/sexual-orientation-by-age-and-sex-time-series-v1.csv data/sexual-orientation-by-age-and-sex-time-series-v2.csv sex-orientation 'v4_2'`

## Sexual orientation by region
https://www.ons.gov.uk/datasets/sexual-orientation-by-age-and-sex/editions/time-series/versions
`node index.js data/sexual-orientation-by-region-time-series-v1.csv data/sexual-orientation-by-region-time-series-v2.csv sex-orientation-region 'v4_3'`
`node delete.js data/sexual-orientation-by-region-time-series-v1.csv data/sexual-orientation-by-region-time-series-v2.csv sex-orientation-region 'v4_3'`

## Wellbeing

`node index.js data/wellbeing-local-authority-time-series-v1.csv data/wellbeing-local-authority-time-series-v2.csv wellbeing-local-authority-2 'V4_3'`
`node delete.js data/wellbeing-local-authority-time-series-v1.csv data/wellbeing-local-authority-time-series-v2.csv wellbeing-local-authority-2 'V4_3'`

`node index.js data/wellbeing-local-authority-time-series-v2.csv data/wellbeing-local-authority-time-series-v3.csv wellbeing-local-authority-3 'V4_3'`
`node delete.js data/wellbeing-local-authority-time-series-v2.csv data/wellbeing-local-authority-time-series-v3.csv wellbeing-local-authority-3 'V4_3'`

## Other old examples
`node index.js data/test-original.csv data/test-new.csv mini 'Aged 16 to 64 years level 3 or above qualifications'`
`node index.js data/original.csv data/new.csv original 'Aged 16 to 64 years level 3 or above qualifications'`
`node index.js data/data-2019.csv data/data-2020.csv year 'Gas Emissions'`


 