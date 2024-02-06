# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl upload-permanent-dwellings.hurl --variable admin_password=loadmewithdata3250 --variable series=Permanent-dwellings-completed --variable datahost-base-url=https://dluhc-pmd5-prototype.publishmydata.com
# run locally with docker compose:
# hurl upload-permanent-dwellings.hurl --variable admin_password=loadmewithdata3250 --variable series=Permanent-dwellings-completed --variable datahost-base-url=http://localhost:8889

DELETE {{datahost-base-url}}/data/{{series}}
[BasicAuth]
idp: {{admin_password}}

HTTP 204


PUT {{datahost-base-url}}/data/{{series}}
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Permanent dwellings completed, England, District By Tenure",
	"dcterms:description": "House building data are collected at local authority district level, but it is important to treat figures at this level with care. House building is unevenly distributed both geographically and over time and patterns of housing development can produce clusters of new homes which make the figures at a low geographic level volatile and difficult to interpret. For detailed definitions of all tenures, see definitions of housing terms on Housing Statistics The district level and county figures are as reported by local authorities and the NHBC. Where a local authority has not submitted a quarterly return to DCLG, no figure has been presented for this local authority (and when relevant its county) for any 12-month period that includes the missing quarter. England total figures include estimates for missing data returns from independent Approved Inspectors and Local Authorities, so the sum of district values may be slightly less than the England totals. *House building completion* – In principle, a dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued whether it is in fact occupied or not. In practice, the reporting of some completions may be delayed and some completions may be missed if no completion certificate was requested by the developer or owner, although this is unusual. *Tenure* – For the purposes of these statistics, the term tenure refers to the nature of the organisation responsible for the development of a new housing start or completion. It does not necessarily describe the terms of occupancy for the dwelling on completion.",
	"dcterms:publisher": "http://reference.data.gov.uk/id/department/dclg",
	"dh:nextUpdate": "September 2018",
    "dcat:theme": "http://opendatacommunities.org/def/concept/folders/themes/house-building",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/",
    "rdfs:comment": "This data set contains annual data, the most recent quarterly data, and rolling four quarter totals (England only) on house building completions. From 2011/12 imputed data is included. Imputed data should not be seen as an estimate for the individual authority but is given on an authority basis to allow custom totals to be constructed. A dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued."
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{datahost-base-url}}/data/{{series}}/release/Permanent-Dwellings
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2009 - Present",
	"dcterms:description": "House building data are collected at local authority district level, but it is important to treat figures at this level with care. House building is unevenly distributed both geographically and over time and patterns of housing development can produce clusters of new homes which make the figures at a low geographic level volatile and difficult to interpret. For detailed definitions of all tenures, see definitions of housing terms on Housing Statistics The district level and county figures are as reported by local authorities and the NHBC. Where a local authority has not submitted a quarterly return to DCLG, no figure has been presented for this local authority (and when relevant its county) for any 12-month period that includes the missing quarter. England total figures include estimates for missing data returns from independent Approved Inspectors and Local Authorities, so the sum of district values may be slightly less than the England totals. *House building completion* – In principle, a dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued whether it is in fact occupied or not. In practice, the reporting of some completions may be delayed and some completions may be missed if no completion certificate was requested by the developer or owner, although this is unusual. *Tenure* – For the purposes of these statistics, the term tenure refers to the nature of the organisation responsible for the development of a new housing start or completion. It does not necessarily describe the terms of occupancy for the dwelling on completion.",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}
HTTP 201

POST {{datahost-base-url}}/data/{{series}}/release/Permanent-Dwellings/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Permanent-dwellings-completed-England-District-By-Tenure-schema.json;

HTTP 201

POST {{datahost-base-url}}/data/{{series}}/release/Permanent-Dwellings/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Data for 2009-2021",
	"dcterms:description": "Initial append containing historic data from 2009 till 2021",
	"dh:publicationDate": "September 26, 2019 09:34",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST {{datahost-base-url}}{{revision1_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Permanent-dwellings-completed-England-District-By-Tenure-filtered.csv;

HTTP 201

POST {{datahost-base-url}}/data/{{series}}/release/Permanent-Dwellings/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Data for 2022",
	"dcterms:description": "appending 2022 data to historic data",
	"dh:publicationDate": "September 26, 2019 09:34",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
revision2_url: header "Location"

POST {{datahost-base-url}}{{revision2_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: 2022 data
description: Appending data for 2022 for {{series}}
format: text/csv
file,Permanent-dwellings-completed-England-District-By-Tenure-filtered-2022.csv;

HTTP 201
# [Asserts]
# body == ""

GET {{datahost-base-url}}/data/{{series}}/release/release-1
Accept: text/csv
[BasicAuth]
idp: {{admin_password}}

HTTP 308