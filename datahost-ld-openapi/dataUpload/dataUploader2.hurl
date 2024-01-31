# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl dataUploader2.hurl --variable admin_password=loadmewithdata3250 --variable series=Permanent-dwellings-completed --variable host_name=dluhc-pmd5-prototype.publishmydata.com --variable scheme=https
# run locally with docker compose:
# hurl dataUploader2.hurl --variable admin_password=loadmewithdata3250 --variable series=Permanent-dwellings-completed --variable host_name=localhost:8889 --variable scheme=http
DELETE {{scheme}}://{{host_name}}/data/{{series}}
  [BasicAuth]
 	idp: {{admin_password}}

 HTTP 204


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Permanent dwellings completed, England, District By Tenure",
	"dcterms:description": "House building data are collected at local authority district level, but it is important to treat figures at this level with care. House building is unevenly distributed both geographically and over time and patterns of housing development can produce clusters of new homes which make the figures at a low geographic level volatile and difficult to interpret. For detailed definitions of all tenures, see definitions of housing terms on Housing Statistics The district level and county figures are as reported by local authorities and the NHBC. Where a local authority has not submitted a quarterly return to DCLG, no figure has been presented for this local authority (and when relevant its county) for any 12-month period that includes the missing quarter. England total figures include estimates for missing data returns from independent Approved Inspectors and Local Authorities, so the sum of district values may be slightly less than the England totals. *House building completion* – In principle, a dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued whether it is in fact occupied or not. In practice, the reporting of some completions may be delayed and some completions may be missed if no completion certificate was requested by the developer or owner, although this is unusual. *Tenure* – For the purposes of these statistics, the term tenure refers to the nature of the organisation responsible for the development of a new housing start or completion. It does not necessarily describe the terms of occupancy for the dwelling on completion. For example, some housing associations develop homes for sale on the open market. Such homes would be reported in the Housing Association tenure of these statistics, but would ultimately most likely be owned and occupied in the private sector. *Housing association* - “Housing associations (HAs)” is used as the generic name for all social landlords not covered by local authorities (see below). In previous editions HAs were referred to as Registered Social Landlords (RSL), and the technical term (private) Registered Provider (pRP) of social housing is also sometimes used. The more all-encompassing description of ‘housing associations’ is now seen as more helpful to users of these statistics. These data were derived from live table [253a](https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/639541/LiveTable_253a.xlsx) (quarterly figures). Rolling totals for England are calculated and published in Tables 2a and 2b of the house building statistics [release](https://www.gov.uk/government/collections/house-building-statistics).",
	"dcterms:publisher": "http://reference.data.gov.uk/id/department/dclg",
	"dh:nextUpdate": "September 2018",
    "dcat:theme": "http://opendatacommunities.org/def/concept/folders/themes/house-building",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/",
	"dcat:keywords": "Not supplied",
    "rdfs:comment": "This data set contains annual data, the most recent quarterly data, and rolling four quarter totals (England only) on house building completions. From 2011/12 imputed data is included. Imputed data should not be seen as an estimate for the individual authority but is given on an authority basis to allow custom totals to be constructed. A dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued."
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Permanent dwellings completed, England, District By Tenure",
	"dcterms:description": "House building data are collected at local authority district level, but it is important to treat figures at this level with care. House building is unevenly distributed both geographically and over time and patterns of housing development can produce clusters of new homes which make the figures at a low geographic level volatile and difficult to interpret. For detailed definitions of all tenures, see definitions of housing terms on Housing Statistics The district level and county figures are as reported by local authorities and the NHBC. Where a local authority has not submitted a quarterly return to DCLG, no figure has been presented for this local authority (and when relevant its county) for any 12-month period that includes the missing quarter. England total figures include estimates for missing data returns from independent Approved Inspectors and Local Authorities, so the sum of district values may be slightly less than the England totals. *House building completion* – In principle, a dwelling is regarded as complete when it becomes ready for occupation or when a completion certificate is issued whether it is in fact occupied or not. In practice, the reporting of some completions may be delayed and some completions may be missed if no completion certificate was requested by the developer or owner, although this is unusual. *Tenure* – For the purposes of these statistics, the term tenure refers to the nature of the organisation responsible for the development of a new housing start or completion. It does not necessarily describe the terms of occupancy for the dwelling on completion. For example, some housing associations develop homes for sale on the open market. Such homes would be reported in the Housing Association tenure of these statistics, but would ultimately most likely be owned and occupied in the private sector. *Housing association* - “Housing associations (HAs)” is used as the generic name for all social landlords not covered by local authorities (see below). In previous editions HAs were referred to as Registered Social Landlords (RSL), and the technical term (private) Registered Provider (pRP) of social housing is also sometimes used. The more all-encompassing description of ‘housing associations’ is now seen as more helpful to users of these statistics. These data were derived from live table [253a](https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/639541/LiveTable_253a.xlsx) (quarterly figures). Rolling totals for England are calculated and published in Tables 2a and 2b of the house building statistics [release](https://www.gov.uk/government/collections/house-building-statistics).",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dh:coverage": "Not supplied",
	"dh:geographyDefinition": "Not supplied",
	"dh:reasonForChange": "Not supplied"
}
HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Permanent-dwellings-completed-England-District-By-Tenure-schema.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Permanent dwellings completed, England, District By Tenure",
	"dcterms:description": "Initial append",
	"dh:publicationDate": "September 26, 2019 09:34",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dh:reasonForChange" : "Not supplied"
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Permanent-dwellings-completed-England-District-By-Tenure-filtered.csv;

HTTP 201



GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv
[BasicAuth]
idp: {{admin_password}}

HTTP 308