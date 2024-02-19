# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl upload-planning-applications.hurl --variable series=Planning-Applications-Decisions --variable datahost-base-url=https://dluhc-pmd5-prototype.publishmydata.com --variable admin_password=$DLUHC_ADMIN_PASSWORD
# run locally with docker compose:
# hurl upload-planning-applications.hurl --variable series=Planning-Applications-Decisions --variable datahost-base-url=http://localhost:8889 --variable admin_password=password


#comment out the next 5 lines if the dataset doesn't exist (after the service is reset)

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
	"dcterms:title": "Planning Applications Decisions",
	"dcterms:description": "District level planning is undertaken by metropolitan and non-metropolitan districts, unitary authorities, national park authorities and urban development corporations. These authorities deal with all other planning applications that are not classified as county matters and mainly include applications for planning permissions on residential, offices, industrial, retail and householder developments.",
	"dcterms:publisher": "http://reference.data.gov.uk/id/department/dclg",
	"dh:nextUpdate": "September 2019",
    "dcat:theme": "http://opendatacommunities.org/def/concept/folders/themes/planning",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/",
    "rdfs:comment": "Planning applications decided by district level planning authority and type of development"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{datahost-base-url}}/data/{{series}}/release/Developments-England
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "Major and Minor Developments, England, District by Development Type",
	"dcterms:description": "District level planning is undertaken by metropolitan and non-metropolitan districts, unitary authorities, national park authorities and urban development corporations. These authorities deal with all other planning applications that are not classified as county matters and mainly include applications for planning permissions on residential, offices, industrial, retail and householder developments.",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/"
}
HTTP 201

POST {{datahost-base-url}}/data/{{series}}/release/Developments-England/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type-schema.json;

HTTP 201

POST {{datahost-base-url}}/data/{{series}}/release/Developments-England/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "District by Development Type",
	"dcterms:description": "Initial append of historical data",
	"dh:publicationDate": "December 19, 2013 16:3",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/"
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
file,Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type-filtered.csv;

HTTP 201
#[Asserts]
#body == ""

POST {{datahost-base-url}}{{revision1_url}}/retractions
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: Deleting a few null observations
description: Some rows were later discovered to be invalid due to interference
format: text/csv
file,Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type-deletes.csv;

HTTP 201

POST {{datahost-base-url}}{{revision1_url}}/corrections
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: Correcting values for London
description: Some errors were made in the initial inputting of the data
format: text/csv
file,Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type-corrections.csv;

HTTP 201



GET {{datahost-base-url}}/data/{{series}}/release/release-1
Accept: text/csv
[BasicAuth]
idp: {{admin_password}}

HTTP 302