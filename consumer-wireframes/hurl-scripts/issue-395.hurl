# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl issue-395.hurl --variable series=x-series-on-GCP --variable host_name=dluhc-pmd5-prototype.publishmydata.com --variable scheme=https
# run locally with docker compose:
# hurl issue-395.hurl --variable series=x-series-on-GCP --variable admin_password=password --variable scheme=http --variable host_name=localhost:3000

 DELETE {{scheme}}://{{host_name}}/data/{{series}}
 HTTP 204


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "A series with all the fields",
	"dcterms:description": "The sixth dataset",
	"dh:nextUpdate": "7 July 2024 9:30:00 July 2024 June - July 2024 July 2024 (provisional)",
    "dcterms:publisher": "https://www.gov.uk/government/organisations/ofcom",
    "dcat:theme": "https://www.ons.gov.uk/businessindustryandtrade/itandinternetindustry",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dcat:keywords": "annual population survey, national vocational qualification, exams"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Release",
	"dcterms:description": "A release with all the metadata",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dh:coverage": "http://statistics.data.gov.uk/id/statistical-geography/E92000001",
	"dh:geographyDefinition": "Local Authority Districts and Unitary Authorities, Regions, Counties, Nation (England)",
	"dh:reasonForChange": "Release: Major restructure of Local Authority reference data following the census leading to many new, updated or removed locations. Revision: The latest version of this data contains additional data for the new year of 2021. In addition some minor corrections to to rounding issues for some historic data in 2011 have been fixed. One duplicate piece of data was also deleted"
}
HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
file,common/schema-qualifications-by-area.json;

HTTP 201

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 1",
	"dcterms:description": "A revision with all the details",
	"dh:publicationDate": "13 July 2023 9:30",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dh:reasonForChange" : "just felt like it"
}

HTTP 201
[Captures]
revision1_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,common/qualifications-by-area3.csv;

HTTP 201

GET {{scheme}}://{{host_name}}{{revision1_url}}/commit/1
Accept: application/json
#[Options]
#location: true
HTTP 200
[Asserts]
body == "hi"