# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl issue-323.hurl --variable series=x-series-on-GCP --variable host_name=dluhc-pmd5-prototype.publishmydata.com --variable scheme=https

#DELETE {{scheme}}://{{host_name}}/data/{{series}}
#HTTP 204


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

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv

HTTP 302

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 2",
	"dcterms:description": "A test revision 2 (with retraction)"
}

HTTP 201
[Captures]
revision2_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision2_url}}/retractions
Content-Type: text/csv
[QueryStringParams]
title: A retraction
description: retraction for the release
format: text/csv
file,common/qualifications-by-area3-deletes.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv

HTTP 302

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 3",
	"dcterms:description": "A test revision 3 (with corrections)",
	"dh:publicationDate": "13 July 2023 9:30",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dh:reasonForChange" : "just felt like it"

}

HTTP 201
[Captures]
revision3_url: header "Location"

POST {{scheme}}://{{host_name}}{{revision3_url}}/corrections
Content-Type: text/csv
[QueryStringParams]
title: A correction
description: correction for the release
format: text/csv
file,common/qualifications-by-area3-corrections.csv;

HTTP 201

GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv

HTTP 302
[Captures]
final_location: header "Location"

GET {{scheme}}://{{host_name}}{{final_location}}
Accept: text/csv

HTTP 200
