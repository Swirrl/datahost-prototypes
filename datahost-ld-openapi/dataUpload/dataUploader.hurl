# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl dataUploader.hurl --variable series=English-Indices-of-Deprivation --variable host_name=dluhc-pmd5-prototype.publishmydata.com --variable scheme=https
# run locally with docker compose:
# hurl dataUploader.hurl --variable series=English-Indices-of-Deprivation --variable host_name=localhost:8889 --variable scheme=http
 
# DELETE {{scheme}}://{{host_name}}/data/{{series}}
# [BasicAuth]
# idp: {{admin_password}}

# HTTP 204


PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2019 - LSOA Level",
	"dcterms:description": "The Indices of Deprivation 2019 provide a set of relative measures of deprivation for small areas (Lower-layer Super Output Areas*) across England, based on seven domains of deprivation. The domains were combined using the following weights to produce the overall Index of Multiple Deprivation (IMD): * Income Deprivation (22.5%) * Employment Deprivation (22.5%) * Education, Skills and Training Deprivation (13.5%) * Health Deprivation and Disability (13.5%) * Crime (9.3%) * Barriers to Housing and Services (9.3%) * Living Environment Deprivation (9.3%) In addition to the Index of Multiple Deprivation and the seven domain indices, there are two supplementary indices: the Income Deprivation Affecting Children Index (IDACI) and the Income Deprivation Affecting Older People Index (IDAOPI). LSOAs (Lower-layer Super Output Areas) are small areas designed to be of a similar population size, with an average of approximately 1,500 residents or 650 households. There are 32,844 Lower-layer Super Output Areas (LSOAs) in England. They were produced by the Office for National Statistics for the reporting of small area statistics. A range of summary measures are available for higher-level geographies including local authority districts and upper-tier local authorities, local enterprise partnerships, and clinical commissioning groups. The Index of Multiple Deprivation, domain indices and the supplementary indices, together with the higher-level geography summaries, are collectively referred to as the Indices of Deprivation (IoD). Chapter 3 of the Research Report gives advice on using and interpreting the data. Most of the data used in the Indices of Deprivation 2019 relates to the tax year 2015/16 - see Chapter 4 of the Technical Report for more details. All of the data files and supporting documents for the English Indices of Deprivation 2019 are available at http://www.gov.uk/government/statistics/english-indices-of-deprivation-2019 For statistical enquiries, please contact: [indices.deprivation@communities.gov.uk](mailtoindices.deprivation@communities.gov.uk)",
	"dh:nextUpdate": "Not supplied",
    "dcat:theme": "http://opendatacommunities.org/def/concept/folders/themes/societal-wellbeing",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"dcat:keywords": "Not supplied",
    "rdfs:comment": "This dataset contains a range of measures which form the Indices of Deprivation 2019 at LSOA level."
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
	"dcterms:title": "English Indices of Deprivation 2019 - LSOA Level",
	"dcterms:description": "This dataset contains a range of measures which form the Indices of Deprivation 2019 at LSOA level.",
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
file,English-Indices-of-Deprivation-2019-schema.json;

HTTP 201


POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2019 - LSOA Level",
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
file,English-Indices-of-Deprivation-2019.csv;

HTTP 400
[Asserts]
body == ""


GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv
[BasicAuth]
idp: {{admin_password}}

HTTP 308