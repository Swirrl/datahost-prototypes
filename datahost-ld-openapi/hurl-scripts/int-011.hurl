# Description: This test ensures appends deletes corrections and their 
# associated metadata are added and retrieved correctly.


#Create series
PUT {{scheme}}://{{host_name}}/data/{{series}}
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Test Dataset",
    "dcterms:description": "A very simple test"
}

HTTP 201


#Create release
PUT {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Release 34",
    "dcterms:description": "Description 34"
}

HTTP 201


#add schema
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/schema
Content-Type: application/json
Authorization: {{auth_token}}
file,common/schema-qualifications-by-area.json;

HTTP 201


#creates a revision with additional metadata
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "a revision for release",
    "dcterms:description": "A test revision",
    "dh:publicationDate": "2023-09-01",
    "dcterms:license": "http://license-link",
    "dh:reasonForChange": "Comment..."
}

HTTP 201
[Captures]
revision1_url: header "Location"
id: jsonpath "$.['@id']"
[Asserts]
#checks that successful post returns normalised release data values
jsonpath "$.['dcterms:title']" == "a revision for release"
jsonpath "$.['dcterms:description']" == "A test revision"
jsonpath "$.['@type']" == "dh:Revision"

# clojure test compares this to the rdf-base-uri which it defines as:
# `(th/sys->rdf-base-uri sys)`
jsonpath "$.['dh:appliesToRelease']" endsWith "{{series}}/release/release-1"

jsonpath "$.['dh:publicationDate']" == "2023-09-01"
jsonpath "$.['dcterms:license']" == "http://license-link"
jsonpath "$.['dh:reasonForChange']" == "Comment..."
#Created with the resource URI provided in the Location header
header "Location" endsWith "{{id}}"


#Fetching an existing Revision as default application/json format works
GET {{scheme}}://{{host_name}}{{revision1_url}}
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.['dcterms:title']" == "a revision for release"
jsonpath "$.['dcterms:description']" == "A test revision"
jsonpath "$.['@type']" == "dh:Revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "{{series}}/release/release-1"
jsonpath "$.['dh:publicationDate']" == "2023-09-01"
jsonpath "$.['dcterms:license']" == "http://license-link"
jsonpath "$.['dh:reasonForChange']" == "Comment..."


#Associated Release gets the Revision inverse triple
GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}

HTTP 200
[Asserts]
jsonpath "$.['dh:hasRevision']" endsWith "{{revision1_url}}"


#appends to the revision
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: change for {{series}}
file,common/qualifications-by-area1.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindAppend"
jsonpath "$.['@id']" == "{{series}}/release/release-1/revision/1/commit/1"
jsonpath "$.['dcterms:title']" == "changes appends"
jsonpath "$.['dcterms:description']" == "change for {{series}}"


GET {{scheme}}://{{host_name}}{{revision1_url}}/commit/1
Accept: text/csv
Content-Type: text/csv
Authorization: {{auth_token}}

HTTP 200
[Asserts]
#The clojure test only counts the number of lines of the csv, I was unable
#to find a way to do that in Hurl.
```
Measure type,Statistical Geography,Year,Aged 16 to 64 years level 3 or above qualifications,Unit of Measure,Upper Confidence Interval,Lower Confidence Interval,Observation Status
Aged 16 to 64 years level 3 or above qualifications,City of London,2019,,per cent,,,https://purl.org/csv-cubed/resources/attributes/af-obs-marker#x
Aged 16 to 64 years level 3 or above qualifications,Hackney,2019,68.3,per cent,75.0,61.6,
Aged 16 to 64 years level 3 or above qualifications,Lewisham,2019,75.6,per cent,81.7,69.5,
Aged 16 to 64 years level 3 or above qualifications,North Lanarkshire,2019,52.5,per cent,58.2,46.8,
Aged 16 to 64 years level 3 or above qualifications,Bury,2019,57.9,per cent,63.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,St. Helens,2019,53.9,per cent,59.6,48.2,
Aged 16 to 64 years level 3 or above qualifications,Sunderland,2019,50.2,per cent,55.3,45.1,
Aged 16 to 64 years level 3 or above qualifications,West Midlands (Combined Authority),2019,48.7,per cent,50.6,46.8,
Aged 16 to 64 years level 3 or above qualifications,Runnymede,2019,73.4,per cent,87.7,59.1,
Aged 16 to 64 years level 3 or above qualifications,Leeds,2019,61.0,per cent,65.5,56.5,
Aged 16 to 64 years level 3 or above qualifications,Three Rivers,2019,74.8,per cent,88.3,61.3,
Aged 16 to 64 years level 3 or above qualifications,Adur,2019,65.5,per cent,85.2,45.8,
Aged 16 to 64 years level 3 or above qualifications,Swale,2019,42.5,per cent,53.6,31.4,
Aged 16 to 64 years level 3 or above qualifications,Bromsgrove,2019,67.7,per cent,79.7,55.7,
Aged 16 to 64 years level 3 or above qualifications,Ribble Valley,2019,76.6,per cent,92.3,60.9,
Aged 16 to 64 years level 3 or above qualifications,West Suffolk,2019,49.1,per cent,59.8,38.4,
Aged 16 to 64 years level 3 or above qualifications,Oadby and Wigston,2019,68.3,per cent,85.0,51.6,
Aged 16 to 64 years level 3 or above qualifications,King's Lynn and West Norfolk,2019,51.8,per cent,62.8,40.8,
Aged 16 to 64 years level 3 or above qualifications,Erewash,2019,55.2,per cent,66.8,43.6,
Aged 16 to 64 years level 3 or above qualifications,West Devon,2019,67.1,per cent,84.2,50.0,
Aged 16 to 64 years level 3 or above qualifications,Scarborough,2019,50.8,per cent,62.6,39.0,
Aged 16 to 64 years level 3 or above qualifications,South Oxfordshire,2019,64.5,per cent,75.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,South West,2019,60.5,per cent,61.9,59.1,
Aged 16 to 64 years level 3 or above qualifications,Stockton-on-Tees,2019,53.6,per cent,59.1,48.1,
Aged 16 to 64 years level 3 or above qualifications,Castle Point,2019,43.3,per cent,59.5,27.1,
Aged 16 to 64 years level 3 or above qualifications,Derby,2019,53.8,per cent,59.0,48.6,
Aged 16 to 64 years level 3 or above qualifications,Great Britain,2019,58.5,per cent,58.9,58.1,
Aged 16 to 64 years level 3 or above qualifications,Plymouth,2019,54.0,per cent,59.4,48.6,
Aged 16 to 64 years level 3 or above qualifications,West Berkshire,2019,65.7,per cent,71.2,60.2,
Aged 16 to 64 years level 3 or above qualifications,Dorset,2019,59.5,per cent,65.6,53.4,
Aged 16 to 64 years level 3 or above qualifications,Scotland,2019,60.8,per cent,61.9,59.7,
```

#ensure we can add more than one change to a revision
POST {{scheme}}://{{host_name}}{{revision1_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes appends
description: a second change for {{series}}
file,common/qualifications-by-area2.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindAppend"
header "Location" == "/data/{{series}}/release/release-1/revision/1/commit/2"

#Get the second change and make sure the appends are concatenated
GET {{scheme}}://{{host_name}}{{revision1_url}}
Accept: text/csv
Content-Type: text/csv
Authorization: {{auth_token}}

HTTP 200
```
Measure type,Statistical Geography,Year,Aged 16 to 64 years level 3 or above qualifications,Unit of Measure,Upper Confidence Interval,Lower Confidence Interval,Observation Status
Aged 16 to 64 years level 3 or above qualifications,City of London,2019,,per cent,,,https://purl.org/csv-cubed/resources/attributes/af-obs-marker#x
Aged 16 to 64 years level 3 or above qualifications,Hackney,2019,68.3,per cent,75.0,61.6,
Aged 16 to 64 years level 3 or above qualifications,Lewisham,2019,75.6,per cent,81.7,69.5,
Aged 16 to 64 years level 3 or above qualifications,North Lanarkshire,2019,52.5,per cent,58.2,46.8,
Aged 16 to 64 years level 3 or above qualifications,Bury,2019,57.9,per cent,63.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,St. Helens,2019,53.9,per cent,59.6,48.2,
Aged 16 to 64 years level 3 or above qualifications,Sunderland,2019,50.2,per cent,55.3,45.1,
Aged 16 to 64 years level 3 or above qualifications,West Midlands (Combined Authority),2019,48.7,per cent,50.6,46.8,
Aged 16 to 64 years level 3 or above qualifications,Runnymede,2019,73.4,per cent,87.7,59.1,
Aged 16 to 64 years level 3 or above qualifications,Leeds,2019,61.0,per cent,65.5,56.5,
Aged 16 to 64 years level 3 or above qualifications,Three Rivers,2019,74.8,per cent,88.3,61.3,
Aged 16 to 64 years level 3 or above qualifications,Adur,2019,65.5,per cent,85.2,45.8,
Aged 16 to 64 years level 3 or above qualifications,Swale,2019,42.5,per cent,53.6,31.4,
Aged 16 to 64 years level 3 or above qualifications,Bromsgrove,2019,67.7,per cent,79.7,55.7,
Aged 16 to 64 years level 3 or above qualifications,Ribble Valley,2019,76.6,per cent,92.3,60.9,
Aged 16 to 64 years level 3 or above qualifications,West Suffolk,2019,49.1,per cent,59.8,38.4,
Aged 16 to 64 years level 3 or above qualifications,Oadby and Wigston,2019,68.3,per cent,85.0,51.6,
Aged 16 to 64 years level 3 or above qualifications,King's Lynn and West Norfolk,2019,51.8,per cent,62.8,40.8,
Aged 16 to 64 years level 3 or above qualifications,Erewash,2019,55.2,per cent,66.8,43.6,
Aged 16 to 64 years level 3 or above qualifications,West Devon,2019,67.1,per cent,84.2,50.0,
Aged 16 to 64 years level 3 or above qualifications,Scarborough,2019,50.8,per cent,62.6,39.0,
Aged 16 to 64 years level 3 or above qualifications,South Oxfordshire,2019,64.5,per cent,75.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,South West,2019,60.5,per cent,61.9,59.1,
Aged 16 to 64 years level 3 or above qualifications,Stockton-on-Tees,2019,53.6,per cent,59.1,48.1,
Aged 16 to 64 years level 3 or above qualifications,Castle Point,2019,43.3,per cent,59.5,27.1,
Aged 16 to 64 years level 3 or above qualifications,Derby,2019,53.8,per cent,59.0,48.6,
Aged 16 to 64 years level 3 or above qualifications,Great Britain,2019,58.5,per cent,58.9,58.1,
Aged 16 to 64 years level 3 or above qualifications,Plymouth,2019,54.0,per cent,59.4,48.6,
Aged 16 to 64 years level 3 or above qualifications,West Berkshire,2019,65.7,per cent,71.2,60.2,
Aged 16 to 64 years level 3 or above qualifications,Dorset,2019,59.5,per cent,65.6,53.4,
Aged 16 to 64 years level 3 or above qualifications,Scotland,2019,60.8,per cent,61.9,59.7,
Aged 16 to 64 years level 3 or above qualifications,Vale of White Horse,2020,72.1,per cent,83.1,61.1,
Aged 16 to 64 years level 3 or above qualifications,Chelmsford,2020,62.9,per cent,73.8,52.0,
Aged 16 to 64 years level 3 or above qualifications,Gloucester,2020,54.5,per cent,67.0,42.0,
Aged 16 to 64 years level 3 or above qualifications,Rushmoor,2020,59.3,per cent,73.2,45.4,
Aged 16 to 64 years level 3 or above qualifications,Buckinghamshire,2020,64.7,per cent,70.2,59.2,
Aged 16 to 64 years level 3 or above qualifications,Essex,2020,55.8,per cent,59.8,51.8,
Aged 16 to 64 years level 3 or above qualifications,North Yorkshire,2020,58.7,per cent,64.2,53.2,
Aged 16 to 64 years level 3 or above qualifications,Worcestershire,2020,59.0,per cent,64.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,Ceredigion,2020,67.7,per cent,73.8,61.6,
Aged 16 to 64 years level 3 or above qualifications,Blaenau Gwent,2020,47.2,per cent,55.1,39.3,
Aged 16 to 64 years level 3 or above qualifications,Gloucestershire,2020,63.7,per cent,68.7,58.7,
Aged 16 to 64 years level 3 or above qualifications,Nottinghamshire,2020,56.6,per cent,61.6,51.6,
Aged 16 to 64 years level 3 or above qualifications,Pembrokeshire,2020,58.6,per cent,64.5,52.7,
Aged 16 to 64 years level 3 or above qualifications,Ashfield,2020,50.6,per cent,63.7,37.5,
Aged 16 to 64 years level 3 or above qualifications,West Oxfordshire,2020,64.2,per cent,77.6,50.8,
Aged 16 to 64 years level 3 or above qualifications,Colchester,2020,54.2,per cent,64.4,44.0,
Aged 16 to 64 years level 3 or above qualifications,Cannock Chase,2020,47.9,per cent,66.3,29.5,
Aged 16 to 64 years level 3 or above qualifications,Stroud,2020,63.2,per cent,74.4,52.0,
```

#Create a new revision with description left blank intentionally
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "another revision for release"
}

HTTP 201
[Captures]
revision2_url: header "Location"
id2: jsonpath "$.['@id']"
[Asserts]
#checks that successful post returns normalised release data values
jsonpath "$.['dcterms:title']" == "another revision for release"
jsonpath "$.['@id']" endsWith "/revision/2"
variable "revision2_url" == "/data/{{id2}}"


#third changes added to second revision
POST {{scheme}}://{{host_name}}{{revision2_url}}/appends
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
description: a third new change
file,common/qualifications-by-area3.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindAppend"
jsonpath "$.['@id']" endsWith "commit/1"


#ensure path header contains a URL when requesting the release
#and redirects to revision 2
GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1
Accept: text/csv
Authorization: {{auth_token}}
HTTP 302
[Captures]
path: header "Link"
[Asserts]
variable "path" matches /<http:\/\/(.+)(:\d+){0,1}(\S+)>; rel="describedBy"; type="application\/csvm\+json"/
header "Location" == "{{revision2_url}}"



GET {{scheme}}://{{host_name}}/data/{{series}}/release/release-1-metadata.json
Accept: application/json 
Content-Type: application/json
Authorization: {{auth_token}}
HTTP 200
[Asserts]
body == "{\"@context\": [\"http://www.w3.org/ns/csvw\", {\"@language\": \"en\"}]}"


#ensure path header contains a URL when requesting revision 2
GET {{scheme}}://{{host_name}}{{revision2_url}}
Accept: text/csv
Content-Type: text/csv
Authorization: {{auth_token}}

HTTP 200
[Captures]
path: header "Link"
[Asserts]
variable "path" matches /<http:\/\/(.+):\d+(\S+)>; rel="describedBy"; type="application\/csvm\+json"/
#returns all three csvs concatenated
```
Measure type,Statistical Geography,Year,Aged 16 to 64 years level 3 or above qualifications,Unit of Measure,Upper Confidence Interval,Lower Confidence Interval,Observation Status
Aged 16 to 64 years level 3 or above qualifications,City of London,2019,,per cent,,,https://purl.org/csv-cubed/resources/attributes/af-obs-marker#x
Aged 16 to 64 years level 3 or above qualifications,Hackney,2019,68.3,per cent,75.0,61.6,
Aged 16 to 64 years level 3 or above qualifications,Lewisham,2019,75.6,per cent,81.7,69.5,
Aged 16 to 64 years level 3 or above qualifications,North Lanarkshire,2019,52.5,per cent,58.2,46.8,
Aged 16 to 64 years level 3 or above qualifications,Bury,2019,57.9,per cent,63.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,St. Helens,2019,53.9,per cent,59.6,48.2,
Aged 16 to 64 years level 3 or above qualifications,Sunderland,2019,50.2,per cent,55.3,45.1,
Aged 16 to 64 years level 3 or above qualifications,West Midlands (Combined Authority),2019,48.7,per cent,50.6,46.8,
Aged 16 to 64 years level 3 or above qualifications,Runnymede,2019,73.4,per cent,87.7,59.1,
Aged 16 to 64 years level 3 or above qualifications,Leeds,2019,61.0,per cent,65.5,56.5,
Aged 16 to 64 years level 3 or above qualifications,Three Rivers,2019,74.8,per cent,88.3,61.3,
Aged 16 to 64 years level 3 or above qualifications,Adur,2019,65.5,per cent,85.2,45.8,
Aged 16 to 64 years level 3 or above qualifications,Swale,2019,42.5,per cent,53.6,31.4,
Aged 16 to 64 years level 3 or above qualifications,Bromsgrove,2019,67.7,per cent,79.7,55.7,
Aged 16 to 64 years level 3 or above qualifications,Ribble Valley,2019,76.6,per cent,92.3,60.9,
Aged 16 to 64 years level 3 or above qualifications,West Suffolk,2019,49.1,per cent,59.8,38.4,
Aged 16 to 64 years level 3 or above qualifications,Oadby and Wigston,2019,68.3,per cent,85.0,51.6,
Aged 16 to 64 years level 3 or above qualifications,King's Lynn and West Norfolk,2019,51.8,per cent,62.8,40.8,
Aged 16 to 64 years level 3 or above qualifications,Erewash,2019,55.2,per cent,66.8,43.6,
Aged 16 to 64 years level 3 or above qualifications,West Devon,2019,67.1,per cent,84.2,50.0,
Aged 16 to 64 years level 3 or above qualifications,Scarborough,2019,50.8,per cent,62.6,39.0,
Aged 16 to 64 years level 3 or above qualifications,South Oxfordshire,2019,64.5,per cent,75.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,South West,2019,60.5,per cent,61.9,59.1,
Aged 16 to 64 years level 3 or above qualifications,Stockton-on-Tees,2019,53.6,per cent,59.1,48.1,
Aged 16 to 64 years level 3 or above qualifications,Castle Point,2019,43.3,per cent,59.5,27.1,
Aged 16 to 64 years level 3 or above qualifications,Derby,2019,53.8,per cent,59.0,48.6,
Aged 16 to 64 years level 3 or above qualifications,Great Britain,2019,58.5,per cent,58.9,58.1,
Aged 16 to 64 years level 3 or above qualifications,Plymouth,2019,54.0,per cent,59.4,48.6,
Aged 16 to 64 years level 3 or above qualifications,West Berkshire,2019,65.7,per cent,71.2,60.2,
Aged 16 to 64 years level 3 or above qualifications,Dorset,2019,59.5,per cent,65.6,53.4,
Aged 16 to 64 years level 3 or above qualifications,Scotland,2019,60.8,per cent,61.9,59.7,
Aged 16 to 64 years level 3 or above qualifications,Vale of White Horse,2020,72.1,per cent,83.1,61.1,
Aged 16 to 64 years level 3 or above qualifications,Chelmsford,2020,62.9,per cent,73.8,52.0,
Aged 16 to 64 years level 3 or above qualifications,Gloucester,2020,54.5,per cent,67.0,42.0,
Aged 16 to 64 years level 3 or above qualifications,Rushmoor,2020,59.3,per cent,73.2,45.4,
Aged 16 to 64 years level 3 or above qualifications,Buckinghamshire,2020,64.7,per cent,70.2,59.2,
Aged 16 to 64 years level 3 or above qualifications,Essex,2020,55.8,per cent,59.8,51.8,
Aged 16 to 64 years level 3 or above qualifications,North Yorkshire,2020,58.7,per cent,64.2,53.2,
Aged 16 to 64 years level 3 or above qualifications,Worcestershire,2020,59.0,per cent,64.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,Ceredigion,2020,67.7,per cent,73.8,61.6,
Aged 16 to 64 years level 3 or above qualifications,Blaenau Gwent,2020,47.2,per cent,55.1,39.3,
Aged 16 to 64 years level 3 or above qualifications,Gloucestershire,2020,63.7,per cent,68.7,58.7,
Aged 16 to 64 years level 3 or above qualifications,Nottinghamshire,2020,56.6,per cent,61.6,51.6,
Aged 16 to 64 years level 3 or above qualifications,Pembrokeshire,2020,58.6,per cent,64.5,52.7,
Aged 16 to 64 years level 3 or above qualifications,Ashfield,2020,50.6,per cent,63.7,37.5,
Aged 16 to 64 years level 3 or above qualifications,West Oxfordshire,2020,64.2,per cent,77.6,50.8,
Aged 16 to 64 years level 3 or above qualifications,Colchester,2020,54.2,per cent,64.4,44.0,
Aged 16 to 64 years level 3 or above qualifications,Cannock Chase,2020,47.9,per cent,66.3,29.5,
Aged 16 to 64 years level 3 or above qualifications,Stroud,2020,63.2,per cent,74.4,52.0,
Aged 16 to 64 years level 3 or above qualifications,Colchester,2021,55.3,per cent,66.9,43.7,
Aged 16 to 64 years level 3 or above qualifications,Cannock Chase,2021,55.8,per cent,70.9,40.7,
Aged 16 to 64 years level 3 or above qualifications,Stroud,2021,69.7,per cent,79.1,60.3,
Aged 16 to 64 years level 3 or above qualifications,Test Valley,2021,63.4,per cent,74.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,Cheshire West and Chester,2021,60.7,per cent,68.1,53.3,
Aged 16 to 64 years level 3 or above qualifications,Greater Manchester (Metropolitan County),2021,57.9,per cent,60.0,55.8,
Aged 16 to 64 years level 3 or above qualifications,North Northamptonshire,2021,52.8,per cent,60.2,45.4,
Aged 16 to 64 years level 3 or above qualifications,Gloucestershire,2021,65.2,per cent,69.8,60.6,
Aged 16 to 64 years level 3 or above qualifications,Nottinghamshire,2021,54.1,per cent,59.0,49.2,
Aged 16 to 64 years level 3 or above qualifications,Pembrokeshire,2021,57.3,per cent,63.4,51.2,
Aged 16 to 64 years level 3 or above qualifications,Ashfield,2021,50.2,per cent,64.2,36.2,
Aged 16 to 64 years level 3 or above qualifications,West Oxfordshire,2021,81.1,per cent,92.1,70.1,
Aged 16 to 64 years level 3 or above qualifications,North East (Combined Authority),2021,50.3,per cent,53.4,47.2,
Aged 16 to 64 years level 3 or above qualifications,North East (Region),2021,54.6,per cent,56.4,52.8,
Aged 16 to 64 years level 3 or above qualifications,Bassetlaw,2021,52.6,per cent,65.7,39.5,
Aged 16 to 64 years level 3 or above qualifications,Eastbourne,2021,56.9,per cent,71.9,41.9,
Aged 16 to 64 years level 3 or above qualifications,Epping Forest,2021,56.7,per cent,68.4,45.0,
Aged 16 to 64 years level 3 or above qualifications,East Staffordshire,2021,48.9,per cent,62.4,35.4,
Aged 16 to 64 years level 3 or above qualifications,Tewkesbury,2021,59.5,per cent,72.3,46.7,
Aged 16 to 64 years level 3 or above qualifications,Winchester,2021,67.4,per cent,80.5,54.3,
Aged 16 to 64 years level 3 or above qualifications,Windsor and Maidenhead,2021,73.8,per cent,78.9,68.7,
Aged 16 to 64 years level 3 or above qualifications,Shropshire,2021,60.8,per cent,67.2,54.4,
Aged 16 to 64 years level 3 or above qualifications,Merseyside,2021,59.6,per cent,62.7,56.5,
Aged 16 to 64 years level 3 or above qualifications,West Northamptonshire,2021,53.8,per cent,60.6,47.0,
Aged 16 to 64 years level 3 or above qualifications,Cambridgeshire,2021,65.7,per cent,70.8,60.6,
Aged 16 to 64 years level 3 or above qualifications,Hampshire,2021,60.6,per cent,64.2,57.0,
```

#create 3rd revision
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 3",
    "dcterms:description": "a 3rd test revision"
}

HTTP 201
[Captures]
revision3_url: header "Location"
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/3"
jsonpath "$.['dcterms:title']" == "Rev 3"
jsonpath "$.['dcterms:description']" == "a 3rd test revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "data/{{series}}/release/release-1"


#Deletion
POST {{scheme}}://{{host_name}}{{revision3_url}}/retractions
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes retractions
description: change for {{series}}
file,common/qualifications-by-area3-deletes.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindRetract"
jsonpath "$.['@id']" == "{{series}}/release/release-1/revision/3/commit/1"
jsonpath "$.['dcterms:title']" == "changes retractions"
jsonpath "$.['dcterms:description']" == "change for {{series}}"


#create 4th revision
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Rev 4",
    "dcterms:description": "a 4th test revision"
}

HTTP 201
[Captures]
revision4_url: header "Location"
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/4"
jsonpath "$.['dcterms:title']" == "Rev 4"
jsonpath "$.['dcterms:description']" == "a 4th test revision"
jsonpath "$.['dh:appliesToRelease']" endsWith "data/{{series}}/release/release-1"


#Correction
POST {{scheme}}://{{host_name}}{{revision4_url}}/corrections
Content-Type: text/csv
Authorization: {{auth_token}}
[QueryStringParams]
title: changes corrections
description: change for {{series}}
file,common/qualifications-by-area3-corrections.csv;

HTTP 201
[Asserts]
jsonpath "$.['dh:changeKind']" == ":dh/ChangeKindCorrect"
jsonpath "$.['@id']" == "{{series}}/release/release-1/revision/4/commit/1"
jsonpath "$.['dcterms:title']" == "changes corrections"
jsonpath "$.['dcterms:description']" == "change for {{series}}"


#ensure data is accurate
GET {{scheme}}://{{host_name}}{{revision4_url}}
Accept: text/csv
Content-Type: text/csv
Authorization: {{auth_token}}

HTTP 200
```
Measure type,Statistical Geography,Year,Aged 16 to 64 years level 3 or above qualifications,Unit of Measure,Upper Confidence Interval,Lower Confidence Interval,Observation Status
Aged 16 to 64 years level 3 or above qualifications,City of London,2019,,per cent,,,https://purl.org/csv-cubed/resources/attributes/af-obs-marker#x
Aged 16 to 64 years level 3 or above qualifications,Hackney,2019,68.3,per cent,75.0,61.6,
Aged 16 to 64 years level 3 or above qualifications,Lewisham,2019,75.6,per cent,81.7,69.5,
Aged 16 to 64 years level 3 or above qualifications,North Lanarkshire,2019,52.5,per cent,58.2,46.8,
Aged 16 to 64 years level 3 or above qualifications,Bury,2019,57.9,per cent,63.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,St. Helens,2019,53.9,per cent,59.6,48.2,
Aged 16 to 64 years level 3 or above qualifications,Sunderland,2019,50.2,per cent,55.3,45.1,
Aged 16 to 64 years level 3 or above qualifications,West Midlands (Combined Authority),2019,48.7,per cent,50.6,46.8,
Aged 16 to 64 years level 3 or above qualifications,Runnymede,2019,73.4,per cent,87.7,59.1,
Aged 16 to 64 years level 3 or above qualifications,Leeds,2019,61.0,per cent,65.5,56.5,
Aged 16 to 64 years level 3 or above qualifications,Three Rivers,2019,74.8,per cent,88.3,61.3,
Aged 16 to 64 years level 3 or above qualifications,Adur,2019,65.5,per cent,85.2,45.8,
Aged 16 to 64 years level 3 or above qualifications,Swale,2019,42.5,per cent,53.6,31.4,
Aged 16 to 64 years level 3 or above qualifications,Bromsgrove,2019,67.7,per cent,79.7,55.7,
Aged 16 to 64 years level 3 or above qualifications,Ribble Valley,2019,76.6,per cent,92.3,60.9,
Aged 16 to 64 years level 3 or above qualifications,West Suffolk,2019,49.1,per cent,59.8,38.4,
Aged 16 to 64 years level 3 or above qualifications,Oadby and Wigston,2019,68.3,per cent,85.0,51.6,
Aged 16 to 64 years level 3 or above qualifications,King's Lynn and West Norfolk,2019,51.8,per cent,62.8,40.8,
Aged 16 to 64 years level 3 or above qualifications,Erewash,2019,55.2,per cent,66.8,43.6,
Aged 16 to 64 years level 3 or above qualifications,West Devon,2019,67.1,per cent,84.2,50.0,
Aged 16 to 64 years level 3 or above qualifications,Scarborough,2019,50.8,per cent,62.6,39.0,
Aged 16 to 64 years level 3 or above qualifications,South Oxfordshire,2019,64.5,per cent,75.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,South West,2019,60.5,per cent,61.9,59.1,
Aged 16 to 64 years level 3 or above qualifications,Stockton-on-Tees,2019,53.6,per cent,59.1,48.1,
Aged 16 to 64 years level 3 or above qualifications,Castle Point,2019,43.3,per cent,59.5,27.1,
Aged 16 to 64 years level 3 or above qualifications,Derby,2019,53.8,per cent,59.0,48.6,
Aged 16 to 64 years level 3 or above qualifications,Great Britain,2019,58.5,per cent,58.9,58.1,
Aged 16 to 64 years level 3 or above qualifications,Plymouth,2019,54.0,per cent,59.4,48.6,
Aged 16 to 64 years level 3 or above qualifications,West Berkshire,2019,65.7,per cent,71.2,60.2,
Aged 16 to 64 years level 3 or above qualifications,Dorset,2019,59.5,per cent,65.6,53.4,
Aged 16 to 64 years level 3 or above qualifications,Scotland,2019,60.8,per cent,61.9,59.7,
Aged 16 to 64 years level 3 or above qualifications,Vale of White Horse,2020,72.1,per cent,83.1,61.1,
Aged 16 to 64 years level 3 or above qualifications,Chelmsford,2020,62.9,per cent,73.8,52.0,
Aged 16 to 64 years level 3 or above qualifications,Gloucester,2020,54.5,per cent,67.0,42.0,
Aged 16 to 64 years level 3 or above qualifications,Rushmoor,2020,59.3,per cent,73.2,45.4,
Aged 16 to 64 years level 3 or above qualifications,Buckinghamshire,2020,64.7,per cent,70.2,59.2,
Aged 16 to 64 years level 3 or above qualifications,Essex,2020,55.8,per cent,59.8,51.8,
Aged 16 to 64 years level 3 or above qualifications,North Yorkshire,2020,58.7,per cent,64.2,53.2,
Aged 16 to 64 years level 3 or above qualifications,Worcestershire,2020,59.0,per cent,64.2,53.8,
Aged 16 to 64 years level 3 or above qualifications,Ceredigion,2020,67.7,per cent,73.8,61.6,
Aged 16 to 64 years level 3 or above qualifications,Blaenau Gwent,2020,47.2,per cent,55.1,39.3,
Aged 16 to 64 years level 3 or above qualifications,Gloucestershire,2020,63.7,per cent,68.7,58.7,
Aged 16 to 64 years level 3 or above qualifications,Nottinghamshire,2020,56.6,per cent,61.6,51.6,
Aged 16 to 64 years level 3 or above qualifications,Pembrokeshire,2020,58.6,per cent,64.5,52.7,
Aged 16 to 64 years level 3 or above qualifications,Ashfield,2020,50.6,per cent,63.7,37.5,
Aged 16 to 64 years level 3 or above qualifications,West Oxfordshire,2020,64.2,per cent,77.6,50.8,
Aged 16 to 64 years level 3 or above qualifications,Colchester,2020,54.2,per cent,64.4,44.0,
Aged 16 to 64 years level 3 or above qualifications,Cannock Chase,2020,47.9,per cent,66.3,29.5,
Aged 16 to 64 years level 3 or above qualifications,Stroud,2020,63.2,per cent,74.4,52.0,
Aged 16 to 64 years level 3 or above qualifications,Colchester,2021,55.3,per cent,66.9,43.7,
Aged 16 to 64 years level 3 or above qualifications,Cannock Chase,2021,55.8,per cent,70.9,40.7,
Aged 16 to 64 years level 3 or above qualifications,Stroud,2021,69.7,per cent,79.1,60.3,
Aged 16 to 64 years level 3 or above qualifications,Test Valley,2021,63.4,per cent,74.1,52.7,
Aged 16 to 64 years level 3 or above qualifications,Cheshire West and Chester,2021,60.7,per cent,68.1,53.3,
Aged 16 to 64 years level 3 or above qualifications,Nottinghamshire,2021,54.1,per cent,59.0,49.2,
Aged 16 to 64 years level 3 or above qualifications,Pembrokeshire,2021,57.3,per cent,63.4,51.2,
Aged 16 to 64 years level 3 or above qualifications,Ashfield,2021,50.2,per cent,64.2,36.2,
Aged 16 to 64 years level 3 or above qualifications,West Oxfordshire,2021,81.1,per cent,92.1,70.1,
Aged 16 to 64 years level 3 or above qualifications,North East (Combined Authority),2021,50.3,per cent,53.4,47.2,
Aged 16 to 64 years level 3 or above qualifications,North East (Region),2021,54.6,per cent,56.4,52.8,
Aged 16 to 64 years level 3 or above qualifications,Bassetlaw,2021,52.6,per cent,65.7,39.5,
Aged 16 to 64 years level 3 or above qualifications,Epping Forest,2021,56.7,per cent,68.4,45.0,
Aged 16 to 64 years level 3 or above qualifications,East Staffordshire,2021,48.9,per cent,62.4,35.4,
Aged 16 to 64 years level 3 or above qualifications,Winchester,2021,67.4,per cent,80.5,54.3,
Aged 16 to 64 years level 3 or above qualifications,Windsor and Maidenhead,2021,73.8,per cent,78.9,68.7,
Aged 16 to 64 years level 3 or above qualifications,Shropshire,2021,60.8,per cent,67.2,54.4,
Aged 16 to 64 years level 3 or above qualifications,Hampshire,2021,60.6,per cent,64.2,57.0,
Aged 16 to 64 years level 3 or above qualifications,Tewkesbury,2021,99.0,per cent,72.3,46.7,
Aged 16 to 64 years level 3 or above qualifications,Merseyside,2021,99.0,per cent,62.7,56.5,
Aged 16 to 64 years level 3 or above qualifications,West Northamptonshire,2021,99.0,per cent,60.6,47.0,
Aged 16 to 64 years level 3 or above qualifications,Cambridgeshire,2021,99.0,per cent,70.8,60.6,
```
#adding another 10 revisions and ensuring autoincrement is correct
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/5"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/6"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/7"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/8"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/9"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/10"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/11"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/12"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/13"

POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
{
    "dcterms:title": "Revisions"
}

HTTP 201
[Asserts]
jsonpath "$.['@id']" endsWith "/revision/14"

#Create revision with query params only
POST {{scheme}}://{{host_name}}/data/{{series}}/release/release-1/revisions
Accept: application/json
Content-Type: application/json
Authorization: {{auth_token}}
[QueryStringParams]
title: Final Revisions


HTTP 201


