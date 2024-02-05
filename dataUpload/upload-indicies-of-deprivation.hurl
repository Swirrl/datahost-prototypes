# Description: This test creates three revisions, 1 commit in each (1st rev + append, 2nd rev + retractions, 
# 3rd rev + corrections).

# hurl upload-indicies-of-deprivation.hurl --variable admin_password=loadmewithdata3250 --variable series=English-Indices-of-Deprivation --variable datahost-base-url=https://dluhc-pmd5-prototype.publishmydata.com
# run locally with docker compose:
# hurl upload-indicies-of-deprivation.hurl --variable admin_password=loadmewithdata3250 --variable series=English-Indices-of-Deprivation --variable datahost-base-url=http://localhost:8889
 
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
	"dcterms:title": "Indices of Deprivation",
	"dcterms:description": "The Indices of Deprivation provide a set of relative measures of deprivation for small areas (Lower-layer Super Output Areas*) across England, based on seven domains of deprivation.",
    "dcat:theme": "http://opendatacommunities.org/def/concept/folders/themes/societal-wellbeing",
    "dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "rdfs:comment": "Statistics on relative deprivation in small areas in England."
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"


PUT {{datahost-base-url}}/data/{{series}}/release/2007-score
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2007 - Score",
	"dcterms:description": "This dataset contains the scores underlying the Index of Multiple Deprivation, 2007. These figures combine values of many indicators into a single score that indicates the overall level of deprivation in each LSOA. A high number indicates a high level of deprivation",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2007.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."
}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2007-score/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Index-of-Multiple-Deprivation-2007-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2007-score/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2007 - score",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "August 02, 2013 14:08",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
2007score_url: header "Location"


POST {{datahost-base-url}}{{2007score_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Index-of-Multiple-Deprivation-2007-Score.csv;

HTTP 201


PUT {{datahost-base-url}}/data/{{series}}/release/2007-rank
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2007 - Rank",
	"dcterms:description": "This dataset contains the main ranking for the Index of Multiple Deprivation, 2007. This dataset puts the 32,482 LSOAs into rank order based on their 2007 IMD score. A rank of 1 is the most deprived.",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2007.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."
}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2007-rank/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Index-of-Multiple-Deprivation-2007-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2007-rank/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2007 - Rank",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "August 02, 2013 14:08",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
2007rank_url: header "Location"


POST {{datahost-base-url}}{{2007rank_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Index-of-Multiple-Deprivation-2007-Rank.csv;

HTTP 201

PUT {{datahost-base-url}}/data/{{series}}/release/2010-score
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2010 - Score",
	"dcterms:description": "This dataset contains the scores underlying the Index of Multiple Deprivation, 2010. These figures combine values of many indicators into a single score that indicates the overall level of deprivation in each LSOA. A high number indicates a high level of deprivation",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2010.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."
}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2010-score/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Index-of-Multiple-Deprivation-2010-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2010-score/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2010 - score",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "August 02, 2013 14:08",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
2010score_url: header "Location"


POST {{datahost-base-url}}{{2010score_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Index-of-Multiple-Deprivation-2010-Score.csv;

HTTP 201


PUT {{datahost-base-url}}/data/{{series}}/release/2010-rank
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2010 - Rank",
	"dcterms:description": "This dataset contains the main ranking for the Index of Multiple Deprivation, 2010. This dataset puts the 32,482 LSOAs into a rank order based on their 2010 IMD score. A rank of 1 is the most deprived.",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2010.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."
}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2010-rank/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,Index-of-Multiple-Deprivation-2010-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2010-rank/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2010 - Rank",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "August 02, 2013 14:08",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
}

HTTP 201
[Captures]
2010rank_url: header "Location"


POST {{datahost-base-url}}{{2010rank_url}}/appends
Content-Type: text/csv
[BasicAuth]
idp: {{admin_password}}
[QueryStringParams]
title: changes (appends)
description: change for {{series}}
format: text/csv
file,Index-of-Multiple-Deprivation-2010-Rank.csv;

HTTP 201


PUT {{datahost-base-url}}/data/{{series}}/release/2015
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2015",
	"dcterms:description": "The Indices of Deprivation 2015 provide a set of relative measures of deprivation for small areas (Lower-layer Super Output Areas*) across England, based on seven domains of deprivation. The domains were combined using the following weights to produce the overall Index of Multiple Deprivation (IMD): • Income Deprivation (22.5%) • Employment Deprivation (22.5%) • Education, Skills and Training Deprivation (13.5%) • Health Deprivation and Disability (13.5%) • Crime (9.3%) • Barriers to Housing and Services (9.3%) • Living Environment Deprivation (9.3%) In addition to the Index of Multiple Deprivation and the seven domain indices, there are two supplementary indices: the Income Deprivation Affecting Children Index (IDACI) and the Income Deprivation Affecting Older People Index (IDAOPI). *LSOAs (Lower-layer Super Output Areas) are small areas designed to be of a similar population size, with an average of approximately 1,500 residents or 650 households.",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2015 at LSOA level.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."

}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2015/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,English-Indices-of-Deprivation-2015-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2015/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2015 - LSOA Level",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "September 30, 2015 08:23",
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
file,English-Indices-of-Deprivation-2015.csv;

HTTP 201



PUT {{datahost-base-url}}/data/{{series}}/release/2019
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "2019",
	"dcterms:description": "The Indices of Deprivation 2019 provide a set of relative measures of deprivation for small areas (Lower-layer Super Output Areas*) across England, based on seven domains of deprivation. The domains were combined using the following weights to produce the overall Index of Multiple Deprivation (IMD): • Income Deprivation (22.5%) • Employment Deprivation (22.5%) • Education, Skills and Training Deprivation (13.5%) • Health Deprivation and Disability (13.5%) • Crime (9.3%) • Barriers to Housing and Services (9.3%) • Living Environment Deprivation (9.3%) In addition to the Index of Multiple Deprivation and the seven domain indices, there are two supplementary indices: the Income Deprivation Affecting Children Index (IDACI) and the Income Deprivation Affecting Older People Index (IDAOPI). *LSOAs (Lower-layer Super Output Areas) are small areas designed to be of a similar population size, with an average of approximately 1,500 residents or 650 households. There are 32,844 Lower-layer Super Output Areas (LSOAs) in England. They were produced by the Office for National Statistics for the reporting of small area statistics. Following the 2011 Census, the geography of Lower-layer Super Output Areas was revised and the number of areas has increased from 32,482 (as used for the Indices of Deprivation 2010, 2007 and 2004) to 32,844 (used for the Indices of Deprivation 2019). The boundaries of the vast majority (96 per cent) of these 32,844 areas are unchanged since the 2010, 2007 and 2004 Indices. A range of summary measures are available for higher-level geographies including local authority districts and upper-tier local authorities, local enterprise partnerships, and clinical commissioning groups. The Index of Multiple Deprivation, domain indices and the supplementary indices, together with the higher-level geography summaries, are collectively referred to as the Indices of Deprivation. Chapter 3 of the Research Report gives advice on using and interpreting the data. Most of the data used in the Indices of Deprivation 2015 relates to the tax year 2012/13 - see Chapter 4 of the Technical Report for more details. All of the data files and supporting documents for the English Indices of Deprivation 2015 are available at http://www.gov.uk/government/statistics/english-indices-of-deprivation-2019 . For statistical enquiries, please contact: mailto:indices.deprivation@communities.gsi.gov.uk",
	"dcterms:license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
	"rdfs:comment": "A range of measures which form the Indices of Deprivation 2019 at LSOA level.",
	"dh:reasonForChange": "Changes between Indices may mean that care should be taken when comparing iterations over time."
}
HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2019/schema
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
file,English-Indices-of-Deprivation-2019-schema.json;

HTTP 201


POST {{datahost-base-url}}/data/{{series}}/release/2019/revisions
Accept: application/json
Content-Type: application/json
[BasicAuth]
idp: {{admin_password}}
{
	"dcterms:title": "English Indices of Deprivation 2019 - LSOA Level",
	"dcterms:description": "Initial upload",
	"dh:publicationDate": "September 30, 2019 08:23",
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
file,English-Indices-of-Deprivation-2019.csv;

HTTP 201


GET {{datahost-base-url}}/data/{{series}}/release/2019
Accept: text/csv
[BasicAuth]
idp: {{admin_password}}

 HTTP 302
[Asserts]
body == ""