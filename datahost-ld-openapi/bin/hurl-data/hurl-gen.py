# hurl file generator
USAGE = """
Let's say you have a bunch of CSV files you want to upload:

	file1.csv
	file2.csv
	file3.csv

And a schema file `schema.json`.

You can generate a hurl script by running this script like this:
	
	python3 hurl-gen.py schema.json appends:file1.csv retractions:file2.csv corrections:file3.csv > bug-repro.hurl

The order is important: schema file should always be first. 
Remaining arguments should have the format `CHANGE_KIND:FILE_NAME`,
where change kind is one of: appends, retractions, corrections.

Hurl expects the file paths to be relative to the hurl script location. 


"""

import sys

RELEASE_SETUP = """
PUT http://localhost:3000/data/{{series}}
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Dataset",
	"dcterms:description": "A very simple test"
}

HTTP 201
[Captures]
dataset: jsonpath "$['dh:baseEntity']"

PUT http://localhost:3000/data/{{series}}/releases/release-1
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Test Release",
	"dcterms:description": "A very simple Release"
}

HTTP 201

POST http://localhost:3000/data/{{series}}/releases/release-1/schema
Content-Type: application/json
file,$SCHEMA_FILE;

HTTP 201
"""

REVISION_SETUP = """
POST http://localhost:3000/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
	"dcterms:title": "Rev 1",
	"dcterms:description": "A test revision"
}

HTTP 201
[Captures]
$REV_URL_VAR: header "Location"

POST http://localhost:3000{{$REV_URL_VAR}}/$CHANGE_KIND
Content-Type: text/csv
[QueryStringParams]
title: changes $CHANGE_KIND
description: change for {{series}}
format: text/csv
file,$CHANGE_FILE;

HTTP 201
"""

def main(argv):
	""" $SCHEMA_FILE """
	result = [RELEASE_SETUP.replace("$SCHEMA_FILE", argv[0])]
	for i in range(1, len(argv)):
		kind, file = argv[i].split(':')
		rev = REVISION_SETUP.replace("$REV_URL_VAR", f"revision{i}_url")
		rev = rev.replace("$CHANGE_KIND", kind)
		rev = rev.replace("$CHANGE_FILE", file)
		result.append(rev)

	result.append("# run: hurl FILE_NAME --variable series=series-01")
	print("\n".join(result))


if __name__ == '__main__':
	if len(sys.argv) > 1:
		main(sys.argv[1:])
	else:
		sys.stderr.write(USAGE)
		sys.exit(1)
