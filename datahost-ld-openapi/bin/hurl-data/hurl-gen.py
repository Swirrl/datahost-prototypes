# hurl file generator
USAGE = """Let's say you have a bunch of CSV files you want to upload:

    file1.csv
    file2.csv
    file3.csv

And a schema file `schema.json`.

You can generate a hurl script by running this script like this:
    
    python3 hurl-gen.py schema.json revision appends:file1.csv retractions:file2.csv corrections:file3.csv > bug-repro.hurl

The order is important: schema file should always be first. 
Remaining arguments should have the format `CHANGE_KIND:FILE_NAME`,
where change kind is one of: appends, retractions, corrections.

Note: Hurl expects the file paths to be relative to the hurl script location. 

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
""".strip()

REVISION_SETUP = """
POST http://localhost:3000/data/{{series}}/releases/release-1/revisions
Accept: application/json
Content-Type: application/json
{
    "dcterms:title": "Rev $REV_NUMBER",
    "dcterms:description": "A test revision"
}

HTTP 201
[Captures]
$REV_URL_VAR: header "Location"
"""

CHANGE_SETUP = """
POST http://localhost:3000{{$REV_URL_VAR}}/$CHANGE_KIND
Content-Type: text/csv
[QueryStringParams]
title: changes $CHANGE_KIND
description: change for {{series}}
file,$CHANGE_FILE;

HTTP 201
"""


def main(args):
    if not args[1] == 'revision':
        raise Exception('sequence should start with a revision')

    rev_number = 1
    result = [RELEASE_SETUP.replace("$SCHEMA_FILE", args[0]),
              REVISION_SETUP.replace("$REV_URL_VAR", f"revision{rev_number}_url")
                            .replace("$REV_NUMBER", f"{rev_number}")]

    for index in range(2, len(args)):
        if args[index] == 'revision':
            rev_number = rev_number + 1
            rev = REVISION_SETUP.replace("$REV_URL_VAR", f"revision{rev_number}_url")
            rev = rev.replace("$REV_NUMBER", f"{rev_number}")
            result.append(rev)
        else:
            kind,file = args[index].split(":")
            change = CHANGE_SETUP.replace("$REV_URL_VAR", f"revision{rev_number}_url")
            change = change.replace("$CHANGE_KIND", kind)
            change = change.replace("$CHANGE_FILE", file)
            result.append(change)
    result.append("# run: hurl FILE_NAME --variable series=series-01")
    return result

if __name__ == '__main__':
    if len(sys.argv) > 2:
        print("\n".join(main(sys.argv[1:])))
    else:
        sys.stderr.write(USAGE)
        sys.exit(1)
