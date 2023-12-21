# datahost-helpers: Manage dataset scripts

Getting started:
`npm install`

Tested with `Node.js v18.17.1`

## Create datasets

Designed to get a set of datasets into a known state.

Will delete then recreate the datasets series, releases and revisions within the `series.json` file. Will also upload the files and schemas referenced there.

`node index.js ./data/series.json`

Some test data can be loaded with

`node index.js ./data/test.json`

### Deletes

`node delete.js all`
`node delete.js http://127.0.0.1:3000/data/test-data`


### Notes

Each script has a toggle to define if running against a local API or the live one. Comment out the appropriate one to point script at that API.

```
const openAPI = "http://127.0.0.1:3000";
// const openAPI = "https://ldapi-prototype.gss-data.org.uk"
```

**Note: Node requires the local URL to be `127.0.0.1` rather than `localhost`**

Running against the live site will require authentication. This can be added using a `user.json` file to the `/data` directory with the following format.

```
{
    "username": "",
    "password": ""
} 
```

