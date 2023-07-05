const fetch = require('node-fetch');
const data = require('./data/series.json');
const FormData = require('form-data');
const fs = require('fs');

// const openAPI = "http://localhost:3000";
const openAPI = "https://ldapi-prototype.gss-data.org.uk"

createSeries = async (series) => {
    let title = data[i].title
    let description = data[i].description

    let url = `${openAPI}/data/${series}?title=${title}&description=${description}`
    const response = await fetch(url, {
        method: 'PUT'
    });
    const api = await response.json();

    url = url.split("?")[0]
    console.log(`Created: ${url}`)

}

createRelease = async (series) => {
    let releases = data[i].releases
    for (j = 0; j < releases.length; j++) {
        let title = releases[j].title
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}?title=${title}`
        const response = await fetch(url, {
            method: 'PUT'
        });
        const api = await response.json();

        url = url.split("?")[0]
        console.log(`Created: ${url}`)

        await createSchemas(releases, series)
    }
}

createSchemas = async (releases, series) => {
    if (releases[j].schema != null) {
        let schemaFile = `./data/${releases[j].schema}`
        let schema = require(schemaFile);
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}/schemas/schema`
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(schema)
            });

            const api = await response.json();
        } catch (e) {
            console.log(e)
            return e;
        }
        console.log(`Added schema to: ${url}`)
    }
}

createRevision = async (series) => {
    let releases = data[i].releases
    body = `
        {"@context": [
          "https://publishmydata.com/def/datahost/context",
          {
            "@base": "https://example.org/data/${series}/"
          }
        ],
        "dcterms:title": "Revision"}`

    for (j = 0; j < releases.length; j++) {
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}/revisions`
        const response = await fetch(url, {
            method: 'POST',
            body: JSON.stringify(body)
        });
        const api = await response.json();

        let revision = api["@id"]
        console.log(`Created: ${url}/${revision}`)

        await uploadData(releases, revision, url)

    }
}

uploadData = async (releases, revision, url) => {
    if (releases[j].file != null) {
        const formData = new FormData();
        file = releases[j].file
        formData.append('appends', fs.createReadStream(file));
        const settings = {
            method: 'POST',
            body: formData
        };
        try {
            url = `${url}/${revision}/changes`
            const fetchResponse = await fetch(url, settings);
            const data = await fetchResponse.json();

            console.log(`Added data to: ${url}/${revision}`)
        } catch (e) {
            console.log(e)
            return e;
        }
    }
}

start = async () => {
    for (i = 0; i < data.length; i++) {
        let series = ""
        if (data[i].id == null) {
            series = `dataset-${i+1}`
        } else {
            series = data[i].id
        }
        await createSeries(series)
        await createRelease(series)
        await createRevision(series)
    }
}

start()