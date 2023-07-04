const fetch = require('node-fetch');
const data = require('./data/series.json');
const FormData = require('form-data');
const fs = require('fs');

const openAPI = "http://localhost:3000";
// const openAPI = "https://ldapi-prototype.gss-data.org.uk"

createSeries = async () => {
    for (i = 0; i < data.length; i++) {
        let title = data[i].title
        let id = ""
        if (data[i].id == null) {
            id = `dataset-${i+1}`
        } else {
            id = data[i].id
        }
        let description = data[i].description

        let url = `${openAPI}/data/${id}?title=${title}&description=${description}`
        const response = await fetch(url, {
            method: 'PUT'
        });
        const api = await response.json();

        url = url.split("?")[0]
        console.log(`Created: ${url}`)
    }
}

createRelease = async () => {
    for (i = 0; i < data.length; i++) {
        let series = ""
        let releases = data[i].releases
        if (data[i].id == null) {
            series = `dataset-${i+1}`
        } else {
            series = data[i].id
        }
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
        }
    }
}

createRevision = async () => {
    for (i = 0; i < data.length; i++) {
        let series = ""
        let releases = data[i].releases
        if (data[i].id == null) {
            series = `dataset-${i+1}`
        } else {
            series = data[i].id
        }
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

            //upload files to revision where provided
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
    }
}

start = async () => {
    console.log("Starting: Loading Dataset Series")
    await createSeries()
    console.log("Complete: Loading Dataset Series")

    console.log("Starting: Loading Dataset Releases")
    await createRelease()
    console.log("Complete: Loading Dataset Releases")

    console.log("Starting: Creating revision for each Release")
    await createRevision()
    console.log("Complete: Creating revision for each Release")
}

start()