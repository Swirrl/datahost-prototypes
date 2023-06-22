const fetch = require('node-fetch');
const data = require('./data/series.json');
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
            let url = `${openAPI}/data/${series}/release/${id}?title=${title}`
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
    body = `
    {"@context": [
      "https://publishmydata.com/def/datahost/context",
      {
        "@base": "https://example.org/data/population-aged-16-to-64-years-level-3-or-above-qualifications/"
      }
    ],
    "dcterms:title": "Revision"}`

    for (i = 0; i < data.length; i++) {
        let series = ""
        let releases = data[i].releases
        if (data[i].id == null) {
            series = `dataset-${i+1}`
        } else {
            series = data[i].id
        }
        for (j = 0; j < releases.length; j++) {
            let id = releases[j].id
            let url = `${openAPI}/data/${series}/release/${id}/revisions`
            const response = await fetch(url, {
                method: 'POST',
                body: JSON.stringify(body)
            });
            const api = await response.json();
            let revision = api["@id"]
            console.log(`Created: ${url}/${revision}`)
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