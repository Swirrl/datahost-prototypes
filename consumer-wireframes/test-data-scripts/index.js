const fetch = require('node-fetch');
const base64 = require('base-64');
const data = require('./data/series.json');
const FormData = require('form-data');
const fs = require('fs');
const user = require('./data/user.json');
const login = user.username
const password = user.password

const openAPI = "http://127.0.0.1:3000";
// const openAPI = "https://ldapi-prototype.gss-data.org.uk"

createSeries = async (series) => {
    let title = data[i].title
    let description = data[i].description
    let url = `${openAPI}/data/${series}?title=${title}&description=${description}`
    const response = await fetch(url, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    });
    const api = await response.json();

    url = url.split("?")[0]
    console.log(`Created: ${url}`)

}

createRelease = async (series) => {
    let releases = data[i].releases
    for (j = 0; j < releases.length; j++) {
        let title = releases[j].title
        let description = releases[j].description
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}?title=${title}&description=${description}`
        const response = await fetch(url, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
            },
        });
        const api = await response.json();
        url = `${openAPI}/data/${api["@id"]}`
        console.log(`Created: ${url}`)

        await createSchemas(releases, series)
    }
}

createSchemas = async (releases, series) => {
    if (releases[j].schema != null) {
        let schemaFile = `./data/${releases[j].schema}`
        let schema = require(schemaFile);
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}/schema`
        // const formData = new FormData();
        // formData.append('schema-file', fs.createReadStream(schemaFile));
        const settings = {
            method: 'POST',
            body: JSON.stringify(schema),
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
            },
        };
        try {
            const fetchResponse = await fetch(url, settings);
            const data = await fetchResponse.json();
            console.log(`Added schema to: ${url}`)
        } catch (e) {
            console.log(e)
            return e;
        }
    }
}

createRevision = async (series) => {
    let releases = data[i].releases
    for (j = 0; j < releases.length; j++) {
        if (releases[j].revisions != null) {
            let id = releases[j].id
            let url = `${openAPI}/data/${series}/releases/${id}/revisions`
            for (k = 0; k < releases[j].revisions.length; k++) {
                let file = releases[j].revisions[k].file
                let title = releases[j].revisions[k].title
                let description = releases[j].revisions[k].description
                body = `{"dcterms:title":"${title}","dcterms:description":"${description}"}`

                await postRevision(url, file, title, description)
            }
        }
    }
}

postRevision = async (url, file, title, description) => {
    url = url + `?title=${title}&description=${description}`
    const response = await fetch(url, {
        method: 'POST',
        body: body,
        headers: {
            'Content-Type': 'application/json',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    });
    const api = await response.json()
    let revision = api["@id"]
    console.log(`Created: ${openAPI}/data/${revision}`)

    await uploadData(revision, file)
}

uploadData = async (revision, file) => {
    const formData = new FormData();
    formData.append('appends', fs.createReadStream(file));
    const settings = {
        method: 'POST',
        body: fs.createReadStream(file),
        headers: {
            'Content-Type': 'text/csv',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    };
    try {
        url = `${openAPI}/data/${revision}/appends?dcterms%3Atitle=test&dcterms%3Adescription=test&dcterms%3Aformat=text%2Fcsv`
        const fetchResponse = await fetch(url, settings);
        const data = await fetchResponse.json();
        console.log(`Added data to: ${url}`)
    } catch (e) {
        console.log(e)
        return e;
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