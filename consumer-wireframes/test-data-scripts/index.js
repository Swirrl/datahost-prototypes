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
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}?title=${title}&description=test`
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
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}/schema`
        const formData = new FormData();
        formData.append('schema-file', fs.createReadStream(schemaFile));
        const settings = {
            method: 'POST',
            body: formData,
            headers: {
                "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
            },
        };
        try {
            const fetchResponse = await fetch(url, settings);
            const data = await fetchResponse.json();
            console.log(`Added data to: ${url}`)
        } catch (e) {
            console.log(e)
            return e;
        }
        console.log(`Added schema to: ${url}`)
    }
}

createRevision = async (series) => {
    let releases = data[i].releases
    body = `{"dcterms:title":"Revision","dcterms:description":"Test"}`
    for (j = 0; j < releases.length; j++) {
        let id = releases[j].id
        let url = `${openAPI}/data/${series}/releases/${id}/revisions`
        if (releases[j].revisions != null) {
            for (k = 0; k < releases[j].revisions.length; k++) {
                let file = releases[j].revisions[k].file

                await postRevision(url, file)
            }
        }
    }
}

postRevision = async (url, file) => {
    url = url + "?title=Revision&description=Test"
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
    const obj = {
        'dcterms:title': "Test",
        'dcterms:description': "Test desc",
        'dcterms:format': "text/csv"
      };
    formData.append('appends', fs.createReadStream(file));
    formData.append("jsonld-doc", JSON.stringify(obj));
    const settings = {
        method: 'POST',
        body: formData,
        headers: {
            'Content-Type': 'application/json-ld',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    };
    try {
        url = `${openAPI}/data/${revision}/changes`
        const fetchResponse = await fetch(url, settings);
        console.log(fetchResponse)
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