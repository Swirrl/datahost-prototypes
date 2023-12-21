const fetch = require('node-fetch');
const base64 = require('base-64');

const user = require('./data/user.json');
const login = user.username
const password = user.password

const args = process.argv.slice(2);
const type = args[0];

const openAPI = "http://127.0.0.1:3000";
// const openAPI = "https://ldapi-prototype.gss-data.org.uk"

deleteSeries = async (series) => {
    const response = await fetch(series, {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    });

    console.log(`Deleted: ${series}`)
}

fetchSeries = async () => {
    let series = `${openAPI}/data`
    const response = await fetch(series, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            "Authorization": `Basic ${base64.encode(`${login}:${password}`)}`
        },
    });
    const data = await response.json();
    datasetSeries = data.contents

    for (i = 0; i < datasetSeries.length; i++) {
        if (openAPI === "http://127.0.0.1:3000") {
            uri = datasetSeries[i]["dh:baseEntity"].replace('localhost:3000', '127.0.0.1:3000')
        } else {
            uri = datasetSeries[i]["dh:baseEntity"]
        }
        await deleteSeries(uri)
    }
}

start = async () => {
    let uri = ""
    if (type === "all") {
        fetchSeries()
    } else {
        console.log("here")
        if (openAPI === "http://127.0.0.1:3000") {
            uri = type.replace('localhost:3000', '127.0.0.1:3000')
        } else {
            uri = type
        }
        await deleteSeries(uri)
    }
}

start()