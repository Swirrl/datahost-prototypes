module.exports = function() {
    return {
      environment: process.env.LDAPI_SERVICE_URL ? process.env.LDAPI_SERVICE_URL : "https://ldapi-dev.gss-data.org.uk"
    };
  };