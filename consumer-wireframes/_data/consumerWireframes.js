module.exports = function() {
    return {
      environment: process.env.LDAPI_BASE_URL ? process.env.LDAPI_BASE_URL : "https://ldapi-dev.gss-data.org.uk"
    };
  };