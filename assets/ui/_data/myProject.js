module.exports = function () {
  return {
    account_id: process.env.AWS_ACCOUNT_ID || "undefined",
    environment: process.env.MY_ENVIRONMENT || "development",
    emr_studio_url: process.env.EMR_STUDIO_URL || "undefined",
    emr_serverless_app_id: process.env.EMR_SERVERLESS_APP_ID || "undefined",
  };
};
