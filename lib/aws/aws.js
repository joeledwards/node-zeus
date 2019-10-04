const sdk = require('aws-sdk')

module.exports = {
  getConfig,
  sdk
}

function getConfig (awsConfig) {
  let config

  if (awsConfig) {
    const {
      region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
      accessKey = process.env.AWS_ACCESS_KEY || process.env.AWS_ACCESS_KEY_ID,
      secretKey = process.env.AWS_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY
    } = awsConfig

    config = {
      region,
      accessKeyId: accessKey,
      secretAccessKey: secretKey
    }
  }

  return config
}
