const s3 = require('./s3')
const aws = require('./aws')
const athena = require('./athena')

module.exports = {
  athena,
  s3,
  sdk: aws.sdk
}
