const s3 = require('./s3')
const aws = require('./aws')
const ses = require('./ses')
const sqs = require('./sqs')
const athena = require('./athena')

module.exports = {
  athena,
  s3,
  ses,
  sqs,
  sdk: aws.sdk
}
