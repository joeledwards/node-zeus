module.exports = {
  command: 'start <query-file>',
  desc: 'start a new Athena query',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query-file', {
      type: 'string',
      desc: 'the SQL file containing the Athena query to invoke'
    })
    .option('result-bucket', {
      type: 'string',
      desc: 'S3 bucket for the query results',
      alias: 'b'
    })
    .option('result-prefix', {
      type: 'string',
      desc: 'S3 prefix for the query results',
      alias: 'p'
    })
    .option('token', {
      type: 'string',
      desc: 'a unique identifier for this query, preventing a re-run of the same query',
      alias: 't'
    })
    .option('json', {
      type: 'boolean',
      desc: 'output summary as a JSON record',
      alias: 'j'
    })
}

async function handler ({
  queryFile,
  resultBucket,
  resultPrefix = '',
  token,
  json
}) {
  const c = require('@buzuli/color')

  if (!resultBucket) {
    console.error(c.red('No result bucket specified.'))
    process.exit(1)
  }

  try {
    const fs = require('fs')
    const aws = require('../lib/aws')
    const buzJson = require('@buzuli/json')
    const promised = require('../lib/promised')

    const athena = aws.athena()

    const query = await promised(h => fs.readFile(queryFile, { encoding: 'utf-8' }, h))

    const {
      queryId,
      resultLocation
    } = await athena.startQuery({
      resultBucket,
      resultPrefix,
      query,
      progress: true,
      token
    })

    if (json) {
      console.info(buzJson({
        token,
        queryId,
        resultLocation
      }))
    } else {
      const { bucket, key } = resultLocation
      console.info(`Started query ${c.yellow(queryId)}`)
      console.info(`  ${c.green('s3')}://${c.blue(bucket)}/${c.yellow(key)}`)
    }
  } catch (error) {
    console.error(c.red(`Error starting query: ${c.yellow(error)}`))
    process.exit(1)
  }
}
