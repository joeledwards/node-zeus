module.exports = {
  command: 'start <query>',
  desc: 'start a new Athena query',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query', {
      type: 'string',
      desc: 'the query or path to a file containing the query'
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
      desc: 'output summary record(s) as JSON',
      alias: 'j'
    })
    .option('sample-size', {
      type: 'number',
      desc: 'number of bytes to sample from the result data',
      default: 1024,
      alias: 's'
    })
    .option('follow', {
      type: 'boolean',
      desc: 'follow a query, with regular progress reports',
      alias: 'f'
    })
    .option('timeout', {
      type: 'number',
      desc: 'max duration (in seconds) to follow the query',
      default: 3600,
      alias: 'T'
    })
    .option('poll-interval', {
      type: 'number',
      desc: 'polling frequency (in seconds) when following',
      default: 5,
      alias: 'P'
    })
    .option('quiet', {
      type: 'boolean',
      desc: 'no output when following; just status code and queryId',
      alias: 'q'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'increase output (especially on error)',
      alias: 'v'
    })
}

async function handler ({
  query: querySource,
  resultBucket,
  resultPrefix = '',
  token,
  json,
  sampleSize,
  follow,
  pollInterval,
  timeout,
  quiet,
  verbose
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
    const statusReport = require('../lib/status-report')

    const getQuery = async (querySource) => {
      let query = querySource
      let isFile = false

      try {
        const fileStat = await promised(h => fs.stat(querySource, h))
        isFile = fileStat.isFile()
      } catch (error) {
        if (error.code !== 'ENOENT') {
          throw error
        }
      }

      if (isFile) {
        query = await promised(h => fs.readFile(querySource, { encoding: 'utf-8' }, h))
      }

      return query
    }

    const athena = aws.athena()

    const query = await getQuery(querySource)

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
      if (quiet) {
        console.info(JSON.stringify({ queryId }))
      } else {
        console.info(buzJson({
          token,
          queryId,
          resultLocation
        }))
      }
    } else {
      if (quiet) {
        console.info(queryId)
      } else {
        const { bucket, key } = resultLocation
        console.info(`Started query ${c.yellow(queryId)}`)
        console.info(`  ${c.green('s3')}://${c.blue(bucket)}/${c.yellow(key)}`)
      }
    }

    if (follow) {
      // Wait for the query to complete
      await athena.queryDone(queryId, {
        timeout: timeout * 1000,
        pollInterval: pollInterval * 1000,
        progress: !quiet,
        quiet
      })

      // Report on the outcome of the query
      await statusReport(queryId, {
        aws,
        json,
        sampleSize,
        athena,
        quiet,
        extended: !quiet
      })
    }
  } catch (error) {
    console.error(`Error ${follow ? 'running' : 'starting'} query: ${c.red(error)}`)
    if (verbose) {
      console.error('Failure Details:\n', error)
    }
    process.exit(1)
  }
}
