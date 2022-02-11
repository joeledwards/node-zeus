const handler = require('../lib/handler')

module.exports = {
  command: 'start <query>',
  desc: 'start a new Athena query',
  builder,
  handler: handler(startQuery)
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query', {
      type: 'string',
      desc: 'the query or path to a file containing the query'
    })
    .option('workgroup', {
      type: 'string',
      desc: 'The Athena workgroup in which the query should be run',
      alias: ['w', 'wg', 'work-group']
    })
    .option('catalog', {
      type: 'string',
      desc: 'The catalog in which to locate tables (defaults to AWS Glue)',
      alias: ['c']
    })
    .option('database', {
      type: 'string',
      desc: 'The database (schema) in which to locate tables which have not database declared',
      alias: ['d', 'db', 'schema']
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

async function startQuery ({
  aws,
  options: {
    query: querySource,
    workgroup: workGroup,
    catalog,
    database,
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
  }
}) {
  const c = require('@buzuli/color')

  if (!resultBucket) {
    console.error(c.red('No result bucket specified (--result-bucket option or ZEUS_RESULT_BUCKET env. var.).'))
    process.exit(1)
  }

  try {
    const fs = require('fs')
    const buzJson = require('@buzuli/json')
    const promised = require('@buzuli/promised')
    const statusReport = require('../lib/status-report')

    const aws = await (require('@buzuli/aws').resolve())

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
      outputLocation
    } = await athena.startQuery({
      workGroup,
      database,
      catalog,
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
          outputLocation
        }))
      }
    } else {
      if (quiet) {
        console.info(queryId)
      } else {
        const { bucket, key } = outputLocation
        const workGroupStr = workGroup ? `in workgroup ${c.blue(workGroup)}` : 'in the default workgroup'
        console.info(`Started query ${c.yellow(queryId)} ${workGroupStr}`)
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
