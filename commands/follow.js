const handler = require('../lib/handler')

module.exports = {
  command: 'follow <query-id>',
  desc: 'follow an existing Athena query',
  builder,
  handler: handler(follow)
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query-id', {
      type: 'string',
      desc: 'the ID of the Athena query to follow'
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

async function follow ({
  aws,
  options: {
    queryId,
    pollInterval,
    timeout,
    quiet,
    verbose
  }
}) {
  const c = require('@buzuli/color')
  const statusReport = require('../lib/status-report')

  const athena = aws.athena()

  try {
    // Wait for the query to complete
    await athena.queryDone(queryId, {
      timeout: timeout * 1000,
      pollInterval: pollInterval * 1000,
      progress: !quiet,
      quiet
    })

    // Report on the outcome of the query
    await statusReport(queryId, { aws, quiet, extended: !quiet })
  } catch (error) {
    console.error(`Error following query ${c.yellow(queryId)}: ${c.red(error)}`)
    if (verbose) {
      console.error('Failure Details:\n', error)
    }
    process.exit(1)
  }
}
