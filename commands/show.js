const handler = require('../lib/handler')

module.exports = {
  command: 'show <query-id>',
  desc: 'show the details of a query',
  builder,
  handler: handler(showQuery)
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query-id', {
      type: 'string',
      desc: 'the ID of the query to detail'
    })
    .option('json', {
      type: 'boolean',
      desc: 'output summary as a JSON record',
      alias: 'j'
    })
    .option('extended', {
      type: 'boolean',
      desc: 'extended data (full query and sample data)',
      alias: 'x'
    })
    .option('sample-size', {
      type: 'number',
      desc: 'number of bytes to sample from the result data',
      default: 1024,
      alias: 's'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'increase output (especially on error)',
      alias: 'v'
    })
}

async function showQuery ({
  aws,
  options: {
    queryId,
    json,
    extended,
    sampleSize,
    verbose
  }
}) {
  const c = require('@buzuli/color')
  const statusReport = require('../lib/status-report')

  try {
    await statusReport(queryId, { aws, json, extended, sampleSize })
  } catch (error) {
    console.error(`Error showing info for query ${c.yellow(queryId)}: ${c.red(error)}`)
    if (verbose) {
      console.error('Failure Details:\n', error)
    }
    process.exit(1)
  }
}
