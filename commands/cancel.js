module.exports = {
  command: 'cancel <query-id>',
  desc: 'cancel the identified query',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .positional('query-id', {
      type: 'string',
      desc: 'the ID of the query to detail'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'increase output (especially on error)',
      alias: 'v'
    })
}

async function handler ({
  queryId,
  verbose
}) {
  const c = require('@buzuli/color')
  const aws = require('../lib/aws')
  const athena = aws.athena()

  try {
    await athena.cancelQuery(queryId)
    console.info(`Query ${c.yellow(queryId)} cancelled.`)
  } catch (error) {
    console.error(`Error cancelling query ${c.yellow(queryId)}: ${c.red(error)}`)
    if (verbose) {
      console.error('Failure Details:\n', error)
    }
    process.exit(1)
  }
}
