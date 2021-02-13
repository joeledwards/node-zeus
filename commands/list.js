const handler = require('../lib/handler')

module.exports = {
  command: 'list',
  desc: 'list Athena queries',
  builder,
  handler: handler(listQueries)
}

function builder (yargs) {
  yargs
    .env('ZEUS')
    .option('limit', {
      type: 'number',
      desc: 'maximum number of queries to list',
      default: 10,
      alias: 'l'
    })
    .option('extended', {
      type: 'boolean',
      desc: 'fetch extended info for each query',
      alias: 'x'
    })
    .option('filter', {
      type: 'string',
      desc: 'a filter which will be applied to extended info if fetched',
      alias: 'f'
    })
    .option('json', {
      type: 'boolean',
      desc: 'output summary as a JSON record',
      alias: 'j'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'increase output (especially on error)',
      alias: 'v'
    })
}

async function listQueries ({
  aws,
  options: {
    limit,
    extended,
    json,
    verbose
  }
}) {
  const c = require('@buzuli/color')

  try {
    const buzJson = require('@buzuli/json')
    const prettyBytes = require('pretty-bytes')
    const { seconds } = require('durations')

    const athena = aws.athena()

    const tf = (v, d) => (f) => (v == null) ? (d || v) : f(v)

    for await (const query of athena.scanQueries({ limit, extended })) {
      if (json) {
        query.submittedAt = tf(query.submittedAt, '--')(s => s.toISOString())
        query.completedAt = tf(query.completedAt, '--')(s => s.toISOString())

        console.info(buzJson(query, { indent: !!extended }))
      } else if (extended) {
        const {
          queryId,
          schema,
          bytesScanned = 0,
          executionTime = 0.0,
          submittedAt,
          state
        } = query

        const idStr = c.yellow(queryId)
        const dbStr = c.blue(schema)
        const stateStr = athena.stateColor(state)
        const sizeStr = c.yellow(prettyBytes(bytesScanned))
        const bytesStr = c.orange(bytesScanned.toLocaleString())
        const costStr = c.green((bytesScanned / 1000000000000 * 5.0).toFixed(2))
        const timeStr = c.blue(seconds(executionTime))
        const startStr = c.grey(tf(submittedAt, '--')(s => s.toISOString()))

        const idInfo = `${idStr} @ ${dbStr}`
        const scanInfo = `scanned ${sizeStr} (${bytesStr} bytes | $${costStr})`
        const timeInfo = `in ${timeStr} {${startStr}}`

        console.info(`${idInfo} [${stateStr}] ${scanInfo} ${timeInfo}`)
      } else {
        console.info(query.queryId)
      }
    }
  } catch (error) {
    console.error(c.red(`Error listing queries: ${c.yellow(error)}`))
    if (verbose) {
      console.error('Failure Details:\n', error)
    }
    process.exit(1)
  }
}
