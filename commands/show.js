module.exports = {
  command: 'show <query-id>',
  desc: 'show the details of a query',
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
    .option('json', {
      type: 'boolean',
      desc: 'output summary as a JSON record',
      alias: 'j'
    })
}

async function handler ({
  queryId,
  json
}) {
  const c = require('@buzuli/color')

  try {
    const aws = require('../lib/aws')
    const buzJson = require('@buzuli/json')
    const prettyBytes = require('pretty-bytes')
    const { seconds } = require('durations')

    const s3 = aws.s3()
    const athena = aws.athena()

    const queryInfo = await athena.queryStatus(queryId)

    if (json) {
      const tf = (v) => (f) => (v == null) ? v : f(v)
      queryInfo.submitted = tf(queryInfo.submitted)(s => s.toISOString())
      queryInfo.completed = tf(queryInfo.completed)(s => s.toISOString())

      console.info(buzJson(queryInfo))
    } else {
      const {
        queryId,
        schema,
        bytesScanned,
        executionTime,
        submitted,
        completed,
        finished,
        state,
        outputLocation: {
          bucket,
          key
        }
      } = queryInfo

      const succeeded = finished && state === 'SUCCEEDED'

      const idStr = c.yellow(queryId)
      const dbStr = c.blue(schema)
      const stateStr = athena.stateColor(state)
      const sizeStr = c.yellow(prettyBytes(bytesScanned))
      const bytesStr = c.orange(bytesScanned.toLocaleString())
      const costStr = c.green((bytesScanned / 1000000000000 * 5.0).toFixed(2))
      const timeStr = c.blue(seconds(executionTime))
      const startStr = c.grey(submitted.toISOString())
      const endStr = c.grey(completed.toISOString())

      console.info(`${idStr}@${dbStr} [${stateStr}]`)
      console.info(`  submitted : ${startStr}`)

      if (finished) {
        console.info(`  completed : ${endStr}`)
      }

      console.info(`   ${finished ? 'duration' : ' elapsed'} : ${timeStr}`)
      console.info(`    scanned : ${sizeStr} (${bytesStr} bytes)`)
      console.info(`       cost : $${costStr}`)

      if (succeeded) {
        console.info(`    results : ${s3.makeUri({ bucket, key, color: true })}`)
      }
    }
  } catch (error) {
    console.error(c.red(`Error showing query info: ${c.yellow(error)}`))
    process.exit(1)
  }
}
