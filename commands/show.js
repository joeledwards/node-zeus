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
    const promised = require('../lib/promised')
    const { seconds } = require('durations')

    const s3 = aws.s3()
    const athena = aws.athena()

    const {
      bytesScanned,
      executionTime,
      finished,
      state,
      outputLocation
    } = await athena.queryStatus(queryId)

    const succeeded = finished && state === 'SUCCEEDED'

    if (json) {
      console.info(buzJson({
        queryId,
        bytesScanned,
        executionTime,
        finished,
        state,
        outputLocation
      }))
    } else {
      const { bucket, key } = outputLocation
      console.info(`Query ${c.yellow(queryId)} ${athena.stateColor(state)} in ${c.blue(seconds(executionTime))}`)
      if (succeeded) {
        console.info(` ${s3.makeUri({ bucket, key, color: true })}`)
      }
    }
  } catch (error) {
    console.error(c.red(`Error showing query info: ${c.yellow(error)}`))
    process.exit(1)
  }
}
