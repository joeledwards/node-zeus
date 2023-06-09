module.exports = statusReport

const c = require('@buzuli/color')
const buzJson = require('@buzuli/json')
const prettyBytes = require('pretty-bytes')
const { seconds } = require('durations')

async function statusReport (queryId, {
  json = false,
  quiet = false,
  sampleSize = 1024,
  extended = false,
  aws,
  log = (...args) => console.info(...args)
} = {}) {
  const athena = aws.athena()
  const s3 = aws.s3()

  const queryInfo = await athena.queryStatus(queryId)

  const {
    query,
    schema,
    queryType,
    bytesScanned = 0,
    durations: {
      total: executionTime,
    } = {},
    submittedAt,
    completedAt,
    finished,
    state,
    stateReason,
    workGroup,
    outputLocation: {
      bucket,
      key
    }
  } = queryInfo

  const succeeded = finished && state === 'SUCCEEDED'

  const sample = (succeeded && extended)
    ? await athena.queryResults(queryId, { sampleSize })
    : {}

  const { data, dataSize, partial } = sample

  if (json) {
    if (quiet) {
      log(buzJson({ queryId, workGroup, state: queryInfo.state }, { indent: false }))
    } else {
      const tf = (v) => (f) => (v == null) ? v : f(v)
      queryInfo.submittedAt = tf(queryInfo.submittedAt)(s => s.toISOString())
      queryInfo.completedAt = tf(queryInfo.completedAt)(s => s.toISOString())

      if (extended && data) {
        queryInfo.sample = {
          partial,
          dataSize,
          sampleSize,
          data: data.toString()
        }
      } else {
        queryInfo.query = undefined
      }

      log(buzJson(queryInfo))
    }
  } else {
    if (quiet) {
      console.info(`${c.yellow(queryId)} ${athena.stateColor(queryInfo.state)}`)
    } else {
      const idStr = c.yellow(queryId)
      const dbStr = schema ? ` @ ${c.blue(schema)}` : ''
      const stateStr = athena.stateColor(state)
      const sizeStr = c.yellow(prettyBytes(bytesScanned))
      const bytesStr = c.orange(bytesScanned.toLocaleString())
      const costStr = c.green((bytesScanned / 1000000000000 * 5.0).toFixed(2))
      const timeStr = c.blue(seconds(Number(executionTime) || 0))
      const startStr = c.grey(submittedAt ? submittedAt.toISOString() : '--')
      const endStr = c.grey(completedAt ? completedAt.toISOString() : '--')

      log(`${idStr}${dbStr} [${stateStr}]`)
      log(`     workgroup : ${c.blue(workGroup)}`)
      log(`          type : ${c.purple(queryType)}`)
      log(`     submitted : ${startStr}`)

      if (finished) {
        log(`     completed : ${endStr}`)
      }

      log(`      ${finished ? 'duration' : ' elapsed'} : ${timeStr}`)
      log(`       scanned : ${sizeStr} (${bytesStr} bytes)`)
      log(`          cost : $${costStr}`)

      if (stateReason) {
        log(`  state reason : ${c.yellow(stateReason)}`)
      }

      if (succeeded) {
        log(`       results : ${s3.util.formatUri(bucket, key, { color: true })}`)
      }

      if (extended) {
        log('')
        log(`${c.green('===')} Query ${c.green('===')}`)
        log(query)
      }

      if (succeeded && data) {
        const sampleStr = c.orange(sampleSize.toLocaleString())
        const sizeStr = c.orange(dataSize.toLocaleString())
        const sizeDetails = `${partial ? `${sampleStr} of ${sizeStr}` : `${sizeStr}`} bytes`
        const heading = `${partial ? 'Sample' : 'Results'} [${sizeDetails}]`
        log('')
        log(`${c.green('===')} ${heading} ${c.green('===')}`)
        log(data.toString())
        if (partial) {
          log('  ...')
        }
      }
    }
  }

  return queryInfo
}
