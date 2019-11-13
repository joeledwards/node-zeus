module.exports = statusReport

const c = require('@buzuli/color')
const aws = require('./aws')
const buzJson = require('@buzuli/json')
const prettyBytes = require('pretty-bytes')
const { seconds } = require('durations')

async function statusReport(queryId, {
  json = false,
  quiet = false,
  sampleSize = 1024,
  extended = false,
  athena = aws.athena(),
  s3 = aws.s3(),
  log = (...args) => console.info(...args)
} = {}) {
  const queryInfo = await athena.queryStatus(queryId)

  const {
    query,
    schema,
    bytesScanned,
    executionTime,
    submitted,
    completed,
    finished,
    state,
    stateReason,
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
      log(buzJson({ queryId, state: queryInfo.state }, { indent: false }))
    } else {
      const tf = (v) => (f) => (v == null) ? v : f(v)
      queryInfo.submitted = tf(queryInfo.submitted)(s => s.toISOString())
      queryInfo.completed = tf(queryInfo.completed)(s => s.toISOString())

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
      const dbStr = c.blue(schema)
      const stateStr = athena.stateColor(state)
      const sizeStr = c.yellow(prettyBytes(bytesScanned))
      const bytesStr = c.orange(bytesScanned.toLocaleString())
      const costStr = c.green((bytesScanned / 1000000000000 * 5.0).toFixed(2))
      const timeStr = c.blue(seconds(executionTime))
      const startStr = c.grey(submitted.toISOString())
      const endStr = c.grey(completed.toISOString())

      log(`${idStr} @ ${dbStr} [${stateStr}]`)
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
        log(`       results : ${s3.makeUri({ bucket, key, color: true })}`)
      }

      if (extended) {
        log('')
        log(`${c.green('===')} Query ${c.green('===')}`)
        log(query)
      }

      if (succeeded) {
        if (data) {
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
  }

  return queryInfo
}
