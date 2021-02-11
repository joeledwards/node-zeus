module.exports = require('mem')(newAthena)

const c = require('@buzuli/color')
const fs = require('fs')
const aws = require('./aws')
const newS3 = require('./s3')
const url = require('@buzuli/url')
const sleep = require('../sleep')
const moment = require('moment')
const promised = require('@buzuli/promised')
const prettyBytes = require('pretty-bytes')
const { seconds, stopwatch } = require('durations')

const now = () => moment.utc().format('YYYYMMDDHHmmss')

function newAthena (awsConfig) {
  const config = aws.getConfig(awsConfig)
  const sdk = new aws.sdk.Athena(config)
  const s3 = newS3(awsConfig)

  return {
    cancelQuery: cancelQuery.bind(null, sdk),
    loadQuery,
    listQueries: listQueries.bind(null, sdk, s3),
    scanQueries: scanQueries.bind(null, sdk, s3),
    queryDone: queryDone.bind(null, sdk, s3),
    queryResults: queryResults.bind(null, sdk, s3),
    queryStatus: queryStatus.bind(null, sdk, s3),
    runQuery: runQuery.bind(null, sdk, s3),
    startQuery: startQuery.bind(null, sdk),
    stateColor,
    sdk
  }
}

// Start an Athena query
async function startQuery (sdk, { resultBucket, resultPrefix, query, token }) {
  return new Promise((resolve, reject) => {
    const prefix = resultPrefix + (token ? `${token}/` : '')
    const outputBaseUrl = `s3://${resultBucket}/${prefix}`

    const params = {
      QueryString: query,
      ResultConfiguration: {
        OutputLocation: outputBaseUrl
      }
    }

    if (token) {
      params.ClientRequestToken = token
    }

    sdk.startQueryExecution(params, (error, data) => {
      if (error) {
        reject(error)
      } else {
        const { QueryExecutionId: queryId } = data
        const key = `${prefix}${queryId}.csv`
        const url = `s3://${resultBucket}/${key}`
        const resultLocation = { bucket: resultBucket, key, url }
        resolve({ queryId, resultLocation })
      }
    })
  })
}

// Check on the status for a query
async function queryStatus (sdk, s3, queryId) {
  const data = await promised(h => sdk.getQueryExecution({ QueryExecutionId: queryId }, h))

  const {
    QueryExecution: {
      Query: query,
      StatementType: queryType,
      ResultConfiguration: {
        OutputLocation: outputUri
      },
      QueryExecutionContext: {
        Database: schema
      },
      Status: {
        State: state,
        StateChangeReason: stateReason,
        SubmissionDateTime: submitted,
        CompletionDateTime: completed
      },
      Statistics: {
        DataScannedInBytes: bytesScanned,
        EngineExecutionTimeInMillis: execMillis,
        DataManifestLocation: manifest
      },
      WorkGroup: workGroup
    }
  } = data

  const { bucket, key } = s3.parseUri({ uri: outputUri })

  let manifestLocation
  if (manifest) {
    const { mBucket, mKey } = s3.parseUri({ uri: manifest })
    manifestLocation = {
      bucket: mBucket,
      key: mKey,
      uri: manifest
    }
  }

  return {
    query,
    workGroup,
    queryId,
    queryType,
    schema,
    bytesScanned,
    execDuration: execMillis / 1000.0,
    submitted,
    completed,
    finished: ['CANCELLED', 'FAILED', 'SUCCEEDED'].includes(state),
    state,
    stateReason,
    manifestLocation,
    outputLocation: {
      bucket,
      key,
      uri: outputUri
    }
  }
}

// Run and monitor query
async function runQuery (sdk, s3, {
  query,
  queryTag,
  resultBucket,
  resultPrefix = '',
  timeout = 600000,
  pollInterval = 5000,
  progress = false,
  quiet = false
}) {
  const token = `tdat-athena-${queryTag}-query-${now()}`
  try {
    if (!quiet) {
      console.info(`Running ${queryTag} query:\n${c.yellow(query)}`)
    }
    const { queryId, resultLocation } = await startQuery(sdk, { resultBucket, resultPrefix, query, token, quiet })
    if (!quiet) {
      console.info(`Query ${c.yellow(queryId)} started.`)
    }
    const { execDuration, bytesScanned, timedOut, success } = await queryDone(sdk, s3, queryId, { timeout, pollInterval, progress, quiet })

    return { queryId, resultLocation, execDuration, bytesScanned, token, success, timedOut }
  } catch (error) {
    if (!quiet) {
      console.error(error)
      console.error(c.red(`Error starting Athena query ${c.yellow(token)}. Details above ${c.yellow('^')}`))
    }
    throw error
  }
}

// Wait for a query to complete
async function queryDone (sdk, s3, queryId, options = {}) {
  const {
    timeout = 600000,
    pollInterval = 5000,
    progress = true,
    quiet = false
  } = options

  const watch = stopwatch().start()

  let first = true
  let done = false
  let result = { timedOut: true }
  let lastBytes = 0
  let lastExecTime = 0

  while (!done && watch.duration().millis() < timeout) {
    try {
      if (first) {
        first = false
      } else {
        const delay = Math.max(0, Math.min(pollInterval, timeout - watch.duration().millis()))
        await sleep(delay)
      }
      const { bytesScanned = 0, execDuration = 0, finished, state } = await queryStatus(sdk, s3, queryId)
      const duration = millis(execDuration)
      if (lastBytes !== bytesScanned || lastExecTime !== execDuration) {
        lastBytes = bytesScanned
        lastExecTime = execDuration

        if (progress === true) {
          const stateStr = stateColor(state)
          const sizeStr = c.yellow(prettyBytes(bytesScanned))
          const bytesStr = c.orange(bytesScanned.toLocaleString())
          const costStr = c.green((bytesScanned / 1000000000000 * 5.0).toFixed(2))
          const timeStr = c.blue(duration)
          console.info(`[${stateStr}] scanned ${sizeStr} (${bytesStr} bytes | $${costStr}) in ${timeStr}`)
        } else if (typeof progress === 'function') {
          progress({
            bytesScanned,
            execDuration,
            finished,
            state
          })
        }
      }
      done = finished
      result = { queryId, execDuration, bytesScanned, state, timedOut: !finished, success: state === 'SUCCEEDED' }
    } catch (error) {
      if (!quiet) {
        console.error('query status error:', error)
      }
      throw error
    }
  }

  return result
}

// Fetch results from a completed query
async function queryResults (sdk, s3, queryId, { sampleSize = 1024 } = {}) {
  const {
    state,
    finished,
    outputLocation: {
      bucket,
      key,
      uri
    }
  } = await queryStatus(sdk, s3, queryId)

  if (!finished) {
    return { state }
  }

  let dataSize = 0

  try {
    const results = await s3.head(bucket, key)
    dataSize = results.ContentLength
  } catch (error) {
    if (error.code !== 'NotFound') {
      throw error
    }
  }
  const { host: bucket, path } = url.parse(uri)
  const key = path.slice(1)

  const {
    ContentLength: dataSize
  } = await s3.head(bucket, key)

  let data
  if (sampleSize !== 0 && dataSize !== 0) {
    const s3Params = {}
    if (sampleSize > 0) {
      s3Params.maxBytes = sampleSize
    }
    const { Body: sampleData } = await s3.get(bucket, key, s3Params)
    data = sampleData
  }

  return {
    state,
    uri,
    bucket,
    key,
    data,
    dataSize,
    partial: dataSize > sampleSize
  }
}

// Cancel a running query
async function cancelQuery (sdk, queryId) {
  return promised(h => sdk.stopQueryExecution({ QueryExecutionId: queryId }, h))
}

function stateColor (state) {
  switch (state) {
    case 'QUEUED': return c.grey(state)
    case 'RUNNING': return c.blue(state)
    case 'SUCCEEDED': return c.green(state)
    case 'FAILED': return c.red(state)
    case 'CANCELLED': return c.red(state)
  }
}

// Load a query from a file, injecting substitutions for {{<sub-field>}}
async function loadQuery (fileName, substitutions = {}) {
  const data = await promised(h => fs.readFile(fileName, h))

  return Object
    .entries(substitutions)
    .reduce(
      (query, [name, value]) => query.replace(`{{${name}}}`, value),
      data.toString()
    )
}

async function * scanQueries (sdk, s3, { limit = 100, extended = false } = {}) {
  let fetched = 0
  let getMore = true
  let nextToken

  while (getMore) {
    const params = {
      MaxResults: limit - fetched
    }

    if (nextToken) {
      params.NextToken = nextToken
    }

    const {
      QueryExecutionIds: queryIds,
      NextToken: token
    } = await promised(h => sdk.listQueryExecutions(params, h))

    for (const queryId of queryIds) {
      fetched++

      if (extended) {
        yield await queryStatus(sdk, s3, queryId)
      } else {
        yield { queryId }
      }
    }

    nextToken = token
    getMore = nextToken && (fetched < limit)
  }
}

async function listQueries (sdk, s3, { limit = 100, extended = false } = {}) {
  const queries = []

  for await (const query of scanQueries(sdk, s3, { extended })) {
    queries.push(query)
  }

  return queries
}
