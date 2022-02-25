const handler = require('../lib/handler')
const desc = `Update the partitions in a table to ensure all variations of an S3 prefix are represented.

A pattern URI should be of the form [s3://]<bucket>/<prefix>/ where <prefix> must contain at least one param.

A param is of the form {{<partition_column_name>}}.

Examples of a valid partition patterns:
- s3://my-bucket/data/partitioned/{{year}}/{{month}}/{{day}}/
- my-bucket/data/partitioned/{{year}}/{{month}}/{{day}}/
- my-bucket/data/partitioned/{{year}}{{month}}{{day}}/`

module.exports = {
  command: 'update-partitions <table> <s3_uri_pattern>',
  desc,
  builder,
  handler: handler(updatePartitions),
  parsePattern
}

function builder (yargs) {
}

async function updatePartitions ({
  aws,
  options: {
    table,
    s3UriPattern,
  }
}) {
  // TODO: validate the pattern and extract at least one partition field
  const partitionColumns = parsePattern(s3UriPattern)

  const c = require('@buzuli/color')
  const meter = require('@buzuli/meter')
  const buzJson = require('@buzuli/json')
  const throttle = require('@buzuli/throttle')
  const statusReport = require('../lib/status-report')

  const metrics = meter()
  const notify = throttle({
    minDelay: 5000,
    maxDelay: 5000,
    reportFunc: () => {
      console.info(`Still going..`)
    },
  })

  const s3 = aws.s3()

  try {
    // TODO: fetch all partitions for the table (also validates that the table exists)

    // TODO: scan prefixes
    const prefixes = await s3.getPrefixes(bucket, prefix)
  } catch (error) {
    console.error(`Error showing updating partitions in ${table}`)
    process.exit(1)
  }
}

function parsePattern (s3UriPattern) {
  const url = require('@buzuli/url')
  const {
    host,
    path
  } = url.parse(s3UriPattern)

  if ((host == null) || (path == null)) {
    throw new Error(`Invalid pattern ${s3UriPattern}`)
  }

  const regex = /(?:(.*?)\{\{([a-bA-B_]\w*)\}\}(.*?))+/g
  const parts = [...path.matchAll(regex)]

  if (parts.length < 1) {
    throw new Error(`Pattern must contain at least 1 param`)
  }

  return parts
}
