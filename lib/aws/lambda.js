module.exports = require('mem')(newLambda)

const aws = require('./aws')
const promised = require('../promised')

function newLambda (awsConfig) {
  const config = aws.getConfig(awsConfig)
  const sdk = new aws.sdk.Lambda(config)

  return {
    create: createLambda.bind(null, sdk),
    get: getLambda.bind(null, sdk),
    invoke: invokeLambda.bind(null, sdk),
    list: list.bind(null, sdk),
    tag: tagLambda.bind(null, sdk),
    tags: getLambdaTags.bind(null, sdk),
    untag: untagLambda.bind(null, sdk),
    updateCode: updateLambdaCode.bind(null, sdk),
    updateConfig: updateLambdaConfig.bind(null, sdk),
    updateConcurrency: updateLambdaConcurrency.bind(null, sdk),
    sdk
  }
}

async function createLambda (sdk, options) {
  return promised(h => sdk.createFunction(options, h))
}

async function getLambda (sdk, name) {
  try {
    const info = await promised(h => sdk.getFunction({ FunctionName: name }, h))
    return info
  } catch (error) {
    if (error.code !== 'ResourceNotFoundException') {
      throw error
    }
  }
}

async function list (sdk, options = {}) {
  // TODO: follow continuation tokens
  return promised(h => sdk.listFunctions(options, h))
}

async function tagLambda (sdk, arn, tags) {
  return promised(h => sdk.tagResource({ Resource: arn, Tags: tags }, h))
}

async function getLambdaTags (sdk, arn) {
  const { Tags: tags } = await promised(h => sdk.listTags({ Resource: arn }, h))

  return tags
}

async function untagLambda (sdk, arn, tagNames) {
  return promised(h => sdk.untagResource({ Resource: arn, TagKeys: tagNames }, h))
}

async function updateLambdaCode (sdk, options) {
  return promised(h => sdk.updateFunctionCode(options, h))
}

async function updateLambdaConfig (sdk, options) {
  return promised(h => sdk.updateFunctionConfiguration(options, h))
}

async function updateLambdaConcurrency (sdk, name, concurrency) {
  return promised(h => sdk.putFunctionConcurrency({
    FunctionName: name,
    ReservedConcurrentExecutions: concurrency
  }, h))
}

async function invokeLambda (sdk, options) {
  return promised(h => sdk.invoke(options, h))
}
