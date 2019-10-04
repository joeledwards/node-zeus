module.exports = require('mem')(newSqs)

const aws = require('./aws')
const promised = require('../promised')

function newSqs (awsConfig) {
  const config = aws.getConfig(awsConfig)
  const sdk = new aws.sdk.SQS(config)

  return {
    ack: ack.bind(null, sdk),
    messageCounts: messageCounts.bind(null, sdk),
    peek: peek.bind(null, sdk),
    queues: queues.bind(null, sdk),
    send: send.bind(null, sdk),
    sdk
  }
}

async function ack (sdk, queue, message) {
  return promised(h => sdk.deleteMessage({
    QueueUrl: queue,
    ReceiptHandle: message.ReceiptHandle
  }, h))
}

async function peek (sdk, queue, options = {}) {
  const {
    limit = 1,
    maxWait,
    requeueDelay
  } = options

  return promised(h => sdk.receiveMessage({
    QueueUrl: queue,
    MaxNumberOfMessages: limit,
    VisibilityTimeout: requeueDelay,
    WaitTimeSeconds: maxWait,
    AttributeNames: ['All'],
    MessageAttributeNames: ['All']
  }, h))
}

async function messageCounts (sdk, queue) {
  const options = {
    QueueUrl: queue,
    AttributeNames: [
      'ApproximateNumberOfMessages',
      'ApproximateNumberOfMessagesDelayed',
      'ApproximateNumberOfMessagesNotVisible'
    ]
  }

  const {
    Attributes: {
      ApproximateNumberOfMessages: a,
      ApproximateNumberOfMessagesDelayed: d,
      ApproximateNumberOfMessagesNotVisible: i
    }
  } = await promised(h => sdk.getQueueAttributes(options, h))

  const [available, delayed, inFlight] = [a, d, i].map(n => Number(n) || 0)
  const total = available + delayed + inFlight

  return { available, delayed, inFlight, total }
}

async function queues (sdk) {
  return promised(h => sdk.listQueues(h)).then(({ QueueUrls: urls }) => urls)
}

async function send (sdk, queue, message, { delay, id, groupId } = {}) {
  const params = {
    QueueUrl: queue,
    MessageBody: message
  }

  const isFifo = queue.slice(-5) === '.fifo'

  if (isFifo && !groupId) {
    throw new Error('groupId is required for FIFO queues')
  }

  if (groupId && !isFifo) {
    throw new Error('groupId is only allowed for FIFO queues')
  }

  if (id) {
    params.MessageDeduplicationId = id
  }

  if (delay) {
    params.DelaySeconds = delay
  }

  if (groupId) {
    params.MessageGroupId = groupId
  }

  return promised(h => sdk.sendMessage(params, h))
}
