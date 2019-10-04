module.exports = require('mem')(newSes)

const aws = require('./aws')
const promised = require('../promised')
const MailComposer = require('nodemailer/lib/mail-composer')

function newSes (awsConfig) {
  const config = aws.getConfig(awsConfig)
  const sdk = new aws.sdk.SES(config)

  return {
    send: send.bind(null, sdk),
    sdk
  }
}

async function send (sdk, sender, options = {}) {
  const {
    to,
    cc,
    bcc,
    reply,
    subject,
    text,
    html,
    attachments
  } = options

  const mailParams = {
    from: sender,
    replyTo: listify(reply),
    to: listify(to),
    cc: listify(cc),
    bcc: listify(bcc),
    subject,
    text,
    html,
    attachments
  }

  const mail = new MailComposer(mailParams)
  const message = await promised(h => mail.compile().build(h))

  const sesParams = {
    RawMessage: {
      Data: message
    }
  }

  return promised(h => sdk.sendRawEmail(sesParams, h))
}

function listify (addr) {
  if (typeof addr === 'string') {
    return [addr]
  }

  return addr
}
