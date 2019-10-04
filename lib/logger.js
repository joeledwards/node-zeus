module.exports = {
  create,
  noop
}

const bole = require('bole')

function noop (handlers = {}) {
  const ignore = () => {}

  const {
    error = ignore,
    warn = ignore,
    info = ignore,
    debug = ignore
  } = handlers

  return { error, warn, info, debug }
}

function create ({ level, name, stream }) {
  bole.output({
    level,
    stream: stream || process.stdout
  })

  const logger = bole(name)

  return logger
}
