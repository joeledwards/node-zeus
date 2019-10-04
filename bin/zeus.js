#!/usr/bin/env node

const yargs = require('yargs')

function run () {
  return yargs
    .commandDir('../commands')
    .demandCommand()
    .strict()
    .help()
    .argv
}

run()
