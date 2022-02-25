const tap = require('tap')
const { parsePattern } = require('../commands/update-partitions')

tap.test('parsePattern reject any string which is not a valid URI', assert => {
  assert.throws(parsePattern('s3://bkt'), 'Invalid pattern s3://bkt')
  assert.throws(parsePattern('s3://bkt/'), 'Invalid pattern s3://bkt/')

  assert.throws(parsePattern('bkt'), 'Invalid pattern bkt')
  assert.throws(parsePattern('bkt/'), 'Invalid pattern bkt/')
})

tap.test('parsePattern reject any string which does not contain at least one param', assert => {
  assert.throws(parsePattern('s3://bkt/pfx'))

  assert.throws(parsePattern('bkt/pfx'))
})

tap.test('parsePattern should extract valid params', assert => {
  assert.same(parsePattern('s3://bkt/{{a}}/{{b}}/'), ['a', 'b'])
  assert.same(parsePattern('s3://bkt/{{a}}{{b}}/'), ['a', 'b'])
})

