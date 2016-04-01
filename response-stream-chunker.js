var assert = require('assert')
var util = require('util')

var debug = require('debug')('rethinkdb-stream-chunker:response-stream-chunker')
var isString = require('101/is-string')

var StreamChunker = require('./stream-chunker.js')

module.exports = ResponseStreamChunker

function ResponseStreamChunker (handshakeComplete, maxChunkLen) {
  if (!(this instanceof ResponseStreamChunker)) {
    return new ResponseStreamChunker(handshakeComplete, maxChunkLen)
  }
  debug('%s: constructor args handshakeComplete:%o, maxChunkLen:%o', handshakeComplete, maxChunkLen)
  StreamChunker.call(this)
  var handshakeLen = 8
  this.init({
    chunkLen: handshakeComplete ? null : handshakeLen,
    handshakeComplete: handshakeComplete,
    maxChunkLen: maxChunkLen
  })
}

util.inherits(ResponseStreamChunker, StreamChunker)

/**
 * validate handshake buffer
 * @param  {Buffer}   buf handshake buffer
 * @return {Boolean}  validHandshake
 */
ResponseStreamChunker.prototype.validateHandshake = function (buf) {
  var str = buf.toString()
  debug('%s: validate handshake "%s"', this.constructor.name, str)
  return ~str.indexOf('SUCCESS')
}

/**
 * insert a client error into the response stream
 * @param  {Buffer}   tokenBuf token to encode
 * @param  {String}   errorMsg error message
 * @return {Array}    backtrace
 */
ResponseStreamChunker.prototype.insertClientError = function (tokenBuf, errorMsg, backtrace) {
  assert(Buffer.isBuffer(tokenBuf), '"token" must be a buffer')
  assert(isString(errorMsg), '"errorMsg" must be a string')
  assert(!backtrace || Array.isArray(backtrace), '"backtrace" must be an array')
  backtrace = backtrace || []
  this.insertAst(tokenBuf, {
    t: 16,
    r: [ errorMsg ],
    b: backtrace
  })
}
