var assert = require('assert')
var util = require('util')

var debug = require('debug')('rethinkdb-stream-chunker:response-stream-chunker')
var isString = require('101/is-string')
var protodef = require('rethinkdb/proto-def')

var StreamChunker = require('./stream-chunker.js')

var isV1_0 = protodef.VersionDummy.Version.V1_0

module.exports = ResponseStreamChunker

function ResponseStreamChunker (handshakeComplete, maxChunkLen) {
  if (!(this instanceof ResponseStreamChunker)) {
    return new ResponseStreamChunker(handshakeComplete, maxChunkLen)
  }
  debug('%s: constructor args handshakeComplete:%o, maxChunkLen:%o', handshakeComplete, maxChunkLen)
  StreamChunker.call(this)
  this.init({
    chunkLen: null,
    handshakeComplete: handshakeComplete,
    handshakeChunks: isV1_0 ? 3 : 1,
    maxChunkLen: maxChunkLen
  })
}

util.inherits(ResponseStreamChunker, StreamChunker)

/**
 * validate handshake buffer
 * @param  {Buffer}   buf handshake buffer
 * @return {Boolean}  validHandshake
 */
ResponseStreamChunker.prototype.validateHandshakeChunk = function (buf) {
  var state = this.__streamChunkerState
  var str
  if (isV1_0) {
    str = buf.toString()
    debug('%s: handshake str "%o"', this.constructor.name, json)
    try {
      var json = JSON.parse(str.slice(0, -1))
      state.handshakeChunksCount++
      if (state.handshakeZeroIndexes.length === state.handshakeChunks) {
        state.handshakeComplete = true
      }
      var valid = json.success
      if (!valid) {
        debug('%s: invalid handshake json.success "%o"', this.constructor.name, json)
      }
      return valid
    } catch (err) {
      debug('%s: invalid json handshake "%s": %s', this.constructor.name, str, err.message)
      return false
    }
  } else {
    str = buf.toString()
    debug('%s: validate handshake "%s"', this.constructor.name, str)
    state.handshakeComplete = true
    return str === 'SUCCESS\0'
  }
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
