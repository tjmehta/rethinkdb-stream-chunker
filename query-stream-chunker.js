var util = require('util')

var debug = require('debug')('rethinkdb-stream-chunker:query-stream-chunker')
var isJSON = require('is-json')
var protodef = require('rethinkdb/proto-def')

var StreamChunker = require('./stream-chunker.js')

var isV1_0 = protodef.VersionDummy.Version.V1_0

module.exports = QueryStreamChunker

function QueryStreamChunker (handshakeComplete, maxChunkLen) {
  if (!(this instanceof QueryStreamChunker)) {
    return new QueryStreamChunker(handshakeComplete, maxChunkLen)
  }
  debug('%s: constructor args handshakeComplete:%o, maxChunkLen:%o', handshakeComplete, maxChunkLen)
  StreamChunker.call(this)
  var state
  if (isV1_0) {
    state = {
      chunkLen: null,
      handshakeComplete: handshakeComplete,
      handshakeChunks: 2,
      maxChunkLen: maxChunkLen
    }
  } else {
    state = {
      chunkLen: handshakeComplete ? null : 12, // handshake length
      handshakeComplete: handshakeComplete,
      maxHandshakeChunkLen: 12,
      maxChunkLen: maxChunkLen
    }
  }
  this.init(state)
}

// inherit from StreamChunker
util.inherits(QueryStreamChunker, StreamChunker)

/**
 * validate handshake buffer
 * @param  {Buffer}   buf handshake buffer
 * @return {Boolean}  validHandshake
 */
QueryStreamChunker.prototype.validateHandshakeChunk = function (buf) {
  var state = this.__streamChunkerState
  var protocolVersion
  var protocolType
  if (isV1_0) {
    if (state.handshakeZeroIndexes.length === 1) {
      // first chunk
      protocolVersion = buf.readUInt32LE(0)
      if (protocolVersion !== protodef.VersionDummy.Version.V1_0) {
        debug('%s: Invalid handshake, protocolVersion %s', this.constructor.name, protocolVersion)
        return false
      }
      buf = buf.slice(4)
    } else {
      // last chunk (there are only two)
      state.handshakeComplete = true
    }
    // ensure chunk is json
    var chunk = buf.slice(0, -1).toString()
    var valid = isJSON(chunk)
    if (!valid) {
      debug('%s: Invalid handshake, chunk not json %o', this.constructor.name, chunk)
    }
    return valid
  } else {
    protocolVersion = buf.readUInt32LE(0)
    if (protocolVersion !== protodef.VersionDummy.Version.V0_4) {
      debug('%s: Invalid handshake, protocolVersion %s', this.constructor.name, protocolVersion)
      return false
    }
    var keyLen = buf.readUInt32LE(4)
    if (keyLen !== 0) {
      debug('%s: Invalid handshake, auth key not supported %o', this.constructor.name, keyLen)
      return false
    }
    protocolType = buf.readUInt32LE(8)
    if (protocolType !== protodef.VersionDummy.Protocol.JSON) {
      debug('%s: Invalid handshake, protocolType %o', this.constructor.name, protocolType)
      return false
    }
    // valid handshake\
    state.handshakeComplete = true
    return true
  }
}
