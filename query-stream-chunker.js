var util = require('util')

var debug = require('debug')('rethinkdb-stream-chunker:query-stream-chunker')
var protodef = require('rethinkdb/proto-def')

var StreamChunker = require('./stream-chunker.js')

module.exports = QueryStreamChunker

function QueryStreamChunker (handshakeComplete, maxChunkLen) {
  if (!(this instanceof QueryStreamChunker)) {
    return new QueryStreamChunker(handshakeComplete, maxChunkLen)
  }
  debug('%s: constructor args handshakeComplete:%o, maxChunkLen:%o', handshakeComplete, maxChunkLen)
  StreamChunker.call(this)
  var handshakeLen = 12
  this.init({
    chunkLen: handshakeComplete ? null : handshakeLen,
    handshakeComplete: handshakeComplete,
    maxChunkLen: maxChunkLen
  })
}

// inherit from StreamChunker
util.inherits(QueryStreamChunker, StreamChunker)

/**
 * validate handshake buffer
 * @param  {Buffer}   buf handshake buffer
 * @return {Boolean}  validHandshake
 */
QueryStreamChunker.prototype.validateHandshake = function (buf) {
  var protocolVersion = buf.readUInt32LE(0)
  if (protocolVersion !== protodef.VersionDummy.Version.V0_4) {
    debug('%s: Invalid handshake, protocolVersion %s', this.constructor.name, protocolVersion)
    return false
  }
  var keyLen = buf.readUInt32LE(4)
  if (keyLen !== 0) {
    debug('%s: Invalid handshake, auth key not supported %o', this.constructor.name, keyLen)
    return false
  }
  var protocolType = buf.readUInt32LE(8)
  if (protocolType !== protodef.VersionDummy.Protocol.JSON) {
    debug('%s: Invalid handshake, protocolType %o', this.constructor.name, protocolType)
    return false
  }
  // valid handshake
  return true
}

