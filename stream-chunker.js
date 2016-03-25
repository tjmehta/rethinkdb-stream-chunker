var assert = require('assert')

var debug = require('debug')('rethinkdb-stream-chunker:stream-chunker')
var defaults = require('101/defaults')
var isFunction = require('101/is-function')
var noop = require('101/noop')
var r = require('rethinkdb')
var splice = require('buffer-splice')
var through2 = require('through2')

var StreamChunker = module.exports = through2.ctor(
  function transform (buf, enc, cb) {
    if (!this.__streamChunkerState) {
      this.init()
    }
    return this.transform(buf, enc, cb)
  },
  function flush (cb) {
    return this.flush(cb)
  })

/**
 * initialize or reset the stream chunker state (`__streamChunkerState`)
 */
StreamChunker.prototype.init =
StreamChunker.prototype.reset = function (state) {
  state = state || {}
  this.__streamChunkerState = defaults(state, {
    buffer: new Buffer(0),
    chunkLen: null,
    handshakeComplete: false,
    insertedChunks: []
  })
  debug('%s: init %o', this.constructor.name, this.__streamChunkerState)
}

/**
 * handle stream data, chunks data
 */
StreamChunker.prototype.transform = function (buf, enc, cb) {
  var state = this.__streamChunkerState
  state.buffer = Buffer.concat([state.buffer, buf])
  debug('%s: len %o', this.constructor.name, state.buffer.length)
  // check if the buffer contains chunk length info
  var chunkLen = this.readChunkLen()
  if (!chunkLen) {
    return this.continueBuffering(cb)
  }
  // check if the buffer is contains a full chunk
  if (state.buffer.length < chunkLen) {
    return this.continueBuffering(cb)
  }
  var i = 0
  while (chunkLen && state.buffer.length >= chunkLen) {
    this.passthroughChunk(state.chunkLen)
  }
  cb()
}

/**
 * read and cache chunk length from the state buffer
 */
StreamChunker.prototype.readChunkLen = function (reset) {
  var state = this.__streamChunkerState
  if (reset) {
    debug('%s: reset chunk len', this.constructor.name)
    delete state.chunkLen
  }
  state.chunkLen = state.chunkLen ||
    ((state.buffer.length > 12)
      ? (12 + state.buffer.readUInt32LE(8))
      : null)
  debug('%s: chunk len %o', this.constructor.name, state.chunkLen)
  return state.chunkLen
}

/**
 * read chunk length from the state buffer
 * @param  {Function} cb  transform callback, to pass data through (passed empty buffer)
 */
StreamChunker.prototype.continueBuffering = function (cb) {
  debug('%s: continue buffering', this.constructor.name)
  cb(null, new Buffer(0))
}

/**
 * pass chunk data through onto the next stream or data listener
 * @param  {Number}   len length to remove from buffer
 */
StreamChunker.prototype.passthroughChunk = function (len) {
  var state = this.__streamChunkerState
  // splice chunk out of state buffer (splice modifies original)
  var chunkBuf = splice(state, 0, len)
  // passthrough query
  if (!state.handshakeComplete) {
    debug('%s: handshake', this.constructor.name, chunkBuf)
    assert(isFunction(this.validateHandshake), '`validateHandshake` not implemented')
    var validHandshake = this.validateHandshake(chunkBuf)
    if (!validHandshake) {
      var err = new Error('Invalid handshake!')
      err.data = { state: this.__streamChunkerState }
      this.emit('error', err)
      this.reset()
      return
    }
  }
  if (state.insertedChunks.length) {
    debug('%s: %o inserted chunks found', this.constructor.name, state.insertedChunks.length)
    while (state.insertedChunks.length) {
      // FIFO
      var insertedChunk = state.insertedChunks.shift()
      debug('%s: inserted chunk', this.constructor.name, insertedChunk)
      this.push(insertedChunk)
    }
  }
  debug('%s: chunk', this.constructor.name, chunkBuf)
  chunkLen = this.readChunkLen(true)
  this.push(chunkBuf)
}

/**
 * pass chunk data through onto the next stream or data listener
 * @param  {Buffer}   chunkBuf chunk buffer to insert into stream
 * @param  {Function} cb  transform callback, to pass data through
 */
StreamChunker.prototype.insertChunk = function (chunkBuf, cb) {
  assert(Buffer.isBuffer(chunkBuf), '"chunk" must be a buffer')
  cb = cb || noop
  var state = this.__streamChunkerState
  if (!state || state.buffer.length === 0) {
    debug('%s: insert chunk now %o', this.constructor.name, chunkBuf)
    this.write(chunkBuf, cb)
  } else {
    debug('%s: insert chunk later %o', this.constructor.name, chunkBuf)
    state.insertedChunks.push(chunkBuf)
    cb()
  }
  return true
}

/**
 * pass chunk data through onto the next stream or data listener
 * @param  {Buffer}   tokenBuf token to encode
 * @param  {Object|Array}   tokenBuf token to encode
 * @param  {Function} cb  transform callback, to pass data through
 */
StreamChunker.prototype.insertAst = function (tokenBuf, ast, cb) {
  assert(Buffer.isBuffer(tokenBuf), '"token" must be a buffer')
  assert(typeof ast === 'object', '"ast" must be an array or object')
  var str = JSON.stringify(ast)
  var lenBuf = new Buffer(4)
  lenBuf.writeUInt32LE(str.length)
  var contentBuf = new Buffer(str)
  return this.insertChunk(Buffer.concat([tokenBuf, lenBuf, contentBuf]), cb)
}

/**
 * handle stream flush, resets the state data
 * @param  {Function} cb callback
 */
StreamChunker.prototype.flush = function (cb) {
  debug('%s: flush', this.constructor.name)
  this.reset()
  cb()
}