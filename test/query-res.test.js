require('./fixtures/load-env.js')

var afterEach = global.afterEach
var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var net = require('net')

var expect = require('chai').expect
var noop = require('101/noop')
var proxyquire = require('proxyquire')
var shimmer = require('shimmer')
var through2 = require('through2')

var createResChunk = require('./fixtures/create-res-chunk.js')
var createStreamChunker = require('../stream-chunker.js')
var parseQueryBuffer = require('./fixtures/parse-query-buffer.js')

describe('query and response tests', function () {
  describe('protocol v0.4 tests', function () {
    var rethinkdb = require('./fixtures/rethinkdb@2.2.2')
    proxyquire.noCallThru()
    var QueryStreamChunker = proxyquire('../query-stream-chunker.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': require('./fixtures/rethinkdb@2.2.2/proto-def.js')
    })
    runTests(rethinkdb, QueryStreamChunker)
  })
  describe('protocol v1.0 tests', function () {
    runTests(require('rethinkdb'), require('../query-stream-chunker.js'))
  })
})

function runTests (r, QueryStreamChunker) {
  beforeEach(function (done) {
    var self = this
    shimmer.wrap(net, 'connect', function (o) {
      return function () {
        var socket = self.socket = o.apply(this, arguments)
        return socket
      }
    })
    var opts = {
      host: process.env.RETHINKDB_HOST
    }
    r.connect(opts, function (err, conn) {
      if (err) { return done(err) }
      self.conn = conn
      done()
    })
    shimmer.unwrap(net, 'connect')
  })
  beforeEach(function (done) {
    var self = this
    this.writeStream = through2()
    this.socket.write = function () {
      // post handshake data, query data, is written to writeStream (and not socket)
      return self.writeStream.write.apply(self.writeStream, arguments)
    }
    done()
  })
  afterEach(function (done) {
    if (this.conn) { this.conn.close() }
    done()
  })

  it('should chunk a rethinkdb query stream (single query)', function (done) {
    var self = this
    var query = r.db('test').tableList()
    this.writeStream.pipe(new QueryStreamChunker(true)).on('data', function (queryBuf) {
      try {
        var token = queryBuf.slice(0, 8)
        var astParts = parseQueryBuffer(queryBuf)
        expect(astParts).to.deep.equal({
          type: 1,
          term: query.build(),
          opts: undefined
        })
        self.socket.write = noop
        self.socket.emit('data', createResChunk({
          t: 16,
          r: ['blah blah blah error'],
          b: []
        }, token))
      } catch (err) {
        done(err)
      }
    })
    query.run(this.conn, function (err) {
      expect(err).to.exist
      expect(err.constructor.name).to.equal('ReqlDriverError')
      expect(err.message).to.match(/blah blah blah error/)
      done()
    })
  })

  it('should create a base stream chunker', function (done) {
    // covers base constructor init call case
    var s = createStreamChunker()
    s.validateHandshakeChunk = function () {
      return true
    }
    s.insertAst(new Buffer(8), {
      hello: 1,
      world: 1
    })
    // test flush
    s.end()
    var state = s.__streamChunkerState
    expect(state.buffer).to.deep.equal(new Buffer(0))
    expect(state.chunkLen).to.equal(null)
    done()
  })
}
