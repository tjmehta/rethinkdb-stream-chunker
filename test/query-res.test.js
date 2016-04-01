var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var expect = require('chai').expect
var r = require('rethinkdb')

var createMockConnection = require('./fixtures/create-mock-connection.js')
var createStreamChunker = require('../stream-chunker.js')
var createResChunk = require('./fixtures/create-res-chunk.js')
var parseQueryBuffer = require('./fixtures/parse-query-buffer.js')
var QueryStreamChunker = require('../index.js').QueryStreamChunker

describe('query and response tests', function () {
  beforeEach(function (done) {
    var self = this
    createMockConnection(function (err, connection) {
      if (err) { return done(err) }
      self.connection = connection
      self.socket = connection.rawSocket
      done()
    })
  })

  it('should chunk a rethinkdb query stream (single query)', function (done) {
    var self = this
    var query = r.table('test-table').get('hey')
    this.socket.writeStream.pipe(new QueryStreamChunker(true)).on('data', function (queryBuf) {
      try {
        var token = queryBuf.slice(0, 8)
        var astParts = parseQueryBuffer(queryBuf)
        expect(astParts).to.deep.equal({
          type: 1,
          term: query.build(),
          opts: undefined
        })
        self.socket.emit('data', createResChunk({
          t: 16,
          r: ['blah blah blah error'],
          b: []
        }, token))
      } catch (err) {
        done(err)
      }
    })
    query.run(this.connection, function (err) {
      expect(err.constructor.name).to.equal('ReqlDriverError')
      expect(err.message).to.match(/blah blah blah error/)
      done()
    })
  })

  it('should create a base stream chunker', function (done) {
    // covers base constructor init call case
    var s = createStreamChunker()
    s.validateHandshake = function () {
      return true
    }
    s.insertAst(new Buffer(8), {
      hello: 1,
      world: 1
    })
    done()
  })
})
