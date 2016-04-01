var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var expect = require('chai').expect
var protodef = require('rethinkdb/proto-def')
var r = require('rethinkdb')

var createMockConnection = require('./fixtures/create-mock-connection.js')
var createQueryChunk = require('./fixtures/create-query-chunk.js')
var parseQueryBuffer = require('./fixtures/parse-query-buffer.js')
var createQueryStreamChunker = require('../index.js').QueryStreamChunker
var reqlQueries = require('./fixtures/reql-queries.js')

describe('query stream chunker functional tests', function () {
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
    var query = r.table('test-table').get('hey')
    this.socket.writeStream.pipe(createQueryStreamChunker(true)).on('data', function (queryBuf) {
      try {
        var astParts = parseQueryBuffer(queryBuf)
        expect(astParts).to.deep.equal({
          type: 1,
          term: query.build(),
          opts: undefined
        })
        done()
      } catch (err) {
        done(err)
      }
    })
    query.run(this.connection, function () {})
  })

  it('should chunk a rethinkdb query stream (multiple queries)', function (done) {
    var self = this
    var queries = reqlQueries().slice(0, 3) // limit to 3 queries
    var chunkCount = 0
    this.socket.writeStream
      .pipe(createQueryStreamChunker(true))
      .on('data', function (queryBuf) {
        try {
          var astParts = parseQueryBuffer(queryBuf)
          expect(astParts).to.deep.equal({
            type: 1,
            term: queries[chunkCount].build(),
            opts: undefined
          })
          chunkCount++
          if (chunkCount === queries.length) {
            done()
          }
        } catch (err) {
          done(err)
        }
      })
    queries.forEach(function (query) {
      query.run(self.connection, function () {})
    })
  })

  it('should allow insertion of queries (mid stream)', function (done) {
    var self = this
    var insertedQuery = r.table('test-table').get('hey')
    var queries = reqlQueries().slice(0, 3)
    var expectedQueries = [insertedQuery].concat(queries)
    var chunkCount = 0
    var queryStreamChunker = createQueryStreamChunker(true)
    this.socket.writeStream.pipe(queryStreamChunker).on('data', function (queryBuf) {
      try {
        var astParts = parseQueryBuffer(queryBuf)
        expect(astParts).to.deep.equal({
          type: 1,
          term: expectedQueries[chunkCount].build(),
          opts: undefined
        })
        chunkCount++
        if (chunkCount === expectedQueries.length) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    queries.forEach(function (query, i) {
      query.run(self.connection, function () {})
      if (i === 2) {
        queryStreamChunker.insertChunk(createQueryChunk(insertedQuery))
      }
    })
  })

  it('should allow insertion of queries (at chunk break)', function (done) {
    var self = this
    var insertedQuery = r.table('test-table').get('hey')
    var queries = reqlQueries().slice(0, 3)
    var expectedQueries = queries.slice(0, 1).concat([insertedQuery]).concat(queries.slice(1))
    var chunkCount = 0
    var queryStreamChunker = createQueryStreamChunker(true)
    this.socket.writeStream.pipe(queryStreamChunker).on('data', function (queryBuf) {
      try {
        var astParts = parseQueryBuffer(queryBuf)
        expect(astParts).to.deep.equal({
          type: 1,
          term: expectedQueries[chunkCount].build(),
          opts: undefined
        })
        if (chunkCount === 0) {
          queryStreamChunker.insertChunk(createQueryChunk(insertedQuery))
        }
        chunkCount++
        if (chunkCount === expectedQueries.length) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    queries.forEach(function (query, i) {
      query.run(self.connection, function () {})
    })
  })

  it('should error if chunk size greater than max', function (done) {
    var query = r.table('test-table').get('hey')
    this.socket.writeStream.pipe(createQueryStreamChunker(true, 1)).on('error', function (err) {
      try {
        expect(err).to.exist
        expect(err.message).to.match(/Chunk length/)
        expect(err.data).to.deep.equal({
          chunkLen: 48,
          maxChunkLen: 1
        })
        done()
      } catch (err) {
        done(err)
      }
    })
    query.run(this.connection, function () {})
  })

  describe('handshake validation', function () {
    it('should validate a valid handshake', function (done) {
      var queryStreamChunker = createQueryStreamChunker()
      var handshake = new Buffer(12)
      handshake.writeUInt32LE(protodef.VersionDummy.Version.V0_4, 0)
      handshake.writeUInt32LE(0, 4)
      handshake.writeUInt32LE(protodef.VersionDummy.Protocol.JSON, 8)
      queryStreamChunker.on('data', function (buf) {
        expect(buf).to.deep.equal(handshake)
        expect(queryStreamChunker.__streamChunkerState.handshakeComplete).to.be.true
        done()
      }).write(handshake)
    })

    it('should validate a valid handshake (parts)', function (done) {
      var queryStreamChunker = createQueryStreamChunker()
      var handshake = new Buffer(12)
      handshake.writeUInt32LE(protodef.VersionDummy.Version.V0_4, 0)
      handshake.writeUInt32LE(0, 4)
      handshake.writeUInt32LE(protodef.VersionDummy.Protocol.JSON, 8)
      queryStreamChunker.on('data', function (buf) {
        expect(buf).to.deep.equal(handshake)
        done()
      })
      // parts and end for coverage
      queryStreamChunker.write(handshake.slice(0, 4))
      queryStreamChunker.write(handshake.slice(4))
      queryStreamChunker.end()
    })

    it('should invalidate a invalid handshake (version)', function (done) {
      var queryStreamChunker = createQueryStreamChunker()
      var handshake = new Buffer(12)
      handshake.writeUInt32LE(protodef.VersionDummy.Version.V0_3, 0)
      handshake.writeUInt32LE(0, 4)
      handshake.writeUInt32LE(protodef.VersionDummy.Protocol.JSON, 8)
      queryStreamChunker.on('error', function (err) {
        expect(err).to.be.an.instanceOf(Error)
        expect(err.message).to.match(/Invalid handshake/)
        done()
      }).write(handshake)
    })

    it('should invalidate a invalid handshake (token)', function (done) {
      var queryStreamChunker = createQueryStreamChunker()
      var handshake = new Buffer(12)
      handshake.writeUInt32LE(protodef.VersionDummy.Version.V0_4, 0)
      handshake.writeUInt32LE(10, 4)
      handshake.writeUInt32LE(protodef.VersionDummy.Protocol.JSON, 8)
      queryStreamChunker.on('error', function (err) {
        expect(err).to.be.an.instanceOf(Error)
        expect(err.message).to.match(/Invalid handshake/)
        done()
      }).write(handshake)
    })

    it('should invalidate a invalid handshake (protocol)', function (done) {
      var queryStreamChunker = createQueryStreamChunker()
      var handshake = new Buffer(12)
      handshake.writeUInt32LE(protodef.VersionDummy.Version.V0_4, 0)
      handshake.writeUInt32LE(0, 4)
      handshake.writeUInt32LE(protodef.VersionDummy.Protocol.PROTOBUF, 8)
      queryStreamChunker.on('error', function (err) {
        expect(err).to.be.an.instanceOf(Error)
        expect(err.message).to.match(/Invalid handshake/)
        done()
      }).write(handshake)
    })
  })
})
