require('./fixtures/load-env.js')

var afterEach = global.afterEach
var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var net = require('net')

var expect = require('chai').expect
var proxyquire = require('proxyquire')
var last = require('101/last')
var shimmer = require('shimmer')
var through2 = require('through2')

var createQueryChunk = require('./fixtures/create-query-chunk.js')
var parseQueryBuffer = require('./fixtures/parse-query-buffer.js')

describe('query stream chunker functional tests', function () {
  describe('protocol v1.0 tests', function () {
    proxyquire.callThru()
    proxyquire.noPreserveCache()
    var rethinkdb = proxyquire('rethinkdb', {})
    var protodef = proxyquire('rethinkdb/proto-def', {})
    var reqlQueries = proxyquire('./fixtures/reql-queries.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': protodef
    })
    var createQueryStreamChunker = proxyquire('../query-stream-chunker.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': protodef
    })
    // runTests(rethinkdb, protodef, reqlQueries, createQueryStreamChunker)
    runTests(
      rethinkdb,
      protodef,
      reqlQueries,
      createQueryStreamChunker)
  })

  describe('protocol v0.4 tests', function () {
    var rethinkdb = require('./fixtures/rethinkdb@2.2.2')
    var protodef = require('./fixtures/rethinkdb@2.2.2/proto-def.js')
    proxyquire.noCallThru()
    proxyquire.noPreserveCache()
    var createQueryStreamChunker = proxyquire('../query-stream-chunker.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': protodef
      // 'net': net
    })
    var reqlQueries = proxyquire('./fixtures/reql-queries.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': protodef
      // 'net': net
    })
    runTests(
      rethinkdb,
      protodef,
      reqlQueries,
      createQueryStreamChunker)
  })
})

function runTests (r, protodef, reqlQueries, createQueryStreamChunker) {
  describe('chunk queries (post handshake)', function () {
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
      // stub socket write
      this.__socketWrite = this.socket.write
      this.socket.write = function () {
        // post handshake data, query data, is written to writeStream (and not socket)
        return self.writeStream.write.apply(self.writeStream, arguments)
      }
      done()
    })
    afterEach(function (done) {
      if (this.__socketWrite) {
        // restore
        this.socket.write = this.__socketWrite
      }
      if (this.conn) { this.conn.close() }
      done()
    })

    it('should chunk a rethinkdb query stream (single query)', function (done) {
      var query = r.table('test-table').get('hey')
      this.writeStream.pipe(createQueryStreamChunker(true)).on('data', function (queryBuf) {
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
      query.run(this.conn, function () {})
    })

    it('should chunk a rethinkdb query stream (multiple queries)', function (done) {
      var self = this
      var queries = reqlQueries().slice(0, 3) // limit to 3 queries
      var chunkCount = 0
      this.writeStream
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
        query.run(self.conn, function () {})
      })
    })

    it('should allow insertion of queries (mid stream)', function (done) {
      var self = this
      var insertedQuery = r.table('test-table').get('hey')
      var queries = reqlQueries().slice(0, 3)
      var expectedQueries = queries.concat([insertedQuery])
      var chunkCount = 0
      var queryStreamChunker = createQueryStreamChunker(true)
      this.writeStream.pipe(queryStreamChunker).on('data', function (queryBuf) {
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
        query.run(self.conn, function () {})
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
      this.writeStream.pipe(queryStreamChunker).on('data', function (queryBuf) {
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
        query.run(self.conn, function () {})
      })
    })

    it('should error if chunk size greater than max', function (done) {
      var query = r.table('test-table').get('hey')
      this.writeStream.pipe(createQueryStreamChunker(true, 1)).on('error', function (err) {
        try {
          expect(err).to.exist
          expect(err.message).to.match(/Chunk length/)
          expect(err.data.state).to.deep.contain({
            chunkLen: 48,
            maxChunkLen: 1
          })
          done()
        } catch (err) {
          done(err)
        }
      })
      query.run(this.conn, function () {})
    })
  })

  describe('handshake validation', function () {
    beforeEach(function (done) {
      var self = this
      shimmer.wrap(net, 'connect', function (o) {
        return function () {
          var socket = self.socket = o.apply(this, arguments)
          var writeStream = self.writeStream = through2()
          // stub socket write
          self.chunker = createQueryStreamChunker()
          self.__socketWrite = socket.write.bind(socket)
          socket.write = writeStream.write.bind(writeStream)
          return socket
        }
      })
      var opts = {
        host: process.env.RETHINKDB_HOST
      }
      r.connect(opts, function (err, conn) {
        if (err) { console.error(err) }
        self.conn = conn
      })
      done()
      shimmer.unwrap(net, 'connect')
    })
    afterEach(function (done) {
      if (this.__socketWrite) {
        // restore
        this.socket.write = this.__socketWrite
      }
      if (this.conn) {
        this.conn.close()
      }
      done()
    })

    it('should invalidate a invalid handshake (version)', function (done) {
      var self = this
      var first = true
      this.writeStream
        .pipe(through2(function (buf, enc, cb) {
          if (first) {
            // change to invalid protocol version
            buf.writeUInt32LE(protodef.VersionDummy.Version.V0_3, 0)
            first = false
          }
          cb(null, buf)
        }))
        .pipe(this.chunker)
        .on('data', function (buf, enc) {
          // write to socket
          self.__socketWrite(buf, enc)
        }).on('error', function (err) {
          expect(err).to.be.an.instanceOf(Error)
          expect(err.message).to.match(/Invalid handshake/)
          done()
        })
    })

    if (protodef.VersionDummy.Version.V1_0) {
      // V1_0 specific tests

      it('should validate a valid handshake V1_0', function (done) {
        var self = this
        var state = this.chunker.__streamChunkerState
        this.writeStream.pipe(this.chunker).on('data', function (buf, enc) {
          // write to socket
          self.__socketWrite(buf, enc)
          // chunk assertions
          var lastZero = last(state.handshakeZeroIndexes)
          expect(buf[lastZero]).to.equal(0)
          expect(buf.length).to.equal(lastZero + 1)
          if (state.handshakeComplete) {
            done()
          }
        })
      })

      it('should invalidate a invalid handshake (non-json)', function (done) {
        var self = this
        var through = through2()
        through
          .pipe(this.chunker)
          .on('data', function (buf, enc) {
            // write to socket
            self.__socketWrite(buf, enc)
          }).on('error', function (err) {
            expect(err).to.be.an.instanceOf(Error)
            expect(err.message).to.match(/Invalid handshake/)
            done()
          })
        var buf = new Buffer('1234FOO\0')
        buf.writeUInt32LE(protodef.VersionDummy.Version.V1_0, 0)
        through.write(buf)
      })
      return
    }

    // V0_4 specific tests

    it('should validate a valid handshake V1_0', function (done) {
      var self = this
      this.writeStream.pipe(this.chunker).on('data', function (buf, enc) {
        // write to socket
        self.__socketWrite(buf, enc)
        // chunk assertions
        expect(buf.length).to.equal(12)
        done()
      })
    })

    it('should invalidate a invalid handshake (token) V0_4', function (done) {
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

    it('should invalidate a invalid handshake (protocol) V0_4', function (done) {
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
}
