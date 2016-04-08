require('./fixtures/load-env.js')

var afterEach = global.afterEach
var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var net = require('net')

var expect = require('chai').expect
var last = require('101/last')
var proxyquire = require('proxyquire')
var shimmer = require('shimmer')
var through2 = require('through2')

var createResChunk = require('./fixtures/create-res-chunk.js')

function parseResBuffer (responseBuf) {
  var ast = JSON.parse(responseBuf.slice(12, responseBuf.length).toString())
  return ast
}

describe('response stream chunker functional tests', function () {
  describe('protocol v0.4 tests', function () {
    proxyquire.noCallThru()
    proxyquire.noPreserveCache()
    var rethinkdb = require('./fixtures/rethinkdb@2.2.2')
    var protodef = require('./fixtures/rethinkdb@2.2.2/proto-def.js')
    var createResponseStreamChunker = proxyquire('../response-stream-chunker.js', {
      'rethinkdb': rethinkdb,
      'rethinkdb/proto-def': protodef
    })
    runTests(rethinkdb, protodef, createResponseStreamChunker)
  })
  describe('protocol v1.0 tests', function () {
    runTests(
      require('rethinkdb'),
      require('rethinkdb/proto-def'),
      require('../response-stream-chunker.js'))
  })
})

function runTests (r, protodef, createResponseStreamChunker) {
  it('should chunk a rethinkdb response stream (single response)', function (done) {
    var res = {
      t: 16,
      r: 'blah blah blah error',
      b: []
    }
    var responseStreamChunker = createResponseStreamChunker(true)
    responseStreamChunker.on('data', function (resBuf) {
      try {
        expect(parseResBuffer(resBuf)).to.deep.equal(res)
        done()
      } catch (err) {
        done(err)
      }
    })
    var resChunk = createResChunk(res)
    var partialChunks = [
      resChunk.slice(0, 3),
      resChunk.slice(3, 6),
      resChunk.slice(6, 9),
      resChunk.slice(9)
    ]
    partialChunks.forEach(function (data) {
      responseStreamChunker.write(data)
    })
  })

  it('should chunk a rethinkdb resposnse stream (partial responses)', function (done) {
    var resArr = [{
      t: 16,
      r: 'error0',
      b: []
    }, {
      t: 16,
      r: 'error1',
      b: []
    }, {
      t: 16,
      r: 'error2',
      b: []
    }]
    var chunkCount = 0
    var responseStreamChunker = createResponseStreamChunker(true)
    responseStreamChunker.on('data', function (resBuf) {
      try {
        expect(parseResBuffer(resBuf)).to.deep.equal(resArr[chunkCount])
        chunkCount++
        if (chunkCount === resArr.length) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    var partialChunks = resArr.reduce(function (partialChunks, res) {
      var resChunk = createResChunk(res)
      return partialChunks.concat([
        resChunk.slice(0, 3),
        resChunk.slice(3, 6),
        resChunk.slice(6, 9),
        resChunk.slice(9)
      ])
    }, [])
    partialChunks.forEach(function (data) {
      responseStreamChunker.write(data)
    })
  })

  it('should chunk a rethinkdb resposnse stream (multiple responses)', function (done) {
    var resArr = [{
      t: 16,
      r: 'error0',
      b: []
    }, {
      t: 16,
      r: 'error1',
      b: []
    }, {
      t: 16,
      r: 'error2',
      b: []
    }]
    var chunkCount = 0
    var responseStreamChunker = createResponseStreamChunker(true)
    responseStreamChunker.on('data', function (resBuf) {
      try {
        expect(parseResBuffer(resBuf)).to.deep.equal(resArr[chunkCount])
        chunkCount++
        if (chunkCount === resArr.length) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    var multipleChunks = resArr.reduce(function (multipleChunks, res) {
      var resChunk = createResChunk(res)
      return Buffer.concat([
        multipleChunks,
        resChunk
      ])
    }, new Buffer(0))
    responseStreamChunker.write(multipleChunks)
  })

  it('should allow insertion of responses (mid stream)', function (done) {
    var resArr = [{
      t: 16,
      r: ['error0'],
      b: []
    }, {
      t: 16,
      r: ['error1'],
      b: []
    }, {
      t: 16,
      r: ['error2'],
      b: []
    }]
    var insertedRes = {
      t: 16,
      r: ['inserted error'],
      b: []
    }
    var expectedResArr = resArr.slice(0, 1).concat(insertedRes).concat(resArr.slice(1))
    var chunkCount = 0
    var responseStreamChunker = createResponseStreamChunker(true)
    responseStreamChunker.on('data', function (resBuf) {
      try {
        expect(parseResBuffer(resBuf)).to.deep.equal(expectedResArr[chunkCount])
        chunkCount++
        if (chunkCount === expectedResArr.length) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    var partialChunks = resArr.reduce(function (partialChunks, res) {
      var resChunk = createResChunk(res)
      return partialChunks.concat([
        resChunk.slice(0, 3),
        resChunk.slice(3, 6),
        resChunk.slice(6, 9),
        resChunk.slice(9)
      ])
    }, [])
    partialChunks.forEach(function (data, i) {
      responseStreamChunker.write(data)
      if (i === 6) { // mid second-chunk
        responseStreamChunker.insertClientError(new Buffer(8), insertedRes.r[0])
      }
    })
  })

  it('should allow insertion of responses (at chunk break)', function (done) {
    var resArr = [{
      t: 16,
      r: ['error0'],
      b: []
    }, {
      t: 16,
      r: ['error1'],
      b: []
    }, {
      t: 16,
      r: ['error2'],
      b: []
    }]
    var insertedRes = {
      t: 16,
      r: ['inserted error'],
      b: []
    }
    var expectedResArr = resArr.slice(0, 2).concat(insertedRes).concat(resArr.slice(2))
    var chunkCount = 0
    var responseStreamChunker = createResponseStreamChunker(true)
    responseStreamChunker.on('data', function (resBuf) {
      try {
        expect(parseResBuffer(resBuf)).to.deep.equal(expectedResArr[chunkCount])
        chunkCount++
        if (chunkCount === resArr.length + 1) {
          done()
        }
      } catch (err) {
        done(err)
      }
    })
    var partialChunks = resArr.reduce(function (partialChunks, res) {
      var resChunk = createResChunk(res)
      return partialChunks.concat([
        resChunk.slice(0, 3),
        resChunk.slice(3, 6),
        resChunk.slice(6, 9),
        resChunk.slice(9)
      ])
    }, [])
    partialChunks.forEach(function (data, i) {
      responseStreamChunker.write(data)
      if (i === 7) { // post second-chunk
        responseStreamChunker.insertClientError(new Buffer(8), insertedRes.r[0], [])
      }
    })
  })

  it('should error if chunk size greater than max', function (done) {
    var res = {
      t: 16,
      r: 'blah blah blah error',
      b: []
    }
    var responseStreamChunker = createResponseStreamChunker(true, 1)
    responseStreamChunker.on('error', function (err) {
      try {
        expect(err).to.exist
        expect(err.message).to.match(/Chunk length/)
        expect(err.data.state).to.deep.contain({
          chunkLen: 54,
          maxChunkLen: 1
        })
        done()
      } catch (err) {
        done(err)
      }
    })
    var resChunk = createResChunk(res)
    var partialChunks = [
      resChunk.slice(0, 3),
      resChunk.slice(3, 6),
      resChunk.slice(6, 9),
      resChunk.slice(9)
    ]
    partialChunks.forEach(function (data) {
      responseStreamChunker.write(data)
    })
  })

  describe('handshake validation', function () {
    describe('successful (using db)', function () {
      beforeEach(function (done) {
        var self = this
        shimmer.wrap(net, 'connect', function (o) {
          return function () {
            var socket = self.socket = o.apply(this, arguments)
            // pipe socket to responseChunker
            self.chunker = createResponseStreamChunker()
            socket.pipe(self.chunker)
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
        if (this.conn) {
          this.conn.close()
        }
        done()
      })

      if (protodef.VersionDummy.Version.V1_0) {
        // V1_0 specific tests

        it('should validate a valid handshake V1_0', function (done) {
          var state = this.chunker.__streamChunkerState
          this.chunker.on('data', function (buf) {
            // chunk assertions
            var lastZero = last(state.handshakeZeroIndexes)
            expect(buf[lastZero]).to.equal(0)
            expect(buf.length).to.equal(lastZero + 1)
            if (state.handshakeComplete) {
              done()
            }
          })
        })

        it('should invalidate a invalid handshake V1_0 (json.success=false)', function (done) {
          var state = this.chunker.__streamChunkerState
          var chunker2 = createResponseStreamChunker()
          this.chunker
            .pipe(through2(function (buf, enc, cb) {
              var json
              if (state.handshakeComplete) {
                try {
                  json = JSON.parse(buf.slice(0, -1).toString())
                  json.success = false
                  buf = new Buffer(JSON.stringify(json) + '\0')
                } catch (err) {
                  done(err)
                }
              }
              cb(null, buf)
            }))
            .pipe(chunker2)
          chunker2.on('error', function (err) {
            expect(err).to.be.an.instanceOf(Error)
            expect(err.message).to.match(/Invalid handshake/)
            done()
          })
        })

        return
      }

      // V0_4 specific tests

      it('should validate a valid handshake', function (done) {
        this.chunker.on('data', function (buf) {
          expect(buf[buf.length - 1]).to.equal(0)
          expect(buf.toString()).to.equal('SUCCESS\0')
          done()
        })
      })
    })

    describe('mock failures', function () {
      if (protodef.VersionDummy.Version.V1_0) {
        // V1_0 specific tests

        it('should invalidate a invalid handshake', function (done) {
          var responseStreamChunker = createResponseStreamChunker()
          var handshake = new Buffer(8)
          handshake.fill(0)
          handshake.write('ERROR\0')
          responseStreamChunker.on('error', function (err) {
            expect(err).to.be.an.instanceOf(Error)
            expect(err.message).to.match(/Invalid handshake/)
            done()
          }).write(handshake)
        })

        it('should invalidate a invalid handshake (max length)', function (done) {
          var responseStreamChunker = createResponseStreamChunker()
          var handshake = new Buffer(2000)
          handshake.fill(1)
          handshake.write('\0', 1999)
          responseStreamChunker.on('error', function (err) {
            expect(err).to.be.an.instanceOf(Error)
            console.log(err.message)
            expect(err.message).to.match(/Invalid handshake/)
            expect(err.message).to.match(/max length/)
            done()
          }).write(handshake)
        })

        return
      }

      // V0_4 specific tests

      it('should invalidate a invalid handshake', function (done) {
        var responseStreamChunker = createResponseStreamChunker()
        var handshake = new Buffer(8)
        handshake.fill(0)
        handshake.write('ERROR\0')
        responseStreamChunker.on('error', function (err) {
          expect(err).to.be.an.instanceOf(Error)
          expect(err.message).to.match(/Invalid handshake/)
          done()
        }).write(handshake)
      })
    })
  })
}
