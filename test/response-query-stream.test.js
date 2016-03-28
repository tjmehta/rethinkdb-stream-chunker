var beforeEach = global.beforeEach
var describe = global.describe
var it = global.it

var expect = require('chai').expect
var r = require('rethinkdb')

var createResChunk = require('./fixtures/create-res-chunk.js')
var createResponseStreamChunker = require('../index.js').ResponseStreamChunker

function parseResBuffer (responseBuf) {
  var ast = JSON.parse(responseBuf.slice(12, responseBuf.length).toString())
  return ast
}

describe('response stream chunker functional tests', function() {
  it('should chunk a rethinkdb response stream (single response)', function (done) {
    var self = this
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
    var self = this
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
    var self = this
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
    var self = this
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
    var self = this
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

  describe('handshake validation', function () {
    it('should validate a valid handshake', function (done) {
      var responseStreamChunker = createResponseStreamChunker()
      var handshake = new Buffer(8)
      handshake.fill(0)
      handshake.write('SUCCESS')
      responseStreamChunker.on('data', function (buf) {
        expect(buf).to.deep.equal(handshake)
        done()
      }).write(handshake)
    })

    it('should invalidate a invalid handshake', function (done) {
      var responseStreamChunker = createResponseStreamChunker()
      var handshake = new Buffer(8)
      handshake.fill(0)
      handshake.write('ERROR')
      responseStreamChunker.on('error', function (err) {
        expect(err).to.be.an.instanceOf(Error)
        expect(err.message).to.match(/Invalid handshake/)
        done()
      }).write(handshake)
    })
  })
})