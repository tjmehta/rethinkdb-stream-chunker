var net = require('net')

var noop = require('101/noop')
var r = require('rethinkdb')
var shimmer = require('shimmer')

var MockSocket = require('./mock-socket.js')

module.exports = createMockConnection

function createMockConnection (cb) {
  var mockSocket = new MockSocket()
  mockSocket.setNoDelay = noop
  mockSocket.setKeepAlive = noop
  // stub `net.connect` to return passthrough stream instead of socket
  shimmer.wrap(net, 'connect', function (orig) {
    return function () {
      return mockSocket
    }
  })
  // connect to rethinkdb
  var conn = r.connect(function (err, conn) {
    // dont let the connection attempt to parse data
    // mockSocket.removeAllListeners('data')
    mockSocket.connected = true // hack
    cb(err, conn)
  })
  // un-stub `net.connect`, so it can be used like normal ( if used elsewhere)
  shimmer.unwrap(net, 'connect')
  // mock socket handshake, connect
  mockSocket.writeMockHandshake()

  return conn
}
