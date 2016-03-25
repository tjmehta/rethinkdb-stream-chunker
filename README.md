# rethinkdb-stream-chunker
Chunk RethinkDB protocol stream into query or response buffers

# Installation
```bash
npm i --save rethinkdb-stream-chunker
```

# Examples

## QueryStreamChunker
Streams piped throught as QueryStreamChunker will be "chunked" in to query buffers.

This enables functionality that require inspection of incoming queries as a whole, such as query validation

```js
var QueryStreamChunker = require('rethinkdb-stream-chunker').QueryStreamChunker
var r = require('rethinkdb')

var queryStream = ...
var reql = r.table('foo').get('bar')
// create a query buffer
var token = new Buffer(8)
var len = new Buffer(4)
var term = JSON.stringify([1, reql.build()])
len.writeUInt32LE(term.length)
term = new Buffer(term)
var queryBuf = Buffer.concat([token, len, term])

// pipe queryStream to chunker
queryStream
  .pipe(new QueryStreamChunker())
  .on('data', function (data) {
    // data will be equivalent to full queryBuf
    // even though it was written to the stream as partial chunks
  })

var partialChunks = [
  queryBuf.slice(0, 5),
  queryBuf.slice(5, 10),
  queryBuf.slice(10, 12),
  queryBuf.slice(12) // to end
]

partialChunks.forEach(function (buf) {
  queryStream.write(buf)
})
```

## ResponseStreamChunker
Streams piped throught as QueryStreamChunker will be "chunked" in to response buffers.

This enables functionality that require inspection of incoming queries as a whole, such as query validation.

ResponseStreamChunker allows for easy injection of custom responses (such as responding w/ an error when a custom validation of a query fails).

```js
var ResponseStreamChunker = require('rethinkdb-stream-chunker').ResponseStreamChunker
var r = require('rethinkdb')
var responseChunker = new ResponseStreamChunker()

r.connect(function (err, conn) {
  if (err) { throw err }
  conn.rawSocket
    .pipe(responseChunker)
    .on(function (responseBuf) {
      // full response buff
      var tokenBuf = responseBuf.slice(0, 8)
      var lenBuf = responseBuf.slice(8, 12)
      var responseAst = JSON.parse(responseBuf.slice(12).toString())
      // ...
    })

  // you can even use responseChunker to write a custom error
  responseChunker.insertClientError(queryTokenBuf, 'access denied')
  // ofcourse, responseChunker should be piped to something else...
})
```

# License
MIT