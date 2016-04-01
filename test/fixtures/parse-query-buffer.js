module.exports = parseQueryBuffer

function parseQueryBuffer (queryBuf) {
  var ast = JSON.parse(queryBuf.slice(12, queryBuf.length).toString())
  return {
    type: ast[0],
    term: ast[1],
    opts: ast[2]
  }
}
