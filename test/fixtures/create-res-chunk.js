module.exports = createResChunk

function createResChunk (resAst, tokenBuf) {
  var resStr = JSON.stringify(resAst)
  tokenBuf = tokenBuf || new Buffer(8)
  var lenBuf = new Buffer(4)
  lenBuf.writeUInt32LE(resStr.length, 0)
  var resBuf = new Buffer(resStr)
  return Buffer.concat([tokenBuf, lenBuf, resBuf])
}
