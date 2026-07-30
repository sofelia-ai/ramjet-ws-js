// The same echo on uWebSockets.js. Compression off on both sides, and no
// idleTimeout, so the two servers differ only in which binding is underneath.
const uWS = require('uWebSockets.js')
const port = Number(process.argv[2] || 9402)

uWS.App().ws('/*', {
  compression: uWS.DISABLED,
  idleTimeout: 0,
  message: (ws, msg, isBinary) => { ws.send(msg, isBinary) },
}).listen(port, (token) => {
  console.log(token ? `uWebSockets.js listening on ${port}` : 'listen failed')
})
