// The same echo on uWebSockets.js. Compression off on both sides, no idle
// timeout, and the same 32 MiB message ceiling as Ramjet. uWebSockets.js
// otherwise defaults to 16 KiB and would reset larger benchmark messages.
const uWS = require('uWebSockets.js')
const port = Number(process.argv[2] || 9402)

uWS.App().ws('/*', {
  compression: uWS.DISABLED,
  idleTimeout: 0,
  maxPayloadLength: 32 * 1024 * 1024,
  message: (ws, msg, isBinary) => { ws.send(msg, isBinary) },
}).listen(port, (token) => {
  console.log(token ? `uWebSockets.js listening on ${port}` : 'listen failed')
})
