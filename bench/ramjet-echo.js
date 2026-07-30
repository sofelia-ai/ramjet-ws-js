// Echo server on ramjet-ws, for the head-to-head against uWebSockets.js.
// Deliberately the smallest possible handler: any work here would be measuring
// the handler rather than the binding.
//
// Not chained, because `.ws()` currently returns undefined rather than the app
// — an incompatibility with uWebSockets.js that is being fixed separately.
const { App } = require('../index.js')
const port = Number(process.argv[2] || 9401)

const app = App()
app.ws('/*', {
  message: (ws, msg, isBinary) => { ws.send(msg, isBinary) },
})
app.listen(port, (ok) => {
  console.log(ok ? `ramjet-ws listening on ${port}` : 'listen failed')
})
