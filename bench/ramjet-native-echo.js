// Opt-in zero-JavaScript message path. Handshake lifecycle remains visible to
// Node, but data frames are decoded and re-encoded entirely on the reactor.
const { App } = require('../index.js')
const port = Number(process.argv[2] || 9401)

const app = App()
app.ws('/*', { nativeEcho: true })
app.listen(port, (ok) => {
  console.log(ok ? `ramjet-ws native echo listening on ${port}` : 'listen failed')
})
