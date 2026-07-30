// Proves the per-connection `ws` handle cache drains.
//
// Its own file, and wired into `npm test`, because a leak test that CI does not
// run is decoration. Deliberately registers NO `close` handler: cleanup used to
// hang off that callback, so an app that did not provide one leaked a JS object
// per connection — invisible in every benchmark and fatal after a few days.
const net = require('node:net')
const crypto = require('node:crypto')
const { App, handleCount } = require('../index.js')

const PORT = 9451
const CONNS = 60

const open = (port) =>
  new Promise((resolve) => {
    const s = net.connect(port, '127.0.0.1', () => {
      const key = crypto.randomBytes(16).toString('base64')
      s.write(
        `GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`
      )
    })
    let handshaked = false
    s.on('data', () => {
      if (!handshaked) {
        handshaked = true
        // One masked text frame, so `message` fires and the handle is built.
        // The cache is lazy: without traffic there is nothing to leak.
        s.write(Buffer.from([0x81, 0x81, 1, 2, 3, 4, 'a'.charCodeAt(0) ^ 1]))
        return
      }
      resolve(s)
    })
    s.on('error', () => resolve(s))
  })

const app = App()
app.ws('/*', { message: (ws, msg, isBinary) => ws.send(msg, isBinary) })

app.listen(PORT, async (ok) => {
  if (!ok) {
    console.error('listen failed')
    process.exit(1)
  }
  const sockets = await Promise.all(Array.from({ length: CONNS }, () => open(PORT)))
  const filled = handleCount()
  console.log(`handles with ${CONNS} connections open: ${filled}`)

  // Asserting only the drain would pass having never cached anything at all —
  // the first version of this test did exactly that and was worthless.
  if (filled !== CONNS) {
    console.error(`FAILED: cache holds ${filled}, expected ${CONNS}`)
    process.exit(1)
  }

  sockets.forEach((s) => s.destroy())
  setTimeout(() => {
    const left = handleCount()
    console.log(`handles after all ${CONNS} closed: ${left}`)
    if (left !== 0) {
      console.error(`FAILED: ${left} handle(s) leaked`)
      process.exit(1)
    }
    console.log(`\nok: cache filled to ${CONNS} and drained to zero`)
    process.exit(0)
  }, 1200)
})
