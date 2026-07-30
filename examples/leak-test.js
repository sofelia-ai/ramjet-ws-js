// Proves the per-connection `ws` handle is actually released, not merely
// forgotten from our map.
//
// Two layers, because the obvious version is worthless: asserting our own
// `handleCount()` drains passes even when the napi `Ref` is never unref'd —
// the map entry goes either way, and the V8 reference is the thing that leaks.
// So the parent runs the server in a child and inspects its shutdown output,
// where napi reports "ObjectRef is not unref" for anything still held.
//
// Registers NO `close` handler on purpose: cleanup used to hang off that
// callback, so an app that did not supply one leaked a JS object per
// connection — invisible in every benchmark and fatal after a few days.
const net = require('node:net')
const crypto = require('node:crypto')
const { spawnSync } = require('node:child_process')

const PORT = 9451
const CONNS = 60
const LEAK_MARKER = 'not unref'

if (!process.argv.includes('--child')) {
  const r = spawnSync(process.execPath, [__filename, '--child'], { encoding: 'utf8' })
  const out = (r.stdout || '') + (r.stderr || '')
  process.stdout.write(r.stdout || '')

  let failed = false
  if (r.status !== 0) {
    console.error(`FAILED: child exited ${r.status}`)
    failed = true
  }
  if (out.includes(LEAK_MARKER)) {
    console.error(`FAILED: napi reported an unreleased handle — the ws Ref leaked`)
    failed = true
  }
  if (!failed) console.log('\nok: handles cached, drained, and released')
  process.exit(failed ? 1 : 0)
}

const { App, handleCount } = require('../index.js')

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
        // One masked text frame so `message` fires and the handle is built:
        // the cache is lazy, so without traffic there is nothing to leak.
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
  if (!ok) process.exit(1)
  const sockets = await Promise.all(Array.from({ length: CONNS }, () => open(PORT)))

  const filled = handleCount()
  console.log(`handles with ${CONNS} connections open: ${filled}`)
  // Asserting only the drain would pass having never cached anything at all.
  if (filled !== CONNS) {
    console.error(`FAILED: cache holds ${filled}, expected ${CONNS}`)
    process.exit(1)
  }

  sockets.forEach((s) => s.destroy())
  setTimeout(() => {
    const left = handleCount()
    console.log(`handles after all ${CONNS} closed: ${left}`)
    if (left !== 0) {
      console.error(`FAILED: ${left} handle(s) still in the map`)
      process.exit(1)
    }
    // Exit cleanly with nothing open: anything napi reports now is a real leak.
    process.exit(0)
  }, 1200)
})
