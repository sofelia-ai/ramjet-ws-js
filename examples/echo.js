// Echo server on the ramjet runtime, in the shape uWebSockets.js users expect.
//
//   node examples/echo.js            # serve on 9001 until interrupted
//   node examples/echo.js --selftest # serve, then drive it and check the bytes
//
// The self-test client is hand-rolled on node:net rather than pulling in `ws`.
// A binding whose only proof is another WebSocket library has tested that
// library as much as itself, and the framing here is eighty lines.

const net = require('node:net')
const crypto = require('node:crypto')
const { App } = require('../index.js')

const PORT = Number(process.env.PORT || 9001)
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

// ---- server ---------------------------------------------------------------

const app = App()
app.ws('/*', {
  open: (ws) => {
    if (process.env.VERBOSE) console.log('open')
  },
  message: (ws, msg, isBinary) => {
    ws.send(msg, isBinary)
  },
  close: (ws, code) => {
    if (process.env.VERBOSE) console.log('close', code)
  },
})

app.listen(PORT, (ok) => {
  if (!ok) {
    console.error(`failed to bind ${PORT}`)
    process.exit(1)
  }
  console.log(`ramjet-ws listening on 0.0.0.0:${PORT}`)
  if (process.argv.includes('--selftest')) {
    selftest().then(
      () => {
        console.log('\nall checks passed')
        process.exit(0)
      },
      (err) => {
        console.error('\nFAILED:', err.message)
        process.exit(1)
      },
    )
  }
})

// ---- a small WebSocket client --------------------------------------------

/** Encode one client frame. Clients must mask; servers must not. */
function frame(opcode, payload, fin = true) {
  const mask = crypto.randomBytes(4)
  const n = payload.length
  let header
  if (n < 126) {
    header = Buffer.from([(fin ? 0x80 : 0) | opcode, 0x80 | n])
  } else if (n <= 0xffff) {
    header = Buffer.alloc(4)
    header[0] = (fin ? 0x80 : 0) | opcode
    header[1] = 0x80 | 126
    header.writeUInt16BE(n, 2)
  } else {
    header = Buffer.alloc(10)
    header[0] = (fin ? 0x80 : 0) | opcode
    header[1] = 0x80 | 127
    header.writeBigUInt64BE(BigInt(n), 2)
  }
  const masked = Buffer.allocUnsafe(n)
  for (let i = 0; i < n; i++) masked[i] = payload[i] ^ mask[i & 3]
  return Buffer.concat([header, mask, masked])
}

/** Pull whole server frames out of a buffer. Returns [frames, leftover]. */
function parse(buf) {
  const out = []
  let i = 0
  while (i + 2 <= buf.length) {
    const opcode = buf[i] & 0x0f
    if (buf[i + 1] & 0x80) throw new Error('server frame must not be masked')
    let len = buf[i + 1] & 0x7f
    let j = i + 2
    if (len === 126) {
      if (j + 2 > buf.length) break
      len = buf.readUInt16BE(j)
      j += 2
    } else if (len === 127) {
      if (j + 8 > buf.length) break
      len = Number(buf.readBigUInt64BE(j))
      j += 8
    }
    if (j + len > buf.length) break
    out.push({ opcode, payload: buf.subarray(j, j + len) })
    i = j + len
  }
  return [out, buf.subarray(i)]
}

function connect(port) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1')
    sock.on('error', reject)
    sock.once('connect', () => {
      const key = crypto.randomBytes(16).toString('base64')
      sock.write(
        `GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n` +
          `Connection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\n` +
          `Sec-WebSocket-Version: 13\r\n\r\n`,
      )
      let buf = Buffer.alloc(0)
      const onData = (chunk) => {
        buf = Buffer.concat([buf, chunk])
        const end = buf.indexOf('\r\n\r\n')
        if (end < 0) return
        const head = buf.subarray(0, end).toString()
        sock.removeListener('data', onData)
        if (!head.startsWith('HTTP/1.1 101')) {
          return reject(new Error(`bad upgrade: ${head.split('\r\n')[0]}`))
        }
        const expect = crypto.createHash('sha1').update(key + GUID).digest('base64')
        if (!head.includes(expect)) return reject(new Error('bad Sec-WebSocket-Accept'))
        resolve({ sock, rest: buf.subarray(end + 4) })
      }
      sock.on('data', onData)
    })
  })
}

/** Wait for one whole frame from the server. */
function next(state) {
  return new Promise((resolve, reject) => {
    const tryParse = () => {
      const [frames, leftover] = parse(state.rest)
      if (frames.length > 0) {
        state.rest = leftover
        state.sock.removeListener('data', onData)
        state.sock.removeListener('error', reject)
        resolve(frames[0])
        return true
      }
      return false
    }
    const onData = (chunk) => {
      state.rest = Buffer.concat([state.rest, chunk])
      tryParse()
    }
    state.sock.on('error', reject)
    if (tryParse()) return
    state.sock.on('data', onData)
  })
}

// ---- the checks -----------------------------------------------------------

async function selftest() {
  const state = await connect(PORT)
  const check = (name, ok, detail = '') => {
    if (!ok) throw new Error(`${name}${detail ? ': ' + detail : ''}`)
    console.log(`ok: ${name}`)
  }

  // Text.
  state.sock.write(frame(0x1, Buffer.from('hello ramjet')))
  let got = await next(state)
  check('text echo', got.opcode === 0x1 && got.payload.toString() === 'hello ramjet')

  // Binary, every byte value so a masking slip shows up.
  const bin = Buffer.from(Array.from({ length: 1024 }, (_, i) => i & 0xff))
  state.sock.write(frame(0x2, bin))
  got = await next(state)
  check('binary echo (1 KiB)', got.opcode === 0x2 && got.payload.equals(bin))

  // One large frame: exercises the 64-bit length path and a payload far larger
  // than a single read.
  const big = crypto.randomBytes(128 * 1024)
  state.sock.write(frame(0x2, big))
  got = await next(state)
  check(
    'binary echo (128 KiB, one frame)',
    got.opcode === 0x2 && got.payload.equals(big),
    `${got.payload.length} bytes back`,
  )

  // The same size split across many frames: first + continuations + last, which
  // is the path where the decoder has to reassemble across reads.
  const parts = []
  for (let i = 0; i < 32; i++) parts.push(crypto.randomBytes(4 * 1024))
  const whole = Buffer.concat(parts)
  state.sock.write(frame(0x2, parts[0], false))
  for (let i = 1; i < parts.length - 1; i++) {
    state.sock.write(frame(0x0, parts[i], false))
  }
  state.sock.write(frame(0x0, parts[parts.length - 1], true))
  got = await next(state)
  check(
    'binary echo (128 KiB across 32 frames)',
    got.opcode === 0x2 && got.payload.equals(whole),
    `${got.payload.length} of ${whole.length} bytes back`,
  )

  // Text again after the big transfers: proves the stream is still in sync
  // rather than merely that the last big read happened to line up.
  state.sock.write(frame(0x1, Buffer.from('still here')))
  got = await next(state)
  check('text echo after large messages', got.payload.toString() === 'still here')

  // A ping is answered by the binding itself, not by the JS handler.
  state.sock.write(frame(0x9, Buffer.from('ping-payload')))
  got = await next(state)
  check('pong', got.opcode === 0xa && got.payload.toString() === 'ping-payload')

  // Clean close: the server echoes the code and hangs up.
  const code = Buffer.alloc(2)
  code.writeUInt16BE(1000)
  state.sock.write(frame(0x8, code))
  got = await next(state)
  check('close echo', got.opcode === 0x8 && got.payload.readUInt16BE(0) === 1000)

  await new Promise((resolve) => {
    state.sock.once('close', resolve)
    state.sock.end()
    setTimeout(resolve, 500)
  })
  check('socket closed', true)
}
