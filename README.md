# ramjet-ws

WebSocket server for Node.js on the [ramjet](https://github.com/sofelia-ai/ramjet)
runtime — io_uring on Linux, kqueue on macOS. A native addon, not a wrapper
around `net`: the socket layer never enters JavaScript.

```js
const { App } = require('ramjet-ws')

App().ws('/*', {
  open:    (ws)                  => console.log('open'),
  message: (ws, msg, isBinary)   => ws.send(msg, isBinary),
  close:   (ws, code)            => console.log('closed', code),
}).listen(9001, (ok) => {
  if (ok) console.log('listening on 9001')
})
```

## Install

```sh
npm install ramjet-ws
```

Prebuilt binaries for Linux and macOS on x64 and arm64. No compiler needed, no
`node-gyp`. There is no Windows build — the underlying runtime has io_uring and
kqueue backends and no IOCP one.

## Why

The engine underneath is measured, not asserted: **+52.5% throughput and 45%
better p99 against uWebSockets** on a real NIC, 100,000 connections in 53 MB,
and roughly one syscall per thousand requests. It passes all 517 Autobahn
conformance cases on both `ws://` and `wss://`.

Numbers and their caveats: [ramjet BENCHMARKS.md](https://github.com/sofelia-ai/ramjet/blob/main/BENCHMARKS.md).

Note that those figures are for the Rust server. Every message crossing into
JavaScript costs a callback into V8, so a Node binding cannot reproduce them —
what it inherits is the connection scaling, the memory profile, and the
protocol correctness.

## API

Deliberately small, and shaped after uWebSockets.js so its users are not
surprised.

| | |
|---|---|
| `App()` | create a server |
| `.ws(pattern, behavior)` | register handlers; `pattern` is accepted and currently ignored |
| `.listen(port, cb)` | start; `cb(true)` on success |
| `behavior.open(ws)` | connection established |
| `behavior.message(ws, msg, isBinary)` | `msg` is a `Buffer` |
| `behavior.close(ws, code)` | connection gone |
| `ws.send(data, isBinary)` | `data` may be a string or `Buffer`; returns `false` if the message was dropped because the connection is at its 4 MiB outbound cap |
| `ws.close()` | close it |

Outbound buffering is capped per connection at 4 MiB, and `ws.send` returns
`false` when a message is dropped for exceeding it. That bounds memory against a
peer that stops reading; it is not the full backpressure contract — there is no
`drain` event yet, so a producer that wants to resume has to retry rather than be
told when there is room.

Not implemented yet: pub/sub, `drain`/backpressure signalling, HTTP routing, TLS
termination, and per-message compression. The Rust runtime supports TLS; the
binding does not expose it yet.

## Building

```sh
npm install
npx napi build --platform --release   # emits index.js, index.d.ts and the .node
node examples/echo.js --selftest
```

Built on `napi` 3. The 2.x line is legacy, and `@napi-rs/cli` 3 — what npm
resolves today — does not emit the JS wrapper for a v2 crate, so a v2 package
would have shipped with a broken layout and a migration still owed.

## License

MIT or Apache-2.0, at your option.
