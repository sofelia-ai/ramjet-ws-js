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
| `ws.send(data, isBinary)` | `data` may be a string or `Buffer` |
| `ws.close()` | close it |

Not implemented yet: pub/sub, backpressure signalling, HTTP routing, TLS
termination, and per-message compression. The Rust runtime supports TLS; the
binding does not expose it yet.

## Building

```sh
npm install
npx napi build --platform --release
node examples/echo.js --selftest
```

### Known gap: the generated `index.js`

`napi build --platform` is supposed to emit an `index.js` that picks the right
platform binary and re-exports it. With `napi`/`napi-derive` 2 and
`@napi-rs/cli` 3 it emits the `.node` and an empty `index.d.ts`, and no
`index.js` at all — the CLI reads type metadata the v2 derive macro does not
write in the shape v3 expects. Adding `features = ["type-def"]` to
`napi-derive` did not change it.

The addon itself is fine: `require('./ramjet-ws.<platform>-<arch>.node')`
exports `App` and everything works, which is what `examples/echo.js` does when
the wrapper is missing. Fixing it properly means either pinning
`@napi-rs/cli` to 2.x or moving the crate to `napi` 3 — a version decision
rather than a code one, so it is left for whoever owns the release.

## License

MIT or Apache-2.0, at your option.
