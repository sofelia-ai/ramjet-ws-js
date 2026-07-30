//! Node.js WebSocket server bindings for the ramjet runtime.
//!
//! The shape is uWebSockets.js: `App().ws(pattern, handlers).listen(port, cb)`.
//! What is underneath is a ramjet reactor on its own thread, because the driver
//! is `!Send` and `wait()` blocks — Node's event loop must never be inside it.
//!
//! So there are two threads and exactly two ways across the boundary:
//!
//!   reactor -> JS    a `ThreadsafeFunction` per registered handler.
//!   JS -> reactor    an `mpsc` queue plus a one-byte write to a `UnixStream`
//!                    the reactor is parked on with `Op::ReadPooled`.
//!
//! The second one is the part that needs care and it is not novel: it is the
//! wake mechanism `examples/echo_mt.rs` in the ramjet repo already uses to hand
//! descriptors to worker threads. `ws.send()` runs on Node's thread while the
//! reactor is asleep inside `wait()`, so queueing the work is not enough —
//! something has to make the reactor's `wait()` return, and a byte down a pipe
//! it is already reading is that something.
//!
//! Connections are identified to JS by an opaque `u64`, never by a descriptor.
//! A descriptor is recycled the instant it closes, so a `ws.send()` that JS
//! issues a moment too late would land on whatever connection inherited the
//! number — the hazard `Op::Close` documents in ramjet. Ids are never reused, an
//! id maps to a descriptor only on the reactor thread, and a send to an id that
//! has gone is silently dropped. Silently, because a race the caller cannot
//! observe is not an error the caller can handle.

use std::collections::HashMap;
use std::io::Write as _;
use std::os::fd::{IntoRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Status, ValueType};
use napi_derive::napi;

use ramjet::net::Listener;
use ramjet::reactor::{Driver, Op, PlatformDriver};
use ramjet_ws::{encode, handshake, Decoder, Event};

/// Autobahn's limit cases run to 16 MiB; sit above that so they echo rather
/// than get refused, but keep a ceiling so a peer cannot name any size it likes.
const MAX_MESSAGE: usize = 32 * 1024 * 1024;

/// Most bytes allowed to be outstanding for one connection: queued on the
/// bridge, or encoded into its outbound buffer and not yet written.
///
/// Without a cap this is a denial-of-service path reachable from the network —
/// a peer that reads slowly, or not at all, makes the server buffer everything
/// JS produces for it until memory runs out. The tradeoff is the usual one: too
/// low and a legitimately bursty producer loses messages it could have sent, too
/// high and one stalled connection can pin the memory anyway. 4 MiB is roughly a
/// second of a slow-but-alive connection at the sizes an echo-shaped workload
/// sees, and 1000 stalled connections at the cap is 4 GiB — which is why the
/// real answer for a fan-out server is a `drain` event and a producer that
/// listens to it, not a bigger number here.
const MAX_OUTBOUND: usize = 4 * 1024 * 1024;

/// Bytes `encode::text`/`encode::binary` will actually produce for a payload of
/// this length: the payload plus a header whose width the length decides.
/// Counting encoded rather than payload bytes is what keeps the accounting exact
/// against what leaves in a write.
fn frame_len(payload: usize) -> usize {
    payload
        + if payload < 126 {
            2
        } else if payload <= usize::from(u16::MAX) {
            4
        } else {
            10
        }
}

const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

// Completion routing: op kind in the high bits, descriptor in the low 32.
const KIND_ACCEPT: u64 = 0;
const KIND_READ: u64 = 1;
const KIND_WRITE: u64 = 2;
const KIND_CLOSE: u64 = 3;
const KIND_WAKE: u64 = 4;

fn tag(kind: u64, fd: RawFd) -> u64 {
    (kind << 32) | u64::from(fd as u32)
}
fn tag_kind(user: u64) -> u64 {
    user >> 32
}
fn tag_fd(user: u64) -> RawFd {
    (user & 0xFFFF_FFFF) as u32 as RawFd
}

/// What JS asks the reactor to do. Everything crossing this way is owned data:
/// nothing borrowed from the JS heap outlives the call that produced it.
enum Cmd {
    Send {
        id: u64,
        data: Vec<u8>,
        binary: bool,
    },
    Close {
        id: u64,
    },
}

/// The JS-side half of the boundary: a queue and the thing that wakes the
/// reactor after something is put on it. Both under one lock, because they are
/// only ever used together and a send that queues without waking is a message
/// that arrives whenever the next unrelated event happens to arrive.
struct Bridge {
    inner: Mutex<(Sender<Cmd>, UnixStream)>,
    listening: AtomicBool,
    /// Bytes outstanding per live connection, shared with the reactor thread.
    ///
    /// This is what lets `ws.send` answer truthfully without a round trip. The
    /// outbound buffer lives on the reactor thread and the JS thread cannot see
    /// it — but it does not need to, because the *same* growth shows up in this
    /// counter, which both threads can touch: incremented here when a message is
    /// accepted, decremented by the reactor when those bytes leave. It therefore
    /// bounds the bridge queue and the outbound buffer together, which matters,
    /// because a message sitting in the channel is exactly as unbounded as one
    /// sitting in `Conn::out`.
    pending: Mutex<HashMap<u64, Arc<AtomicUsize>>>,
}

impl Bridge {
    /// Queue a command and wake the reactor. Returns whether it was queued.
    fn send(&self, cmd: Cmd) -> bool {
        let Ok(mut guard) = self.inner.lock() else {
            return false; // reactor thread panicked; nothing to deliver to
        };
        if guard.0.send(cmd).is_err() {
            return false; // reactor is gone
        }
        // One byte, and its value carries nothing: the reactor drains the whole
        // queue on any wake, so a burst of sends needs no more than one byte to
        // still be delivered in order.
        let _ = guard.1.write_all(&[1]);
        true
    }

    /// Charge `bytes` against a connection's budget, or refuse.
    ///
    /// The whole check is one compare-and-swap, so two `ws.send` calls racing on
    /// the JS thread cannot both slip past a nearly-full budget.
    fn reserve(&self, id: u64, bytes: usize) -> bool {
        let Ok(map) = self.pending.lock() else {
            return false;
        };
        // No counter means the connection is gone. Dropping is right: the id is
        // never reused, so this cannot be somebody else's connection.
        let Some(counter) = map.get(&id) else {
            return false;
        };
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                (cur + bytes <= MAX_OUTBOUND).then_some(cur + bytes)
            })
            .is_ok()
    }

    /// Give budget back once bytes have left, or been abandoned.
    fn release(&self, id: u64, bytes: usize) {
        if let Ok(map) = self.pending.lock() {
            if let Some(counter) = map.get(&id) {
                // Saturating: control frames the reactor generates itself are
                // never charged (the protocol caps them at 125 bytes each), so a
                // write can legitimately retire more bytes than were reserved.
                let _ = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                    Some(cur.saturating_sub(bytes))
                });
            }
        }
    }

    fn track(&self, id: u64) {
        if let Ok(mut map) = self.pending.lock() {
            map.insert(id, Arc::new(AtomicUsize::new(0)));
        }
    }

    fn forget(&self, id: u64) {
        if let Ok(mut map) = self.pending.lock() {
            map.remove(&id);
        }
    }
}

/// A handler that can be called from the reactor thread.
///
/// `CalleeHandled = false` is napi 3's spelling of what napi 2 called
/// `ErrorStrategy::Fatal`: the reactor passes a value, not a `Result`, because a
/// throwing handler is a bug in the application rather than a condition the
/// reactor could do anything about. The args the JS side actually receives are
/// built in the callback, so they are a separate type from what is sent.
type Callback<T, Args> = ThreadsafeFunction<T, Unknown<'static>, Args, Status, false>;

/// The `ws` handle every handler receives as its first argument.
type WsArg = Object<'static>;

/// What the reactor sends for a message, and what JS receives for one. Named
/// rather than inlined so the `Handlers` fields stay readable.
type MessageIn = (u64, Vec<u8>, bool);
type MessageOut = FnArgs<(WsArg, Buffer, bool)>;

/// The same for a close: what the reactor sends, and what JS receives.
///
/// `FnArgs` rather than a bare tuple is load-bearing — a plain tuple arrives in
/// JS as one array-like argument rather than as separate parameters, which
/// shows up as `ws.send is not a function` when the handler destructures it.
type CloseIn = (u64, u16);
type CloseOut = FnArgs<(WsArg, u16)>;

/// Handlers registered from JS, already wrapped for cross-thread calling.
#[derive(Default)]
struct Handlers {
    open: Option<Callback<u64, WsArg>>,
    /// Connection id, payload, and whether it arrived as binary.
    message: Option<Callback<MessageIn, MessageOut>>,
    /// Connection id and close code.
    close: Option<Callback<CloseIn, CloseOut>>,
}

/// Build the `ws` handle JS handlers receive: an object with `send` and
/// `close`, each closing over the connection id and the bridge.
fn ws_handle(env: &Env, bridge: &Arc<Bridge>, id: u64) -> Result<Object<'static>> {
    let mut obj = Object::new(env)?;

    let b = Arc::clone(bridge);
    let send: Function<Unknown, bool> =
        env.create_function_from_closure("send", move |cx: FunctionCallContext| {
            let value: Unknown = cx.get(0)?;
            // A string is text and a Buffer is binary; an explicit second argument
            // overrides both, which is how uWebSockets.js behaves.
            let (data, mut binary) = match value.get_type()? {
                ValueType::String => {
                    let s = value.coerce_to_string()?.into_utf8()?;
                    (s.as_str()?.as_bytes().to_vec(), false)
                }
                _ => {
                    let buf = unsafe { value.cast::<Buffer>()? };
                    (buf.to_vec(), true)
                }
            };
            if let Ok(flag) = cx.get::<bool>(1) {
                binary = flag;
            }
            // Charged before it is queued, so the boolean is about this message
            // rather than about the one before it.
            if !b.reserve(id, frame_len(data.len())) {
                return Ok(false);
            }
            Ok(b.send(Cmd::Send { id, data, binary }))
        })?;
    obj.set_named_property("send", send)?;

    let b = Arc::clone(bridge);
    let close: Function<Unknown, ()> =
        env.create_function_from_closure("close", move |_cx: FunctionCallContext| {
            b.send(Cmd::Close { id });
            Ok(())
        })?;
    obj.set_named_property("close", close)?;
    // The object is handed straight to a JS handler and never held past the
    // call, so widening it to 'static here is sound and is what lets it be
    // returned from the threadsafe callback.
    Ok(unsafe { std::mem::transmute::<Object<'_>, Object<'static>>(obj) })
}

/// One client on the reactor thread. Mirrors `examples/ws_echo.rs`: an HTTP
/// request buffer until the upgrade succeeds, then a frame decoder.
struct Conn {
    id: u64,
    request: Option<Vec<u8>>,
    decoder: Decoder,
    /// Bytes waiting to go out. The driver allows one write per descriptor, so
    /// this is where a burst coalesces into a single `Op::Write`.
    out: Vec<u8>,
    writing: bool,
    close_when_flushed: bool,
    /// The conversation is over; anything further from the peer is ignored.
    ignoring: bool,
    /// Whether JS has been told this connection exists, so `close` is only
    /// reported for connections that got an `open`.
    opened: bool,
}

impl Conn {
    fn new(id: u64) -> Self {
        Conn {
            id,
            request: Some(Vec::new()),
            decoder: Decoder::with_max_message(MAX_MESSAGE),
            out: Vec::new(),
            writing: false,
            close_when_flushed: false,
            ignoring: false,
            opened: false,
        }
    }

    fn send_close(&mut self, code: u16) {
        let _ = encode::close(&mut self.out, code, "");
        self.close_when_flushed = true;
        self.ignoring = true;
    }
}

struct Server {
    d: PlatformDriver,
    conns: HashMap<RawFd, Conn>,
    by_id: HashMap<u64, RawFd>,
    next_id: u64,
    handlers: Arc<Handlers>,
    bridge: Arc<Bridge>,
    listener: RawFd,
    wake: RawFd,
}

impl Server {
    /// Submit whatever this connection has pending. Returns true once it has
    /// been closed and should be forgotten.
    fn pump(&mut self, fd: RawFd) -> std::io::Result<bool> {
        let Some(conn) = self.conns.get_mut(&fd) else {
            return Ok(false);
        };
        if conn.writing {
            return Ok(false);
        }
        if !conn.out.is_empty() {
            let buf = std::mem::take(&mut conn.out);
            conn.writing = true;
            self.d
                .submit_with(Op::Write { fd, buf }, tag(KIND_WRITE, fd))?;
            return Ok(false);
        }
        if conn.close_when_flushed {
            self.d.submit_with(Op::Close { fd }, tag(KIND_CLOSE, fd))?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Plaintext bytes off the wire, in whichever phase the connection is in.
    fn on_bytes(&mut self, fd: RawFd, bytes: &[u8]) {
        let Some(conn) = self.conns.get_mut(&fd) else {
            return;
        };
        if conn.ignoring {
            return;
        }
        if let Some(mut request) = conn.request.take() {
            request.extend_from_slice(bytes);
            match handshake::upgrade(&request) {
                Ok(handshake::Upgrade::NeedMore) => {
                    conn.request = Some(request);
                    return;
                }
                Ok(handshake::Upgrade::Accept { response, consumed }) => {
                    conn.out.extend_from_slice(&response);
                    conn.opened = true;
                    let id = conn.id;
                    if let Some(open) = &self.handlers.open {
                        open.call(id, ThreadsafeFunctionCallMode::NonBlocking);
                    }
                    // A client may pipeline frames behind the request.
                    let leftover = request[consumed..].to_vec();
                    self.on_frames(fd, &leftover);
                    return;
                }
                Err(_) => {
                    conn.out.extend_from_slice(BAD_REQUEST);
                    conn.close_when_flushed = true;
                    conn.ignoring = true;
                    return;
                }
            }
        }
        self.on_frames(fd, bytes);
    }

    fn on_frames(&mut self, fd: RawFd, bytes: &[u8]) {
        match self.conns.get_mut(&fd) {
            Some(conn) => conn.decoder.feed(bytes),
            None => return,
        }
        loop {
            let event = match self.conns.get_mut(&fd).map(|c| c.decoder.next_event()) {
                Some(Ok(Some(e))) => e,
                Some(Ok(None)) | None => return,
                Some(Err(e)) => {
                    // The codec already knows which close code each failure
                    // earns: 1002 protocol, 1007 bad UTF-8, 1009 too large.
                    if let Some(c) = self.conns.get_mut(&fd) {
                        c.send_close(e.close_code());
                    }
                    return;
                }
            };
            if self.on_event(fd, event) {
                return;
            }
        }
    }

    /// Handle one event. Returns true when the conversation is finished.
    fn on_event(&mut self, fd: RawFd, event: Event) -> bool {
        let Some(conn) = self.conns.get_mut(&fd) else {
            return true;
        };
        let id = conn.id;
        match event {
            // Data messages go to JS; the reply comes back as a command, which
            // is what makes `ws.send()` inside `message` work at all.
            Event::Text(text) => {
                if let Some(cb) = &self.handlers.message {
                    cb.call(
                        (id, text.into_bytes(), false),
                        ThreadsafeFunctionCallMode::NonBlocking,
                    );
                }
            }
            Event::Binary(data) => {
                if let Some(cb) = &self.handlers.message {
                    cb.call((id, data, true), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
            // Protocol-level and answered here: a pong is not application data
            // and JS has no way to get it wrong.
            Event::Ping(payload) => {
                let _ = encode::pong(&mut conn.out, &payload);
            }
            Event::Pong(_) => {}
            Event::Close(frame) => {
                let code = frame.map_or(1000, |f| f.code);
                conn.send_close(code);
                return true;
            }
        }
        false
    }

    /// Everything JS has queued since the last wake.
    fn drain_commands(&mut self, rx: &Receiver<Cmd>) {
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                Cmd::Send { id, data, binary } => {
                    // A send to a connection that has gone is dropped, not an
                    // error: JS cannot have known, and the id can never have
                    // been reassigned to someone else.
                    let Some(&fd) = self.by_id.get(&id) else {
                        continue;
                    };
                    if let Some(conn) = self.conns.get_mut(&fd) {
                        if conn.ignoring {
                            continue;
                        }
                        if binary {
                            encode::binary(&mut conn.out, &data);
                        } else {
                            match std::str::from_utf8(&data) {
                                Ok(s) => encode::text(&mut conn.out, s),
                                // A text frame must be UTF-8; sending it as
                                // binary would be a protocol lie, so refuse the
                                // frame rather than corrupt the stream.
                                Err(_) => continue,
                            }
                        }
                    }
                }
                Cmd::Close { id } => {
                    let Some(&fd) = self.by_id.get(&id) else {
                        continue;
                    };
                    if let Some(conn) = self.conns.get_mut(&fd) {
                        conn.send_close(1000);
                    }
                }
            }
        }
    }

    /// Tell JS a connection is gone and forget it. Only fires for connections
    /// that were reported open, so JS never sees a close it has no open for.
    fn retire(&mut self, fd: RawFd, code: u16) {
        if let Some(conn) = self.conns.remove(&fd) {
            self.by_id.remove(&conn.id);
            // Drop the budget with the connection, so a `send` on a dead id is
            // refused rather than charged against a counter nobody will drain.
            self.bridge.forget(conn.id);
            if conn.opened {
                if let Some(cb) = &self.handlers.close {
                    cb.call((conn.id, code), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
        }
    }
}

fn reactor_thread(
    listener: RawFd,
    wake: RawFd,
    rx: Receiver<Cmd>,
    handlers: Arc<Handlers>,
    bridge: Arc<Bridge>,
) -> std::io::Result<()> {
    let mut s = Server {
        d: PlatformDriver::new()?,
        conns: HashMap::new(),
        by_id: HashMap::new(),
        next_id: 1,
        handlers,
        bridge,
        listener,
        wake,
    };

    s.d.submit_with(Op::Accept { fd: listener }, tag(KIND_ACCEPT, listener))?;
    // The parked read on the wake pipe is what makes `wait()` interruptible.
    s.d.submit_with(Op::ReadPooled { fd: wake }, tag(KIND_WAKE, wake))?;

    let mut done = Vec::new();
    loop {
        s.d.wait(&mut done)?;
        if done.is_empty() {
            return Ok(());
        }
        for c in done.drain(..) {
            match tag_kind(c.user) {
                KIND_ACCEPT => match c.result {
                    Ok(fd) => {
                        let fd = fd as RawFd;
                        let l = s.listener;
                        s.d.submit_with(Op::Accept { fd: l }, tag(KIND_ACCEPT, l))?;
                        let id = s.next_id;
                        s.next_id += 1;
                        s.conns.insert(fd, Conn::new(id));
                        s.by_id.insert(id, fd);
                        s.bridge.track(id);
                        s.d.submit_with(Op::ReadPooled { fd }, tag(KIND_READ, fd))?;
                    }
                    // The pending connection died before we took it, which says
                    // nothing about the listener. macOS reports this as EINVAL.
                    Err(ref e)
                        if matches!(
                            e.raw_os_error(),
                            Some(
                                libc::ECONNABORTED | libc::ECONNRESET | libc::EINVAL | libc::EINTR
                            )
                        ) =>
                    {
                        let l = s.listener;
                        s.d.submit_with(Op::Accept { fd: l }, tag(KIND_ACCEPT, l))?;
                    }
                    Err(e) => return Err(e),
                },

                KIND_WAKE => {
                    // The byte itself carries nothing; its arrival is the whole
                    // message. Drain the queue and re-arm.
                    if let Some(buf) = c.buf {
                        s.d.recycle(buf);
                    }
                    s.drain_commands(&rx);
                    let fds: Vec<RawFd> = s.conns.keys().copied().collect();
                    for fd in fds {
                        if s.pump(fd)? {
                            s.retire(fd, 1000);
                        }
                    }
                    let w = s.wake;
                    s.d.submit_with(Op::ReadPooled { fd: w }, tag(KIND_WAKE, w))?;
                }

                KIND_READ => {
                    let fd = tag_fd(c.user);
                    if matches!(&c.result, Err(e) if e.raw_os_error() == Some(libc::ECANCELED)) {
                        continue;
                    }
                    match (c.result, c.buf) {
                        (Ok(n), Some(buf)) if n > 0 => {
                            s.on_bytes(fd, &buf);
                            s.d.recycle(buf);
                            let wants_more =
                                s.conns.get(&fd).is_some_and(|c| !c.close_when_flushed);
                            if s.pump(fd)? {
                                s.retire(fd, 1000);
                            } else if wants_more {
                                s.d.submit_with(Op::ReadPooled { fd }, tag(KIND_READ, fd))?;
                            }
                        }
                        // EOF or a dead connection: the peer went without a
                        // close frame, which is 1006 by convention.
                        (_, buf) => {
                            if let Some(buf) = buf {
                                s.d.recycle(buf);
                            }
                            s.retire(fd, 1006);
                            s.d.submit_with(Op::Close { fd }, tag(KIND_CLOSE, fd))?;
                        }
                    }
                }

                KIND_WRITE => {
                    let fd = tag_fd(c.user);
                    // These bytes have left the process: give their budget back
                    // before anything else, so a producer waiting on `send` is
                    // unblocked as early as it can be.
                    let written = c.buf.as_ref().map_or(0, |b| b.len());
                    if let Some(buf) = c.buf {
                        s.d.recycle(buf);
                    }
                    let failed = c.result.is_err();
                    let mut abandoned = 0;
                    if let Some(conn) = s.conns.get_mut(&fd) {
                        conn.writing = false;
                        if failed {
                            abandoned = conn.out.len();
                            conn.out.clear();
                            conn.close_when_flushed = true;
                            conn.ignoring = true;
                        }
                    }
                    if let Some(conn) = s.conns.get(&fd) {
                        let id = conn.id;
                        s.bridge.release(id, written + abandoned);
                    }
                    if s.pump(fd)? {
                        s.retire(fd, 1000);
                    }
                }

                // A Close completion: the descriptor is already gone, and the
                // connection was retired when the close was decided.
                _ => {}
            }
        }
    }
}

/// A WebSocket server, shaped like uWebSockets.js.
///
/// Named `WsApp` rather than `App` because napi registers it as a class, and a
/// class and the `App()` factory below cannot share one export name. Callers
/// only ever meet it as the return value of `App()`.
#[napi]
pub struct WsApp {
    handlers: Handlers,
    bridge: Arc<Bridge>,
    /// The reactor's end of the wake pipe, handed over when `listen` is called.
    reactor_wake: Option<UnixStream>,
    rx: Option<Receiver<Cmd>>,
}

#[napi]
impl WsApp {
    /// Register handlers for a route.
    ///
    /// `pattern` is accepted and ignored in v0.1 — every connection is served by
    /// the one handler set. It is in the signature so that code written against
    /// uWebSockets.js does not have to change shape when routing arrives.
    #[napi]
    pub fn ws(&mut self, _pattern: String, handlers: Object) -> Result<()> {
        if let Ok(f) = handlers.get_named_property::<Function>("open") {
            let b = Arc::clone(&self.bridge);
            self.handlers.open = Some(
                f.build_threadsafe_function::<u64>()
                    .callee_handled::<false>()
                    .build_callback(move |ctx: ThreadsafeCallContext<u64>| {
                        ws_handle(&ctx.env, &b, ctx.value)
                    })?,
            );
        }
        if let Ok(f) = handlers.get_named_property::<Function>("message") {
            let b = Arc::clone(&self.bridge);
            self.handlers.message = Some(
                f.build_threadsafe_function::<(u64, Vec<u8>, bool)>()
                    .callee_handled::<false>()
                    .build_callback(move |ctx: ThreadsafeCallContext<MessageIn>| {
                        let (id, data, binary) = ctx.value;
                        Ok((ws_handle(&ctx.env, &b, id)?, Buffer::from(data), binary).into())
                    })?,
            );
        }
        if let Ok(f) = handlers.get_named_property::<Function>("close") {
            let b = Arc::clone(&self.bridge);
            self.handlers.close = Some(
                f.build_threadsafe_function::<CloseIn>()
                    .callee_handled::<false>()
                    .build_callback(move |ctx: ThreadsafeCallContext<CloseIn>| {
                        let (id, code) = ctx.value;
                        Ok((ws_handle(&ctx.env, &b, id)?, code).into())
                    })?,
            );
        }
        Ok(())
    }

    /// Bind the port and start the reactor. The callback is invoked with
    /// whether the bind succeeded, matching uWebSockets.js.
    #[napi]
    pub fn listen(&mut self, port: u16, callback: Function<bool, ()>) -> Result<()> {
        if self.bridge.listening.swap(true, Ordering::SeqCst) {
            return Err(Error::from_reason("listen() called twice on one App"));
        }
        let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, port));
        let listener = match Listener::bind(addr) {
            Ok(l) => l,
            Err(_) => {
                // A bind failure is reported through the callback, not thrown:
                // that is what uWebSockets.js does and what callers check.
                callback.call(false)?;
                return Ok(());
            }
        };
        let listener_fd = listener.into_raw_fd();

        let wake = self
            .reactor_wake
            .take()
            .ok_or_else(|| Error::from_reason("listen() called twice on one App"))?;
        // The driver requires non-blocking descriptors and does not set them.
        wake.set_nonblocking(true)
            .map_err(|e| Error::from_reason(format!("wake pipe: {e}")))?;
        let wake_fd = wake.into_raw_fd();

        let rx = self
            .rx
            .take()
            .ok_or_else(|| Error::from_reason("listen() called twice on one App"))?;
        let handlers = Arc::new(std::mem::take(&mut self.handlers));
        let bridge = Arc::clone(&self.bridge);

        thread::Builder::new()
            .name("ramjet-reactor".into())
            .spawn(move || {
                if let Err(e) = reactor_thread(listener_fd, wake_fd, rx, handlers, bridge) {
                    eprintln!("ramjet reactor exited: {e}");
                }
            })
            .map_err(|e| Error::from_reason(format!("reactor thread: {e}")))?;

        callback.call(true)?;
        Ok(())
    }
}

/// Create a server. Named to match uWebSockets.js, where `App()` is a function
/// rather than a constructor.
#[napi(js_name = "App")]
pub fn app() -> Result<WsApp> {
    let (tx, rx) = channel();
    let (js_end, reactor_end) =
        UnixStream::pair().map_err(|e| Error::from_reason(format!("wake pipe: {e}")))?;
    Ok(WsApp {
        handlers: Handlers::default(),
        bridge: Arc::new(Bridge {
            inner: Mutex::new((tx, js_end)),
            listening: AtomicBool::new(false),
            pending: Mutex::new(HashMap::new()),
        }),
        reactor_wake: Some(reactor_end),
        rx: Some(rx),
    })
}
