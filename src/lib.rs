//! Node.js WebSocket server bindings for the ramjet runtime.
//!
//! The shape is uWebSockets.js: `App().ws(pattern, handlers).listen(port, cb)`.
//! What is underneath is a ramjet reactor on its own thread, because the driver
//! is `!Send` and `wait()` blocks — Node's event loop must never be inside it.
//!
//! So there are two threads and exactly two ways across the boundary:
//!
//!   reactor -> JS    one ordered, batched `ThreadsafeFunction` dispatcher.
//!   JS -> reactor    an `mpsc` queue plus a coalesced wake on a `UnixStream`
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

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write as _;
use std::ops::Range;
use std::os::fd::{IntoRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Status, ValueType};
use napi_derive::napi;

use ramjet::net::Listener;
use ramjet::reactor::{Driver, Op, PlatformDriver};
use ramjet_ws::{encode, handshake, Decoder, Event, MessageKind};

/// Autobahn's limit cases run to 16 MiB; sit above that so they echo rather
/// than get refused, but keep a ceiling so a peer cannot name any size it likes.
const MAX_MESSAGE: usize = 32 * 1024 * 1024;

/// An incomplete HTTP upgrade cannot grow forever. A normal WebSocket request
/// is a few hundred bytes; 16 KiB leaves ample room for cookies and proxies
/// while bounding pre-upgrade memory per connection.
const MAX_HANDSHAKE: usize = 16 * 1024;

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

/// For small messages a memcpy into a Node-owned Buffer is cheaper than V8's
/// external-BackingStore allocation and finalizer bookkeeping. Large messages
/// keep the existing zero-copy handoff. The 4 KiB crossover is benchmarked on
/// both sides rather than inferred from tiny-message results.
const SMALL_MESSAGE_COPY_MAX: usize = 4 * 1024;

/// A lone tiny frame is faster through the compact streaming path than by
/// holding its pooled receive buffer until `WriteFrom` completes. Above this
/// read size — including two or more 64-byte pipelined frames — in-place echo
/// wins by removing assembly or corking the whole batch into one write.
/// Measured on the C7i target rather than guessed from allocation counts.
const NATIVE_FUSED_MIN_READ: usize = 128;

/// Maximum decoded payload retained between the reactor and Node. Unlike a
/// queue-length cap this remains meaningful when message sizes vary by six
/// orders of magnitude. One oversized batch is allowed when the queue is empty
/// so the configured 32 MiB maximum message can always make progress.
const MAX_DISPATCH_BYTES: usize = 32 * 1024 * 1024;

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

/// Precomputed short reply headers keep the smallest native-echo case to two
/// loads. Server frames have FIN set and are never masked, so payload length is
/// the only varying byte below 126 bytes.
const TEXT_HEADERS: [[u8; 2]; 126] = short_headers(0x80 | 0x1);
const BINARY_HEADERS: [[u8; 2]; 126] = short_headers(0x80 | 0x2);

const fn short_headers(first: u8) -> [[u8; 2]; 126] {
    let mut table = [[0u8; 2]; 126];
    let mut len = 0;
    while len < 126 {
        table[len] = [first, len as u8];
        len += 1;
    }
    table
}

fn reply_header(kind: MessageKind, len: usize) -> ([u8; 10], usize) {
    let table = match kind {
        MessageKind::Text => &TEXT_HEADERS,
        MessageKind::Binary => &BINARY_HEADERS,
    };
    let first = table[0][0];
    let mut out = [0u8; 10];
    if len < 126 {
        out[..2].copy_from_slice(&table[len]);
        (out, 2)
    } else if len <= usize::from(u16::MAX) {
        out[0] = first;
        out[1] = 126;
        out[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        (out, 4)
    } else {
        out[0] = first;
        out[1] = 127;
        out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        (out, 10)
    }
}

/// Lay a server header into the four bytes the client's masking key no longer
/// needs, leaving the payload exactly where the kernel placed it.
fn reply_header_into(buf: &mut [u8], kind: MessageKind, payload: &Range<usize>) -> usize {
    let (header, len) = reply_header(kind, payload.len());
    debug_assert!(payload.start >= len);
    let start = payload.start - len;
    buf[start..payload.start].copy_from_slice(&header[..len]);
    start
}

const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

// Completion routing: op kind in the high bits, descriptor in the low 32.
const KIND_ACCEPT: u64 = 0;
const KIND_READ: u64 = 1;
const KIND_WRITE: u64 = 2;
const KIND_CLOSE: u64 = 3;
const KIND_WAKE: u64 = 4;

// JS-side connection state is shared by every App on one Node thread, so ids
// must be process-wide rather than restarting at one for each reactor.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

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
/// reactor after something is put on it. Queue insertion happens before the
/// coalesced wake, so the reactor never observes a wake without the associated
/// command already being visible.
struct Bridge {
    tx: Sender<Cmd>,
    wake: UnixStream,
    /// True while a byte is already on its way to the reactor. The command
    /// queue is drained wholesale, so writing another byte for every command
    /// only adds syscalls and lock traffic without carrying information.
    wake_pending: AtomicBool,
    listening: AtomicBool,
}

impl Bridge {
    /// Queue a command and wake the reactor. Returns whether it was queued.
    fn send(&self, cmd: Cmd) -> bool {
        if self.tx.send(cmd).is_err() {
            return false; // reactor is gone
        }
        if !self.wake_pending.swap(true, Ordering::AcqRel) {
            let mut wake = &self.wake;
            if wake.write_all(&[1]).is_err() {
                self.wake_pending.store(false, Ordering::Release);
                return false;
            }
        }
        true
    }

    /// Mark the current wake as consumed before draining. A producer that
    /// arrives after this store writes a fresh byte; one that arrived before it
    /// already put its command in the queue that is about to be drained.
    fn consume_wake(&self) {
        self.wake_pending.store(false, Ordering::Release);
    }
}

/// State shared only by one connection's reactor entry and cached JS handle.
/// Keeping the budget here removes a mutex and a hash lookup from every send
/// and every write completion.
struct ConnectionState {
    pending: AtomicUsize,
    live: AtomicBool,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            pending: AtomicUsize::new(0),
            live: AtomicBool::new(true),
        }
    }

    fn reserve(&self, bytes: usize) -> bool {
        if !self.live.load(Ordering::Acquire) {
            return false;
        }
        self.pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                cur.checked_add(bytes).filter(|next| *next <= MAX_OUTBOUND)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
    }

    fn close(&self) {
        self.live.store(false, Ordering::Release);
    }
}

/// The `ws` handle every handler receives as its first argument.
type WsArg = Object<'static>;

type MessageOut = FnArgs<(WsArg, Object<'static>, bool)>;
type CloseOut = FnArgs<(WsArg, u16)>;

fn message_buffer(env: &Env, data: Vec<u8>) -> Result<Object<'static>> {
    let raw = if data.len() <= SMALL_MESSAGE_COPY_MAX {
        let buffer = BufferSlice::copy_from(env, &data)?;
        unsafe { <&BufferSlice<'_> as ToNapiValue>::to_napi_value(env.raw(), &buffer)? }
    } else {
        unsafe { Buffer::to_napi_value(env.raw(), Buffer::from(data))? }
    };
    Ok(Object::from_raw(env.raw(), raw))
}

/// Every user-visible event crosses through the same queue. Apart from making
/// one native-to-JS hop serve many messages, this guarantees `open`, `message`,
/// and `close` stay ordered relative to one another; separate threadsafe
/// functions do not provide a cross-queue ordering contract.
enum JsEvent {
    Open {
        id: u64,
        state: Arc<ConnectionState>,
    },
    Message {
        id: u64,
        data: Vec<u8>,
        binary: bool,
    },
    Close {
        id: u64,
        code: u16,
    },
}

#[derive(Default)]
struct DispatchBudget {
    pending: AtomicUsize,
    waiting: AtomicBool,
    wait_lock: Mutex<()>,
    available: Condvar,
}

impl DispatchBudget {
    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.pending.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(bytes);
            if current != 0 && next > MAX_DISPATCH_BYTES {
                return false;
            }
            match self.pending.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn reserve(&self, bytes: usize) {
        if self.try_reserve(bytes) {
            return;
        }
        let mut guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.waiting.store(true, Ordering::Release);
        while !self.try_reserve(bytes) {
            guard = self
                .available
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        self.waiting.store(false, Ordering::Release);
    }

    fn release(&self, bytes: usize) {
        if self.waiting.load(Ordering::Acquire) {
            let _guard = self
                .wait_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.release_inner(bytes);
            self.available.notify_one();
            return;
        }
        self.release_inner(bytes);
    }

    fn release_inner(&self, bytes: usize) {
        let previous = self.pending.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes);
    }
}

struct DispatchBatch {
    events: Option<Vec<JsEvent>>,
    bytes: usize,
    budget: Arc<DispatchBudget>,
}

impl Drop for DispatchBatch {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

type Dispatch = ThreadsafeFunction<DispatchBatch, (), (), Status, false>;

#[derive(Default)]
struct Handlers {
    dispatch: Option<Dispatch>,
    wants_message: bool,
    native_echo: bool,
    budget: Arc<DispatchBudget>,
}

struct JsConnection {
    state: Arc<ConnectionState>,
    handle: Option<ObjectRef>,
}

// The live connections and their lazily-built `ws` handles. The ordered
// dispatcher inserts on Open before handling any Message and removes only
// after Close, so the state never needs to ride along with every message.
//
// Thread-local, and that is a safety property rather than a shortcut. Every
// threadsafe-function callback body runs on the JS thread by construction, so
// this map is only ever touched from there — which matters twice over: a napi
// `Ref` is thread-affine and must be *released* on the owning JS thread, and a
// thread-local fails safe if that assumption were ever wrong (a second thread
// would see an empty map and rebuild, rather than corrupt this one).
//
// Profiled motivation: building the handle per message cost ~26% of the JS
// thread — two `v8::Function::New` instantiations and two property sets for
// every echoed message, where uWebSockets.js hands out one persistent object
// per connection.
thread_local! {
    static CONNECTIONS: RefCell<HashMap<u64, JsConnection>> =
        RefCell::new(HashMap::new());
}

fn register_connection(id: u64, state: Arc<ConnectionState>) {
    CONNECTIONS.with(|connections| {
        connections.borrow_mut().insert(
            id,
            JsConnection {
                state,
                handle: None,
            },
        );
    });
}

/// The handle for `id`, built on first use and reused afterwards.
fn ws_for(env: &Env, bridge: &Arc<Bridge>, id: u64) -> Result<Object<'static>> {
    CONNECTIONS.with(|connections| {
        let state = {
            let cache = connections.borrow();
            let conn = cache
                .get(&id)
                .ok_or_else(|| Error::from_reason("connection is already closed"))?;
            if let Some(r) = &conn.handle {
                let v = r.get_value(env)?;
                return Ok(unsafe { std::mem::transmute::<Object<'_>, Object<'static>>(v) });
            }
            Arc::clone(&conn.state)
        };
        let obj = ws_handle(env, bridge, id, state)?;
        let r = obj.create_ref()?;
        if let Some(conn) = connections.borrow_mut().get_mut(&id) {
            conn.handle = Some(r);
        }
        Ok(obj)
    })
}

/// Drop a connection's handle. Called from the `close` callback, which runs on
/// the JS thread — the only thread allowed to release a napi `Ref`.
fn forget_ws(env: &Env, id: u64) {
    CONNECTIONS.with(|connections| {
        if let Some(mut conn) = connections.borrow_mut().remove(&id) {
            if let Some(r) = conn.handle.take() {
                // Explicit rather than dropped: `unref` is what tells V8 the object
                // may be collected, and it must happen on this thread.
                let _ = r.unref(env);
            }
        }
    });
}

/// Build the `ws` handle JS handlers receive: an object with `send` and
/// `close`, each closing over the connection id and the bridge.
fn ws_handle(
    env: &Env,
    bridge: &Arc<Bridge>,
    id: u64,
    state: Arc<ConnectionState>,
) -> Result<Object<'static>> {
    let mut obj = Object::new(env)?;

    let b = Arc::clone(bridge);
    let send_state = Arc::clone(&state);
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
            let charged = frame_len(data.len());
            if !send_state.reserve(charged) {
                return Ok(false);
            }
            if b.send(Cmd::Send { id, data, binary }) {
                Ok(true)
            } else {
                send_state.release(charged);
                Ok(false)
            }
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
    state: Arc<ConnectionState>,
    request: Option<Vec<u8>>,
    decoder: Decoder,
    /// Bytes waiting to go out. The driver allows one write per descriptor, so
    /// this is where a burst coalesces into a single `Op::Write`.
    out: Vec<u8>,
    /// Portion of `out` charged to backpressure. Small handshake/close replies
    /// share the byte buffer but are never reserved and must not release budget.
    out_charged: usize,
    writing: bool,
    /// Charged portion of the buffer currently owned by `Op::Write`.
    writing_charged: usize,
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
            state: Arc::new(ConnectionState::new()),
            request: Some(Vec::new()),
            decoder: Decoder::with_max_message(MAX_MESSAGE),
            out: Vec::new(),
            out_charged: 0,
            writing: false,
            writing_charged: 0,
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

    /// The receive buffer can become the next write only when no older bytes
    /// are waiting and the streaming decoder has not claimed the connection.
    /// The codec checks its own partial-frame state separately.
    fn can_echo_in_place(&self) -> bool {
        self.request.is_none() && !self.ignoring && !self.writing && self.out.is_empty()
    }
}

enum NativeEchoRead {
    /// No complete unfragmented data frame was available. The ordinary
    /// streaming decoder still owns these bytes.
    Unclaimed(Vec<u8>),
    /// The receive buffer was submitted directly as a write.
    Submitted,
    /// The input was terminally rejected; recycle its buffer, then flush the
    /// close frame already queued on the connection.
    Rejected(Vec<u8>),
}

struct Server {
    d: PlatformDriver,
    conns: HashMap<RawFd, Conn>,
    by_id: HashMap<u64, RawFd>,
    handlers: Arc<Handlers>,
    events: Vec<JsEvent>,
    bridge: Arc<Bridge>,
    listener: RawFd,
    wake: RawFd,
}

impl Server {
    fn dispatch_events(&mut self) {
        if self.events.is_empty() {
            return;
        }
        if let Some(dispatch) = &self.handlers.dispatch {
            let events = std::mem::take(&mut self.events);
            let bytes = events.iter().fold(
                events
                    .capacity()
                    .saturating_mul(std::mem::size_of::<JsEvent>()),
                |total, event| match event {
                    JsEvent::Message { data, .. } => total.saturating_add(data.capacity()),
                    _ => total,
                },
            );
            self.handlers.budget.reserve(bytes);
            dispatch.call(
                DispatchBatch {
                    events: Some(events),
                    bytes,
                    budget: Arc::clone(&self.handlers.budget),
                },
                ThreadsafeFunctionCallMode::NonBlocking,
            );
        } else {
            self.events.clear();
        }
    }

    /// Native echo's fast lane. A single whole frame is unmasked in place and
    /// written from the reply header inserted ahead of its unmoved payload. A
    /// pipelined read is fused and corked: unmask plus compaction in one pass,
    /// all replies in the same pooled buffer, one write. Fragmented, control,
    /// and partial frames fall back to the streaming decoder unchanged.
    fn try_native_echo_read(
        &mut self,
        fd: RawFd,
        mut buf: Vec<u8>,
    ) -> std::io::Result<NativeEchoRead> {
        let Some(conn) = self.conns.get_mut(&fd) else {
            return Ok(NativeEchoRead::Rejected(buf));
        };
        if !conn.can_echo_in_place() {
            return Ok(NativeEchoRead::Unclaimed(buf));
        }

        match conn.decoder.take_whole_frame(&mut buf) {
            Ok(Some(frame)) => {
                let charged = frame_len(frame.payload.len());
                if !conn.state.reserve(charged) {
                    conn.send_close(1008);
                    return Ok(NativeEchoRead::Rejected(buf));
                }
                let start = reply_header_into(&mut buf, frame.kind, &frame.payload);
                conn.writing = true;
                conn.writing_charged = charged;
                self.d
                    .submit_with(Op::WriteFrom { fd, buf, start }, tag(KIND_WRITE, fd))?;
                return Ok(NativeEchoRead::Submitted);
            }
            Ok(None) => {}
            Err(error) => {
                conn.send_close(error.close_code());
                return Ok(NativeEchoRead::Rejected(buf));
            }
        }

        // `take_whole_frame` promised not to touch a multi-frame buffer. Pack
        // every complete frame at its front now; each reply is four bytes
        // shorter than its masked input, so the write cursor can never catch
        // the read cursor.
        let original_len = buf.len();
        let (written, from, error_code) = {
            let mut written = 0;
            let mut from = 0;
            let mut error_code = None;
            loop {
                match conn.decoder.take_echo_frame_at(&mut buf, from, written) {
                    Ok(Some(frame)) => {
                        written = frame.frame.end;
                        from = frame.consumed;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        error_code = Some(error.close_code());
                        break;
                    }
                }
            }
            (written, from, error_code)
        };

        if written == 0 {
            if let Some(code) = error_code {
                conn.send_close(code);
                return Ok(NativeEchoRead::Rejected(buf));
            }
            return Ok(NativeEchoRead::Unclaimed(buf));
        }
        if !conn.state.reserve(written) {
            conn.send_close(1008);
            return Ok(NativeEchoRead::Rejected(buf));
        }

        // Valid replies before a later bad frame still go out before the close.
        if let Some(code) = error_code {
            conn.send_close(code);
        } else if from < original_len {
            // Copy only the rare unclaimed tail before the pooled buffer becomes
            // write-owned. The common all-data batch never allocates or copies.
            let tail = buf[from..].to_vec();
            self.on_frames(fd, &tail);
        }

        buf.truncate(written);
        let conn = self
            .conns
            .get_mut(&fd)
            .expect("native echo connection disappeared");
        conn.writing = true;
        conn.writing_charged = written;
        self.d
            .submit_with(Op::Write { fd, buf }, tag(KIND_WRITE, fd))?;
        Ok(NativeEchoRead::Submitted)
    }

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
            conn.writing_charged = std::mem::take(&mut conn.out_charged);
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
                    if request.len() > MAX_HANDSHAKE {
                        conn.out.extend_from_slice(BAD_REQUEST);
                        conn.close_when_flushed = true;
                        conn.ignoring = true;
                        return;
                    }
                    conn.request = Some(request);
                    return;
                }
                Ok(handshake::Upgrade::Accept { response, consumed }) => {
                    if consumed > MAX_HANDSHAKE {
                        conn.out.extend_from_slice(BAD_REQUEST);
                        conn.close_when_flushed = true;
                        conn.ignoring = true;
                        return;
                    }
                    conn.out.extend_from_slice(&response);
                    conn.opened = true;
                    let id = conn.id;
                    let state = Arc::clone(&conn.state);
                    self.events.push(JsEvent::Open { id, state });
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
                if self.handlers.native_echo {
                    let charged = frame_len(text.len());
                    if !conn.state.reserve(charged) {
                        conn.send_close(1008);
                        return true;
                    }
                    encode::text(&mut conn.out, &text);
                    conn.out_charged += charged;
                } else if self.handlers.wants_message {
                    self.events.push(JsEvent::Message {
                        id,
                        data: text.into_bytes(),
                        binary: false,
                    });
                }
            }
            Event::Binary(data) => {
                if self.handlers.native_echo {
                    let charged = frame_len(data.len());
                    if !conn.state.reserve(charged) {
                        conn.send_close(1008);
                        return true;
                    }
                    encode::binary(&mut conn.out, &data);
                    conn.out_charged += charged;
                } else if self.handlers.wants_message {
                    self.events.push(JsEvent::Message {
                        id,
                        data,
                        binary: true,
                    });
                }
            }
            // Protocol-level and answered here: a pong is not application data
            // and JS has no way to get it wrong.
            Event::Ping(payload) => {
                let charged = frame_len(payload.len());
                if conn.state.reserve(charged) {
                    let _ = encode::pong(&mut conn.out, &payload);
                    conn.out_charged += charged;
                } else {
                    // A full-duplex peer can keep sending pings while refusing
                    // to read pongs. Stop reading once those protocol replies
                    // reach the same cap as application output.
                    conn.send_close(1008);
                    return true;
                }
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
                        let charged = frame_len(data.len());
                        if conn.ignoring {
                            conn.state.release(charged);
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
                                Err(_) => {
                                    conn.state.release(charged);
                                    continue;
                                }
                            }
                        }
                        conn.out_charged += charged;
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
            conn.state.close();
            if conn.opened {
                self.events.push(JsEvent::Close { id: conn.id, code });
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
        handlers,
        events: Vec::new(),
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
                        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                        s.conns.insert(fd, Conn::new(id));
                        s.by_id.insert(id, fd);
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
                    s.bridge.consume_wake();
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
                            let read =
                                if s.handlers.native_echo && buf.len() > NATIVE_FUSED_MIN_READ {
                                    s.try_native_echo_read(fd, buf)?
                                } else {
                                    NativeEchoRead::Unclaimed(buf)
                                };
                            match read {
                                NativeEchoRead::Unclaimed(buf) => {
                                    s.on_bytes(fd, &buf);
                                    s.d.recycle(buf);
                                }
                                NativeEchoRead::Submitted => {}
                                NativeEchoRead::Rejected(buf) => s.d.recycle(buf),
                            }
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
                    if let Some(buf) = c.buf {
                        s.d.recycle(buf);
                    }
                    let failed = c.result.is_err();
                    let mut abandoned = 0;
                    let mut completed = 0;
                    if let Some(conn) = s.conns.get_mut(&fd) {
                        conn.writing = false;
                        completed = std::mem::take(&mut conn.writing_charged);
                        if failed {
                            abandoned = std::mem::take(&mut conn.out_charged);
                            conn.out.clear();
                            conn.close_when_flushed = true;
                            conn.ignoring = true;
                        }
                    }
                    if let Some(conn) = s.conns.get(&fd) {
                        // Give back only reserved bytes. Handshake and close
                        // replies may share the write but were never charged.
                        conn.state.release(completed + abandoned);
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
        s.dispatch_events();
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
    pub fn ws(&mut self, env: &Env, _pattern: String, handlers: Object) -> Result<()> {
        let native_echo = if handlers.has_named_property("nativeEcho")? {
            handlers.get_named_property::<bool>("nativeEcho")?
        } else {
            false
        };
        let open = handlers
            .get_named_property::<Function<WsArg, Unknown>>("open")
            .ok()
            .map(|f| f.create_ref())
            .transpose()?;
        let message = handlers
            .get_named_property::<Function<MessageOut, Unknown>>("message")
            .ok()
            .map(|f| f.create_ref())
            .transpose()?;
        let close = handlers
            .get_named_property::<Function<CloseOut, Unknown>>("close")
            .ok()
            .map(|f| f.create_ref())
            .transpose()?;

        if native_echo && message.is_some() {
            return Err(Error::new(
                Status::InvalidArg,
                "nativeEcho and message are mutually exclusive",
            ));
        }

        self.handlers.wants_message = message.is_some();
        self.handlers.native_echo = native_echo;
        let bridge = Arc::clone(&self.bridge);
        // N-API requires a JS function for a threadsafe function even though
        // the transform below performs the real dispatch. It is called once
        // per native batch, not once per user message.
        let tick: Function<Unknown, ()> =
            env.create_function_from_closure("_ramjetDispatch", |_| Ok(()))?;
        self.handlers.dispatch = Some(
            tick.build_threadsafe_function::<DispatchBatch>()
                .callee_handled::<false>()
                .build_callback(move |mut ctx: ThreadsafeCallContext<DispatchBatch>| {
                    let open = open.as_ref().map(|f| f.borrow_back(&ctx.env)).transpose()?;
                    let message = message
                        .as_ref()
                        .map(|f| f.borrow_back(&ctx.env))
                        .transpose()?;
                    let close = close
                        .as_ref()
                        .map(|f| f.borrow_back(&ctx.env))
                        .transpose()?;

                    for event in ctx.value.events.take().expect("dispatch events") {
                        match event {
                            JsEvent::Open { id, state } => {
                                register_connection(id, state);
                                if let Some(f) = &open {
                                    let _ = f.call(ws_for(&ctx.env, &bridge, id)?)?;
                                }
                            }
                            JsEvent::Message { id, data, binary } => {
                                if let Some(f) = &message {
                                    let ws = ws_for(&ctx.env, &bridge, id)?;
                                    let buffer = message_buffer(&ctx.env, data)?;
                                    let _ = f.call((ws, buffer, binary).into())?;
                                }
                            }
                            JsEvent::Close { id, code } => {
                                if let Some(f) = &close {
                                    let ws = ws_for(&ctx.env, &bridge, id)?;
                                    // Release our reference before entering user
                                    // code, so a throwing close handler cannot
                                    // strand the cache entry.
                                    forget_ws(&ctx.env, id);
                                    let _ = f.call((ws, code).into())?;
                                } else {
                                    forget_ws(&ctx.env, id);
                                }
                            }
                        }
                    }
                    Ok(())
                })?,
        );
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

/// How many per-connection handles are currently cached.
///
/// Diagnostic, and the assertion the leak test is built on: after every
/// connection has closed this must be zero, or each one has left a JS object
/// behind. Reads thread-local state, so it is only meaningful from the JS
/// thread — which is where a test runs it.
#[napi]
pub fn handle_count() -> u32 {
    CONNECTIONS.with(|connections| {
        connections
            .borrow()
            .values()
            .filter(|conn| conn.handle.is_some())
            .count() as u32
    })
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
            tx,
            wake: js_end,
            wake_pending: AtomicBool::new(false),
            listening: AtomicBool::new(false),
        }),
        reactor_wake: Some(reactor_end),
        rx: Some(rx),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn native_reply_headers_cover_every_wire_length_width() {
        let (header, len) = reply_header(MessageKind::Text, 125);
        assert_eq!((&header[..len], len), (&[0x81, 125][..], 2));

        let (header, len) = reply_header(MessageKind::Binary, 126);
        assert_eq!((&header[..len], len), (&[0x82, 126, 0, 126][..], 4));

        let (header, len) = reply_header(MessageKind::Text, 65_535);
        assert_eq!((&header[..len], len), (&[0x81, 126, 0xff, 0xff][..], 4));

        let (header, len) = reply_header(MessageKind::Binary, 65_536);
        assert_eq!(
            (&header[..len], len),
            (&[0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0][..], 10),
        );
    }

    #[test]
    fn dispatch_budget_applies_pressure_and_wakes_without_a_lost_signal() {
        let budget = Arc::new(DispatchBudget::default());
        budget.reserve(MAX_DISPATCH_BYTES);

        let waiter = Arc::clone(&budget);
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            waiter.reserve(1);
            acquired_tx.send(()).expect("test receiver");
            waiter.release(1);
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while !budget.waiting.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(budget.waiting.load(Ordering::Acquire));
        assert!(acquired_rx.try_recv().is_err());

        budget.release(MAX_DISPATCH_BYTES);
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter must wake when JS releases a batch");
        thread.join().expect("budget waiter");
        assert_eq!(budget.pending.load(Ordering::Acquire), 0);
    }

    #[test]
    fn one_oversized_dispatch_batch_can_make_progress() {
        let budget = DispatchBudget::default();
        let oversized = MAX_DISPATCH_BYTES + 1;
        budget.reserve(oversized);
        assert_eq!(budget.pending.load(Ordering::Acquire), oversized);
        budget.release(oversized);
    }
}
