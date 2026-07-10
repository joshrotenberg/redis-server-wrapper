//! Byte-level TCP fault-injection proxy for testing Redis client resilience.
//!
//! [`FaultProxy`] sits between a test client and an upstream Redis node,
//! forwarding bytes over TCP while allowing tests to inject faults: per-
//! direction delay, mid-frame connection drops (clean or reset), live-
//! connection stalls, chunked writes with inter-chunk delay, deterministic
//! connection rejection, a proxy-down mode, and a black-hole mode. Unlike
//! [`crate::chaos`], which operates on the server process, this module
//! operates purely on the wire and needs no Docker or root privileges.
//!
//! Cumulative counters are available via [`FaultProxy::stats`] so a test can
//! assert that a configured fault actually fired, rather than trusting that
//! it did.
//!
//! # Example
//!
//! ```no_run
//! use redis_server_wrapper::{Direction, FaultProxy, RedisServer};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let server = RedisServer::new().port(6400).start().await.unwrap();
//! let proxy = FaultProxy::spawn(server.addr()).await.unwrap();
//!
//! // Route a client through `proxy.addr()` instead of `server.addr()`.
//!
//! // Drop the connection after 8 bytes of the server's response.
//! proxy.close_after(Direction::UpstreamToClient, 8);
//!
//! // ... assert the client sees a clean mid-frame connection error ...
//!
//! // Back to clean passthrough for the next connection.
//! proxy.reset();
//! # }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::error::Result;

/// A direction of byte flow through a [`FaultProxy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Bytes flowing from the test client to the upstream Redis node.
    ClientToUpstream,
    /// Bytes flowing from the upstream Redis node to the test client.
    UpstreamToClient,
}

impl Direction {
    fn idx(self) -> usize {
        match self {
            Direction::ClientToUpstream => 0,
            Direction::UpstreamToClient => 1,
        }
    }
}

/// How a connection is closed by [`FaultProxy::close_after_with`],
/// [`FaultProxy::reject_next`], [`FaultProxy::reset_peer`], and
/// [`FaultProxy::disable`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CloseKind {
    /// A clean TCP close (FIN). The peer sees a normal EOF.
    #[default]
    Fin,
    /// An abrupt TCP reset (`SO_LINGER` set to zero before close). The peer
    /// sees `ECONNRESET` rather than EOF, matching a crashed server rather
    /// than a cleanly closed one.
    Rst,
}

/// A delay applied before forwarding data in one direction.
#[derive(Clone, Copy, Debug)]
pub enum Delay {
    /// Always sleep for exactly this long before forwarding.
    Fixed(Duration),
    /// Sleep for a pseudo-random duration in `[min, max)` before forwarding.
    ///
    /// This uses a small local PRNG for jitter, not a cryptographic source --
    /// it's meant to simulate jittery network latency in tests, not to be
    /// unpredictable.
    Random {
        /// Minimum delay, inclusive.
        min: Duration,
        /// Maximum delay, exclusive.
        max: Duration,
    },
}

impl Delay {
    fn resolve(self) -> Duration {
        match self {
            Delay::Fixed(d) => d,
            Delay::Random { min, max } => {
                if max <= min {
                    return min;
                }
                let span = (max - min).as_nanos().max(1) as u64;
                min + Duration::from_nanos(next_pseudo_random() % span)
            }
        }
    }
}

/// A small local PRNG (splitmix64-style) used only for jitter -- avoids
/// pulling in the `rand` crate for a test-only feature.
fn next_pseudo_random() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

/// Cumulative counters for a [`FaultProxy`] since it was spawned.
///
/// A `Copy` snapshot taken with `Relaxed` atomic loads; cheap enough to call
/// from a synchronous assertion or the planned CLI status output. Every
/// counter only ever increases -- call [`FaultProxy::stats`] again and
/// compare against a previous snapshot to measure activity in a window.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProxyStats {
    /// Total connections accepted by the listener, including ones that were
    /// black-holed or rejected.
    pub connections_accepted: u64,
    /// Connections accepted while black-hole mode ([`FaultProxy::set_drop_all`])
    /// was active.
    pub connections_black_holed: u64,
    /// Connections accepted then immediately closed by a pending
    /// [`FaultProxy::reject_next`] countdown.
    pub connections_rejected: u64,
    /// Total bytes forwarded from the test client to the upstream node.
    pub bytes_client_to_upstream: u64,
    /// Total bytes forwarded from the upstream node to the test client.
    pub bytes_upstream_to_client: u64,
    /// Number of read-chunks delayed by [`FaultProxy::set_delay`] while
    /// forwarding client-to-upstream traffic.
    pub delays_client_to_upstream: u64,
    /// Number of read-chunks delayed by [`FaultProxy::set_delay`] while
    /// forwarding upstream-to-client traffic.
    pub delays_upstream_to_client: u64,
    /// Number of times a [`FaultProxy::close_after`] /
    /// [`FaultProxy::close_after_with`] threshold tripped in the
    /// client-to-upstream direction.
    pub closes_client_to_upstream: u64,
    /// Number of times a [`FaultProxy::close_after`] /
    /// [`FaultProxy::close_after_with`] threshold tripped in the
    /// upstream-to-client direction.
    pub closes_upstream_to_client: u64,
    /// Number of reads that were split into more than one write by
    /// [`FaultProxy::set_chunk_size`] in the client-to-upstream direction.
    pub chunked_writes_client_to_upstream: u64,
    /// Number of reads that were split into more than one write by
    /// [`FaultProxy::set_chunk_size`] in the upstream-to-client direction.
    pub chunked_writes_upstream_to_client: u64,
}

/// Lock-free cumulative counters backing [`ProxyStats`], held outside the
/// `RwLock<FaultState>` so that [`FaultProxy::reset`] never silently zeroes
/// them.
#[derive(Debug)]
struct StatsInner {
    connections_accepted: AtomicU64,
    connections_black_holed: AtomicU64,
    connections_rejected: AtomicU64,
    bytes: [AtomicU64; 2],
    delays: [AtomicU64; 2],
    closes: [AtomicU64; 2],
    chunked_writes: [AtomicU64; 2],
}

impl Default for StatsInner {
    fn default() -> Self {
        StatsInner {
            connections_accepted: AtomicU64::new(0),
            connections_black_holed: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            bytes: [AtomicU64::new(0), AtomicU64::new(0)],
            delays: [AtomicU64::new(0), AtomicU64::new(0)],
            closes: [AtomicU64::new(0), AtomicU64::new(0)],
            chunked_writes: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}

impl StatsInner {
    fn snapshot(&self) -> ProxyStats {
        let c2u = Direction::ClientToUpstream.idx();
        let u2c = Direction::UpstreamToClient.idx();
        ProxyStats {
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_black_holed: self.connections_black_holed.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            bytes_client_to_upstream: self.bytes[c2u].load(Ordering::Relaxed),
            bytes_upstream_to_client: self.bytes[u2c].load(Ordering::Relaxed),
            delays_client_to_upstream: self.delays[c2u].load(Ordering::Relaxed),
            delays_upstream_to_client: self.delays[u2c].load(Ordering::Relaxed),
            closes_client_to_upstream: self.closes[c2u].load(Ordering::Relaxed),
            closes_upstream_to_client: self.closes[u2c].load(Ordering::Relaxed),
            chunked_writes_client_to_upstream: self.chunked_writes[c2u].load(Ordering::Relaxed),
            chunked_writes_upstream_to_client: self.chunked_writes[u2c].load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct FaultState {
    delay: [Option<Delay>; 2],
    close_after: [Option<(u64, CloseKind)>; 2],
    chunk_size: Option<usize>,
    chunk_delay: Option<Duration>,
    drop_all: bool,
    stall: [bool; 2],
    reject_remaining: u32,
    reject_kind: CloseKind,
}

/// Generation-counted broadcast used to sever every live connection at once,
/// from [`FaultProxy::reset_peer`], [`FaultProxy::disable`], and the deadline
/// scheduled by [`FaultProxy::stall_then_close`].
#[derive(Clone, Copy, Debug)]
struct ShutdownSignal {
    generation: u64,
    kind: CloseKind,
}

/// A TCP proxy that forwards bytes between test clients and an upstream
/// Redis node while injecting configurable network faults.
///
/// Construct with [`FaultProxy::spawn`]. Fault controls can be changed at
/// any time via the returned handle; delay, close-after, chunk-size, and
/// stall take effect on data already in flight (checked on every read),
/// while [`FaultProxy::set_drop_all`] and [`FaultProxy::reject_next`] are
/// snapshotted once per accepted connection. The proxy stops accepting new
/// connections when the handle is dropped; already-established connections
/// run to completion.
pub struct FaultProxy {
    addr: SocketAddr,
    upstream_addr: SocketAddr,
    state: Arc<RwLock<FaultState>>,
    stats: Arc<StatsInner>,
    notify: Arc<Notify>,
    shutdown_tx: watch::Sender<ShutdownSignal>,
    accept_task: Mutex<Option<JoinHandle<()>>>,
    stall_timer: Mutex<Option<JoinHandle<()>>>,
}

impl FaultProxy {
    /// Bind an ephemeral local TCP listener that proxies to `upstream_addr`
    /// and return a handle to it. The proxy starts in clean-passthrough mode.
    pub async fn spawn(upstream_addr: impl ToSocketAddrs) -> Result<FaultProxy> {
        let upstream_addr = tokio::net::lookup_host(upstream_addr)
            .await?
            .next()
            .ok_or_else(|| {
                crate::error::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "could not resolve upstream address",
                ))
            })?;

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(RwLock::new(FaultState::default()));
        let stats = Arc::new(StatsInner::default());
        let notify = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownSignal {
            generation: 0,
            kind: CloseKind::Fin,
        });

        let accept_task = spawn_accept_task(
            listener,
            upstream_addr,
            state.clone(),
            stats.clone(),
            notify.clone(),
            shutdown_rx,
        );

        Ok(FaultProxy {
            addr,
            upstream_addr,
            state,
            stats,
            notify,
            shutdown_tx,
            accept_task: Mutex::new(Some(accept_task)),
            stall_timer: Mutex::new(None),
        })
    }

    /// The local address clients should connect to. Stable across
    /// [`FaultProxy::disable`] / [`FaultProxy::enable`] cycles.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Cumulative counters since [`FaultProxy::spawn`]. Lock-free snapshot;
    /// safe to call from a synchronous assertion.
    pub fn stats(&self) -> ProxyStats {
        self.stats.snapshot()
    }

    /// Set (or replace) the delay applied before forwarding data in `direction`.
    pub fn set_delay(&self, direction: Direction, delay: Delay) {
        self.state.write().unwrap().delay[direction.idx()] = Some(delay);
    }

    /// Remove any delay configured for `direction`.
    pub fn clear_delay(&self, direction: Direction) {
        self.state.write().unwrap().delay[direction.idx()] = None;
    }

    /// Close the connection once `bytes` total have been forwarded in
    /// `direction`, cutting mid-buffer if the triggering read straddles the
    /// threshold. Applies independently to every connection accepted while
    /// this setting is active. Closes with a clean FIN; use
    /// [`FaultProxy::close_after_with`] for a TCP reset instead.
    pub fn close_after(&self, direction: Direction, bytes: u64) {
        self.state.write().unwrap().close_after[direction.idx()] = Some((bytes, CloseKind::Fin));
    }

    /// Like [`FaultProxy::close_after`], but the connection is closed with
    /// `kind` once the threshold trips.
    pub fn close_after_with(&self, direction: Direction, bytes: u64, kind: CloseKind) {
        self.state.write().unwrap().close_after[direction.idx()] = Some((bytes, kind));
    }

    /// Remove the close-after threshold configured for `direction`.
    pub fn clear_close_after(&self, direction: Direction) {
        self.state.write().unwrap().close_after[direction.idx()] = None;
    }

    /// Split forwarded writes into pieces of at most `size` bytes, in both
    /// directions. Use `size == 1` to exercise incremental decoders. Combine
    /// with [`FaultProxy::set_chunk_delay`] so the pieces genuinely arrive as
    /// separate reads instead of being coalesced by the kernel on loopback.
    pub fn set_chunk_size(&self, size: usize) {
        self.state.write().unwrap().chunk_size = Some(size.max(1));
    }

    /// Stop chunking writes; forward reads in whatever size they arrive.
    pub fn clear_chunk_size(&self) {
        self.state.write().unwrap().chunk_size = None;
    }

    /// Sleep `delay` between chunked write pieces so the client observes each
    /// piece as a separate read. No effect unless [`FaultProxy::set_chunk_size`]
    /// splits a read into more than one piece.
    pub fn set_chunk_delay(&self, delay: Duration) {
        self.state.write().unwrap().chunk_delay = Some(delay);
    }

    /// Remove the inter-chunk delay configured by [`FaultProxy::set_chunk_delay`].
    pub fn clear_chunk_delay(&self) {
        self.state.write().unwrap().chunk_delay = None;
    }

    /// Enable or disable black-hole mode. While enabled, newly accepted
    /// connections are held open (no upstream connection is made, nothing is
    /// ever forwarded or written back) until the client disconnects or the
    /// proxy is dropped. Existing connections are unaffected; use
    /// [`FaultProxy::set_stall`] to affect connections already established.
    pub fn set_drop_all(&self, drop_all: bool) {
        self.state.write().unwrap().drop_all = drop_all;
    }

    /// Stop forwarding bytes in `direction` on every connection, existing and
    /// new. Client writes still succeed; reads on the other end never
    /// complete until [`FaultProxy::clear_stall`] is called or the connection
    /// is severed by a pending [`FaultProxy::stall_then_close`] deadline.
    pub fn set_stall(&self, direction: Direction) {
        self.state.write().unwrap().stall[direction.idx()] = true;
    }

    /// Resume forwarding in `direction`; bytes read while stalled are flushed
    /// (matches toxiproxy's `timeout` toxic, which delays data rather than
    /// discarding it).
    pub fn clear_stall(&self, direction: Direction) {
        self.state.write().unwrap().stall[direction.idx()] = false;
        self.notify.notify_waiters();
    }

    /// Stall both directions now, then sever every connection (existing and
    /// new) `after` later. Use [`FaultProxy::set_stall`] alone for an
    /// indefinite hold. Replaces any previously pending deadline.
    pub fn stall_then_close(&self, after: Duration) {
        {
            let mut state = self.state.write().unwrap();
            state.stall[Direction::ClientToUpstream.idx()] = true;
            state.stall[Direction::UpstreamToClient.idx()] = true;
        }
        self.cancel_pending_stall_close();

        let shutdown_tx = self.shutdown_tx.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(after).await;
            shutdown_tx.send_modify(|signal| {
                signal.generation = signal.generation.wrapping_add(1);
                signal.kind = CloseKind::Fin;
            });
        });
        *self.stall_timer.lock().unwrap() = Some(handle);
    }

    /// Cancel any pending [`FaultProxy::stall_then_close`] deadline and clear
    /// both stalls.
    pub fn clear_stall_all(&self) {
        self.cancel_pending_stall_close();
        {
            let mut state = self.state.write().unwrap();
            state.stall[0] = false;
            state.stall[1] = false;
        }
        self.notify.notify_waiters();
    }

    fn cancel_pending_stall_close(&self) {
        if let Some(handle) = self.stall_timer.lock().unwrap().take() {
            handle.abort();
        }
    }

    /// Sever every live connection right now with a TCP RST, simulating a
    /// crashed server. New connections are unaffected; combine with
    /// [`FaultProxy::disable`] to also stop accepting.
    pub fn reset_peer(&self) {
        self.sever_all(CloseKind::Rst);
    }

    fn sever_all(&self, kind: CloseKind) {
        self.shutdown_tx.send_modify(|signal| {
            signal.generation = signal.generation.wrapping_add(1);
            signal.kind = kind;
        });
    }

    /// Fail the next `n` accepted connections by closing them immediately
    /// (with `kind`) before any upstream connect; passthrough resumes after.
    /// Deterministic replacement for toxiproxy's probabilistic `toxicity`.
    pub fn reject_next(&self, n: u32, kind: CloseKind) {
        let mut state = self.state.write().unwrap();
        state.reject_remaining = n;
        state.reject_kind = kind;
    }

    /// Cancel any pending [`FaultProxy::reject_next`] countdown.
    pub fn clear_reject(&self) {
        self.state.write().unwrap().reject_remaining = 0;
    }

    /// Stop listening and sever all live connections; new connect attempts
    /// fail with `ECONNREFUSED`. The port is released and re-claimed by
    /// [`FaultProxy::enable`].
    ///
    /// Note: another process can claim the port while disabled (rare for
    /// localhost tests, but a real race).
    pub async fn disable(&self) -> Result<()> {
        let handle = self.accept_task.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.abort();
            // Wait for the aborted task (and the listener it owns) to
            // actually finish tearing down before returning, so a
            // subsequent `enable()` can reliably rebind the same port.
            let _ = handle.await;
        }
        self.sever_all(CloseKind::Fin);
        Ok(())
    }

    /// Rebind the same local port and resume passthrough with the current
    /// fault state. Fails if the port was taken while disabled. A no-op if
    /// the proxy is already enabled.
    pub async fn enable(&self) -> Result<()> {
        if self.accept_task.lock().unwrap().is_some() {
            return Ok(());
        }

        let listener = TcpListener::bind(self.addr).await?;
        let handle = spawn_accept_task(
            listener,
            self.upstream_addr,
            self.state.clone(),
            self.stats.clone(),
            self.notify.clone(),
            self.shutdown_tx.subscribe(),
        );
        *self.accept_task.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Clear every fault control, returning the proxy to clean passthrough
    /// for new connections. Also cancels any pending
    /// [`FaultProxy::stall_then_close`] deadline and wakes any connections
    /// currently blocked on a stall.
    pub fn reset(&self) {
        self.cancel_pending_stall_close();
        *self.state.write().unwrap() = FaultState::default();
        self.notify.notify_waiters();
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        if let Some(handle) = self.accept_task.lock().unwrap().take() {
            handle.abort();
        }
        if let Some(handle) = self.stall_timer.lock().unwrap().take() {
            handle.abort();
        }
    }
}

/// Run the accept loop for a bound `listener`: accept connections, apply the
/// [`FaultProxy::reject_next`] countdown, and spawn [`handle_connection`] for
/// everything else. Shared between [`FaultProxy::spawn`] and
/// [`FaultProxy::enable`].
fn spawn_accept_task(
    listener: TcpListener,
    upstream_addr: SocketAddr,
    state: Arc<RwLock<FaultState>>,
    stats: Arc<StatsInner>,
    notify: Arc<Notify>,
    shutdown_rx: watch::Receiver<ShutdownSignal>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (client, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            stats.connections_accepted.fetch_add(1, Ordering::Relaxed);

            let reject_kind = {
                let mut s = state.write().unwrap();
                if s.reject_remaining > 0 {
                    s.reject_remaining -= 1;
                    Some(s.reject_kind)
                } else {
                    None
                }
            };

            if let Some(kind) = reject_kind {
                stats.connections_rejected.fetch_add(1, Ordering::Relaxed);
                if kind == CloseKind::Rst {
                    let _ = client.set_zero_linger();
                }
                drop(client);
                continue;
            }

            tokio::spawn(handle_connection(
                client,
                upstream_addr,
                state.clone(),
                stats.clone(),
                notify.clone(),
                shutdown_rx.clone(),
            ));
        }
    })
}

async fn handle_connection(
    mut client: TcpStream,
    upstream_addr: SocketAddr,
    state: Arc<RwLock<FaultState>>,
    stats: Arc<StatsInner>,
    notify: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<ShutdownSignal>,
) {
    if state.read().unwrap().drop_all {
        stats
            .connections_black_holed
            .fetch_add(1, Ordering::Relaxed);
        let mut buf = [0u8; 4096];
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && shutdown_rx.borrow().kind == CloseKind::Rst {
                        let _ = client.set_zero_linger();
                    }
                    return;
                }
                result = client.read(&mut buf) => {
                    match result {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                }
            }
        }
    }

    let mut upstream = match TcpStream::connect(upstream_addr).await {
        Ok(s) => s,
        Err(_) => return,
    };

    let outcome = {
        let (client_r, client_w) = client.split();
        let (upstream_r, upstream_w) = upstream.split();
        tokio::select! {
            outcome = forward(
                client_r,
                upstream_w,
                state.clone(),
                stats.clone(),
                notify.clone(),
                shutdown_rx.clone(),
                Direction::ClientToUpstream,
            ) => outcome,
            outcome = forward(
                upstream_r,
                client_w,
                state,
                stats,
                notify,
                shutdown_rx,
                Direction::UpstreamToClient,
            ) => outcome,
        }
    };

    if outcome == Some(CloseKind::Rst) {
        let _ = client.set_zero_linger();
        let _ = upstream.set_zero_linger();
    }
    // `client` and `upstream` drop here, actually closing the sockets with
    // whatever linger setting was just applied.
}

/// Copy bytes from `reader` to `writer`, applying whatever delay,
/// close-after, stall, and chunk-size/chunk-delay controls are configured
/// for `direction` at the time each chunk is read. Returns `None` on a
/// natural EOF/error (the caller should let the sockets close with a plain
/// FIN), or `Some(kind)` when the proxy itself decided to end the
/// connection (a close-after threshold, or a shutdown broadcast from
/// [`FaultProxy::reset_peer`], [`FaultProxy::disable`], or a
/// [`FaultProxy::stall_then_close`] deadline).
async fn forward<R, W>(
    mut reader: R,
    mut writer: W,
    state: Arc<RwLock<FaultState>>,
    stats: Arc<StatsInner>,
    notify: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<ShutdownSignal>,
    direction: Direction,
) -> Option<CloseKind>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let idx = direction.idx();
    let mut total: u64 = 0;
    let mut buf = [0u8; 4096];

    loop {
        let n = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                return if changed.is_ok() { Some(shutdown_rx.borrow().kind) } else { None };
            }
            result = reader.read(&mut buf) => {
                match result {
                    Ok(0) | Err(_) => return None,
                    Ok(n) => n,
                }
            }
        };
        let mut data = &buf[..n];

        // Hold already-read data until the direction is unstalled or the
        // connection is severed. Existing and new connections both observe
        // this, since it's checked fresh on every read.
        if state.read().unwrap().stall[idx] {
            loop {
                let notified = notify.notified();
                if !state.read().unwrap().stall[idx] {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    changed = shutdown_rx.changed() => {
                        return if changed.is_ok() { Some(shutdown_rx.borrow().kind) } else { None };
                    }
                }
            }
        }

        let (delay, close_after, chunk_size, chunk_delay) = {
            let s = state.read().unwrap();
            (
                s.delay[idx],
                s.close_after[idx],
                s.chunk_size,
                s.chunk_delay,
            )
        };

        if let Some((limit, kind)) = close_after {
            if total >= limit {
                stats.closes[idx].fetch_add(1, Ordering::Relaxed);
                return Some(kind);
            }
            let remaining = (limit - total) as usize;
            if data.len() > remaining {
                data = &data[..remaining];
            }
        }

        if let Some(delay) = delay {
            stats.delays[idx].fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(delay.resolve()).await;
        }

        let chunk = chunk_size.unwrap_or(data.len()).max(1);
        let mut pieces = data.chunks(chunk).peekable();
        let mut chunked = false;
        while let Some(piece) = pieces.next() {
            if writer.write_all(piece).await.is_err() {
                return None;
            }
            if pieces.peek().is_some() {
                chunked = true;
                if let Some(delay) = chunk_delay {
                    tokio::time::sleep(delay).await;
                }
            }
        }
        if chunked {
            stats.chunked_writes[idx].fetch_add(1, Ordering::Relaxed);
        }
        total += data.len() as u64;
        stats.bytes[idx].fetch_add(data.len() as u64, Ordering::Relaxed);

        if let Some((limit, kind)) = close_after
            && total >= limit
        {
            stats.closes[idx].fetch_add(1, Ordering::Relaxed);
            return Some(kind);
        }
    }
}
