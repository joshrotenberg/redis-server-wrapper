//! Byte-level TCP fault-injection proxy for testing Redis client resilience.
//!
//! [`FaultProxy`] sits between a test client and an upstream Redis node,
//! forwarding bytes over TCP while allowing tests to inject faults: per-
//! direction delay, mid-frame connection drops, chunked writes, and a
//! black-hole mode. Unlike [`crate::chaos`], which operates on the server
//! process, this module operates purely on the wire and needs no Docker or
//! root privileges.
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
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
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

#[derive(Clone, Debug, Default)]
struct FaultState {
    delay: [Option<Delay>; 2],
    close_after: [Option<u64>; 2],
    chunk_size: Option<usize>,
    drop_all: bool,
}

/// A TCP proxy that forwards bytes between test clients and an upstream
/// Redis node while injecting configurable network faults.
///
/// Construct with [`FaultProxy::spawn`]. Fault controls can be changed at
/// any time via the returned handle; delay, close-after, and chunk-size take
/// effect on data already in flight (checked on every read), while
/// [`FaultProxy::set_drop_all`] is snapshotted once per accepted connection.
/// The proxy stops accepting new connections when the handle is dropped;
/// already-established connections run to completion.
pub struct FaultProxy {
    addr: SocketAddr,
    state: Arc<RwLock<FaultState>>,
    accept_task: JoinHandle<()>,
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

        let accept_state = state.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let (client, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                tokio::spawn(handle_connection(
                    client,
                    upstream_addr,
                    accept_state.clone(),
                ));
            }
        });

        Ok(FaultProxy {
            addr,
            state,
            accept_task,
        })
    }

    /// The local address clients should connect to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
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
    /// this setting is active.
    pub fn close_after(&self, direction: Direction, bytes: u64) {
        self.state.write().unwrap().close_after[direction.idx()] = Some(bytes);
    }

    /// Remove the close-after threshold configured for `direction`.
    pub fn clear_close_after(&self, direction: Direction) {
        self.state.write().unwrap().close_after[direction.idx()] = None;
    }

    /// Split forwarded writes into pieces of at most `size` bytes, in both
    /// directions. Use `size == 1` to exercise incremental decoders.
    pub fn set_chunk_size(&self, size: usize) {
        self.state.write().unwrap().chunk_size = Some(size.max(1));
    }

    /// Stop chunking writes; forward reads in whatever size they arrive.
    pub fn clear_chunk_size(&self) {
        self.state.write().unwrap().chunk_size = None;
    }

    /// Enable or disable black-hole mode. While enabled, newly accepted
    /// connections are held open (no upstream connection is made, nothing is
    /// ever forwarded or written back) until the client disconnects or the
    /// proxy is dropped. Existing connections are unaffected.
    pub fn set_drop_all(&self, drop_all: bool) {
        self.state.write().unwrap().drop_all = drop_all;
    }

    /// Clear every fault control, returning the proxy to clean passthrough
    /// for new connections.
    pub fn reset(&self) {
        *self.state.write().unwrap() = FaultState::default();
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn handle_connection(
    mut client: TcpStream,
    upstream_addr: SocketAddr,
    state: Arc<RwLock<FaultState>>,
) {
    if state.read().unwrap().drop_all {
        let mut buf = [0u8; 4096];
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }

    let upstream = match TcpStream::connect(upstream_addr).await {
        Ok(s) => s,
        Err(_) => return,
    };

    let (client_r, client_w) = client.into_split();
    let (upstream_r, upstream_w) = upstream.into_split();

    tokio::select! {
        _ = forward(client_r, upstream_w, state.clone(), Direction::ClientToUpstream) => {},
        _ = forward(upstream_r, client_w, state, Direction::UpstreamToClient) => {},
    }
}

/// Copy bytes from `reader` to `writer`, applying whatever delay,
/// close-after, and chunk-size controls are configured for `direction` at
/// the time each chunk is read. Returns when the source hits EOF/error, the
/// sink fails, or the close-after threshold for `direction` is reached.
async fn forward(
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    state: Arc<RwLock<FaultState>>,
    direction: Direction,
) {
    let idx = direction.idx();
    let mut total: u64 = 0;
    let mut buf = [0u8; 4096];

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let mut data = &buf[..n];

        let (delay, close_after, chunk_size) = {
            let s = state.read().unwrap();
            (s.delay[idx], s.close_after[idx], s.chunk_size)
        };

        if let Some(limit) = close_after {
            if total >= limit {
                return;
            }
            let remaining = (limit - total) as usize;
            if data.len() > remaining {
                data = &data[..remaining];
            }
        }

        if let Some(delay) = delay {
            tokio::time::sleep(delay.resolve()).await;
        }

        let chunk = chunk_size.unwrap_or(data.len()).max(1);
        for piece in data.chunks(chunk) {
            if writer.write_all(piece).await.is_err() {
                return;
            }
        }
        total += data.len() as u64;

        if let Some(limit) = close_after
            && total >= limit
        {
            return;
        }
    }
}
