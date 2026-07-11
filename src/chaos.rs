//! Fault injection primitives for testing Redis client resilience.
//!
//! This module provides operations for simulating failures in Redis
//! topologies: killing nodes, freezing processes (SIGSTOP/SIGCONT),
//! triggering failovers, and more. All operations work with the handle
//! types returned by the server, cluster, and sentinel builders.
//!
//! Unix-only: the node-kill and freeze/resume operations send POSIX signals
//! via `kill`. See the crate-level "Platform Support" docs for details.
//!
//! # Example
//!
//! ```no_run
//! use redis_server_wrapper::{RedisCluster, chaos};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let cluster = RedisCluster::builder()
//!     .masters(3)
//!     .replicas_per_master(1)
//!     .base_port(7100)
//!     .start()
//!     .await
//!     .unwrap();
//!
//! // Freeze a node (SIGSTOP) -- it stops processing but stays in memory.
//! chaos::freeze_node(cluster.node(0)).unwrap();
//!
//! // ... test client behavior with a frozen node ...
//!
//! // Resume the node (SIGCONT).
//! chaos::resume_node(cluster.node(0)).unwrap();
//! # }
//! ```

#[cfg(feature = "tokio")]
use crate::cluster::RedisClusterHandle;
#[cfg(feature = "tokio")]
use crate::error::{Error, Result};
#[cfg(feature = "tokio")]
use crate::sentinel::RedisSentinelHandle;
#[cfg(feature = "tokio")]
use crate::server::RedisServerHandle;

#[cfg(feature = "tokio")]
use std::os::unix::fs::PermissionsExt;
#[cfg(feature = "tokio")]
use std::path::PathBuf;
use std::process::Command;
#[cfg(feature = "tokio")]
use std::sync::Arc;
#[cfg(feature = "tokio")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "tokio")]
use std::time::Duration;
#[cfg(feature = "tokio")]
use tokio::io::AsyncReadExt;

/// Send a POSIX signal to `pid` via `kill <signal_flag> <pid>`.
///
/// Propagates both failure to spawn `kill` at all and a non-zero exit status
/// (e.g. the target PID no longer exists) as [`Error::Signal`]. Shared by
/// every signal-sending function in this module so none of them silently
/// swallows a failed kill.
#[cfg(feature = "tokio")]
fn send_signal(pid: u32, signal_flag: &str) -> Result<()> {
    let output = Command::new("kill")
        .args([signal_flag, &pid.to_string()])
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(Error::Signal {
            signal: signal_flag.to_string(),
            pid,
            detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Node-level operations
// ---------------------------------------------------------------------------

/// Kill a node immediately with SIGKILL.
///
/// The process is terminated without any chance to clean up. This simulates
/// a hard crash (e.g., OOM kill, hardware failure).
#[cfg(feature = "tokio")]
pub fn kill_node(handle: &RedisServerHandle) -> Result<()> {
    send_signal(handle.pid(), "-9")
}

/// Freeze a node by sending SIGSTOP.
///
/// The process is suspended -- it stops processing commands and won't
/// respond to PING, but stays in memory. Clients will see timeouts.
/// Use [`resume_node`] to unfreeze it.
///
/// This is more useful than [`kill_node`] for testing timeout handling and
/// partition scenarios because the node can be resumed without losing state.
#[cfg(feature = "tokio")]
pub fn freeze_node(handle: &RedisServerHandle) -> Result<()> {
    send_signal(handle.pid(), "-STOP")
}

/// Resume a frozen node by sending SIGCONT.
///
/// The process resumes from where it was suspended. Buffered writes and
/// replication will catch up automatically.
#[cfg(feature = "tokio")]
pub fn resume_node(handle: &RedisServerHandle) -> Result<()> {
    send_signal(handle.pid(), "-CONT")
}

/// Freeze a node for a fixed duration, then resume it automatically.
///
/// Sends SIGSTOP immediately and returns once that signal is confirmed
/// delivered. A background tokio task then sleeps for `duration` and sends
/// SIGCONT, so the node comes back on its own -- there's no need to call
/// [`resume_node`] or [`recover`] afterward. Useful for testing timeout
/// handling where the outage has a bounded, known length.
///
/// The initial SIGSTOP's success is surfaced via the returned `Result`. The
/// deferred SIGCONT sent by the background task is best-effort -- by the
/// time it runs there's no caller left to hand an error back to, so a failed
/// resume is silently ignored the way it always was.
#[cfg(feature = "tokio")]
pub fn pause_node(handle: &RedisServerHandle, duration: Duration) -> Result<()> {
    let pid = handle.pid();
    send_signal(pid, "-STOP")?;
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let _ = send_signal(pid, "-CONT");
    });
    Ok(())
}

/// Pause client connections for a duration using `CLIENT PAUSE ... ALL`
/// (the default mode since Redis 6.2).
///
/// Unlike [`freeze_node`], the server process stays responsive for
/// replication and cluster protocol. Only client commands are delayed.
/// After the duration expires, clients resume automatically.
///
/// See [`slow_down_writes`] to pause only write commands instead.
#[cfg(feature = "tokio")]
pub async fn slow_down(handle: &RedisServerHandle, millis: u64) -> Result<String> {
    handle.run(&["CLIENT", "PAUSE", &millis.to_string()]).await
}

/// Pause only write commands for a duration using `CLIENT PAUSE ... WRITE`
/// (available since Redis 6.2).
///
/// Unlike [`slow_down`], reads keep flowing while the pause is in effect --
/// useful for testing client behavior around write-availability windows
/// (e.g. during a failover) without blocking reads too.
#[cfg(feature = "tokio")]
pub async fn slow_down_writes(handle: &RedisServerHandle, millis: u64) -> Result<String> {
    handle
        .run(&["CLIENT", "PAUSE", &millis.to_string(), "WRITE"])
        .await
}

/// Trigger a background RDB save.
#[cfg(feature = "tokio")]
pub async fn trigger_save(handle: &RedisServerHandle) -> Result<String> {
    handle.run(&["BGSAVE"]).await
}

/// Flush all data from a node.
#[cfg(feature = "tokio")]
pub async fn flushall(handle: &RedisServerHandle) -> Result<String> {
    handle.run(&["FLUSHALL"]).await
}

/// Fill a node with `count` keys holding fixed-size values.
///
/// Writes keys named `<prefix>0` through `<prefix>{count-1}`, each holding a
/// 1 KiB value. Useful for exercising `maxmemory` and eviction-policy
/// behavior with a deterministic, bounded key count.
///
/// All `count` writes are issued through a single `EVAL` call (one
/// `redis-cli` invocation total) instead of spawning a `redis-cli` process
/// per key, which is the difference between one process and `count`
/// processes for a large fill.
///
/// Passing a cluster node directly (rather than going through
/// `redis-cli -c`) restricts the script to keys that hash to slots owned by
/// that node -- non-owned keys would fail the same way a plain per-key `SET`
/// against a single node already does in cluster mode (a `-MOVED` error), so
/// this isn't a behavior change for cluster use, just a faster single-node
/// one.
#[cfg(feature = "tokio")]
pub async fn fill_memory(handle: &RedisServerHandle, prefix: &str, count: usize) -> Result<()> {
    let value = "x".repeat(1024);
    handle
        .run(&[
            "EVAL",
            "for i=0,tonumber(ARGV[1])-1 do redis.call('SET', ARGV[2]..i, ARGV[3]) end",
            "0",
            &count.to_string(),
            prefix,
            &value,
        ])
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Process / connection / persistence fault primitives
// ---------------------------------------------------------------------------

/// Relaunch a node killed with [`kill_node`] from its existing on-disk
/// `redis.conf`.
///
/// Completes the crash-recovery loop `kill_node` starts: without this, a
/// killed node is a one-way door and tests cannot exercise client
/// reconnect-after-crash. The config file and data directory both survive
/// `SIGKILL`, so this replays the exact same launch path the server used the
/// first time it started, then refreshes `handle`'s pid.
///
/// This proves the process comes back and answers `PING`, not that any
/// particular key survived -- that depends entirely on the save/`appendonly`
/// configuration the node was started with, not on this call.
#[cfg(feature = "tokio")]
pub async fn restart_node(handle: &mut RedisServerHandle, timeout: Duration) -> Result<()> {
    handle.restart(timeout).await
}

/// Connection-type filter used by `CLIENT KILL TYPE`, matching the values
/// Redis accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    /// A normal (non-replication, non-pubsub) client connection.
    Normal,
    /// This server's connection to its own master (i.e. this server is a
    /// replica).
    Master,
    /// A connection from a replica of this server.
    Replica,
    /// A client subscribed via `SUBSCRIBE`/`PSUBSCRIBE`.
    PubSub,
}

impl ClientType {
    fn as_redis_arg(self) -> &'static str {
        match self {
            ClientType::Normal => "normal",
            ClientType::Master => "master",
            ClientType::Replica => "replica",
            ClientType::PubSub => "pubsub",
        }
    }
}

/// Filter selecting which connections [`kill_client_connections`] closes.
///
/// At least one of `ty` / `addr` should be set: an empty filter is sent to
/// Redis as a bare `CLIENT KILL` with no filter arguments, which Redis
/// rejects as a syntax error. Build one directly, or use
/// [`ClientKillFilter::of_type`] / [`ClientKillFilter::by_addr`].
#[derive(Debug, Clone, Default)]
pub struct ClientKillFilter {
    /// Restrict to this connection type (`CLIENT KILL TYPE <ty>`).
    pub ty: Option<ClientType>,
    /// Restrict to this remote address (`CLIENT KILL ADDR <ip:port>`).
    pub addr: Option<String>,
}

impl ClientKillFilter {
    /// Filter by connection type only.
    pub fn of_type(ty: ClientType) -> Self {
        Self {
            ty: Some(ty),
            addr: None,
        }
    }

    /// Filter by remote address only.
    pub fn by_addr(addr: impl Into<String>) -> Self {
        Self {
            ty: None,
            addr: Some(addr.into()),
        }
    }
}

/// Sever client connections matching `filter` via `CLIENT KILL`, returning
/// the number of connections killed.
///
/// `SKIPME` defaults to `yes` (Redis's own default), so the `redis-cli`
/// connection this call runs over is never counted or killed -- the harness
/// can't accidentally cut off its own control channel.
///
/// This is the mechanism Redis Sentinel itself uses to force clients to
/// reconnect after a reconfiguration: killing [`ClientType::Replica`]
/// connections on a master forces every replica to reconnect and resync
/// (see [`force_full_resync`] for pairing this with a full resync), and
/// killing [`ClientType::Normal`] connections (optionally narrowed with
/// `addr`) simulates a server-side connection reset that a client's own
/// reconnect logic must handle. The client only notices the connection is
/// gone the next time it sends a command.
#[cfg(feature = "tokio")]
pub async fn kill_client_connections(
    handle: &RedisServerHandle,
    filter: ClientKillFilter,
) -> Result<u64> {
    let mut args: Vec<String> = vec!["CLIENT".into(), "KILL".into()];
    if let Some(ty) = filter.ty {
        args.push("TYPE".into());
        args.push(ty.as_redis_arg().into());
    }
    if let Some(addr) = filter.addr {
        args.push("ADDR".into());
        args.push(addr);
    }
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = handle.run(&arg_refs).await?;
    out.trim().parse::<u64>().map_err(|_| Error::Timeout {
        message: format!("could not parse CLIENT KILL response: {out}"),
    })
}

/// Block the server's event loop for `duration` via `DEBUG SLEEP`.
///
/// Requires the server to have been started with
/// `.enable_debug_command("yes")` (Redis 7+ gates `DEBUG` behind that
/// setting); otherwise the spawned `DEBUG SLEEP` call fails with an unknown
/// command error that this function has no way to report back, since it
/// returns before that reply comes in.
///
/// Spawns a background tokio task that issues `DEBUG SLEEP <seconds>` and
/// returns to the caller immediately. A direct
/// `handle.run(&["DEBUG", "SLEEP", ..])` call would block the calling task
/// for the full duration instead: `redis-cli` doesn't get a reply until the
/// server wakes back up, so awaiting it directly would defeat the point of a
/// fire-and-forget fault -- the caller wants to keep running other
/// assertions (e.g. against a *different* node, or the client under test)
/// while this node is wedged.
///
/// Distinct from [`slow_down`] (`CLIENT PAUSE`, which keeps the event loop
/// and replication alive) and [`freeze_node`] (`SIGSTOP`, which suspends the
/// whole process): `DEBUG SLEEP` keeps the process running and connections
/// accepted by the kernel, but the single command-processing thread never
/// answers anything until the sleep ends -- the same shape a runaway Lua
/// script or a large O(N) command produces in a real incident.
#[cfg(feature = "tokio")]
pub fn block_event_loop(handle: &RedisServerHandle, duration: Duration) -> Result<()> {
    let cli = handle.cli().clone();
    tokio::spawn(async move {
        let secs = duration.as_secs_f64().to_string();
        let _ = cli.run(&["DEBUG", "SLEEP", &secs]).await;
    });
    Ok(())
}

/// Force the next replica reconnect on `master` to be a full resync instead
/// of a partial one.
///
/// Runs `DEBUG CHANGE-REPL-ID` to invalidate the master's replication
/// history, then `CLIENT KILL TYPE replica` to drop the existing replica
/// links -- when they reconnect, `PSYNC` no longer recognizes the
/// replication ID they last saw and falls back to a full sync. Requires the
/// server to have been started with `.enable_debug_command("yes")` (Redis 7+
/// gates `DEBUG` behind that setting).
///
/// Useful for testing client and application behavior during the full-sync
/// window: the master forking/`BGSAVE`-ing to build the snapshot, the
/// replica flushing its dataset and reloading, and stale reads on the
/// replica while that happens.
#[cfg(feature = "tokio")]
pub async fn force_full_resync(master: &RedisServerHandle) -> Result<()> {
    master.run(&["DEBUG", "CHANGE-REPL-ID"]).await?;
    master.run(&["CLIENT", "KILL", "TYPE", "replica"]).await?;
    Ok(())
}

/// Returns `true` if the current process is running as root (`id -u` == 0).
///
/// Used to short-circuit [`break_persistence`]: chmod permission bits are
/// ignored for root, and CI containers commonly run as root, so without this
/// check the function would silently fail to break anything instead of
/// erroring out.
#[cfg(feature = "tokio")]
fn running_as_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim() == "0")
        .unwrap_or(false)
}

/// Get a single scalar config value via `CONFIG GET <key>`. `redis-cli`'s
/// raw output for `CONFIG GET` is the key name on one line followed by the
/// value on the next; this returns just the value.
#[cfg(feature = "tokio")]
async fn config_get_single(handle: &RedisServerHandle, key: &str) -> Result<String> {
    let raw = handle.run(&["CONFIG", "GET", key]).await?;
    let mut lines = raw.lines();
    lines.next(); // the key itself, echoed back
    let value = lines.next().ok_or_else(|| Error::Timeout {
        message: format!("CONFIG GET {key} returned unexpected output: {raw}"),
    })?;
    Ok(value.trim().to_string())
}

/// Guard returned by [`break_persistence`]. Restores write access to the
/// node's data directory and confirms persistence has recovered.
///
/// If dropped without calling [`PersistenceGuard::restore`], makes a
/// best-effort synchronous attempt to restore the directory's original
/// permissions -- matching [`ReshardGuard`]'s Drop behavior -- but cannot
/// run the confirming `BGSAVE`/poll since `Drop` cannot `await`. Call
/// [`PersistenceGuard::restore`] explicitly whenever the test needs writes
/// working again afterward.
#[cfg(feature = "tokio")]
pub struct PersistenceGuard<'a> {
    handle: &'a RedisServerHandle,
    dir: PathBuf,
    original_mode: u32,
    resolved: bool,
}

/// Make a node's data directory unwritable so `BGSAVE` fails and, with the
/// default `stop-writes-on-bgsave-error yes`, subsequent writes are rejected
/// with `-MISCONF` until [`PersistenceGuard::restore`] is called.
///
/// Reproduces one of the most common real Redis production incidents:
/// clients must surface a failed-persistence `-MISCONF` correctly rather
/// than treating it like any other error.
///
/// Errors immediately if the current process is running as root
/// ([`Error::PrivilegeRequired`]): chmod permission bits are ignored for
/// root, so nothing would actually break, and CI containers commonly run as
/// root.
#[cfg(feature = "tokio")]
pub async fn break_persistence(
    handle: &RedisServerHandle,
    timeout: Duration,
) -> Result<PersistenceGuard<'_>> {
    if running_as_root() {
        return Err(Error::PrivilegeRequired {
            message: "break_persistence cannot chmod the data directory unwritable while \
                      running as root (permission bits are ignored for root); refusing rather \
                      than silently failing to break anything"
                .to_string(),
        });
    }

    let dir = PathBuf::from(config_get_single(handle, "dir").await?);
    let metadata = std::fs::metadata(&dir)?;
    let original_mode = metadata.permissions().mode();

    let mut perms = metadata.permissions();
    perms.set_mode(0o500);
    std::fs::set_permissions(&dir, perms)?;

    handle.run(&["BGSAVE"]).await?;

    crate::wait::wait_for(
        || async {
            matches!(
                handle.info(Some("persistence")).await,
                Ok(info) if info.get("rdb_last_bgsave_status").map(String::as_str) == Some("err")
            )
        },
        timeout,
        Duration::from_millis(250),
        "BGSAVE did not report a failed status after breaking persistence",
    )
    .await?;

    Ok(PersistenceGuard {
        handle,
        dir,
        original_mode,
        resolved: false,
    })
}

#[cfg(feature = "tokio")]
impl<'a> PersistenceGuard<'a> {
    /// Restore write access to the data directory, then confirm persistence
    /// has recovered by triggering a `BGSAVE` and polling `INFO persistence`
    /// for `rdb_last_bgsave_status:ok`.
    ///
    /// Writes stay rejected with `-MISCONF` until a save actually succeeds,
    /// so restoring permissions alone is not enough to undo
    /// [`break_persistence`].
    pub async fn restore(mut self, timeout: Duration) -> Result<()> {
        let mut perms = std::fs::metadata(&self.dir)?.permissions();
        perms.set_mode(self.original_mode);
        std::fs::set_permissions(&self.dir, perms)?;

        self.handle.run(&["BGSAVE"]).await?;

        crate::wait::wait_for(
            || async {
                matches!(
                    self.handle.info(Some("persistence")).await,
                    Ok(info) if info.get("rdb_last_bgsave_status").map(String::as_str) == Some("ok")
                )
            },
            timeout,
            Duration::from_millis(250),
            "BGSAVE did not report success after restoring persistence",
        )
        .await?;

        self.resolved = true;
        Ok(())
    }
}

#[cfg(feature = "tokio")]
impl Drop for PersistenceGuard<'_> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        // Best-effort: at least restore write access synchronously so the
        // node's data directory isn't left unwritable if the test never
        // calls `restore`. Can't await here, so this doesn't confirm BGSAVE
        // has recovered -- call `restore` explicitly to be sure.
        if let Ok(metadata) = std::fs::metadata(&self.dir) {
            let mut perms = metadata.permissions();
            perms.set_mode(self.original_mode);
            let _ = std::fs::set_permissions(&self.dir, perms);
        }
    }
}

/// Guard returned by [`exhaust_maxclients`]. Holds every TCP connection
/// opened to fill the server's `maxclients` limit, plus the previous
/// `maxclients` value if this call lowered it.
///
/// Dropping the guard without calling [`ClientFloodGuard::release`] closes
/// the held sockets (freeing up client slots) but leaves `maxclients` at the
/// lowered value -- restoring it needs a live connection, which `Drop`
/// cannot obtain since it cannot `await`. Call
/// [`ClientFloodGuard::release`] explicitly to restore the limit too.
#[cfg(feature = "tokio")]
pub struct ClientFloodGuard<'a> {
    handle: &'a RedisServerHandle,
    sockets: Vec<tokio::net::TcpStream>,
    previous_maxclients: Option<String>,
}

/// Open and hold connections until the server's `maxclients` limit is
/// reached, so the next connection attempt is rejected with `-ERR max
/// number of clients reached`.
///
/// If `maxclients` is `Some(n)`, first runs `CONFIG SET maxclients n` to
/// lower the ceiling (remembering the previous value so
/// [`ClientFloodGuard::release`] can restore it) -- this makes exhaustion
/// cheap and deterministic instead of opening potentially thousands of
/// connections. If `None`, opens connections against whatever `maxclients`
/// is already configured.
///
/// Detects exhaustion by reading the reply on each newly opened connection
/// (bounded by a short per-connection read timeout) rather than by counting
/// connects: the kernel accept backlog completes the TCP handshake
/// regardless of whether Redis itself has a free client slot, so only the
/// application-level `-ERR max number of clients reached` reply (sent
/// immediately, unprompted, before the connection is closed) tells rejection
/// apart from a live slot.
///
/// While the guard is held, `handle.run()` / `handle.is_alive()` and any
/// other new connection to this node are also rejected -- the harness's own
/// control connections are not exempt. Call [`ClientFloodGuard::release`] to
/// free the held sockets and restore `maxclients`.
#[cfg(feature = "tokio")]
pub async fn exhaust_maxclients(
    handle: &RedisServerHandle,
    maxclients: Option<u32>,
) -> Result<ClientFloodGuard<'_>> {
    let previous_maxclients = if let Some(n) = maxclients {
        let previous = config_get_single(handle, "maxclients").await?;
        handle
            .run(&["CONFIG", "SET", "maxclients", &n.to_string()])
            .await?;
        Some(previous)
    } else {
        None
    };

    let effective_maxclients: usize = config_get_single(handle, "maxclients")
        .await?
        .parse()
        .unwrap_or(10_000);
    // Safety cap so a detection mismatch (e.g. an unexpected error string)
    // can't spin this loop forever instead of returning an error.
    let attempt_cap = effective_maxclients + 8;

    let host = handle.host().to_string();
    let port = handle.port();
    let mut sockets = Vec::new();
    let mut exhausted = false;
    for _ in 0..attempt_cap {
        let mut stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
        let mut buf = [0u8; 128];
        match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {
                // Connection closed immediately with no data: exhausted.
                exhausted = true;
                break;
            }
            Ok(Ok(n))
                if String::from_utf8_lossy(&buf[..n]).contains("max number of clients reached") =>
            {
                exhausted = true;
                break;
            }
            _ => sockets.push(stream),
        }
    }

    if !exhausted {
        return Err(Error::Timeout {
            message: format!(
                "did not observe \"max number of clients reached\" within {attempt_cap} connection attempts"
            ),
        });
    }

    Ok(ClientFloodGuard {
        handle,
        sockets,
        previous_maxclients,
    })
}

#[cfg(feature = "tokio")]
impl<'a> ClientFloodGuard<'a> {
    /// Free the held sockets, then restore `maxclients` to its value before
    /// [`exhaust_maxclients`] was called (if it lowered one).
    ///
    /// Order matters: sockets must close *before* the `CONFIG SET` call --
    /// while the server is exhausted, even the harness's own `redis-cli`
    /// invocations are rejected the same way external clients are, so there
    /// is no free slot to run `CONFIG SET` on until connections are freed
    /// first.
    pub async fn release(mut self) -> Result<()> {
        self.sockets.clear();
        if let Some(previous) = self.previous_maxclients.take() {
            self.handle
                .run(&["CONFIG", "SET", "maxclients", &previous])
                .await?;
        }
        Ok(())
    }
}

/// Guard returned by [`flap_node`]. Stops the flap loop and guarantees the
/// node is left running (a final `SIGCONT`) when dropped.
#[cfg(feature = "tokio")]
pub struct FlapGuard {
    pid: u32,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

/// Cycle a node down (`SIGSTOP`) and up (`SIGCONT`) on a timer in a
/// background tokio task, simulating a flapping host that repeatedly enters
/// and leaves failure-detection windows (`cluster-node-timeout`, Sentinel's
/// `down-after-milliseconds`). Stresses client retry/backoff logic and
/// half-triggered failovers in a way a single [`pause_node`] does not.
///
/// Sends the first `SIGSTOP` immediately and returns once that signal is
/// confirmed delivered -- the same convention [`pause_node`] uses -- so a
/// dead target process surfaces as an error right away instead of a guard
/// that silently never flaps. The rest of the cycle (the deferred
/// `SIGCONT`/`SIGSTOP` pairs) runs in a background task and is best-effort,
/// same as [`pause_node`]'s deferred resume.
///
/// Drop the returned [`FlapGuard`] to stop the loop; it always sends a final
/// `SIGCONT` on drop (harmless if the node is already running), so the node
/// is never left frozen.
#[cfg(feature = "tokio")]
pub fn flap_node(handle: &RedisServerHandle, down: Duration, up: Duration) -> Result<FlapGuard> {
    let pid = handle.pid();
    send_signal(pid, "-STOP")?;

    let stop = Arc::new(AtomicBool::new(false));
    let task = {
        let stop = stop.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(down).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let _ = send_signal(pid, "-CONT");
                tokio::time::sleep(up).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let _ = send_signal(pid, "-STOP");
            }
        })
    };

    Ok(FlapGuard { pid, stop, task })
}

#[cfg(feature = "tokio")]
impl Drop for FlapGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.task.abort();
        let _ = send_signal(self.pid, "-CONT");
    }
}

// ---------------------------------------------------------------------------
// Cluster-level operations
// ---------------------------------------------------------------------------

/// Kill the master node that owns a given hash slot.
///
/// Queries `CLUSTER SLOTS` on the seed node to find which node owns the
/// slot, then sends SIGKILL to that process.
///
/// Returns `Ok(port)` of the killed node, or an error if the slot owner
/// could not be determined or the signal could not be delivered.
#[cfg(feature = "tokio")]
pub async fn kill_master_by_slot(cluster: &RedisClusterHandle, slot: u16) -> Result<u16> {
    let owner = find_slot_owner(cluster, slot).await?;
    send_signal(owner.pid(), "-9")?;
    Ok(owner.port())
}

/// Kill the master node that owns the hash slot for a given key.
///
/// Computes the slot via `CLUSTER KEYSLOT` then delegates to
/// [`kill_master_by_slot`].
#[cfg(feature = "tokio")]
pub async fn kill_master_by_key(cluster: &RedisClusterHandle, key: &str) -> Result<u16> {
    let slot = keyslot(cluster, key).await?;
    kill_master_by_slot(cluster, slot).await
}

/// Freeze the master node that owns a given hash slot.
///
/// Like [`kill_master_by_slot`] but sends SIGSTOP instead of SIGKILL.
/// The node can be recovered with [`recover`].
#[cfg(feature = "tokio")]
pub async fn freeze_master_by_slot(cluster: &RedisClusterHandle, slot: u16) -> Result<u16> {
    let owner = find_slot_owner(cluster, slot).await?;
    send_signal(owner.pid(), "-STOP")?;
    Ok(owner.port())
}

/// Trigger a `CLUSTER FAILOVER` on a replica node.
///
/// If the initial failover fails because the master is down -- either the
/// command returns an error result containing `"ERR"`, or the `redis-cli`
/// invocation itself fails (e.g. the master is unreachable) -- retries with
/// `CLUSTER FAILOVER FORCE`.
#[cfg(feature = "tokio")]
pub async fn trigger_failover(replica: &RedisServerHandle) -> Result<String> {
    match replica.run(&["CLUSTER", "FAILOVER"]).await {
        Ok(result) if !result.contains("ERR") => Ok(result),
        _ => replica.run(&["CLUSTER", "FAILOVER", "FORCE"]).await,
    }
}

/// Simulate a network partition by freezing every node not in `reachable`.
///
/// `reachable` holds the indices, matching the order of
/// [`RedisClusterHandle::nodes`], of the nodes that stay up; every other
/// node is sent SIGSTOP. Returns the ports of the frozen nodes, or an error
/// if any signal failed to deliver -- nodes frozen before the failing one
/// stay frozen; call [`recover`] to heal the partition either way.
#[cfg(feature = "tokio")]
pub fn partition(cluster: &RedisClusterHandle, reachable: &[usize]) -> Result<Vec<u16>> {
    let mut frozen = Vec::new();
    for (i, node) in cluster.nodes().iter().enumerate() {
        if !reachable.contains(&i) {
            send_signal(node.pid(), "-STOP")?;
            frozen.push(node.port());
        }
    }
    Ok(frozen)
}

/// Resume all nodes in a cluster by sending SIGCONT.
///
/// Useful after freezing nodes for partition simulation. Sends SIGCONT to
/// every node regardless of whether it was frozen -- signaling an
/// already-running node with SIGCONT is a no-op, not an error.
///
/// Every node is attempted before any failure is reported, so a dead node
/// (e.g. one removed with [`kill_node`]) does not prevent later frozen nodes
/// from resuming. Returns the first failure, if any, after the full pass.
#[cfg(feature = "tokio")]
pub fn recover(cluster: &RedisClusterHandle) -> Result<()> {
    let mut first_err = None;
    for node in cluster.nodes() {
        if let Err(e) = send_signal(node.pid(), "-CONT")
            && first_err.is_none()
        {
            first_err = Some(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Sentinel-level operations
// ---------------------------------------------------------------------------

/// Which point in a failover to crash a sentinel at, via `SENTINEL
/// SIMULATE-FAILURE`.
///
/// Both points are implemented in Redis's `sentinel.c` (since Redis 3.2),
/// purpose-built for testing an interrupted failover: the armed sentinel
/// crashes immediately after reaching the chosen point, leaving another
/// sentinel to notice and finish the job.
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentinelCrashPoint {
    /// Crash immediately after winning the leader election, before
    /// promoting the replica.
    AfterElection,
    /// Crash immediately after promoting the replica to master.
    AfterPromotion,
}

/// Arm a sentinel to crash mid-failover, then trigger the failover on that
/// same sentinel.
///
/// `SENTINEL FAILOVER` is executed by the sentinel that receives it, so the
/// sentinel armed via `SENTINEL SIMULATE-FAILURE` must also be the one that
/// triggers the failover -- arming one sentinel and triggering on another
/// would never exercise the crash at all.
///
/// Both `SIMULATE-FAILURE` and `FAILOVER` reply `+OK` before the process
/// actually crashes, so a successful `Ok(())` here means the crash was armed
/// and the failover requested, not that the crash has already happened. The
/// targeted sentinel process crashes and is left down afterward -- this
/// crate does not restart it; [`RedisSentinelHandle`]'s `stop()`/`Drop`
/// already tolerate a dead sentinel PID. The topology is intentionally left
/// with one fewer sentinel for the remaining sentinels to finish the
/// failover.
#[cfg(feature = "tokio")]
pub async fn crash_sentinel_during_failover(
    handle: &RedisSentinelHandle,
    sentinel_idx: usize,
    point: SentinelCrashPoint,
) -> Result<()> {
    let cli = handle.sentinel_cli(sentinel_idx)?;
    let mode = match point {
        SentinelCrashPoint::AfterElection => "crash-after-election",
        SentinelCrashPoint::AfterPromotion => "crash-after-promotion",
    };
    cli.run(&["SENTINEL", "SIMULATE-FAILURE", mode]).await?;
    cli.run(&["SENTINEL", "FAILOVER", handle.master_name()])
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Slot migration (reshard)
// ---------------------------------------------------------------------------

/// Migrate a single hash slot from one master to another.
///
/// Runs the standard Redis Cluster reshard sequence: `CLUSTER SETSLOT
/// IMPORTING` on `to`, `SETSLOT MIGRATING` on `from`, `CLUSTER
/// GETKEYSINSLOT` + `MIGRATE` for every key in the slot, then `SETSLOT
/// NODE` on every master so the new ownership propagates through the
/// cluster.
///
/// Returns the number of keys migrated.
///
/// While the migration is in flight, clients that address `from` for a key
/// already moved to `to` get a `-ASK` redirect. To hold the cluster in that
/// window deterministically (e.g. to test client ASK handling), use
/// [`ReshardGuard`] directly instead of this function.
#[cfg(feature = "tokio")]
pub async fn migrate_slot(
    cluster: &RedisClusterHandle,
    slot: u16,
    from: &RedisServerHandle,
    to: &RedisServerHandle,
) -> Result<usize> {
    let guard = ReshardGuard::start(cluster, slot, from, to).await?;
    let moved = guard.migrate_keys().await?;
    guard.complete().await?;
    Ok(moved)
}

/// Migrate a range of hash slots from one master to another.
///
/// Calls [`migrate_slot`] for each slot in `slots` in order. Returns the
/// total number of keys migrated across the whole range.
#[cfg(feature = "tokio")]
pub async fn migrate_slots(
    cluster: &RedisClusterHandle,
    slots: std::ops::RangeInclusive<u16>,
    from: &RedisServerHandle,
    to: &RedisServerHandle,
) -> Result<usize> {
    let mut moved = 0;
    for slot in slots {
        moved += migrate_slot(cluster, slot, from, to).await?;
    }
    Ok(moved)
}

/// Holds a hash slot in the `MIGRATING`/`IMPORTING` state so tests can
/// deterministically observe `-ASK` redirects, then finishes or reverts the
/// migration.
///
/// Construct with [`ReshardGuard::start`]. While the guard is alive, `slot`
/// is `MIGRATING` on the source node and `IMPORTING` on the target node --
/// the window in which Redis Cluster returns `-ASK` for keys in that slot
/// that live on the source but haven't been moved yet, and clients that
/// address the target directly get `-TRYAGAIN`/`MOVED` depending on the
/// key.
///
/// Call [`ReshardGuard::migrate_keys`] any number of times to move keys
/// without changing ownership (this is what lets a test provoke `-ASK`
/// deterministically: move some keys, leave others, then issue commands
/// against them). Call [`ReshardGuard::complete`] to migrate any remaining
/// keys and hand the slot to the target, or [`ReshardGuard::abort`] to hand
/// it back to the source.
///
/// If the guard is dropped without calling either, it makes a best-effort
/// synchronous attempt to reset the slot to `STABLE` on both nodes so the
/// cluster doesn't get stuck straddling the migration.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::{RedisCluster, chaos::ReshardGuard};
///
/// # async fn example() {
/// let cluster = RedisCluster::builder()
///     .masters(2)
///     .base_port(7200)
///     .start()
///     .await
///     .unwrap();
///
/// let guard = ReshardGuard::start(&cluster, 0, cluster.node(0), cluster.node(1))
///     .await
///     .unwrap();
///
/// // ... issue commands against a key in slot 0 and assert on -ASK ...
///
/// guard.complete().await.unwrap();
/// # }
/// ```
#[cfg(feature = "tokio")]
pub struct ReshardGuard<'a> {
    cluster: &'a RedisClusterHandle,
    slot: u16,
    from: &'a RedisServerHandle,
    to: &'a RedisServerHandle,
    to_id: String,
    resolved: bool,
}

#[cfg(feature = "tokio")]
impl<'a> ReshardGuard<'a> {
    /// Put `slot` into `MIGRATING` on `from` and `IMPORTING` on `to`.
    ///
    /// No keys are moved yet -- this only opens the migration window.
    pub async fn start(
        cluster: &'a RedisClusterHandle,
        slot: u16,
        from: &'a RedisServerHandle,
        to: &'a RedisServerHandle,
    ) -> Result<ReshardGuard<'a>> {
        let from_id = node_id(from).await?;
        let to_id = node_id(to).await?;
        let slot_str = slot.to_string();
        to.run(&["CLUSTER", "SETSLOT", &slot_str, "IMPORTING", &from_id])
            .await?;
        from.run(&["CLUSTER", "SETSLOT", &slot_str, "MIGRATING", &to_id])
            .await?;
        Ok(ReshardGuard {
            cluster,
            slot,
            from,
            to,
            to_id,
            resolved: false,
        })
    }

    /// Migrate every key currently in the slot from `from` to `to`, without
    /// changing slot ownership.
    ///
    /// Safe to call more than once (e.g. to sweep up keys written after a
    /// previous call). Returns the number of keys moved by this call.
    pub async fn migrate_keys(&self) -> Result<usize> {
        let password = self.cluster.password();
        let to_host = self.to.host().to_string();
        let to_port = self.to.port().to_string();
        let mut moved = 0;
        loop {
            let keys = get_keys_in_slot(self.from, self.slot, 100).await?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let mut args = vec![
                    to_host.as_str(),
                    to_port.as_str(),
                    key.as_str(),
                    "0",
                    "5000",
                ];
                if let Some(password) = password {
                    args.push("AUTH");
                    args.push(password);
                }
                let mut cmd = vec!["MIGRATE"];
                cmd.extend(args);
                self.from.run(&cmd).await?;
                moved += 1;
            }
        }
        Ok(moved)
    }

    /// Finish the migration: sweep up any remaining keys, then reassign
    /// `slot` to the target node on every master.
    pub async fn complete(mut self) -> Result<usize> {
        let moved = self.migrate_keys().await?;
        let slot_str = self.slot.to_string();
        for node in self.cluster.master_nodes() {
            node.run(&["CLUSTER", "SETSLOT", &slot_str, "NODE", &self.to_id])
                .await?;
        }
        self.resolved = true;
        Ok(moved)
    }

    /// Abort the migration: reset `slot` back to `STABLE` on both nodes,
    /// leaving ownership with the source.
    ///
    /// Any keys already moved to the target by [`ReshardGuard::migrate_keys`]
    /// stay there -- `MIGRATE` does not roll back, so a partial abort can
    /// leave a few keys reachable only via the target until the next
    /// reshard picks them up.
    pub async fn abort(mut self) -> Result<()> {
        let slot_str = self.slot.to_string();
        self.from
            .run(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"])
            .await?;
        self.to
            .run(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"])
            .await?;
        self.resolved = true;
        Ok(())
    }
}

#[cfg(feature = "tokio")]
impl Drop for ReshardGuard<'_> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let slot_str = self.slot.to_string();
        self.from
            .cli()
            .fire_and_forget(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"]);
        self.to
            .cli()
            .fire_and_forget(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"]);
    }
}

/// Get this node's cluster node ID via `CLUSTER MYID`.
#[cfg(feature = "tokio")]
async fn node_id(handle: &RedisServerHandle) -> Result<String> {
    let id = handle.run(&["CLUSTER", "MYID"]).await?;
    Ok(id.trim().to_string())
}

/// Get up to `count` keys in `slot` via `CLUSTER GETKEYSINSLOT`.
#[cfg(feature = "tokio")]
async fn get_keys_in_slot(
    handle: &RedisServerHandle,
    slot: u16,
    count: u32,
) -> Result<Vec<String>> {
    let output = handle
        .run(&[
            "CLUSTER",
            "GETKEYSINSLOT",
            &slot.to_string(),
            &count.to_string(),
        ])
        .await?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the hash slot for a key via `CLUSTER KEYSLOT`.
#[cfg(feature = "tokio")]
pub async fn keyslot(cluster: &RedisClusterHandle, key: &str) -> Result<u16> {
    let output = cluster.cli().run(&["CLUSTER", "KEYSLOT", key]).await?;
    let slot: u16 = output
        .trim()
        .parse()
        .map_err(|_| crate::error::Error::Timeout {
            message: format!("could not parse CLUSTER KEYSLOT response: {output}"),
        })?;
    Ok(slot)
}

/// The port of the master node that currently owns `slot`.
///
/// Thin public wrapper around the same `CLUSTER NODES` lookup
/// [`kill_master_by_slot`] and friends use internally. Returns the owner's
/// port rather than a borrowed node handle, since `CLUSTER FAILOVER` is
/// asynchronous (redis.io: "does not execute a failover synchronously...
/// only schedules a manual failover") and the topology can change out from
/// under a borrowed reference; a plain port value is what
/// [`wait_for_slot_owner_change`] polls for and what callers can compare or
/// store.
#[cfg(feature = "tokio")]
pub async fn slot_owner(cluster: &RedisClusterHandle, slot: u16) -> Result<u16> {
    find_slot_owner(cluster, slot).await.map(|node| node.port())
}

/// Poll [`slot_owner`] until `slot` is owned by a port different from
/// `old_port`, or timeout. Returns the new owner's port.
///
/// The assertion twin of [`trigger_failover`] / [`kill_master_by_slot`] /
/// [`freeze_master_by_slot`]: since `CLUSTER FAILOVER` completes
/// asynchronously, those functions return before the topology has actually
/// settled, leaving tests to either sleep blindly or poll `CLUSTER NODES`
/// themselves. Each poll is bounded so a frozen former-owner node can't hang
/// the wait.
#[cfg(feature = "tokio")]
pub async fn wait_for_slot_owner_change(
    cluster: &RedisClusterHandle,
    slot: u16,
    old_port: u16,
    timeout: Duration,
) -> Result<u16> {
    // A `Cell` avoids the closure needing a unique (`&mut`) capture of
    // `new_owner` across separate invocations, which the borrow checker
    // otherwise rejects for an `FnMut` closure returning a future.
    let new_owner = std::cell::Cell::new(old_port);
    crate::wait::wait_for(
        || async {
            match tokio::time::timeout(crate::cli::HEALTH_CHECK_TIMEOUT, slot_owner(cluster, slot))
                .await
            {
                Ok(Ok(port)) if port != old_port => {
                    new_owner.set(port);
                    true
                }
                _ => false,
            }
        },
        timeout,
        Duration::from_millis(250),
        format!("slot {slot} owner did not change from port {old_port} in time"),
    )
    .await?;
    Ok(new_owner.get())
}

/// Number of keys currently stored in `slot`, via `CLUSTER COUNTKEYSINSLOT`
/// on the slot's current owner (see [`slot_owner`]).
#[cfg(feature = "tokio")]
pub async fn count_keys_in_slot(cluster: &RedisClusterHandle, slot: u16) -> Result<usize> {
    let owner = find_slot_owner(cluster, slot).await?;
    let raw = owner
        .run(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()])
        .await?;
    raw.trim().parse().map_err(|_| Error::Timeout {
        message: format!("could not parse CLUSTER COUNTKEYSINSLOT response: {raw}"),
    })
}

/// Find the cluster node that owns a given slot by querying CLUSTER SLOTS.
///
/// CLUSTER SLOTS returns ranges like:
/// ```text
/// 1) 1) (integer) 0
///    2) (integer) 5460
///    3) 1) "127.0.0.1"
///       2) (integer) 7000
///       3) "node-id..."
/// ```
///
/// We parse the port from each range and match it to our node handles.
#[cfg(feature = "tokio")]
async fn find_slot_owner(cluster: &RedisClusterHandle, slot: u16) -> Result<&RedisServerHandle> {
    // Use CLUSTER NODES which gives a simpler text format to parse.
    // Each line: <id> <ip:port@bus> <flags> <master> <ping> <pong> <epoch> <link> <slot-ranges...>
    for node in cluster.nodes() {
        if let Ok(info) = node.run(&["CLUSTER", "NODES"]).await {
            for line in info.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() < 9 {
                    continue;
                }
                let flags = parts[2];
                if !flags.contains("master") {
                    continue;
                }
                // Check slot ranges (parts[8..])
                for range_str in &parts[8..] {
                    if slot_in_range(range_str, slot) {
                        // Extract port from ip:port@bus
                        if let Some(port) = parse_cluster_node_port(parts[1]) {
                            // Find the matching handle
                            for n in cluster.nodes() {
                                if n.port() == port {
                                    return Ok(n);
                                }
                            }
                        }
                    }
                }
            }
            // Only need to query one node.
            break;
        }
    }
    Err(crate::error::Error::Timeout {
        message: format!("could not find owner of slot {slot}"),
    })
}

/// Check if a slot falls within a range string like "0-5460" or "5461".
fn slot_in_range(range_str: &str, slot: u16) -> bool {
    // Skip import/migration markers like [123->-node] or [123-<-node]
    if range_str.starts_with('[') {
        return false;
    }
    if let Some((start, end)) = range_str.split_once('-') {
        let Ok(start) = start.parse::<u16>() else {
            return false;
        };
        let Ok(end) = end.parse::<u16>() else {
            return false;
        };
        slot >= start && slot <= end
    } else {
        range_str.parse::<u16>().ok() == Some(slot)
    }
}

/// Parse port from "ip:port@busport" format.
fn parse_cluster_node_port(addr: &str) -> Option<u16> {
    let host_port = addr.split('@').next()?;
    let port_str = host_port.rsplit(':').next()?;
    port_str.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_in_range_single() {
        assert!(slot_in_range("5461", 5461));
        assert!(!slot_in_range("5461", 5462));
    }

    #[test]
    fn slot_in_range_range() {
        assert!(slot_in_range("0-5460", 0));
        assert!(slot_in_range("0-5460", 5460));
        assert!(slot_in_range("0-5460", 1000));
        assert!(!slot_in_range("0-5460", 5461));
    }

    #[test]
    fn slot_in_range_import_marker() {
        assert!(!slot_in_range("[123->-abc]", 123));
        assert!(!slot_in_range("[123-<-abc]", 123));
    }

    #[test]
    fn parse_port_from_cluster_nodes() {
        assert_eq!(parse_cluster_node_port("127.0.0.1:7000@17000"), Some(7000));
        assert_eq!(parse_cluster_node_port("127.0.0.1:7001@17001"), Some(7001));
        assert_eq!(parse_cluster_node_port("garbage"), None);
    }
}
