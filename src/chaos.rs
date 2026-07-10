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
use crate::server::RedisServerHandle;

use std::process::Command;
#[cfg(feature = "tokio")]
use std::time::Duration;

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
async fn keyslot(cluster: &RedisClusterHandle, key: &str) -> Result<u16> {
    let output = cluster.cli().run(&["CLUSTER", "KEYSLOT", key]).await?;
    let slot: u16 = output
        .trim()
        .parse()
        .map_err(|_| crate::error::Error::Timeout {
            message: format!("could not parse CLUSTER KEYSLOT response: {output}"),
        })?;
    Ok(slot)
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
