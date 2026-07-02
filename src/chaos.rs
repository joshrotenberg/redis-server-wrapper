//! Fault injection primitives for testing Redis client resilience.
//!
//! This module provides operations for simulating failures in Redis
//! topologies: killing nodes, freezing processes (SIGSTOP/SIGCONT),
//! triggering failovers, and more. All operations work with the handle
//! types returned by the server, cluster, and sentinel builders.
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
//! chaos::freeze_node(cluster.node(0));
//!
//! // ... test client behavior with a frozen node ...
//!
//! // Resume the node (SIGCONT).
//! chaos::resume_node(cluster.node(0));
//! # }
//! ```

#[cfg(feature = "tokio")]
use crate::cluster::RedisClusterHandle;
#[cfg(feature = "tokio")]
use crate::error::Result;
#[cfg(feature = "tokio")]
use crate::server::RedisServerHandle;

use std::process::Command;

// ---------------------------------------------------------------------------
// Node-level operations
// ---------------------------------------------------------------------------

/// Kill a node immediately with SIGKILL.
///
/// The process is terminated without any chance to clean up. This simulates
/// a hard crash (e.g., OOM kill, hardware failure).
#[cfg(feature = "tokio")]
pub fn kill_node(handle: &RedisServerHandle) {
    let pid = handle.pid().to_string();
    let _ = Command::new("kill").args(["-9", &pid]).output();
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
pub fn freeze_node(handle: &RedisServerHandle) {
    let pid = handle.pid().to_string();
    let _ = Command::new("kill").args(["-STOP", &pid]).output();
}

/// Resume a frozen node by sending SIGCONT.
///
/// The process resumes from where it was suspended. Buffered writes and
/// replication will catch up automatically.
#[cfg(feature = "tokio")]
pub fn resume_node(handle: &RedisServerHandle) {
    let pid = handle.pid().to_string();
    let _ = Command::new("kill").args(["-CONT", &pid]).output();
}

/// Pause client connections for a duration using `CLIENT PAUSE`.
///
/// Unlike [`freeze_node`], the server process stays responsive for
/// replication and cluster protocol. Only client commands are delayed.
/// After the duration expires, clients resume automatically.
#[cfg(feature = "tokio")]
pub async fn slow_down(handle: &RedisServerHandle, millis: u64) -> Result<String> {
    handle.run(&["CLIENT", "PAUSE", &millis.to_string()]).await
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

// ---------------------------------------------------------------------------
// Cluster-level operations
// ---------------------------------------------------------------------------

/// Kill the master node that owns a given hash slot.
///
/// Queries `CLUSTER SLOTS` on the seed node to find which node owns the
/// slot, then sends SIGKILL to that process.
///
/// Returns `Ok(port)` of the killed node, or an error if the slot owner
/// could not be determined.
#[cfg(feature = "tokio")]
pub async fn kill_master_by_slot(cluster: &RedisClusterHandle, slot: u16) -> Result<u16> {
    let owner = find_slot_owner(cluster, slot).await?;
    let pid = owner.pid().to_string();
    let _ = Command::new("kill").args(["-9", &pid]).output();
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
    let pid = owner.pid().to_string();
    let _ = Command::new("kill").args(["-STOP", &pid]).output();
    Ok(owner.port())
}

/// Trigger a `CLUSTER FAILOVER` on a replica node.
///
/// If the initial failover fails because the master is down, retries with
/// `CLUSTER FAILOVER FORCE`.
#[cfg(feature = "tokio")]
pub async fn trigger_failover(replica: &RedisServerHandle) -> Result<String> {
    let result = replica.run(&["CLUSTER", "FAILOVER"]).await?;
    if result.contains("ERR") {
        return replica.run(&["CLUSTER", "FAILOVER", "FORCE"]).await;
    }
    Ok(result)
}

/// Resume all nodes in a cluster by sending SIGCONT.
///
/// Useful after freezing nodes for partition simulation. Sends SIGCONT to
/// every node regardless of whether it was frozen.
#[cfg(feature = "tokio")]
pub fn recover(cluster: &RedisClusterHandle) {
    for node in cluster.nodes() {
        let pid = node.pid().to_string();
        let _ = Command::new("kill").args(["-CONT", &pid]).output();
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
