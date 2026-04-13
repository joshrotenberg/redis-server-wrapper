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
