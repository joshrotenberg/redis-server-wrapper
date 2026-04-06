//! Redis Cluster lifecycle management built on `RedisServer`.

use std::collections::HashMap;
use std::time::Duration;

use crate::cli::RedisCli;
use crate::error::{Error, Result};
use crate::server::{RedisServer, RedisServerHandle, SavePolicy};

/// Context passed to the per-node configuration callback.
///
/// Provides information about the node being configured so the callback
/// can make per-node decisions (e.g., different config for masters vs. replicas,
/// or for a specific node index).
pub struct NodeContext {
    /// The pre-configured [`RedisServer`] builder for this node.
    ///
    /// All uniform cluster-level settings have already been applied.
    /// The callback should modify and return this builder.
    pub server: RedisServer,
    /// Zero-based index of this node in the cluster.
    ///
    /// Nodes are ordered by port: masters occupy indices `0..masters`,
    /// replicas occupy indices `masters..total`.
    pub index: usize,
    /// The port assigned to this node.
    pub port: u16,
    /// Total number of nodes in the cluster.
    pub total_nodes: u16,
    /// Number of master nodes.
    pub masters: u16,
    /// Number of replicas per master.
    pub replicas_per_master: u16,
}

impl NodeContext {
    /// Whether this node is a master (by initial topology order).
    pub fn is_master(&self) -> bool {
        self.index < self.masters as usize
    }

    /// Whether this node is a replica (by initial topology order).
    pub fn is_replica(&self) -> bool {
        !self.is_master()
    }
}

/// Builder for a Redis Cluster.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::RedisCluster;
///
/// # async fn example() {
/// let cluster = RedisCluster::builder()
///     .masters(3)
///     .replicas_per_master(1)
///     .base_port(7000)
///     .start()
///     .await
///     .unwrap();
///
/// assert!(cluster.is_healthy().await);
/// // Stopped automatically on Drop.
/// # }
/// ```
pub struct RedisClusterBuilder {
    masters: u16,
    replicas_per_master: u16,
    base_port: u16,
    bind: String,
    password: Option<String>,
    logfile: Option<String>,
    save: Option<SavePolicy>,
    appendonly: Option<bool>,
    cluster_node_timeout: Option<u64>,
    cluster_require_full_coverage: Option<bool>,
    cluster_allow_reads_when_down: Option<bool>,
    cluster_allow_pubsubshard_when_down: Option<bool>,
    cluster_allow_replica_migration: Option<bool>,
    cluster_migration_barrier: Option<u32>,
    cluster_announce_hostname: Option<String>,
    cluster_announce_human_nodename: Option<String>,
    cluster_preferred_endpoint_type: Option<String>,
    cluster_replica_no_failover: Option<bool>,
    cluster_replica_validity_factor: Option<u32>,
    cluster_announce_ip: Option<String>,
    cluster_announce_port: Option<u16>,
    cluster_announce_bus_port: Option<u16>,
    cluster_announce_tls_port: Option<u16>,
    cluster_port: Option<u16>,
    cluster_link_sendbuf_limit: Option<u64>,
    cluster_compatibility_sample_ratio: Option<u32>,
    cluster_slot_migration_handoff_max_lag_bytes: Option<u64>,
    cluster_slot_migration_write_pause_timeout: Option<u64>,
    cluster_slot_stats_enabled: Option<bool>,
    min_replicas_to_write: Option<u32>,
    min_replicas_max_lag: Option<u32>,
    repl_diskless_sync: Option<bool>,
    repl_diskless_sync_delay: Option<u32>,
    repl_ping_replica_period: Option<u32>,
    repl_timeout: Option<u32>,
    extra: HashMap<String, String>,
    redis_server_bin: String,
    redis_cli_bin: String,
    node_config_fn: Option<Box<dyn FnMut(NodeContext) -> RedisServer + Send>>,
}

impl RedisClusterBuilder {
    /// Set the number of master nodes (default: `3`).
    pub fn masters(mut self, n: u16) -> Self {
        self.masters = n;
        self
    }

    /// Set the number of replicas per master (default: `0`).
    pub fn replicas_per_master(mut self, n: u16) -> Self {
        self.replicas_per_master = n;
        self
    }

    /// Set the base port for cluster nodes (default: `7000`).
    ///
    /// Nodes are assigned consecutive ports starting at this value.
    pub fn base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

    /// Set the bind address for all cluster nodes (default: `"127.0.0.1"`).
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    /// Set a `requirepass` password for all cluster nodes.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the log file path for all cluster nodes.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.logfile = Some(path.into());
        self
    }

    /// Set the RDB save policy for all cluster nodes.
    ///
    /// `true` omits the `save` directive (Redis defaults apply).
    /// `false` emits `save ""` to disable RDB entirely.
    pub fn save(mut self, save: bool) -> Self {
        self.save = Some(if save {
            SavePolicy::Default
        } else {
            SavePolicy::Disabled
        });
        self
    }

    /// Set a custom RDB save schedule for all cluster nodes.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.save = Some(SavePolicy::Custom(schedule));
        self
    }

    /// Enable or disable AOF persistence for all cluster nodes.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.appendonly = Some(appendonly);
        self
    }

    /// Set the cluster node timeout in milliseconds for all nodes (default: `5000`).
    pub fn cluster_node_timeout(mut self, ms: u64) -> Self {
        self.cluster_node_timeout = Some(ms);
        self
    }

    /// Require full hash slot coverage for the cluster to accept writes.
    pub fn cluster_require_full_coverage(mut self, require: bool) -> Self {
        self.cluster_require_full_coverage = Some(require);
        self
    }

    /// Allow reads when the cluster is down.
    pub fn cluster_allow_reads_when_down(mut self, allow: bool) -> Self {
        self.cluster_allow_reads_when_down = Some(allow);
        self
    }

    /// Allow pubsub shard channels when the cluster is down.
    pub fn cluster_allow_pubsubshard_when_down(mut self, allow: bool) -> Self {
        self.cluster_allow_pubsubshard_when_down = Some(allow);
        self
    }

    /// Allow automatic replica migration between masters.
    pub fn cluster_allow_replica_migration(mut self, allow: bool) -> Self {
        self.cluster_allow_replica_migration = Some(allow);
        self
    }

    /// Set the minimum number of replicas a master must retain before one can migrate.
    pub fn cluster_migration_barrier(mut self, barrier: u32) -> Self {
        self.cluster_migration_barrier = Some(barrier);
        self
    }

    /// Set the hostname each node announces to the cluster.
    pub fn cluster_announce_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.cluster_announce_hostname = Some(hostname.into());
        self
    }

    /// Set the preferred endpoint type for cluster redirections (e.g. `"ip"`, `"hostname"`).
    pub fn cluster_preferred_endpoint_type(mut self, endpoint_type: impl Into<String>) -> Self {
        self.cluster_preferred_endpoint_type = Some(endpoint_type.into());
        self
    }

    /// Prevent replicas from attempting automatic failover.
    ///
    /// Manual failover via `CLUSTER FAILOVER` still works.
    pub fn cluster_replica_no_failover(mut self, no_failover: bool) -> Self {
        self.cluster_replica_no_failover = Some(no_failover);
        self
    }

    /// Set the replica validity factor for failover eligibility.
    ///
    /// A replica will not failover if it has been disconnected from the master
    /// for more than `(node-timeout * factor) + repl-ping-replica-period` seconds.
    /// Set to `0` to allow any replica to failover regardless of staleness.
    pub fn cluster_replica_validity_factor(mut self, factor: u32) -> Self {
        self.cluster_replica_validity_factor = Some(factor);
        self
    }

    /// Set the IP address nodes announce for client redirects (MOVED/ASKING).
    pub fn cluster_announce_ip(mut self, ip: impl Into<String>) -> Self {
        self.cluster_announce_ip = Some(ip.into());
        self
    }

    /// Set the client port nodes announce for redirects.
    pub fn cluster_announce_port(mut self, port: u16) -> Self {
        self.cluster_announce_port = Some(port);
        self
    }

    /// Set the cluster bus port nodes announce for gossip.
    pub fn cluster_announce_bus_port(mut self, port: u16) -> Self {
        self.cluster_announce_bus_port = Some(port);
        self
    }

    /// Set the TLS client port nodes announce for redirects.
    pub fn cluster_announce_tls_port(mut self, port: u16) -> Self {
        self.cluster_announce_tls_port = Some(port);
        self
    }

    /// Set a friendly node name broadcast for debugging/admin display.
    pub fn cluster_announce_human_nodename(mut self, name: impl Into<String>) -> Self {
        self.cluster_announce_human_nodename = Some(name.into());
        self
    }

    /// Set a dedicated cluster bus port (default: client port + 10000).
    pub fn cluster_port(mut self, port: u16) -> Self {
        self.cluster_port = Some(port);
        self
    }

    /// Set the maximum memory for a cluster bus link's output buffer.
    ///
    /// When exceeded, the link is disconnected. Set to `0` for unlimited.
    pub fn cluster_link_sendbuf_limit(mut self, limit: u64) -> Self {
        self.cluster_link_sendbuf_limit = Some(limit);
        self
    }

    /// Set the cluster compatibility sample ratio.
    pub fn cluster_compatibility_sample_ratio(mut self, ratio: u32) -> Self {
        self.cluster_compatibility_sample_ratio = Some(ratio);
        self
    }

    /// Set the maximum replication lag in bytes before slot migration handoff.
    pub fn cluster_slot_migration_handoff_max_lag_bytes(mut self, bytes: u64) -> Self {
        self.cluster_slot_migration_handoff_max_lag_bytes = Some(bytes);
        self
    }

    /// Set the write pause timeout in milliseconds during slot migration.
    pub fn cluster_slot_migration_write_pause_timeout(mut self, ms: u64) -> Self {
        self.cluster_slot_migration_write_pause_timeout = Some(ms);
        self
    }

    /// Enable per-slot statistics tracking.
    pub fn cluster_slot_stats_enabled(mut self, enable: bool) -> Self {
        self.cluster_slot_stats_enabled = Some(enable);
        self
    }

    // -- replication directives relevant in cluster mode --

    /// Set the minimum number of connected replicas before the master accepts writes.
    ///
    /// Useful for split-brain protection: a partitioned master with no reachable
    /// replicas stops accepting writes, reducing data loss during partitions.
    pub fn min_replicas_to_write(mut self, n: u32) -> Self {
        self.min_replicas_to_write = Some(n);
        self
    }

    /// Set the maximum replication lag (seconds) before a replica is considered disconnected.
    ///
    /// Used with `min_replicas_to_write` to determine if enough replicas are connected.
    pub fn min_replicas_max_lag(mut self, seconds: u32) -> Self {
        self.min_replicas_max_lag = Some(seconds);
        self
    }

    /// Enable or disable diskless replication sync.
    ///
    /// Diskless sync is faster but uses more memory during transfer.
    pub fn repl_diskless_sync(mut self, enable: bool) -> Self {
        self.repl_diskless_sync = Some(enable);
        self
    }

    /// Set the delay in seconds before starting a diskless replication transfer.
    ///
    /// Allows batching multiple replicas syncing at once.
    pub fn repl_diskless_sync_delay(mut self, seconds: u32) -> Self {
        self.repl_diskless_sync_delay = Some(seconds);
        self
    }

    /// Set how often replicas ping the master (seconds).
    ///
    /// Used in the replica validity calculation:
    /// `(node-timeout * validity-factor) + repl-ping-replica-period`.
    pub fn repl_ping_replica_period(mut self, seconds: u32) -> Self {
        self.repl_ping_replica_period = Some(seconds);
        self
    }

    /// Set the replication timeout in seconds.
    ///
    /// If a replica doesn't hear from master for this long, it considers the link dead.
    pub fn repl_timeout(mut self, seconds: u32) -> Self {
        self.repl_timeout = Some(seconds);
        self
    }

    /// Set a per-node configuration callback.
    ///
    /// The callback receives a [`NodeContext`] containing the pre-configured
    /// [`RedisServer`] builder (with all uniform settings already applied) and
    /// metadata about the node's position in the cluster. It must return the
    /// (possibly modified) `RedisServer` builder.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_server_wrapper::RedisCluster;
    ///
    /// # async fn example() {
    /// let cluster = RedisCluster::builder()
    ///     .masters(3)
    ///     .replicas_per_master(1)
    ///     .base_port(7000)
    ///     .with_node_config(|ctx| {
    ///         let is_replica = ctx.is_replica();
    ///         let index = ctx.index;
    ///         let mut server = ctx.server;
    ///         if is_replica {
    ///             server = server.cluster_replica_no_failover(true);
    ///         }
    ///         if index == 0 {
    ///             server = server.maxmemory("512mb");
    ///         }
    ///         server
    ///     })
    ///     .start()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn with_node_config(
        mut self,
        f: impl FnMut(NodeContext) -> RedisServer + Send + 'static,
    ) -> Self {
        self.node_config_fn = Some(Box::new(f));
        self
    }

    /// Set an arbitrary config directive for all cluster nodes.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    /// Set a custom `redis-server` binary path.
    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.redis_server_bin = bin.into();
        self
    }

    /// Set a custom `redis-cli` binary path.
    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.redis_cli_bin = bin.into();
        self
    }

    fn total_nodes(&self) -> u16 {
        self.masters * (1 + self.replicas_per_master)
    }

    fn ports(&self) -> impl Iterator<Item = u16> {
        let base = self.base_port;
        let total = self.total_nodes();
        (0..total).map(move |i| base + i)
    }

    /// Start all nodes and form the cluster.
    pub async fn start(mut self) -> Result<RedisClusterHandle> {
        // Stop any leftover nodes from previous runs.
        for port in self.ports() {
            let mut cli = RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port);
            if let Some(ref password) = self.password {
                cli = cli.password(password);
            }
            cli.shutdown();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start each node.
        let total_nodes = self.total_nodes();
        let ports: Vec<u16> = self.ports().collect();
        let mut nodes = Vec::new();
        for (index, port) in ports.into_iter().enumerate() {
            let node_dir = std::env::temp_dir().join(format!("redis-cluster-wrapper/node-{port}"));
            let _ = std::fs::remove_dir_all(&node_dir);
            let mut server = RedisServer::new()
                .port(port)
                .bind(&self.bind)
                .dir(node_dir)
                .cluster_enabled(true)
                .cluster_node_timeout(self.cluster_node_timeout.unwrap_or(5000))
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin);
            if let Some(v) = self.cluster_require_full_coverage {
                server = server.cluster_require_full_coverage(v);
            }
            if let Some(v) = self.cluster_allow_reads_when_down {
                server = server.cluster_allow_reads_when_down(v);
            }
            if let Some(v) = self.cluster_allow_pubsubshard_when_down {
                server = server.cluster_allow_pubsubshard_when_down(v);
            }
            if let Some(v) = self.cluster_allow_replica_migration {
                server = server.cluster_allow_replica_migration(v);
            }
            if let Some(barrier) = self.cluster_migration_barrier {
                server = server.cluster_migration_barrier(barrier);
            }
            if let Some(ref hostname) = self.cluster_announce_hostname {
                server = server.cluster_announce_hostname(hostname.clone());
            }
            if let Some(ref endpoint_type) = self.cluster_preferred_endpoint_type {
                server = server.cluster_preferred_endpoint_type(endpoint_type.clone());
            }
            if let Some(v) = self.cluster_replica_no_failover {
                server = server.cluster_replica_no_failover(v);
            }
            if let Some(factor) = self.cluster_replica_validity_factor {
                server = server.cluster_replica_validity_factor(factor);
            }
            if let Some(ref ip) = self.cluster_announce_ip {
                server = server.cluster_announce_ip(ip.clone());
            }
            if let Some(port) = self.cluster_announce_port {
                server = server.cluster_announce_port(port);
            }
            if let Some(port) = self.cluster_announce_bus_port {
                server = server.cluster_announce_bus_port(port);
            }
            if let Some(port) = self.cluster_announce_tls_port {
                server = server.cluster_announce_tls_port(port);
            }
            if let Some(ref name) = self.cluster_announce_human_nodename {
                server = server.cluster_announce_human_nodename(name.clone());
            }
            if let Some(port) = self.cluster_port {
                server = server.cluster_port(port);
            }
            if let Some(limit) = self.cluster_link_sendbuf_limit {
                server = server.cluster_link_sendbuf_limit(limit);
            }
            if let Some(ratio) = self.cluster_compatibility_sample_ratio {
                server = server.cluster_compatibility_sample_ratio(ratio);
            }
            if let Some(bytes) = self.cluster_slot_migration_handoff_max_lag_bytes {
                server = server.cluster_slot_migration_handoff_max_lag_bytes(bytes);
            }
            if let Some(ms) = self.cluster_slot_migration_write_pause_timeout {
                server = server.cluster_slot_migration_write_pause_timeout(ms);
            }
            if let Some(v) = self.cluster_slot_stats_enabled {
                server = server.cluster_slot_stats_enabled(v);
            }
            if let Some(n) = self.min_replicas_to_write {
                server = server.min_replicas_to_write(n);
            }
            if let Some(seconds) = self.min_replicas_max_lag {
                server = server.min_replicas_max_lag(seconds);
            }
            if let Some(v) = self.repl_diskless_sync {
                server = server.repl_diskless_sync(v);
            }
            if let Some(seconds) = self.repl_diskless_sync_delay {
                server = server.repl_diskless_sync_delay(seconds);
            }
            if let Some(seconds) = self.repl_ping_replica_period {
                server = server.repl_ping_replica_period(seconds);
            }
            if let Some(seconds) = self.repl_timeout {
                server = server.repl_timeout(seconds);
            }
            if let Some(ref password) = self.password {
                server = server.password(password).masterauth(password);
            }
            if let Some(ref logfile) = self.logfile {
                server = server.logfile(logfile.clone());
            }
            if let Some(ref save) = self.save {
                match save {
                    SavePolicy::Disabled => server = server.save(false),
                    SavePolicy::Default => server = server.save(true),
                    SavePolicy::Custom(pairs) => {
                        server = server.save_schedule(pairs.clone());
                    }
                }
            }
            if let Some(appendonly) = self.appendonly {
                server = server.appendonly(appendonly);
            }
            for (key, value) in &self.extra {
                server = server.extra(key.clone(), value.clone());
            }
            // Apply per-node customization if configured.
            if let Some(ref mut f) = self.node_config_fn {
                server = f(NodeContext {
                    server,
                    index,
                    port,
                    total_nodes,
                    masters: self.masters,
                    replicas_per_master: self.replicas_per_master,
                });
            }
            let handle = server.start().await?;
            nodes.push(handle);
        }

        // Form the cluster.
        let node_addrs: Vec<String> = nodes.iter().map(|n| n.addr()).collect();
        let mut cli = RedisCli::new()
            .bin(&self.redis_cli_bin)
            .host(&self.bind)
            .port(self.base_port);
        if let Some(ref password) = self.password {
            cli = cli.password(password);
        }
        cli.cluster_create(&node_addrs, self.replicas_per_master)
            .await?;

        // Wait for convergence.
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(RedisClusterHandle {
            nodes,
            bind: self.bind,
            base_port: self.base_port,
            password: self.password,
            redis_cli_bin: self.redis_cli_bin,
        })
    }
}

/// A running Redis Cluster. Stops all nodes on Drop.
pub struct RedisClusterHandle {
    nodes: Vec<RedisServerHandle>,
    bind: String,
    base_port: u16,
    password: Option<String>,
    redis_cli_bin: String,
}

/// Entry point for building a Redis Cluster topology.
///
/// Call [`RedisCluster::builder`] to obtain a [`RedisClusterBuilder`], then
/// configure it and call [`RedisClusterBuilder::start`] to launch the cluster.
pub struct RedisCluster;

impl RedisCluster {
    /// Create a new cluster builder with defaults (3 masters, 0 replicas, port 7000).
    pub fn builder() -> RedisClusterBuilder {
        RedisClusterBuilder {
            masters: 3,
            replicas_per_master: 0,
            base_port: 7000,
            bind: "127.0.0.1".into(),
            password: None,
            logfile: None,
            save: None,
            appendonly: None,
            cluster_node_timeout: None,
            cluster_require_full_coverage: None,
            cluster_allow_reads_when_down: None,
            cluster_allow_pubsubshard_when_down: None,
            cluster_allow_replica_migration: None,
            cluster_migration_barrier: None,
            cluster_announce_hostname: None,
            cluster_announce_human_nodename: None,
            cluster_preferred_endpoint_type: None,
            cluster_replica_no_failover: None,
            cluster_replica_validity_factor: None,
            cluster_announce_ip: None,
            cluster_announce_port: None,
            cluster_announce_bus_port: None,
            cluster_announce_tls_port: None,
            cluster_port: None,
            cluster_link_sendbuf_limit: None,
            cluster_compatibility_sample_ratio: None,
            cluster_slot_migration_handoff_max_lag_bytes: None,
            cluster_slot_migration_write_pause_timeout: None,
            cluster_slot_stats_enabled: None,
            min_replicas_to_write: None,
            min_replicas_max_lag: None,
            repl_diskless_sync: None,
            repl_diskless_sync_delay: None,
            repl_ping_replica_period: None,
            repl_timeout: None,
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            node_config_fn: None,
        }
    }
}

impl RedisClusterHandle {
    /// The seed address (first node).
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind, self.base_port)
    }

    /// All node addresses.
    pub fn node_addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr()).collect()
    }

    /// The PIDs of all `redis-server` processes in the cluster.
    pub fn pids(&self) -> Vec<u32> {
        self.nodes.iter().map(|n| n.pid()).collect()
    }

    /// Check if all nodes are alive.
    pub async fn all_alive(&self) -> bool {
        for node in &self.nodes {
            if !node.is_alive().await {
                return false;
            }
        }
        true
    }

    /// Check CLUSTER INFO for state=ok and all slots assigned.
    pub async fn is_healthy(&self) -> bool {
        for node in &self.nodes {
            if let Ok(info) = node.run(&["CLUSTER", "INFO"]).await {
                if info.contains("cluster_state:ok") && info.contains("cluster_slots_ok:16384") {
                    return true;
                }
            }
        }
        false
    }

    /// Wait until the cluster is healthy or timeout.
    pub async fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.is_healthy().await {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    message: "cluster did not become healthy in time".into(),
                });
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Get a `RedisCli` for the seed node.
    pub fn cli(&self) -> RedisCli {
        let mut cli = RedisCli::new()
            .bin(&self.redis_cli_bin)
            .host(&self.bind)
            .port(self.base_port);
        if let Some(ref password) = self.password {
            cli = cli.password(password);
        }
        cli
    }
}

impl Drop for RedisClusterHandle {
    fn drop(&mut self) {
        // RedisServerHandle::drop() handles each node.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = RedisCluster::builder();
        assert_eq!(b.masters, 3);
        assert_eq!(b.replicas_per_master, 0);
        assert_eq!(b.base_port, 7000);
        assert_eq!(b.password, None);
        assert!(b.logfile.is_none());
        assert!(b.extra.is_empty());
        assert_eq!(b.total_nodes(), 3);
        assert!(b.cluster_node_timeout.is_none());
        assert!(b.cluster_require_full_coverage.is_none());
        assert!(b.cluster_allow_reads_when_down.is_none());
        assert!(b.cluster_allow_pubsubshard_when_down.is_none());
        assert!(b.cluster_allow_replica_migration.is_none());
        assert!(b.cluster_migration_barrier.is_none());
        assert!(b.cluster_announce_hostname.is_none());
        assert!(b.cluster_preferred_endpoint_type.is_none());
    }

    #[test]
    fn builder_with_replicas() {
        let b = RedisCluster::builder().masters(3).replicas_per_master(1);
        assert_eq!(b.total_nodes(), 6);
        let ports: Vec<u16> = b.ports().collect();
        assert_eq!(ports, vec![7000, 7001, 7002, 7003, 7004, 7005]);
    }

    #[test]
    fn builder_password() {
        let b = RedisCluster::builder().password("secret");
        assert_eq!(b.password.as_deref(), Some("secret"));
    }

    #[test]
    fn builder_cluster_directives() {
        let b = RedisCluster::builder()
            .cluster_node_timeout(10000)
            .cluster_require_full_coverage(false)
            .cluster_allow_reads_when_down(true)
            .cluster_allow_pubsubshard_when_down(true)
            .cluster_allow_replica_migration(false)
            .cluster_migration_barrier(2)
            .cluster_announce_hostname("node.example.com")
            .cluster_preferred_endpoint_type("hostname")
            .cluster_replica_no_failover(true)
            .cluster_replica_validity_factor(0)
            .cluster_announce_ip("10.0.0.1")
            .cluster_announce_port(7000)
            .cluster_announce_bus_port(17000)
            .cluster_announce_tls_port(7100)
            .cluster_announce_human_nodename("node-1")
            .cluster_port(17000)
            .cluster_link_sendbuf_limit(67108864)
            .cluster_compatibility_sample_ratio(50)
            .cluster_slot_migration_handoff_max_lag_bytes(1048576)
            .cluster_slot_migration_write_pause_timeout(5000)
            .cluster_slot_stats_enabled(true);
        assert_eq!(b.cluster_node_timeout, Some(10000));
        assert_eq!(b.cluster_require_full_coverage, Some(false));
        assert_eq!(b.cluster_allow_reads_when_down, Some(true));
        assert_eq!(b.cluster_allow_pubsubshard_when_down, Some(true));
        assert_eq!(b.cluster_allow_replica_migration, Some(false));
        assert_eq!(b.cluster_migration_barrier, Some(2));
        assert_eq!(
            b.cluster_announce_hostname.as_deref(),
            Some("node.example.com")
        );
        assert_eq!(
            b.cluster_preferred_endpoint_type.as_deref(),
            Some("hostname")
        );
        assert_eq!(b.cluster_replica_no_failover, Some(true));
        assert_eq!(b.cluster_replica_validity_factor, Some(0));
        assert_eq!(b.cluster_announce_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(b.cluster_announce_port, Some(7000));
        assert_eq!(b.cluster_announce_bus_port, Some(17000));
        assert_eq!(b.cluster_announce_tls_port, Some(7100));
        assert_eq!(b.cluster_announce_human_nodename.as_deref(), Some("node-1"));
        assert_eq!(b.cluster_port, Some(17000));
        assert_eq!(b.cluster_link_sendbuf_limit, Some(67108864));
        assert_eq!(b.cluster_compatibility_sample_ratio, Some(50));
        assert_eq!(
            b.cluster_slot_migration_handoff_max_lag_bytes,
            Some(1048576)
        );
        assert_eq!(b.cluster_slot_migration_write_pause_timeout, Some(5000));
        assert_eq!(b.cluster_slot_stats_enabled, Some(true));
    }

    #[test]
    fn builder_replication_directives() {
        let b = RedisCluster::builder()
            .min_replicas_to_write(1)
            .min_replicas_max_lag(10)
            .repl_diskless_sync(true)
            .repl_diskless_sync_delay(0)
            .repl_ping_replica_period(5)
            .repl_timeout(30);
        assert_eq!(b.min_replicas_to_write, Some(1));
        assert_eq!(b.min_replicas_max_lag, Some(10));
        assert_eq!(b.repl_diskless_sync, Some(true));
        assert_eq!(b.repl_diskless_sync_delay, Some(0));
        assert_eq!(b.repl_ping_replica_period, Some(5));
        assert_eq!(b.repl_timeout, Some(30));
    }

    #[test]
    fn builder_logfile_and_extra() {
        let b = RedisCluster::builder()
            .logfile("/tmp/cluster.log")
            .extra("maxmemory", "10mb");
        assert_eq!(b.logfile.as_deref(), Some("/tmp/cluster.log"));
        assert_eq!(b.extra.get("maxmemory").map(String::as_str), Some("10mb"));
    }
}
