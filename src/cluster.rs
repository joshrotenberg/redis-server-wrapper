//! Redis Cluster lifecycle management built on `RedisServer`.

use std::collections::HashMap;
use std::time::Duration;

use crate::cli::RedisCli;
use crate::error::{Error, Result};
use crate::server::{RedisServer, RedisServerHandle};

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
    extra: HashMap<String, String>,
    redis_server_bin: String,
    redis_cli_bin: String,
}

impl RedisClusterBuilder {
    pub fn masters(mut self, n: u16) -> Self {
        self.masters = n;
        self
    }

    pub fn replicas_per_master(mut self, n: u16) -> Self {
        self.replicas_per_master = n;
        self
    }

    pub fn base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

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

    /// Set an arbitrary config directive for all cluster nodes.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.redis_server_bin = bin.into();
        self
    }

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
    pub async fn start(self) -> Result<RedisClusterHandle> {
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
        let mut nodes = Vec::new();
        for port in self.ports() {
            let node_dir = std::env::temp_dir().join(format!("redis-cluster-wrapper/node-{port}"));
            let _ = std::fs::remove_dir_all(&node_dir);
            let mut server = RedisServer::new()
                .port(port)
                .bind(&self.bind)
                .dir(node_dir)
                .cluster_enabled(true)
                .cluster_node_timeout(5000)
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin);
            if let Some(ref password) = self.password {
                server = server.password(password).masterauth(password);
            }
            if let Some(ref logfile) = self.logfile {
                server = server.logfile(logfile.clone());
            }
            for (key, value) in &self.extra {
                server = server.extra(key.clone(), value.clone());
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

/// Convenience constructor.
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
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
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
    fn builder_logfile_and_extra() {
        let b = RedisCluster::builder()
            .logfile("/tmp/cluster.log")
            .extra("maxmemory", "10mb");
        assert_eq!(b.logfile.as_deref(), Some("/tmp/cluster.log"));
        assert_eq!(b.extra.get("maxmemory").map(String::as_str), Some("10mb"));
    }
}
