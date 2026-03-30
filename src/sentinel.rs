//! Redis Sentinel topology management built on `RedisServer`.

use std::collections::HashMap;
use std::fs;
use std::time::Duration;

use tokio::process::Command;

use crate::cli::RedisCli;
use crate::error::{Error, Result};
use crate::server::{RedisServer, RedisServerHandle};

/// Builder for a Redis Sentinel topology.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::RedisSentinel;
///
/// # async fn example() {
/// let sentinel = RedisSentinel::builder()
///     .master_name("mymaster")
///     .master_port(6390)
///     .replicas(2)
///     .sentinels(3)
///     .start()
///     .await
///     .unwrap();
///
/// assert!(sentinel.is_healthy().await);
/// # }
/// ```
pub struct RedisSentinelBuilder {
    master_name: String,
    master_port: u16,
    num_replicas: u16,
    replica_base_port: u16,
    num_sentinels: u16,
    sentinel_base_port: u16,
    quorum: u16,
    bind: String,
    logfile: Option<String>,
    down_after_ms: u64,
    failover_timeout_ms: u64,
    extra: HashMap<String, String>,
    redis_server_bin: String,
    redis_cli_bin: String,
}

impl RedisSentinelBuilder {
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.master_name = name.into();
        self
    }

    pub fn master_port(mut self, port: u16) -> Self {
        self.master_port = port;
        self
    }

    pub fn replicas(mut self, n: u16) -> Self {
        self.num_replicas = n;
        self
    }

    pub fn replica_base_port(mut self, port: u16) -> Self {
        self.replica_base_port = port;
        self
    }

    pub fn sentinels(mut self, n: u16) -> Self {
        self.num_sentinels = n;
        self
    }

    pub fn sentinel_base_port(mut self, port: u16) -> Self {
        self.sentinel_base_port = port;
        self
    }

    pub fn quorum(mut self, q: u16) -> Self {
        self.quorum = q;
        self
    }

    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.logfile = Some(path.into());
        self
    }

    pub fn down_after_ms(mut self, ms: u64) -> Self {
        self.down_after_ms = ms;
        self
    }

    pub fn failover_timeout_ms(mut self, ms: u64) -> Self {
        self.failover_timeout_ms = ms;
        self
    }

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

    fn replica_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.replica_base_port;
        let n = self.num_replicas;
        (0..n).map(move |i| base + i)
    }

    fn sentinel_ports(&self) -> impl Iterator<Item = u16> {
        let base = self.sentinel_base_port;
        let n = self.num_sentinels;
        (0..n).map(move |i| base + i)
    }

    /// Start the full topology: master, replicas, sentinels.
    pub async fn start(self) -> Result<RedisSentinelHandle> {
        // Kill leftover processes.
        let cli_for_shutdown = |port: u16| {
            RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port)
                .shutdown();
        };
        cli_for_shutdown(self.master_port);
        for port in self.replica_ports() {
            cli_for_shutdown(port);
        }
        for port in self.sentinel_ports() {
            cli_for_shutdown(port);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let base_dir = std::env::temp_dir().join("redis-sentinel-wrapper");
        if base_dir.exists() {
            let _ = fs::remove_dir_all(&base_dir);
        }

        // 1. Start master.
        let mut master = RedisServer::new()
            .port(self.master_port)
            .bind(&self.bind)
            .dir(base_dir.join("master"))
            .appendonly(true)
            .redis_server_bin(&self.redis_server_bin)
            .redis_cli_bin(&self.redis_cli_bin);
        if let Some(ref logfile) = self.logfile {
            master = master.logfile(logfile.clone());
        }
        for (key, value) in &self.extra {
            master = master.extra(key.clone(), value.clone());
        }
        let master = master.start().await?;

        // 2. Start replicas.
        let mut replicas = Vec::new();
        for port in self.replica_ports() {
            let mut replica = RedisServer::new()
                .port(port)
                .bind(&self.bind)
                .dir(base_dir.join(format!("replica-{port}")))
                .appendonly(true)
                .replicaof(self.bind.clone(), self.master_port)
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin);
            if let Some(ref logfile) = self.logfile {
                replica = replica.logfile(logfile.clone());
            }
            for (key, value) in &self.extra {
                replica = replica.extra(key.clone(), value.clone());
            }
            let replica = replica.start().await?;
            replicas.push(replica);
        }

        // Let replication link up.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 3. Start sentinels.
        let mut sentinel_handles = Vec::new();
        for port in self.sentinel_ports() {
            let dir = base_dir.join(format!("sentinel-{port}"));
            fs::create_dir_all(&dir)?;
            let conf_path = dir.join("sentinel.conf");
            let logfile = self
                .logfile
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{}/sentinel.log", dir.display()));
            let mut conf = format!(
                "port {port}\n\
                 bind {bind}\n\
                 daemonize yes\n\
                 pidfile {dir}/sentinel.pid\n\
                 logfile {logfile}\n\
                 dir {dir}\n\
                 sentinel monitor {name} {master_host} {master_port} {quorum}\n\
                 sentinel down-after-milliseconds {name} {down_after}\n\
                 sentinel failover-timeout {name} {failover_timeout}\n\
                 sentinel parallel-syncs {name} 1\n",
                port = port,
                bind = self.bind,
                dir = dir.display(),
                logfile = logfile,
                name = self.master_name,
                master_host = self.bind,
                master_port = self.master_port,
                quorum = self.quorum,
                down_after = self.down_after_ms,
                failover_timeout = self.failover_timeout_ms,
            );
            for (key, value) in &self.extra {
                conf.push_str(&format!("{key} {value}\n"));
            }
            fs::write(&conf_path, conf)?;

            let status = Command::new(&self.redis_server_bin)
                .arg(&conf_path)
                .arg("--sentinel")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await?;

            if !status.success() {
                return Err(Error::SentinelStart { port });
            }

            let cli = RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(port);
            cli.wait_for_ready(Duration::from_secs(10)).await?;

            let pid_path = dir.join("sentinel.pid");
            let pid: u32 = fs::read_to_string(&pid_path)?
                .trim()
                .parse()
                .map_err(|_| Error::SentinelStart { port })?;

            sentinel_handles.push((port, pid, cli));
        }

        // Wait for sentinels to discover each other.
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(RedisSentinelHandle {
            master,
            replicas,
            sentinel_ports: sentinel_handles.iter().map(|(p, _, _)| *p).collect(),
            sentinel_pids: sentinel_handles.iter().map(|(_, pid, _)| *pid).collect(),
            master_name: self.master_name,
            bind: self.bind,
            redis_cli_bin: self.redis_cli_bin,
            num_sentinels: self.num_sentinels,
            num_replicas: self.num_replicas,
        })
    }
}

/// A running Redis Sentinel topology. Stops everything on Drop.
pub struct RedisSentinelHandle {
    master: RedisServerHandle,
    #[allow(dead_code)] // Kept alive for Drop cleanup
    replicas: Vec<RedisServerHandle>,
    sentinel_ports: Vec<u16>,
    sentinel_pids: Vec<u32>,
    master_name: String,
    bind: String,
    redis_cli_bin: String,
    num_sentinels: u16,
    num_replicas: u16,
}

/// Convenience constructor.
pub struct RedisSentinel;

impl RedisSentinel {
    /// Create a new sentinel builder with defaults.
    pub fn builder() -> RedisSentinelBuilder {
        RedisSentinelBuilder {
            master_name: "mymaster".into(),
            master_port: 6390,
            num_replicas: 2,
            replica_base_port: 6391,
            num_sentinels: 3,
            sentinel_base_port: 26389,
            quorum: 2,
            bind: "127.0.0.1".into(),
            logfile: None,
            down_after_ms: 5000,
            failover_timeout_ms: 10000,
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

impl RedisSentinelHandle {
    /// The master's address.
    pub fn master_addr(&self) -> String {
        self.master.addr()
    }

    /// The PIDs of all processes in the topology (master, replicas, sentinels).
    pub fn pids(&self) -> Vec<u32> {
        let mut pids = Vec::with_capacity(1 + self.replicas.len() + self.sentinel_pids.len());
        pids.push(self.master.pid());
        for replica in &self.replicas {
            pids.push(replica.pid());
        }
        pids.extend_from_slice(&self.sentinel_pids);
        pids
    }

    /// All sentinel addresses.
    pub fn sentinel_addrs(&self) -> Vec<String> {
        self.sentinel_ports
            .iter()
            .map(|p| format!("{}:{}", self.bind, p))
            .collect()
    }

    /// The monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Query a sentinel for the current master status.
    pub async fn poke(&self) -> Result<HashMap<String, String>> {
        for port in &self.sentinel_ports {
            let cli = RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(*port);
            if let Ok(raw) = cli.run(&["SENTINEL", "MASTER", &self.master_name]).await {
                return Ok(parse_flat_kv(&raw));
            }
        }
        Err(Error::NoReachableSentinel)
    }

    /// Check if the topology is healthy.
    pub async fn is_healthy(&self) -> bool {
        if let Ok(info) = self.poke().await {
            let flags = info.get("flags").map(|s| s.as_str()).unwrap_or("");
            let num_slaves: u64 = info
                .get("num-slaves")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let num_sentinels: u64 = info
                .get("num-other-sentinels")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
                + 1;
            flags == "master"
                && num_slaves >= self.num_replicas as u64
                && num_sentinels >= self.num_sentinels as u64
        } else {
            false
        }
    }

    /// Wait until the topology is healthy or timeout.
    pub async fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.is_healthy().await {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    message: "sentinel topology did not become healthy in time".into(),
                });
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Stop everything.
    pub fn stop(&self) {
        // Sentinels first.
        for port in &self.sentinel_ports {
            RedisCli::new()
                .bin(&self.redis_cli_bin)
                .host(&self.bind)
                .port(*port)
                .shutdown();
        }
        // Replicas and master stopped by their handles' Drop.
    }
}

impl Drop for RedisSentinelHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Parse alternating key/value lines from sentinel output.
fn parse_flat_kv(raw: &str) -> HashMap<String, String> {
    let lines: Vec<&str> = raw.lines().map(|l| l.trim()).collect();
    let mut map = HashMap::new();
    let mut i = 0;
    while i + 1 < lines.len() {
        map.insert(lines[i].to_string(), lines[i + 1].to_string());
        i += 2;
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = RedisSentinel::builder();
        assert_eq!(b.master_port, 6390);
        assert_eq!(b.num_replicas, 2);
        assert_eq!(b.num_sentinels, 3);
        assert_eq!(b.quorum, 2);
        assert!(b.logfile.is_none());
        assert!(b.extra.is_empty());
    }

    #[test]
    fn builder_chain() {
        let b = RedisSentinel::builder()
            .master_name("custom")
            .master_port(6500)
            .replicas(1)
            .sentinels(5)
            .quorum(3)
            .logfile("/tmp/sentinel.log")
            .extra("maxmemory", "10mb");
        assert_eq!(b.master_name, "custom");
        assert_eq!(b.master_port, 6500);
        assert_eq!(b.num_replicas, 1);
        assert_eq!(b.num_sentinels, 5);
        assert_eq!(b.quorum, 3);
        assert_eq!(b.logfile.as_deref(), Some("/tmp/sentinel.log"));
        assert_eq!(b.extra.get("maxmemory").map(String::as_str), Some("10mb"));
    }

    #[test]
    fn parse_sentinel_output() {
        let raw = "name\nmymaster\nip\n127.0.0.1\nport\n6380\n";
        let map = parse_flat_kv(raw);
        assert_eq!(map.get("name").unwrap(), "mymaster");
        assert_eq!(map.get("ip").unwrap(), "127.0.0.1");
        assert_eq!(map.get("port").unwrap(), "6380");
    }
}
