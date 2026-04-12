//! Redis Sentinel topology management built on `RedisServer`.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::process::Command;

use crate::cli::RedisCli;
use crate::error::{Error, Result};
use crate::server::{RedisServer, RedisServerHandle, SavePolicy};

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
    save: Option<SavePolicy>,
    appendonly: Option<bool>,
    down_after_ms: u64,
    failover_timeout_ms: u64,
    tls_port: Option<u16>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_ca_cert_file: Option<PathBuf>,
    tls_ca_cert_dir: Option<PathBuf>,
    tls_auth_clients: Option<bool>,
    tls_replication: Option<bool>,
    extra: HashMap<String, String>,
    redis_server_bin: String,
    redis_cli_bin: String,
    monitored_masters: Vec<MonitoredMaster>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MonitoredMaster {
    name: String,
    host: String,
    port: u16,
    expected_replicas: u16,
}

impl RedisSentinelBuilder {
    /// Set the name of the monitored master (default: `"mymaster"`).
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.master_name = name.into();
        self
    }

    /// Set the master's port (default: `6390`).
    pub fn master_port(mut self, port: u16) -> Self {
        self.master_port = port;
        self
    }

    /// Set the number of replicas to start (default: `2`).
    pub fn replicas(mut self, n: u16) -> Self {
        self.num_replicas = n;
        self
    }

    /// Set the base port for replica nodes (default: `6391`).
    ///
    /// Replicas are assigned consecutive ports starting at this value.
    pub fn replica_base_port(mut self, port: u16) -> Self {
        self.replica_base_port = port;
        self
    }

    /// Set the number of sentinel processes to start (default: `3`).
    pub fn sentinels(mut self, n: u16) -> Self {
        self.num_sentinels = n;
        self
    }

    /// Set the base port for sentinel processes (default: `26389`).
    ///
    /// Sentinels are assigned consecutive ports starting at this value.
    pub fn sentinel_base_port(mut self, port: u16) -> Self {
        self.sentinel_base_port = port;
        self
    }

    /// Set the quorum count — how many sentinels must agree before a failover is triggered (default: `2`).
    pub fn quorum(mut self, q: u16) -> Self {
        self.quorum = q;
        self
    }

    /// Set the bind address for all processes in the topology (default: `"127.0.0.1"`).
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    /// Set the log file path for all processes in the topology.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.logfile = Some(path.into());
        self
    }

    /// Set the `down-after-milliseconds` threshold for all monitored masters (default: `5000`).
    ///
    /// A master is considered down after it fails to respond within this many milliseconds.
    pub fn down_after_ms(mut self, ms: u64) -> Self {
        self.down_after_ms = ms;
        self
    }

    /// Set the `failover-timeout` for all monitored masters in milliseconds (default: `10000`).
    pub fn failover_timeout_ms(mut self, ms: u64) -> Self {
        self.failover_timeout_ms = ms;
        self
    }

    /// Set the RDB save policy for all data-bearing processes in the topology.
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

    /// Set a custom RDB save schedule for all data-bearing processes in the topology.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.save = Some(SavePolicy::Custom(schedule));
        self
    }

    /// Enable or disable AOF persistence for all data-bearing processes in the topology.
    ///
    /// When not set, the builder defaults to `appendonly yes` for the master
    /// and replicas.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.appendonly = Some(appendonly);
        self
    }

    // -- TLS directives --

    /// Set the TLS listening port for the master and replica nodes.
    pub fn tls_port(mut self, port: u16) -> Self {
        self.tls_port = Some(port);
        self
    }

    /// Set the TLS certificate file path for all nodes.
    pub fn tls_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_cert_file = Some(path.into());
        self
    }

    /// Set the TLS private key file path for all nodes.
    pub fn tls_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_key_file = Some(path.into());
        self
    }

    /// Set the TLS CA certificate file path for all nodes.
    pub fn tls_ca_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_ca_cert_file = Some(path.into());
        self
    }

    /// Set the TLS CA certificate directory for all nodes.
    pub fn tls_ca_cert_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_ca_cert_dir = Some(path.into());
        self
    }

    /// Require TLS client authentication for all nodes.
    pub fn tls_auth_clients(mut self, auth: bool) -> Self {
        self.tls_auth_clients = Some(auth);
        self
    }

    /// Use TLS for replication traffic between nodes.
    pub fn tls_replication(mut self, enable: bool) -> Self {
        self.tls_replication = Some(enable);
        self
    }

    /// Set an arbitrary config directive for all processes in the topology.
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

    /// Add an additional master for the sentinels to monitor.
    ///
    /// The builder-managed topology still creates the primary master configured by
    /// [`Self::master_name`] and [`Self::master_port`]. Additional monitored
    /// masters are expected to already be running.
    pub fn monitor(mut self, name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        self.monitored_masters.push(MonitoredMaster {
            name: name.into(),
            host: host.into(),
            port,
            expected_replicas: 0,
        });
        self
    }

    /// Add an additional master and the minimum number of replicas expected for it.
    pub fn monitor_with_replicas(
        mut self,
        name: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        expected_replicas: u16,
    ) -> Self {
        self.monitored_masters.push(MonitoredMaster {
            name: name.into(),
            host: host.into(),
            port,
            expected_replicas,
        });
        self
    }

    /// Whether TLS is configured (cert + key present).
    fn has_tls(&self) -> bool {
        self.tls_cert_file.is_some() && self.tls_key_file.is_some()
    }

    /// Apply TLS flags to a CLI instance based on builder config.
    fn apply_tls_to_cli(&self, mut cli: RedisCli) -> RedisCli {
        if self.has_tls() {
            cli = cli.tls(true);
            if let Some(ref ca) = self.tls_ca_cert_file {
                cli = cli.cacert(ca);
            } else {
                cli = cli.insecure(true);
            }
            if let Some(ref cert) = self.tls_cert_file {
                cli = cli.cert(cert);
            }
            if let Some(ref key) = self.tls_key_file {
                cli = cli.key(key);
            }
        }
        cli
    }

    /// Apply TLS config to a server builder.
    fn apply_tls_to_server(&self, mut server: RedisServer) -> RedisServer {
        if let Some(port) = self.tls_port {
            server = server.tls_port(port);
        }
        if let Some(ref path) = self.tls_cert_file {
            server = server.tls_cert_file(path);
        }
        if let Some(ref path) = self.tls_key_file {
            server = server.tls_key_file(path);
        }
        if let Some(ref path) = self.tls_ca_cert_file {
            server = server.tls_ca_cert_file(path);
        }
        if let Some(ref path) = self.tls_ca_cert_dir {
            server = server.tls_ca_cert_dir(path);
        }
        if let Some(v) = self.tls_auth_clients {
            server = server.tls_auth_clients(v);
        }
        if let Some(v) = self.tls_replication {
            server = server.tls_replication(v);
        }
        server
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
        let mut monitored_masters = Vec::with_capacity(1 + self.monitored_masters.len());
        monitored_masters.push(MonitoredMaster {
            name: self.master_name.clone(),
            host: self.bind.clone(),
            port: self.master_port,
            expected_replicas: self.num_replicas,
        });
        monitored_masters.extend(self.monitored_masters.iter().cloned());

        // Kill leftover processes.
        let cli_for_shutdown = |port: u16| {
            let cli = self.apply_tls_to_cli(
                RedisCli::new()
                    .bin(&self.redis_cli_bin)
                    .host(&self.bind)
                    .port(port),
            );
            cli.shutdown();
        };
        cli_for_shutdown(self.master_port);
        for port in self.replica_ports() {
            cli_for_shutdown(port);
        }
        for port in self.sentinel_ports() {
            cli_for_shutdown(port);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let base_dir = std::env::temp_dir().join(format!(
            "redis-sentinel-wrapper-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&base_dir)?;

        // 1. Start master.
        let appendonly = self.appendonly.unwrap_or(true);
        let mut master = RedisServer::new()
            .port(self.master_port)
            .bind(&self.bind)
            .dir(base_dir.join("master"))
            .appendonly(appendonly)
            .redis_server_bin(&self.redis_server_bin)
            .redis_cli_bin(&self.redis_cli_bin);
        master = self.apply_tls_to_server(master);
        if let Some(ref logfile) = self.logfile {
            master = master.logfile(logfile.clone());
        }
        if let Some(ref save) = self.save {
            match save {
                SavePolicy::Disabled => master = master.save(false),
                SavePolicy::Default => master = master.save(true),
                SavePolicy::Custom(pairs) => master = master.save_schedule(pairs.clone()),
            }
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
                .appendonly(appendonly)
                .replicaof(self.bind.clone(), self.master_port)
                .redis_server_bin(&self.redis_server_bin)
                .redis_cli_bin(&self.redis_cli_bin);
            replica = self.apply_tls_to_server(replica);
            if let Some(ref logfile) = self.logfile {
                replica = replica.logfile(logfile.clone());
            }
            if let Some(ref save) = self.save {
                match save {
                    SavePolicy::Disabled => replica = replica.save(false),
                    SavePolicy::Default => replica = replica.save(true),
                    SavePolicy::Custom(pairs) => {
                        replica = replica.save_schedule(pairs.clone());
                    }
                }
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
                 dir {dir}\n",
                port = port,
                bind = self.bind,
                dir = dir.display(),
                logfile = logfile,
            );
            for master in &monitored_masters {
                conf.push_str(&format!(
                    "sentinel monitor {name} {host} {master_port} {quorum}\n\
                     sentinel down-after-milliseconds {name} {down_after}\n\
                     sentinel failover-timeout {name} {failover_timeout}\n\
                     sentinel parallel-syncs {name} 1\n",
                    name = master.name,
                    host = master.host,
                    master_port = master.port,
                    quorum = self.quorum,
                    down_after = self.down_after_ms,
                    failover_timeout = self.failover_timeout_ms,
                ));
            }
            // TLS directives for sentinels.
            if let Some(ref path) = self.tls_cert_file {
                conf.push_str(&format!("tls-cert-file {}\n", path.display()));
            }
            if let Some(ref path) = self.tls_key_file {
                conf.push_str(&format!("tls-key-file {}\n", path.display()));
            }
            if let Some(ref path) = self.tls_ca_cert_file {
                conf.push_str(&format!("tls-ca-cert-file {}\n", path.display()));
            }
            if let Some(ref path) = self.tls_ca_cert_dir {
                conf.push_str(&format!("tls-ca-cert-dir {}\n", path.display()));
            }
            if let Some(tls_port) = self.tls_port {
                conf.push_str(&format!("tls-port {tls_port}\n"));
            }
            if let Some(v) = self.tls_auth_clients {
                conf.push_str(&format!(
                    "tls-auth-clients {}\n",
                    if v { "yes" } else { "no" }
                ));
            }
            if let Some(v) = self.tls_replication {
                conf.push_str(&format!(
                    "tls-replication {}\n",
                    if v { "yes" } else { "no" }
                ));
            }
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

            let cli = self.apply_tls_to_cli(
                RedisCli::new()
                    .bin(&self.redis_cli_bin)
                    .host(&self.bind)
                    .port(port),
            );
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
            monitored_masters,
            tls: TlsConfig {
                cert_file: self.tls_cert_file,
                key_file: self.tls_key_file,
                ca_cert_file: self.tls_ca_cert_file,
            },
        })
    }
}

/// TLS configuration snapshot stored in the handle for building CLI instances.
#[derive(Clone, Debug, Default)]
struct TlsConfig {
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    ca_cert_file: Option<PathBuf>,
}

impl TlsConfig {
    fn has_tls(&self) -> bool {
        self.cert_file.is_some() && self.key_file.is_some()
    }

    fn apply(&self, mut cli: RedisCli) -> RedisCli {
        if self.has_tls() {
            cli = cli.tls(true);
            if let Some(ref ca) = self.ca_cert_file {
                cli = cli.cacert(ca);
            } else {
                cli = cli.insecure(true);
            }
            if let Some(ref cert) = self.cert_file {
                cli = cli.cert(cert);
            }
            if let Some(ref key) = self.key_file {
                cli = cli.key(key);
            }
        }
        cli
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
    monitored_masters: Vec<MonitoredMaster>,
    tls: TlsConfig,
}

/// Entry point for building a Redis Sentinel topology.
///
/// Call [`RedisSentinel::builder`] to obtain a [`RedisSentinelBuilder`], then
/// configure it and call [`RedisSentinelBuilder::start`] to launch the topology.
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
            save: None,
            appendonly: None,
            down_after_ms: 5000,
            failover_timeout_ms: 10000,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_cert_file: None,
            tls_ca_cert_dir: None,
            tls_auth_clients: None,
            tls_replication: None,
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
            monitored_masters: Vec::new(),
        }
    }
}

impl RedisSentinelHandle {
    /// The master's address.
    pub fn master_addr(&self) -> String {
        self.master.addr()
    }

    /// All monitored master names.
    pub fn monitored_master_names(&self) -> Vec<&str> {
        self.monitored_masters
            .iter()
            .map(|master| master.name.as_str())
            .collect()
    }

    /// All monitored master addresses.
    pub fn monitored_master_addrs(&self) -> Vec<String> {
        self.monitored_masters
            .iter()
            .map(|master| format!("{}:{}", master.host, master.port))
            .collect()
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

    /// Query a sentinel for the primary monitored master's status.
    ///
    /// Iterates over the sentinel processes until one responds, then runs
    /// `SENTINEL MASTER <name>` and returns the result as a flat key/value map.
    ///
    /// Common keys in the returned map include `"ip"`, `"port"`, `"flags"`,
    /// `"num-slaves"`, and `"num-other-sentinels"`.
    ///
    /// Returns [`Error::NoReachableSentinel`] if no sentinel responds.
    pub async fn poke(&self) -> Result<HashMap<String, String>> {
        self.poke_master(&self.master_name).await
    }

    /// Query a sentinel for a specific monitored master's status.
    ///
    /// Like [`poke`](Self::poke) but targets `master_name` instead of the
    /// primary master configured for this topology.
    pub async fn poke_master(&self, master_name: &str) -> Result<HashMap<String, String>> {
        for port in &self.sentinel_ports {
            let cli = self.tls.apply(
                RedisCli::new()
                    .bin(&self.redis_cli_bin)
                    .host(&self.bind)
                    .port(*port),
            );
            if let Ok(raw) = cli.run(&["SENTINEL", "MASTER", master_name]).await {
                return Ok(parse_flat_kv(&raw));
            }
        }
        Err(Error::NoReachableSentinel)
    }

    /// Check if the topology is healthy.
    pub async fn is_healthy(&self) -> bool {
        for master in &self.monitored_masters {
            let Ok(info) = self.poke_master(&master.name).await else {
                return false;
            };
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
            if flags != "master"
                || num_slaves < master.expected_replicas as u64
                || num_sentinels < self.num_sentinels as u64
            {
                return false;
            }
        }
        true
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

    /// Stop everything via an escalating shutdown strategy.
    ///
    /// 1. Sends `SHUTDOWN NOSAVE` to each sentinel process.
    /// 2. Waits 500ms for them to exit.
    /// 3. For each sentinel PID that is still alive, calls [`crate::process::force_kill`].
    /// 4. Calls [`crate::process::kill_by_port`] for each sentinel port as a safety net.
    ///
    /// Replicas and the master are stopped by their own handles' [`Drop`] impls,
    /// which also use the escalating strategy.
    pub fn stop(&self) {
        // Step 1: graceful shutdown for each sentinel.
        for port in &self.sentinel_ports {
            self.tls
                .apply(
                    RedisCli::new()
                        .bin(&self.redis_cli_bin)
                        .host(&self.bind)
                        .port(*port),
                )
                .shutdown();
        }
        // Step 2: grace period.
        std::thread::sleep(std::time::Duration::from_millis(500));
        // Step 3: force kill any sentinels still alive.
        for pid in &self.sentinel_pids {
            if crate::process::pid_alive(*pid) {
                crate::process::force_kill(*pid);
            }
        }
        // Step 4: port cleanup as safety net.
        for port in &self.sentinel_ports {
            crate::process::kill_by_port(*port);
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
        assert!(b.monitored_masters.is_empty());
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
            .extra("maxmemory", "10mb")
            .monitor("backup", "127.0.0.1", 6501);
        assert_eq!(b.master_name, "custom");
        assert_eq!(b.master_port, 6500);
        assert_eq!(b.num_replicas, 1);
        assert_eq!(b.num_sentinels, 5);
        assert_eq!(b.quorum, 3);
        assert_eq!(b.logfile.as_deref(), Some("/tmp/sentinel.log"));
        assert_eq!(b.extra.get("maxmemory").map(String::as_str), Some("10mb"));
        assert_eq!(b.monitored_masters.len(), 1);
        assert_eq!(
            b.monitored_masters[0],
            MonitoredMaster {
                name: "backup".into(),
                host: "127.0.0.1".into(),
                port: 6501,
                expected_replicas: 0,
            }
        );
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
