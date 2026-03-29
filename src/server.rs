//! Type-safe wrapper for `redis-server` with builder pattern.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::cli::RedisCli;
use crate::error::{Error, Result};

/// Builder and lifecycle manager for a single `redis-server` process.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::RedisServer;
///
/// let server = RedisServer::new()
///     .port(6400)
///     .bind("127.0.0.1")
///     .save(false)
///     .start()
///     .unwrap();
///
/// assert!(server.is_alive());
/// // Stopped automatically on Drop.
/// ```
#[derive(Debug, Clone)]
pub struct RedisServerConfig {
    // -- network --
    pub port: u16,
    pub bind: String,
    pub protected_mode: bool,
    pub tcp_backlog: Option<u32>,
    pub unixsocket: Option<PathBuf>,
    pub unixsocketperm: Option<u32>,
    pub timeout: Option<u32>,
    pub tcp_keepalive: Option<u32>,

    // -- tls --
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub tls_ca_cert_file: Option<PathBuf>,
    pub tls_auth_clients: Option<bool>,

    // -- general --
    pub daemonize: bool,
    pub dir: PathBuf,
    pub loglevel: LogLevel,
    pub databases: Option<u32>,

    // -- memory --
    pub maxmemory: Option<String>,
    pub maxmemory_policy: Option<String>,
    pub maxclients: Option<u32>,

    // -- persistence --
    pub save: bool,
    pub appendonly: bool,

    // -- replication --
    pub replicaof: Option<(String, u16)>,
    pub masterauth: Option<String>,

    // -- security --
    pub password: Option<String>,
    pub acl_file: Option<PathBuf>,

    // -- cluster --
    pub cluster_enabled: bool,
    pub cluster_node_timeout: Option<u64>,

    // -- modules --
    pub loadmodule: Vec<PathBuf>,

    // -- advanced --
    pub hz: Option<u32>,
    pub io_threads: Option<u32>,
    pub io_threads_do_reads: Option<bool>,
    pub notify_keyspace_events: Option<String>,

    // -- catch-all for anything not covered above --
    pub extra: HashMap<String, String>,

    // -- binary paths --
    pub redis_server_bin: String,
    pub redis_cli_bin: String,
}

/// Redis log level.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => f.write_str("debug"),
            LogLevel::Verbose => f.write_str("verbose"),
            LogLevel::Notice => f.write_str("notice"),
            LogLevel::Warning => f.write_str("warning"),
        }
    }
}

impl Default for RedisServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            bind: "127.0.0.1".into(),
            protected_mode: false,
            tcp_backlog: None,
            unixsocket: None,
            unixsocketperm: None,
            timeout: None,
            tcp_keepalive: None,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_cert_file: None,
            tls_auth_clients: None,
            daemonize: true,
            dir: std::env::temp_dir().join("redis-server-wrapper"),
            loglevel: LogLevel::Notice,
            databases: None,
            maxmemory: None,
            maxmemory_policy: None,
            maxclients: None,
            save: false,
            appendonly: false,
            replicaof: None,
            masterauth: None,
            password: None,
            acl_file: None,
            cluster_enabled: false,
            cluster_node_timeout: None,
            loadmodule: Vec::new(),
            hz: None,
            io_threads: None,
            io_threads_do_reads: None,
            notify_keyspace_events: None,
            extra: HashMap::new(),
            redis_server_bin: "redis-server".into(),
            redis_cli_bin: "redis-cli".into(),
        }
    }
}

/// Builder for a Redis server.
pub struct RedisServer {
    config: RedisServerConfig,
}

impl RedisServer {
    pub fn new() -> Self {
        Self {
            config: RedisServerConfig::default(),
        }
    }

    // -- network --

    /// Set the listening port (default: 6379).
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Set the bind address (default: `127.0.0.1`).
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.config.bind = bind.into();
        self
    }

    /// Enable or disable protected mode (default: off).
    pub fn protected_mode(mut self, protected: bool) -> Self {
        self.config.protected_mode = protected;
        self
    }

    /// Set the TCP backlog queue length.
    pub fn tcp_backlog(mut self, backlog: u32) -> Self {
        self.config.tcp_backlog = Some(backlog);
        self
    }

    /// Set a Unix socket path for connections.
    pub fn unixsocket(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.unixsocket = Some(path.into());
        self
    }

    /// Set Unix socket permissions (e.g. `700`).
    pub fn unixsocketperm(mut self, perm: u32) -> Self {
        self.config.unixsocketperm = Some(perm);
        self
    }

    /// Close idle client connections after this many seconds (0 = disabled).
    pub fn timeout(mut self, seconds: u32) -> Self {
        self.config.timeout = Some(seconds);
        self
    }

    /// Set TCP keepalive interval in seconds.
    pub fn tcp_keepalive(mut self, seconds: u32) -> Self {
        self.config.tcp_keepalive = Some(seconds);
        self
    }

    // -- tls --

    /// Set TLS listening port.
    pub fn tls_port(mut self, port: u16) -> Self {
        self.config.tls_port = Some(port);
        self
    }

    /// Set the TLS certificate file path.
    pub fn tls_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_cert_file = Some(path.into());
        self
    }

    /// Set the TLS private key file path.
    pub fn tls_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_key_file = Some(path.into());
        self
    }

    /// Set the TLS CA certificate file path.
    pub fn tls_ca_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_ca_cert_file = Some(path.into());
        self
    }

    /// Require TLS client authentication.
    pub fn tls_auth_clients(mut self, require: bool) -> Self {
        self.config.tls_auth_clients = Some(require);
        self
    }

    // -- general --

    /// Set the working directory for data files.
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.dir = dir.into();
        self
    }

    /// Set the log level (default: [`LogLevel::Notice`]).
    pub fn loglevel(mut self, level: LogLevel) -> Self {
        self.config.loglevel = level;
        self
    }

    /// Set the number of databases (default: 16).
    pub fn databases(mut self, n: u32) -> Self {
        self.config.databases = Some(n);
        self
    }

    // -- memory --

    /// Set the maximum memory limit (e.g. `"256mb"`, `"1gb"`).
    pub fn maxmemory(mut self, limit: impl Into<String>) -> Self {
        self.config.maxmemory = Some(limit.into());
        self
    }

    /// Set the eviction policy when maxmemory is reached.
    pub fn maxmemory_policy(mut self, policy: impl Into<String>) -> Self {
        self.config.maxmemory_policy = Some(policy.into());
        self
    }

    /// Set the maximum number of simultaneous client connections.
    pub fn maxclients(mut self, n: u32) -> Self {
        self.config.maxclients = Some(n);
        self
    }

    // -- persistence --

    /// Enable or disable RDB snapshots (default: off).
    pub fn save(mut self, save: bool) -> Self {
        self.config.save = save;
        self
    }

    /// Enable or disable AOF persistence.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.config.appendonly = appendonly;
        self
    }

    // -- replication --

    /// Configure this server as a replica of the given master.
    pub fn replicaof(mut self, host: impl Into<String>, port: u16) -> Self {
        self.config.replicaof = Some((host.into(), port));
        self
    }

    /// Set the password for authenticating with a master.
    pub fn masterauth(mut self, password: impl Into<String>) -> Self {
        self.config.masterauth = Some(password.into());
        self
    }

    // -- security --

    /// Set a `requirepass` password for client connections.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = Some(password.into());
        self
    }

    /// Set the path to an ACL file.
    pub fn acl_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.acl_file = Some(path.into());
        self
    }

    // -- cluster --

    /// Enable Redis Cluster mode.
    pub fn cluster_enabled(mut self, enabled: bool) -> Self {
        self.config.cluster_enabled = enabled;
        self
    }

    /// Set the cluster node timeout in milliseconds.
    pub fn cluster_node_timeout(mut self, ms: u64) -> Self {
        self.config.cluster_node_timeout = Some(ms);
        self
    }

    // -- modules --

    /// Load a Redis module at startup.
    pub fn loadmodule(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.loadmodule.push(path.into());
        self
    }

    // -- advanced --

    /// Set the server tick frequency in Hz (default: 10).
    pub fn hz(mut self, hz: u32) -> Self {
        self.config.hz = Some(hz);
        self
    }

    /// Set the number of I/O threads.
    pub fn io_threads(mut self, n: u32) -> Self {
        self.config.io_threads = Some(n);
        self
    }

    /// Enable I/O threads for reads as well as writes.
    pub fn io_threads_do_reads(mut self, enable: bool) -> Self {
        self.config.io_threads_do_reads = Some(enable);
        self
    }

    /// Set keyspace notification events (e.g. `"KEA"`).
    pub fn notify_keyspace_events(mut self, events: impl Into<String>) -> Self {
        self.config.notify_keyspace_events = Some(events.into());
        self
    }

    // -- binary paths --

    /// Set a custom `redis-server` binary path.
    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.config.redis_server_bin = bin.into();
        self
    }

    /// Set a custom `redis-cli` binary path.
    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.config.redis_cli_bin = bin.into();
        self
    }

    /// Set an arbitrary config directive not covered by dedicated methods.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.extra.insert(key.into(), value.into());
        self
    }

    /// Start the server. Returns a handle that stops the server on Drop.
    ///
    /// Verifies that `redis-server` and `redis-cli` binaries are available
    /// before attempting to launch anything.
    pub fn start(self) -> Result<RedisServerHandle> {
        if which::which(&self.config.redis_server_bin).is_err() {
            return Err(Error::BinaryNotFound {
                binary: self.config.redis_server_bin.clone(),
            });
        }
        if which::which(&self.config.redis_cli_bin).is_err() {
            return Err(Error::BinaryNotFound {
                binary: self.config.redis_cli_bin.clone(),
            });
        }

        let node_dir = self.config.dir.join(format!("node-{}", self.config.port));
        fs::create_dir_all(&node_dir)?;

        let conf_path = node_dir.join("redis.conf");
        let conf_content = self.generate_config(&node_dir);
        fs::write(&conf_path, conf_content)?;

        let status = Command::new(&self.config.redis_server_bin)
            .arg(&conf_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if !status.success() {
            return Err(Error::ServerStart {
                port: self.config.port,
            });
        }

        let mut cli = RedisCli::new()
            .bin(&self.config.redis_cli_bin)
            .host(&self.config.bind)
            .port(self.config.port);
        if let Some(ref pw) = self.config.password {
            cli = cli.password(pw);
        }

        cli.wait_for_ready(Duration::from_secs(10))?;

        Ok(RedisServerHandle {
            config: self.config,
            cli,
        })
    }

    fn generate_config(&self, node_dir: &std::path::Path) -> String {
        let yn = |b: bool| if b { "yes" } else { "no" };

        let mut conf = format!(
            "port {port}\n\
             bind {bind}\n\
             daemonize {daemonize}\n\
             pidfile {dir}/redis.pid\n\
             logfile {dir}/redis.log\n\
             dir {dir}\n\
             loglevel {level}\n\
             protected-mode {protected}\n",
            port = self.config.port,
            bind = self.config.bind,
            daemonize = yn(self.config.daemonize),
            dir = node_dir.display(),
            level = self.config.loglevel,
            protected = yn(self.config.protected_mode),
        );

        // -- network --
        if let Some(backlog) = self.config.tcp_backlog {
            conf.push_str(&format!("tcp-backlog {backlog}\n"));
        }
        if let Some(ref path) = self.config.unixsocket {
            conf.push_str(&format!("unixsocket {}\n", path.display()));
        }
        if let Some(perm) = self.config.unixsocketperm {
            conf.push_str(&format!("unixsocketperm {perm}\n"));
        }
        if let Some(t) = self.config.timeout {
            conf.push_str(&format!("timeout {t}\n"));
        }
        if let Some(ka) = self.config.tcp_keepalive {
            conf.push_str(&format!("tcp-keepalive {ka}\n"));
        }

        // -- tls --
        if let Some(port) = self.config.tls_port {
            conf.push_str(&format!("tls-port {port}\n"));
        }
        if let Some(ref path) = self.config.tls_cert_file {
            conf.push_str(&format!("tls-cert-file {}\n", path.display()));
        }
        if let Some(ref path) = self.config.tls_key_file {
            conf.push_str(&format!("tls-key-file {}\n", path.display()));
        }
        if let Some(ref path) = self.config.tls_ca_cert_file {
            conf.push_str(&format!("tls-ca-cert-file {}\n", path.display()));
        }
        if let Some(auth) = self.config.tls_auth_clients {
            conf.push_str(&format!("tls-auth-clients {}\n", yn(auth)));
        }

        // -- general --
        if let Some(n) = self.config.databases {
            conf.push_str(&format!("databases {n}\n"));
        }

        // -- memory --
        if let Some(ref limit) = self.config.maxmemory {
            conf.push_str(&format!("maxmemory {limit}\n"));
        }
        if let Some(ref policy) = self.config.maxmemory_policy {
            conf.push_str(&format!("maxmemory-policy {policy}\n"));
        }
        if let Some(n) = self.config.maxclients {
            conf.push_str(&format!("maxclients {n}\n"));
        }

        // -- persistence --
        if !self.config.save {
            conf.push_str("save \"\"\n");
        }
        if self.config.appendonly {
            conf.push_str("appendonly yes\n");
        }

        // -- replication --
        if let Some((ref host, port)) = self.config.replicaof {
            conf.push_str(&format!("replicaof {host} {port}\n"));
        }
        if let Some(ref pw) = self.config.masterauth {
            conf.push_str(&format!("masterauth {pw}\n"));
        }

        // -- security --
        if let Some(ref pw) = self.config.password {
            conf.push_str(&format!("requirepass {pw}\n"));
        }
        if let Some(ref path) = self.config.acl_file {
            conf.push_str(&format!("aclfile {}\n", path.display()));
        }

        // -- cluster --
        if self.config.cluster_enabled {
            conf.push_str("cluster-enabled yes\n");
            conf.push_str(&format!(
                "cluster-config-file {}/nodes.conf\n",
                node_dir.display()
            ));
            if let Some(timeout) = self.config.cluster_node_timeout {
                conf.push_str(&format!("cluster-node-timeout {timeout}\n"));
            }
        }

        // -- modules --
        for path in &self.config.loadmodule {
            conf.push_str(&format!("loadmodule {}\n", path.display()));
        }

        // -- advanced --
        if let Some(hz) = self.config.hz {
            conf.push_str(&format!("hz {hz}\n"));
        }
        if let Some(n) = self.config.io_threads {
            conf.push_str(&format!("io-threads {n}\n"));
        }
        if let Some(enable) = self.config.io_threads_do_reads {
            conf.push_str(&format!("io-threads-do-reads {}\n", yn(enable)));
        }
        if let Some(ref events) = self.config.notify_keyspace_events {
            conf.push_str(&format!("notify-keyspace-events {events}\n"));
        }

        // -- catch-all --
        for (key, value) in &self.config.extra {
            conf.push_str(&format!("{key} {value}\n"));
        }

        conf
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a running Redis server. Stops the server on Drop.
pub struct RedisServerHandle {
    config: RedisServerConfig,
    cli: RedisCli,
}

impl RedisServerHandle {
    /// The server's address as "host:port".
    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.bind, self.config.port)
    }

    /// The server's port.
    pub fn port(&self) -> u16 {
        self.config.port
    }

    /// The server's bind address.
    pub fn host(&self) -> &str {
        &self.config.bind
    }

    /// Check if the server is alive via PING.
    pub fn is_alive(&self) -> bool {
        self.cli.ping()
    }

    /// Get a `RedisCli` configured for this server.
    pub fn cli(&self) -> &RedisCli {
        &self.cli
    }

    /// Run a redis-cli command against this server.
    pub fn run(&self, args: &[&str]) -> Result<String> {
        self.cli.run(args)
    }

    /// Stop the server via SHUTDOWN NOSAVE.
    pub fn stop(&self) {
        self.cli.shutdown();
    }

    /// Wait until the server is ready (PING -> PONG).
    pub fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        self.cli.wait_for_ready(timeout)
    }
}

impl Drop for RedisServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let s = RedisServer::new();
        assert_eq!(s.config.port, 6379);
        assert_eq!(s.config.bind, "127.0.0.1");
        assert!(!s.config.save);
    }

    #[test]
    fn builder_chain() {
        let s = RedisServer::new()
            .port(6400)
            .bind("0.0.0.0")
            .save(true)
            .appendonly(true)
            .password("secret")
            .loglevel(LogLevel::Warning)
            .extra("maxmemory", "100mb");

        assert_eq!(s.config.port, 6400);
        assert_eq!(s.config.bind, "0.0.0.0");
        assert!(s.config.save);
        assert!(s.config.appendonly);
        assert_eq!(s.config.password.as_deref(), Some("secret"));
        assert_eq!(s.config.extra.get("maxmemory").unwrap(), "100mb");
    }

    #[test]
    fn cluster_config() {
        let s = RedisServer::new()
            .port(7000)
            .cluster_enabled(true)
            .cluster_node_timeout(5000);

        assert!(s.config.cluster_enabled);
        assert_eq!(s.config.cluster_node_timeout, Some(5000));
    }
}
