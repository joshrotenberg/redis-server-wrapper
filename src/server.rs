//! Type-safe wrapper for `redis-server` with builder pattern.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use tokio::process::Command;

use crate::cli::RedisCli;
use crate::error::{Error, Result};

/// Full configuration snapshot for a single `redis-server` process.
///
/// This struct is populated by the [`RedisServer`] builder and passed to
/// [`RedisServer::start`]. You rarely need to construct it directly; use the
/// builder instead.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::RedisServer;
///
/// # async fn example() {
/// let server = RedisServer::new()
///     .port(6400)
///     .bind("127.0.0.1")
///     .save(false)
///     .start()
///     .await
///     .unwrap();
///
/// assert!(server.is_alive().await);
/// // Stopped automatically on Drop.
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RedisServerConfig {
    // -- network --
    /// TCP port the server listens on (default: `6379`).
    pub port: u16,
    /// IP address to bind (default: `"127.0.0.1"`).
    pub bind: String,
    /// Whether protected mode is enabled (default: `false`).
    pub protected_mode: bool,
    /// TCP backlog queue length, if set.
    pub tcp_backlog: Option<u32>,
    /// Unix domain socket path, if set.
    pub unixsocket: Option<PathBuf>,
    /// Unix socket file permissions (e.g. `700`), if set.
    pub unixsocketperm: Option<u32>,
    /// Idle client timeout in seconds (`0` = disabled), if set.
    pub timeout: Option<u32>,
    /// TCP keepalive interval in seconds, if set.
    pub tcp_keepalive: Option<u32>,

    // -- tls --
    /// TLS listening port, if set.
    pub tls_port: Option<u16>,
    /// Path to the TLS certificate file, if set.
    pub tls_cert_file: Option<PathBuf>,
    /// Path to the TLS private key file, if set.
    pub tls_key_file: Option<PathBuf>,
    /// Path to the TLS CA certificate file, if set.
    pub tls_ca_cert_file: Option<PathBuf>,
    /// Whether TLS client authentication is required, if set.
    pub tls_auth_clients: Option<bool>,

    // -- general --
    /// Whether the server daemonizes itself (default: `true`).
    pub daemonize: bool,
    /// Working directory for data files (default: a sub-directory of `$TMPDIR`).
    pub dir: PathBuf,
    /// Path to the log file, if set. Defaults to `redis.log` inside the node directory.
    pub logfile: Option<String>,
    /// Server log verbosity (default: [`LogLevel::Notice`]).
    pub loglevel: LogLevel,
    /// Number of databases, if set (Redis default: `16`).
    pub databases: Option<u32>,

    // -- memory --
    /// Maximum memory limit (e.g. `"256mb"`), if set.
    pub maxmemory: Option<String>,
    /// Eviction policy when `maxmemory` is reached, if set.
    pub maxmemory_policy: Option<String>,
    /// Maximum number of simultaneous client connections, if set.
    pub maxclients: Option<u32>,

    // -- persistence --
    /// RDB save policy (default: [`SavePolicy::Disabled`]).
    pub save: SavePolicy,
    /// Whether AOF persistence is enabled (default: `false`).
    pub appendonly: bool,
    /// AOF fsync policy, if set.
    pub appendfsync: Option<AppendFsync>,
    /// AOF filename, if set (Redis default: `"appendonly.aof"`).
    pub appendfilename: Option<String>,
    /// AOF directory name, if set (Redis default: `"appendonlydir"`).
    pub appenddirname: Option<PathBuf>,
    /// Whether the AOF file uses an RDB preamble, if set.
    pub aof_use_rdb_preamble: Option<bool>,
    /// Whether truncated AOF files are loaded, if set.
    pub aof_load_truncated: Option<bool>,
    /// Maximum allowed size of a corrupt AOF tail, if set (e.g. `"32mb"`).
    pub aof_load_corrupt_tail_max_size: Option<String>,
    /// Whether AOF rewrite performs incremental fsync, if set.
    pub aof_rewrite_incremental_fsync: Option<bool>,
    /// Whether timestamps are recorded in the AOF file, if set.
    pub aof_timestamp_enabled: Option<bool>,
    /// Trigger an AOF rewrite when the file grows by this percentage, if set.
    pub auto_aof_rewrite_percentage: Option<u32>,
    /// Minimum AOF size before an automatic rewrite is triggered, if set (e.g. `"64mb"`).
    pub auto_aof_rewrite_min_size: Option<String>,
    /// Whether fsync is suppressed during AOF rewrites, if set.
    pub no_appendfsync_on_rewrite: Option<bool>,

    // -- replication --
    /// Master host and port to replicate from, if set.
    pub replicaof: Option<(String, u16)>,
    /// Password for authenticating with a master, if set.
    pub masterauth: Option<String>,

    // -- security --
    /// `requirepass` password for client connections, if set.
    pub password: Option<String>,
    /// Path to an ACL file, if set.
    pub acl_file: Option<PathBuf>,

    // -- cluster --
    /// Whether Redis Cluster mode is enabled (default: `false`).
    pub cluster_enabled: bool,
    /// Cluster node timeout in milliseconds, if set.
    pub cluster_node_timeout: Option<u64>,

    // -- modules --
    /// List of Redis module paths to load at startup.
    pub loadmodule: Vec<PathBuf>,

    // -- advanced --
    /// Server tick frequency in Hz, if set (Redis default: `10`).
    pub hz: Option<u32>,
    /// Number of I/O threads, if set.
    pub io_threads: Option<u32>,
    /// Whether I/O threads also handle reads, if set.
    pub io_threads_do_reads: Option<bool>,
    /// Keyspace notification event mask (e.g. `"KEA"`), if set.
    pub notify_keyspace_events: Option<String>,

    // -- catch-all for anything not covered above --
    /// Arbitrary key/value directives forwarded verbatim to the config file.
    pub extra: HashMap<String, String>,

    // -- binary paths --
    /// Path to the `redis-server` binary (default: `"redis-server"`).
    pub redis_server_bin: String,
    /// Path to the `redis-cli` binary (default: `"redis-cli"`).
    pub redis_cli_bin: String,
}

/// AOF fsync policy.
#[derive(Debug, Clone, Copy)]
pub enum AppendFsync {
    /// Fsync after every write operation.
    Always,
    /// Fsync once per second (Redis default).
    Everysec,
    /// Let the OS decide when to flush.
    No,
}

impl std::fmt::Display for AppendFsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendFsync::Always => f.write_str("always"),
            AppendFsync::Everysec => f.write_str("everysec"),
            AppendFsync::No => f.write_str("no"),
        }
    }
}

/// RDB save policy.
///
/// Controls whether and how the `save` directive is emitted in the Redis
/// configuration file.
#[derive(Debug, Clone, Default)]
pub enum SavePolicy {
    /// Emit `save ""` to disable RDB snapshots entirely.
    #[default]
    Disabled,
    /// Omit the `save` directive and let Redis use its built-in defaults.
    Default,
    /// Emit one `save <seconds> <changes>` line for each pair.
    Custom(Vec<(u64, u64)>),
}

/// Redis log level.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    /// Very verbose output, useful for diagnosing Redis internals.
    Debug,
    /// Slightly less verbose than `Debug`.
    Verbose,
    /// Informational messages only (default).
    Notice,
    /// Only critical events are logged.
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
            logfile: None,
            loglevel: LogLevel::Notice,
            databases: None,
            maxmemory: None,
            maxmemory_policy: None,
            maxclients: None,
            save: SavePolicy::Disabled,
            appendonly: false,
            appendfsync: None,
            appendfilename: None,
            appenddirname: None,
            aof_use_rdb_preamble: None,
            aof_load_truncated: None,
            aof_load_corrupt_tail_max_size: None,
            aof_rewrite_incremental_fsync: None,
            aof_timestamp_enabled: None,
            auto_aof_rewrite_percentage: None,
            auto_aof_rewrite_min_size: None,
            no_appendfsync_on_rewrite: None,
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
    /// Create a new builder with default settings.
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

    /// Set the log file path. Defaults to `redis.log` inside the node directory.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.config.logfile = Some(path.into());
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
    ///
    /// `true` omits the `save` directive (Redis built-in defaults apply).
    /// `false` emits `save ""` to disable RDB entirely.
    pub fn save(mut self, save: bool) -> Self {
        self.config.save = if save {
            SavePolicy::Default
        } else {
            SavePolicy::Disabled
        };
        self
    }

    /// Set a custom RDB save schedule.
    ///
    /// Each `(seconds, changes)` pair emits a `save <seconds> <changes>` line.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.config.save = SavePolicy::Custom(schedule);
        self
    }

    /// Enable or disable AOF persistence.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.config.appendonly = appendonly;
        self
    }

    /// Set the AOF fsync policy.
    pub fn appendfsync(mut self, policy: AppendFsync) -> Self {
        self.config.appendfsync = Some(policy);
        self
    }

    /// Set the AOF filename.
    pub fn appendfilename(mut self, name: impl Into<String>) -> Self {
        self.config.appendfilename = Some(name.into());
        self
    }

    /// Set the AOF directory name.
    pub fn appenddirname(mut self, name: impl Into<PathBuf>) -> Self {
        self.config.appenddirname = Some(name.into());
        self
    }

    /// Enable or disable the RDB preamble in AOF files.
    pub fn aof_use_rdb_preamble(mut self, enable: bool) -> Self {
        self.config.aof_use_rdb_preamble = Some(enable);
        self
    }

    /// Control whether truncated AOF files are loaded.
    pub fn aof_load_truncated(mut self, enable: bool) -> Self {
        self.config.aof_load_truncated = Some(enable);
        self
    }

    /// Set the maximum allowed size of a corrupt AOF tail (e.g. `"32mb"`).
    pub fn aof_load_corrupt_tail_max_size(mut self, size: impl Into<String>) -> Self {
        self.config.aof_load_corrupt_tail_max_size = Some(size.into());
        self
    }

    /// Enable or disable incremental fsync during AOF rewrites.
    pub fn aof_rewrite_incremental_fsync(mut self, enable: bool) -> Self {
        self.config.aof_rewrite_incremental_fsync = Some(enable);
        self
    }

    /// Enable or disable timestamps in the AOF file.
    pub fn aof_timestamp_enabled(mut self, enable: bool) -> Self {
        self.config.aof_timestamp_enabled = Some(enable);
        self
    }

    /// Set the percentage growth that triggers an automatic AOF rewrite.
    pub fn auto_aof_rewrite_percentage(mut self, pct: u32) -> Self {
        self.config.auto_aof_rewrite_percentage = Some(pct);
        self
    }

    /// Set the minimum AOF size before an automatic rewrite is triggered (e.g. `"64mb"`).
    pub fn auto_aof_rewrite_min_size(mut self, size: impl Into<String>) -> Self {
        self.config.auto_aof_rewrite_min_size = Some(size.into());
        self
    }

    /// Control whether fsync is suppressed during AOF rewrites.
    pub fn no_appendfsync_on_rewrite(mut self, enable: bool) -> Self {
        self.config.no_appendfsync_on_rewrite = Some(enable);
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
    pub async fn start(self) -> Result<RedisServerHandle> {
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
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await?;

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

        cli.wait_for_ready(Duration::from_secs(10)).await?;

        let pid_path = node_dir.join("redis.pid");
        let pid: u32 = fs::read_to_string(&pid_path)
            .map_err(Error::Io)?
            .trim()
            .parse()
            .map_err(|_| Error::ServerStart {
                port: self.config.port,
            })?;

        Ok(RedisServerHandle {
            config: self.config,
            cli,
            pid,
            detached: false,
        })
    }

    fn generate_config(&self, node_dir: &std::path::Path) -> String {
        let yn = |b: bool| if b { "yes" } else { "no" };

        let mut conf = format!(
            "port {port}\n\
             bind {bind}\n\
             daemonize {daemonize}\n\
             pidfile {dir}/redis.pid\n\
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

        let logfile = self
            .config
            .logfile
            .as_deref()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{}/redis.log", node_dir.display()));
        conf.push_str(&format!("logfile {logfile}\n"));

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
        match &self.config.save {
            SavePolicy::Disabled => conf.push_str("save \"\"\n"),
            SavePolicy::Default => {}
            SavePolicy::Custom(pairs) => {
                for (secs, changes) in pairs {
                    conf.push_str(&format!("save {secs} {changes}\n"));
                }
            }
        }
        if self.config.appendonly {
            conf.push_str("appendonly yes\n");
        }
        if let Some(ref policy) = self.config.appendfsync {
            conf.push_str(&format!("appendfsync {policy}\n"));
        }
        if let Some(ref name) = self.config.appendfilename {
            conf.push_str(&format!("appendfilename \"{name}\"\n"));
        }
        if let Some(ref name) = self.config.appenddirname {
            conf.push_str(&format!("appenddirname \"{}\"\n", name.display()));
        }
        if let Some(v) = self.config.aof_use_rdb_preamble {
            conf.push_str(&format!("aof-use-rdb-preamble {}\n", yn(v)));
        }
        if let Some(v) = self.config.aof_load_truncated {
            conf.push_str(&format!("aof-load-truncated {}\n", yn(v)));
        }
        if let Some(ref size) = self.config.aof_load_corrupt_tail_max_size {
            conf.push_str(&format!("aof-load-corrupt-tail-max-size {size}\n"));
        }
        if let Some(v) = self.config.aof_rewrite_incremental_fsync {
            conf.push_str(&format!("aof-rewrite-incremental-fsync {}\n", yn(v)));
        }
        if let Some(v) = self.config.aof_timestamp_enabled {
            conf.push_str(&format!("aof-timestamp-enabled {}\n", yn(v)));
        }
        if let Some(pct) = self.config.auto_aof_rewrite_percentage {
            conf.push_str(&format!("auto-aof-rewrite-percentage {pct}\n"));
        }
        if let Some(ref size) = self.config.auto_aof_rewrite_min_size {
            conf.push_str(&format!("auto-aof-rewrite-min-size {size}\n"));
        }
        if let Some(v) = self.config.no_appendfsync_on_rewrite {
            conf.push_str(&format!("no-appendfsync-on-rewrite {}\n", yn(v)));
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
    pid: u32,
    detached: bool,
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

    /// The PID of the `redis-server` process.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Check if the server is alive via PING.
    pub async fn is_alive(&self) -> bool {
        self.cli.ping().await
    }

    /// Get a `RedisCli` configured for this server.
    pub fn cli(&self) -> &RedisCli {
        &self.cli
    }

    /// Run a redis-cli command against this server.
    pub async fn run(&self, args: &[&str]) -> Result<String> {
        self.cli.run(args).await
    }

    /// Consume the handle without stopping the server.
    pub fn detach(mut self) {
        self.detached = true;
    }

    /// Stop the server via SHUTDOWN NOSAVE.
    pub fn stop(&self) {
        self.cli.shutdown();
    }

    /// Wait until the server is ready (PING -> PONG).
    pub async fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        self.cli.wait_for_ready(timeout).await
    }
}

impl Drop for RedisServerHandle {
    fn drop(&mut self) {
        if !self.detached {
            self.stop();
        }
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
        assert!(matches!(s.config.save, SavePolicy::Disabled));
    }

    #[test]
    fn builder_chain() {
        let s = RedisServer::new()
            .port(6400)
            .bind("0.0.0.0")
            .save(true)
            .appendonly(true)
            .password("secret")
            .logfile("/tmp/redis.log")
            .loglevel(LogLevel::Warning)
            .extra("maxmemory", "100mb");

        assert_eq!(s.config.port, 6400);
        assert_eq!(s.config.bind, "0.0.0.0");
        assert!(matches!(s.config.save, SavePolicy::Default));
        assert!(s.config.appendonly);
        assert_eq!(s.config.password.as_deref(), Some("secret"));
        assert_eq!(s.config.logfile.as_deref(), Some("/tmp/redis.log"));
        assert_eq!(s.config.extra.get("maxmemory").unwrap(), "100mb");
    }

    #[test]
    fn save_schedule() {
        let s = RedisServer::new().save_schedule(vec![(900, 1), (300, 10)]);
        match &s.config.save {
            SavePolicy::Custom(pairs) => {
                assert_eq!(pairs, &[(900, 1), (300, 10)]);
            }
            _ => panic!("expected SavePolicy::Custom"),
        }
    }

    #[test]
    fn aof_tuning() {
        let s = RedisServer::new()
            .appendonly(true)
            .appendfsync(AppendFsync::Always)
            .appendfilename("my.aof")
            .aof_use_rdb_preamble(true)
            .auto_aof_rewrite_percentage(100)
            .auto_aof_rewrite_min_size("64mb")
            .no_appendfsync_on_rewrite(true);

        assert!(s.config.appendonly);
        assert!(matches!(s.config.appendfsync, Some(AppendFsync::Always)));
        assert_eq!(s.config.appendfilename.as_deref(), Some("my.aof"));
        assert_eq!(s.config.aof_use_rdb_preamble, Some(true));
        assert_eq!(s.config.auto_aof_rewrite_percentage, Some(100));
        assert_eq!(s.config.auto_aof_rewrite_min_size.as_deref(), Some("64mb"));
        assert_eq!(s.config.no_appendfsync_on_rewrite, Some(true));
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
