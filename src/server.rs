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
    /// Whether RDB snapshots are enabled (default: `false`).
    pub save: bool,
    /// Whether AOF persistence is enabled (default: `false`).
    pub appendonly: bool,

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

    // -- slow log --
    /// Log queries slower than this many microseconds (`0` = log everything, `-1` = disabled).
    pub slowlog_log_slower_than: Option<i64>,
    /// Maximum number of entries in the slow log.
    pub slowlog_max_len: Option<u32>,

    // -- latency tracking --
    /// Latency monitor threshold in milliseconds (`0` = disabled).
    pub latency_monitor_threshold: Option<u64>,
    /// Enable the extended latency tracking system.
    pub latency_tracking: Option<bool>,
    /// Percentiles reported by the latency tracking system (e.g. `"50 99 99.9"`).
    pub latency_tracking_info_percentiles: Option<String>,

    // -- active defragmentation --
    /// Enable active defragmentation.
    pub activedefrag: Option<bool>,
    /// Minimum amount of fragmentation waste to start defragmentation.
    pub active_defrag_ignore_bytes: Option<String>,
    /// Minimum percentage of fragmentation to start defragmentation.
    pub active_defrag_threshold_lower: Option<u32>,
    /// Maximum percentage of fragmentation at which we use maximum effort.
    pub active_defrag_threshold_upper: Option<u32>,
    /// Minimal effort for defragmentation as a percentage of CPU time.
    pub active_defrag_cycle_min: Option<u32>,
    /// Maximum effort for defragmentation as a percentage of CPU time.
    pub active_defrag_cycle_max: Option<u32>,
    /// Maximum number of set/hash/zset/list fields processed per defrag scan step.
    pub active_defrag_max_scan_fields: Option<u32>,

    // -- logging and process --
    /// Enable logging to syslog.
    pub syslog_enabled: Option<bool>,
    /// Syslog identity string.
    pub syslog_ident: Option<String>,
    /// Syslog facility (e.g. `"local0"`).
    pub syslog_facility: Option<String>,
    /// Supervision mode (`"upstart"`, `"systemd"`, `"auto"`, or `"no"`).
    pub supervised: Option<String>,
    /// Show the Redis logo on startup.
    pub always_show_logo: Option<bool>,
    /// Set the process title.
    pub set_proc_title: Option<bool>,
    /// Template for the process title.
    pub proc_title_template: Option<String>,

    // -- security and ACL --
    /// Default pub/sub permissions for ACL users (`"allchannels"` or `"resetchannels"`).
    pub acl_pubsub_default: Option<String>,
    /// Maximum length of the ACL log.
    pub acllog_max_len: Option<u32>,
    /// Enable the DEBUG command (`"yes"`, `"local"`, or `"no"`).
    pub enable_debug_command: Option<String>,
    /// Enable the MODULE command (`"yes"`, `"local"`, or `"no"`).
    pub enable_module_command: Option<String>,
    /// Allow CONFIG SET to modify protected configs.
    pub enable_protected_configs: Option<String>,
    /// Rename a command (command, new-name). Empty new-name disables the command.
    pub rename_command: Vec<(String, String)>,
    /// Sanitize dump payload on restore (`"yes"`, `"no"`, or `"clients"`).
    pub sanitize_dump_payload: Option<String>,
    /// Hide user data from log messages.
    pub hide_user_data_from_log: Option<bool>,

    // -- networking (additional) --
    /// Source address for outgoing connections.
    pub bind_source_addr: Option<String>,
    /// Busy reply threshold in milliseconds.
    pub busy_reply_threshold: Option<u64>,
    /// Client output buffer limits (e.g. `"normal 0 0 0"`, `"replica 256mb 64mb 60"`).
    pub client_output_buffer_limit: Vec<String>,
    /// Maximum size of a single client query buffer.
    pub client_query_buffer_limit: Option<String>,
    /// Maximum size of a single protocol bulk request.
    pub proto_max_bulk_len: Option<String>,
    /// Maximum number of new connections per event loop cycle.
    pub max_new_connections_per_cycle: Option<u32>,
    /// Maximum number of new TLS connections per event loop cycle.
    pub max_new_tls_connections_per_cycle: Option<u32>,
    /// Socket mark ID for outgoing connections.
    pub socket_mark_id: Option<u32>,

    // -- RDB (additional) --
    /// RDB dump filename.
    pub dbfilename: Option<String>,
    /// Enable RDB compression.
    pub rdbcompression: Option<bool>,
    /// Enable RDB checksum.
    pub rdbchecksum: Option<bool>,
    /// Incremental fsync during RDB save.
    pub rdb_save_incremental_fsync: Option<bool>,
    /// Delete RDB sync files used by diskless replication.
    pub rdb_del_sync_files: Option<bool>,
    /// Stop accepting writes when bgsave fails.
    pub stop_writes_on_bgsave_error: Option<bool>,

    // -- shutdown --
    /// Shutdown behavior on SIGINT (e.g. `"default"`, `"save"`, `"nosave"`, `"now"`, `"force"`).
    pub shutdown_on_sigint: Option<String>,
    /// Shutdown behavior on SIGTERM.
    pub shutdown_on_sigterm: Option<String>,
    /// Maximum seconds to wait during shutdown for lagging replicas.
    pub shutdown_timeout: Option<u32>,

    // -- other --
    /// Enable active rehashing.
    pub activerehashing: Option<bool>,
    /// Enable crash log on crash.
    pub crash_log_enabled: Option<bool>,
    /// Enable crash memory check on crash.
    pub crash_memcheck_enabled: Option<bool>,
    /// Disable transparent huge pages.
    pub disable_thp: Option<bool>,
    /// Enable dynamic Hz adjustment.
    pub dynamic_hz: Option<bool>,
    /// Ignore specific warnings (e.g. `"ARM64-COW-BUG"`).
    pub ignore_warnings: Option<String>,
    /// Include another config file.
    pub include: Vec<PathBuf>,
    /// Enable jemalloc background thread.
    pub jemalloc_bg_thread: Option<bool>,
    /// Locale collation setting.
    pub locale_collate: Option<String>,
    /// Lua script time limit in milliseconds.
    pub lua_time_limit: Option<u64>,
    /// OOM score adjustment mode (`"yes"`, `"no"`, or `"absolute"`).
    pub oom_score_adj: Option<String>,
    /// OOM score adjustment values (e.g. `"0 200 800"`).
    pub oom_score_adj_values: Option<String>,
    /// Propagation error behavior (`"panic"` or `"ignore"`).
    pub propagation_error_behavior: Option<String>,
    /// Maximum number of keys in the tracking table.
    pub tracking_table_max_keys: Option<u64>,

    // -- catch-all for anything not covered above --
    /// Arbitrary key/value directives forwarded verbatim to the config file.
    pub extra: HashMap<String, String>,

    // -- binary paths --
    /// Path to the `redis-server` binary (default: `"redis-server"`).
    pub redis_server_bin: String,
    /// Path to the `redis-cli` binary (default: `"redis-cli"`).
    pub redis_cli_bin: String,
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
            slowlog_log_slower_than: None,
            slowlog_max_len: None,
            latency_monitor_threshold: None,
            latency_tracking: None,
            latency_tracking_info_percentiles: None,
            activedefrag: None,
            active_defrag_ignore_bytes: None,
            active_defrag_threshold_lower: None,
            active_defrag_threshold_upper: None,
            active_defrag_cycle_min: None,
            active_defrag_cycle_max: None,
            active_defrag_max_scan_fields: None,
            syslog_enabled: None,
            syslog_ident: None,
            syslog_facility: None,
            supervised: None,
            always_show_logo: None,
            set_proc_title: None,
            proc_title_template: None,
            acl_pubsub_default: None,
            acllog_max_len: None,
            enable_debug_command: None,
            enable_module_command: None,
            enable_protected_configs: None,
            rename_command: Vec::new(),
            sanitize_dump_payload: None,
            hide_user_data_from_log: None,
            bind_source_addr: None,
            busy_reply_threshold: None,
            client_output_buffer_limit: Vec::new(),
            client_query_buffer_limit: None,
            proto_max_bulk_len: None,
            max_new_connections_per_cycle: None,
            max_new_tls_connections_per_cycle: None,
            socket_mark_id: None,
            dbfilename: None,
            rdbcompression: None,
            rdbchecksum: None,
            rdb_save_incremental_fsync: None,
            rdb_del_sync_files: None,
            stop_writes_on_bgsave_error: None,
            shutdown_on_sigint: None,
            shutdown_on_sigterm: None,
            shutdown_timeout: None,
            activerehashing: None,
            crash_log_enabled: None,
            crash_memcheck_enabled: None,
            disable_thp: None,
            dynamic_hz: None,
            ignore_warnings: None,
            include: Vec::new(),
            jemalloc_bg_thread: None,
            locale_collate: None,
            lua_time_limit: None,
            oom_score_adj: None,
            oom_score_adj_values: None,
            propagation_error_behavior: None,
            tracking_table_max_keys: None,
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

    // -- slow log --

    /// Set the slow log threshold in microseconds (`0` = log everything, `-1` = disabled).
    pub fn slowlog_log_slower_than(mut self, us: i64) -> Self {
        self.config.slowlog_log_slower_than = Some(us);
        self
    }

    /// Set the maximum number of slow log entries.
    pub fn slowlog_max_len(mut self, n: u32) -> Self {
        self.config.slowlog_max_len = Some(n);
        self
    }

    // -- latency tracking --

    /// Set the latency monitor threshold in milliseconds (`0` = disabled).
    pub fn latency_monitor_threshold(mut self, ms: u64) -> Self {
        self.config.latency_monitor_threshold = Some(ms);
        self
    }

    /// Enable or disable the extended latency tracking system.
    pub fn latency_tracking(mut self, enable: bool) -> Self {
        self.config.latency_tracking = Some(enable);
        self
    }

    /// Set percentiles reported by the latency tracking system (e.g. `"50 99 99.9"`).
    pub fn latency_tracking_info_percentiles(mut self, percentiles: impl Into<String>) -> Self {
        self.config.latency_tracking_info_percentiles = Some(percentiles.into());
        self
    }

    // -- active defragmentation --

    /// Enable or disable active defragmentation.
    pub fn activedefrag(mut self, enable: bool) -> Self {
        self.config.activedefrag = Some(enable);
        self
    }

    /// Set the minimum fragmentation waste to start defragmentation (e.g. `"100mb"`).
    pub fn active_defrag_ignore_bytes(mut self, bytes: impl Into<String>) -> Self {
        self.config.active_defrag_ignore_bytes = Some(bytes.into());
        self
    }

    /// Set the minimum fragmentation percentage to start defragmentation.
    pub fn active_defrag_threshold_lower(mut self, pct: u32) -> Self {
        self.config.active_defrag_threshold_lower = Some(pct);
        self
    }

    /// Set the fragmentation percentage at which maximum effort is used.
    pub fn active_defrag_threshold_upper(mut self, pct: u32) -> Self {
        self.config.active_defrag_threshold_upper = Some(pct);
        self
    }

    /// Set the minimal CPU effort for defragmentation (percentage).
    pub fn active_defrag_cycle_min(mut self, pct: u32) -> Self {
        self.config.active_defrag_cycle_min = Some(pct);
        self
    }

    /// Set the maximum CPU effort for defragmentation (percentage).
    pub fn active_defrag_cycle_max(mut self, pct: u32) -> Self {
        self.config.active_defrag_cycle_max = Some(pct);
        self
    }

    /// Set the maximum fields processed per defrag scan step.
    pub fn active_defrag_max_scan_fields(mut self, n: u32) -> Self {
        self.config.active_defrag_max_scan_fields = Some(n);
        self
    }

    // -- logging and process --

    /// Enable logging to syslog.
    pub fn syslog_enabled(mut self, enable: bool) -> Self {
        self.config.syslog_enabled = Some(enable);
        self
    }

    /// Set the syslog identity string.
    pub fn syslog_ident(mut self, ident: impl Into<String>) -> Self {
        self.config.syslog_ident = Some(ident.into());
        self
    }

    /// Set the syslog facility (e.g. `"local0"`).
    pub fn syslog_facility(mut self, facility: impl Into<String>) -> Self {
        self.config.syslog_facility = Some(facility.into());
        self
    }

    /// Set the supervision mode (`"upstart"`, `"systemd"`, `"auto"`, or `"no"`).
    pub fn supervised(mut self, mode: impl Into<String>) -> Self {
        self.config.supervised = Some(mode.into());
        self
    }

    /// Show the Redis logo on startup.
    pub fn always_show_logo(mut self, enable: bool) -> Self {
        self.config.always_show_logo = Some(enable);
        self
    }

    /// Enable setting the process title.
    pub fn set_proc_title(mut self, enable: bool) -> Self {
        self.config.set_proc_title = Some(enable);
        self
    }

    /// Set the process title template.
    pub fn proc_title_template(mut self, template: impl Into<String>) -> Self {
        self.config.proc_title_template = Some(template.into());
        self
    }

    // -- security and ACL --

    /// Set the default pub/sub ACL permissions (`"allchannels"` or `"resetchannels"`).
    pub fn acl_pubsub_default(mut self, default: impl Into<String>) -> Self {
        self.config.acl_pubsub_default = Some(default.into());
        self
    }

    /// Set the maximum length of the ACL log.
    pub fn acllog_max_len(mut self, n: u32) -> Self {
        self.config.acllog_max_len = Some(n);
        self
    }

    /// Enable the DEBUG command (`"yes"`, `"local"`, or `"no"`).
    pub fn enable_debug_command(mut self, mode: impl Into<String>) -> Self {
        self.config.enable_debug_command = Some(mode.into());
        self
    }

    /// Enable the MODULE command (`"yes"`, `"local"`, or `"no"`).
    pub fn enable_module_command(mut self, mode: impl Into<String>) -> Self {
        self.config.enable_module_command = Some(mode.into());
        self
    }

    /// Allow CONFIG SET to modify protected configs (`"yes"`, `"local"`, or `"no"`).
    pub fn enable_protected_configs(mut self, mode: impl Into<String>) -> Self {
        self.config.enable_protected_configs = Some(mode.into());
        self
    }

    /// Rename a command. Pass an empty new name to disable the command entirely.
    pub fn rename_command(
        mut self,
        command: impl Into<String>,
        new_name: impl Into<String>,
    ) -> Self {
        self.config
            .rename_command
            .push((command.into(), new_name.into()));
        self
    }

    /// Set dump payload sanitization mode (`"yes"`, `"no"`, or `"clients"`).
    pub fn sanitize_dump_payload(mut self, mode: impl Into<String>) -> Self {
        self.config.sanitize_dump_payload = Some(mode.into());
        self
    }

    /// Hide user data from log messages.
    pub fn hide_user_data_from_log(mut self, enable: bool) -> Self {
        self.config.hide_user_data_from_log = Some(enable);
        self
    }

    // -- networking (additional) --

    /// Set the source address for outgoing connections.
    pub fn bind_source_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.bind_source_addr = Some(addr.into());
        self
    }

    /// Set the busy reply threshold in milliseconds.
    pub fn busy_reply_threshold(mut self, ms: u64) -> Self {
        self.config.busy_reply_threshold = Some(ms);
        self
    }

    /// Add a client output buffer limit (e.g. `"normal 0 0 0"` or `"replica 256mb 64mb 60"`).
    pub fn client_output_buffer_limit(mut self, limit: impl Into<String>) -> Self {
        self.config.client_output_buffer_limit.push(limit.into());
        self
    }

    /// Set the maximum size of a single client query buffer.
    pub fn client_query_buffer_limit(mut self, limit: impl Into<String>) -> Self {
        self.config.client_query_buffer_limit = Some(limit.into());
        self
    }

    /// Set the maximum size of a single protocol bulk request.
    pub fn proto_max_bulk_len(mut self, len: impl Into<String>) -> Self {
        self.config.proto_max_bulk_len = Some(len.into());
        self
    }

    /// Set the maximum number of new connections per event loop cycle.
    pub fn max_new_connections_per_cycle(mut self, n: u32) -> Self {
        self.config.max_new_connections_per_cycle = Some(n);
        self
    }

    /// Set the maximum number of new TLS connections per event loop cycle.
    pub fn max_new_tls_connections_per_cycle(mut self, n: u32) -> Self {
        self.config.max_new_tls_connections_per_cycle = Some(n);
        self
    }

    /// Set the socket mark ID for outgoing connections.
    pub fn socket_mark_id(mut self, id: u32) -> Self {
        self.config.socket_mark_id = Some(id);
        self
    }

    // -- RDB (additional) --

    /// Set the RDB dump filename.
    pub fn dbfilename(mut self, name: impl Into<String>) -> Self {
        self.config.dbfilename = Some(name.into());
        self
    }

    /// Enable or disable RDB compression.
    pub fn rdbcompression(mut self, enable: bool) -> Self {
        self.config.rdbcompression = Some(enable);
        self
    }

    /// Enable or disable RDB checksum.
    pub fn rdbchecksum(mut self, enable: bool) -> Self {
        self.config.rdbchecksum = Some(enable);
        self
    }

    /// Enable incremental fsync during RDB save.
    pub fn rdb_save_incremental_fsync(mut self, enable: bool) -> Self {
        self.config.rdb_save_incremental_fsync = Some(enable);
        self
    }

    /// Delete RDB sync files used by diskless replication.
    pub fn rdb_del_sync_files(mut self, enable: bool) -> Self {
        self.config.rdb_del_sync_files = Some(enable);
        self
    }

    /// Stop accepting writes when bgsave fails.
    pub fn stop_writes_on_bgsave_error(mut self, enable: bool) -> Self {
        self.config.stop_writes_on_bgsave_error = Some(enable);
        self
    }

    // -- shutdown --

    /// Set shutdown behavior on SIGINT (e.g. `"default"`, `"save"`, `"nosave"`).
    pub fn shutdown_on_sigint(mut self, behavior: impl Into<String>) -> Self {
        self.config.shutdown_on_sigint = Some(behavior.into());
        self
    }

    /// Set shutdown behavior on SIGTERM.
    pub fn shutdown_on_sigterm(mut self, behavior: impl Into<String>) -> Self {
        self.config.shutdown_on_sigterm = Some(behavior.into());
        self
    }

    /// Set the maximum seconds to wait during shutdown for lagging replicas.
    pub fn shutdown_timeout(mut self, seconds: u32) -> Self {
        self.config.shutdown_timeout = Some(seconds);
        self
    }

    // -- other --

    /// Enable or disable active rehashing.
    pub fn activerehashing(mut self, enable: bool) -> Self {
        self.config.activerehashing = Some(enable);
        self
    }

    /// Enable crash log on crash.
    pub fn crash_log_enabled(mut self, enable: bool) -> Self {
        self.config.crash_log_enabled = Some(enable);
        self
    }

    /// Enable crash memory check on crash.
    pub fn crash_memcheck_enabled(mut self, enable: bool) -> Self {
        self.config.crash_memcheck_enabled = Some(enable);
        self
    }

    /// Disable transparent huge pages.
    pub fn disable_thp(mut self, enable: bool) -> Self {
        self.config.disable_thp = Some(enable);
        self
    }

    /// Enable dynamic Hz adjustment.
    pub fn dynamic_hz(mut self, enable: bool) -> Self {
        self.config.dynamic_hz = Some(enable);
        self
    }

    /// Ignore specific warnings (e.g. `"ARM64-COW-BUG"`).
    pub fn ignore_warnings(mut self, warning: impl Into<String>) -> Self {
        self.config.ignore_warnings = Some(warning.into());
        self
    }

    /// Include another config file.
    pub fn include(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.include.push(path.into());
        self
    }

    /// Enable or disable jemalloc background thread.
    pub fn jemalloc_bg_thread(mut self, enable: bool) -> Self {
        self.config.jemalloc_bg_thread = Some(enable);
        self
    }

    /// Set the locale collation setting.
    pub fn locale_collate(mut self, locale: impl Into<String>) -> Self {
        self.config.locale_collate = Some(locale.into());
        self
    }

    /// Set the Lua script time limit in milliseconds.
    pub fn lua_time_limit(mut self, ms: u64) -> Self {
        self.config.lua_time_limit = Some(ms);
        self
    }

    /// Set the OOM score adjustment mode (`"yes"`, `"no"`, or `"absolute"`).
    pub fn oom_score_adj(mut self, mode: impl Into<String>) -> Self {
        self.config.oom_score_adj = Some(mode.into());
        self
    }

    /// Set the OOM score adjustment values (e.g. `"0 200 800"`).
    pub fn oom_score_adj_values(mut self, values: impl Into<String>) -> Self {
        self.config.oom_score_adj_values = Some(values.into());
        self
    }

    /// Set the propagation error behavior (`"panic"` or `"ignore"`).
    pub fn propagation_error_behavior(mut self, behavior: impl Into<String>) -> Self {
        self.config.propagation_error_behavior = Some(behavior.into());
        self
    }

    /// Set the maximum number of keys in the tracking table.
    pub fn tracking_table_max_keys(mut self, n: u64) -> Self {
        self.config.tracking_table_max_keys = Some(n);
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

        // -- slow log --
        if let Some(us) = self.config.slowlog_log_slower_than {
            conf.push_str(&format!("slowlog-log-slower-than {us}\n"));
        }
        if let Some(n) = self.config.slowlog_max_len {
            conf.push_str(&format!("slowlog-max-len {n}\n"));
        }

        // -- latency tracking --
        if let Some(ms) = self.config.latency_monitor_threshold {
            conf.push_str(&format!("latency-monitor-threshold {ms}\n"));
        }
        if let Some(enable) = self.config.latency_tracking {
            conf.push_str(&format!("latency-tracking {}\n", yn(enable)));
        }
        if let Some(ref pcts) = self.config.latency_tracking_info_percentiles {
            conf.push_str(&format!("latency-tracking-info-percentiles \"{pcts}\"\n"));
        }

        // -- active defragmentation --
        if let Some(enable) = self.config.activedefrag {
            conf.push_str(&format!("activedefrag {}\n", yn(enable)));
        }
        if let Some(ref bytes) = self.config.active_defrag_ignore_bytes {
            conf.push_str(&format!("active-defrag-ignore-bytes {bytes}\n"));
        }
        if let Some(pct) = self.config.active_defrag_threshold_lower {
            conf.push_str(&format!("active-defrag-threshold-lower {pct}\n"));
        }
        if let Some(pct) = self.config.active_defrag_threshold_upper {
            conf.push_str(&format!("active-defrag-threshold-upper {pct}\n"));
        }
        if let Some(pct) = self.config.active_defrag_cycle_min {
            conf.push_str(&format!("active-defrag-cycle-min {pct}\n"));
        }
        if let Some(pct) = self.config.active_defrag_cycle_max {
            conf.push_str(&format!("active-defrag-cycle-max {pct}\n"));
        }
        if let Some(n) = self.config.active_defrag_max_scan_fields {
            conf.push_str(&format!("active-defrag-max-scan-fields {n}\n"));
        }

        // -- logging and process --
        if let Some(enable) = self.config.syslog_enabled {
            conf.push_str(&format!("syslog-enabled {}\n", yn(enable)));
        }
        if let Some(ref ident) = self.config.syslog_ident {
            conf.push_str(&format!("syslog-ident {ident}\n"));
        }
        if let Some(ref facility) = self.config.syslog_facility {
            conf.push_str(&format!("syslog-facility {facility}\n"));
        }
        if let Some(ref mode) = self.config.supervised {
            conf.push_str(&format!("supervised {mode}\n"));
        }
        if let Some(enable) = self.config.always_show_logo {
            conf.push_str(&format!("always-show-logo {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.set_proc_title {
            conf.push_str(&format!("set-proc-title {}\n", yn(enable)));
        }
        if let Some(ref template) = self.config.proc_title_template {
            conf.push_str(&format!("proc-title-template \"{template}\"\n"));
        }

        // -- security and ACL --
        if let Some(ref default) = self.config.acl_pubsub_default {
            conf.push_str(&format!("acl-pubsub-default {default}\n"));
        }
        if let Some(n) = self.config.acllog_max_len {
            conf.push_str(&format!("acllog-max-len {n}\n"));
        }
        if let Some(ref mode) = self.config.enable_debug_command {
            conf.push_str(&format!("enable-debug-command {mode}\n"));
        }
        if let Some(ref mode) = self.config.enable_module_command {
            conf.push_str(&format!("enable-module-command {mode}\n"));
        }
        if let Some(ref mode) = self.config.enable_protected_configs {
            conf.push_str(&format!("enable-protected-configs {mode}\n"));
        }
        for (cmd, new_name) in &self.config.rename_command {
            conf.push_str(&format!("rename-command {cmd} \"{new_name}\"\n"));
        }
        if let Some(ref mode) = self.config.sanitize_dump_payload {
            conf.push_str(&format!("sanitize-dump-payload {mode}\n"));
        }
        if let Some(enable) = self.config.hide_user_data_from_log {
            conf.push_str(&format!("hide-user-data-from-log {}\n", yn(enable)));
        }

        // -- networking (additional) --
        if let Some(ref addr) = self.config.bind_source_addr {
            conf.push_str(&format!("bind-source-addr {addr}\n"));
        }
        if let Some(ms) = self.config.busy_reply_threshold {
            conf.push_str(&format!("busy-reply-threshold {ms}\n"));
        }
        for limit in &self.config.client_output_buffer_limit {
            conf.push_str(&format!("client-output-buffer-limit {limit}\n"));
        }
        if let Some(ref limit) = self.config.client_query_buffer_limit {
            conf.push_str(&format!("client-query-buffer-limit {limit}\n"));
        }
        if let Some(ref len) = self.config.proto_max_bulk_len {
            conf.push_str(&format!("proto-max-bulk-len {len}\n"));
        }
        if let Some(n) = self.config.max_new_connections_per_cycle {
            conf.push_str(&format!("max-new-connections-per-cycle {n}\n"));
        }
        if let Some(n) = self.config.max_new_tls_connections_per_cycle {
            conf.push_str(&format!("max-new-tls-connections-per-cycle {n}\n"));
        }
        if let Some(id) = self.config.socket_mark_id {
            conf.push_str(&format!("socket-mark-id {id}\n"));
        }

        // -- RDB (additional) --
        if let Some(ref name) = self.config.dbfilename {
            conf.push_str(&format!("dbfilename {name}\n"));
        }
        if let Some(enable) = self.config.rdbcompression {
            conf.push_str(&format!("rdbcompression {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.rdbchecksum {
            conf.push_str(&format!("rdbchecksum {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.rdb_save_incremental_fsync {
            conf.push_str(&format!("rdb-save-incremental-fsync {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.rdb_del_sync_files {
            conf.push_str(&format!("rdb-del-sync-files {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.stop_writes_on_bgsave_error {
            conf.push_str(&format!("stop-writes-on-bgsave-error {}\n", yn(enable)));
        }

        // -- shutdown --
        if let Some(ref behavior) = self.config.shutdown_on_sigint {
            conf.push_str(&format!("shutdown-on-sigint {behavior}\n"));
        }
        if let Some(ref behavior) = self.config.shutdown_on_sigterm {
            conf.push_str(&format!("shutdown-on-sigterm {behavior}\n"));
        }
        if let Some(seconds) = self.config.shutdown_timeout {
            conf.push_str(&format!("shutdown-timeout {seconds}\n"));
        }

        // -- other --
        if let Some(enable) = self.config.activerehashing {
            conf.push_str(&format!("activerehashing {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.crash_log_enabled {
            conf.push_str(&format!("crash-log-enabled {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.crash_memcheck_enabled {
            conf.push_str(&format!("crash-memcheck-enabled {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.disable_thp {
            conf.push_str(&format!("disable-thp {}\n", yn(enable)));
        }
        if let Some(enable) = self.config.dynamic_hz {
            conf.push_str(&format!("dynamic-hz {}\n", yn(enable)));
        }
        if let Some(ref warning) = self.config.ignore_warnings {
            conf.push_str(&format!("ignore-warnings {warning}\n"));
        }
        for path in &self.config.include {
            conf.push_str(&format!("include {}\n", path.display()));
        }
        if let Some(enable) = self.config.jemalloc_bg_thread {
            conf.push_str(&format!("jemalloc-bg-thread {}\n", yn(enable)));
        }
        if let Some(ref locale) = self.config.locale_collate {
            conf.push_str(&format!("locale-collate {locale}\n"));
        }
        if let Some(ms) = self.config.lua_time_limit {
            conf.push_str(&format!("lua-time-limit {ms}\n"));
        }
        if let Some(ref mode) = self.config.oom_score_adj {
            conf.push_str(&format!("oom-score-adj {mode}\n"));
        }
        if let Some(ref values) = self.config.oom_score_adj_values {
            conf.push_str(&format!("oom-score-adj-values {values}\n"));
        }
        if let Some(ref behavior) = self.config.propagation_error_behavior {
            conf.push_str(&format!("propagation-error-behavior {behavior}\n"));
        }
        if let Some(n) = self.config.tracking_table_max_keys {
            conf.push_str(&format!("tracking-table-max-keys {n}\n"));
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
            .logfile("/tmp/redis.log")
            .loglevel(LogLevel::Warning)
            .extra("maxmemory", "100mb");

        assert_eq!(s.config.port, 6400);
        assert_eq!(s.config.bind, "0.0.0.0");
        assert!(s.config.save);
        assert!(s.config.appendonly);
        assert_eq!(s.config.password.as_deref(), Some("secret"));
        assert_eq!(s.config.logfile.as_deref(), Some("/tmp/redis.log"));
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
