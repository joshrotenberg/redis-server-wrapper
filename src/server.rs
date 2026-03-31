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
    /// Passphrase for the TLS private key file, if set.
    pub tls_key_file_pass: Option<String>,
    /// Path to the TLS CA certificate file, if set.
    pub tls_ca_cert_file: Option<PathBuf>,
    /// Path to a directory containing TLS CA certificates, if set.
    pub tls_ca_cert_dir: Option<PathBuf>,
    /// Whether TLS client authentication is required, if set.
    pub tls_auth_clients: Option<bool>,
    /// Path to the TLS client certificate file (for outgoing connections), if set.
    pub tls_client_cert_file: Option<PathBuf>,
    /// Path to the TLS client private key file (for outgoing connections), if set.
    pub tls_client_key_file: Option<PathBuf>,
    /// Passphrase for the TLS client private key file, if set.
    pub tls_client_key_file_pass: Option<String>,
    /// Path to the DH parameters file for DHE ciphers, if set.
    pub tls_dh_params_file: Option<PathBuf>,
    /// Allowed TLS 1.2 ciphers (OpenSSL cipher list format), if set.
    pub tls_ciphers: Option<String>,
    /// Allowed TLS 1.3 ciphersuites (colon-separated), if set.
    pub tls_ciphersuites: Option<String>,
    /// Allowed TLS protocol versions (e.g. `"TLSv1.2 TLSv1.3"`), if set.
    pub tls_protocols: Option<String>,
    /// Whether the server prefers its own cipher order, if set.
    pub tls_prefer_server_ciphers: Option<bool>,
    /// Whether TLS session caching is enabled, if set.
    pub tls_session_caching: Option<bool>,
    /// Number of entries in the TLS session cache, if set.
    pub tls_session_cache_size: Option<u32>,
    /// Timeout in seconds for cached TLS sessions, if set.
    pub tls_session_cache_timeout: Option<u32>,
    /// Whether replication traffic uses TLS, if set.
    pub tls_replication: Option<bool>,
    /// Whether cluster bus communication uses TLS, if set.
    pub tls_cluster: Option<bool>,

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
    /// Number of keys sampled per eviction round, if set (Redis default: `5`).
    pub maxmemory_samples: Option<u32>,
    /// Per-client memory limit (e.g. `"0"` = disabled), if set.
    pub maxmemory_clients: Option<String>,
    /// Eviction processing effort (1-100), if set (Redis default: `10`).
    pub maxmemory_eviction_tenacity: Option<u32>,
    /// Maximum number of simultaneous client connections, if set.
    pub maxclients: Option<u32>,
    /// Logarithmic factor for the LFU frequency counter, if set (Redis default: `10`).
    pub lfu_log_factor: Option<u32>,
    /// LFU counter decay time in minutes, if set (Redis default: `1`).
    pub lfu_decay_time: Option<u32>,
    /// Effort spent on active key expiration (1-100), if set (Redis default: `10`).
    pub active_expire_effort: Option<u32>,

    // -- lazyfree --
    /// Whether eviction uses background deletion, if set.
    pub lazyfree_lazy_eviction: Option<bool>,
    /// Whether expired-key deletion uses background threads, if set.
    pub lazyfree_lazy_expire: Option<bool>,
    /// Whether implicit `DEL` commands (e.g. `RENAME`) use background deletion, if set.
    pub lazyfree_lazy_server_del: Option<bool>,
    /// Whether explicit `DEL` behaves like `UNLINK`, if set.
    pub lazyfree_lazy_user_del: Option<bool>,
    /// Whether `FLUSHDB`/`FLUSHALL` default to `ASYNC`, if set.
    pub lazyfree_lazy_user_flush: Option<bool>,

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
    /// Username for authenticating with a master, if set.
    pub masteruser: Option<String>,
    /// Replication backlog size (e.g. `"1mb"`), if set.
    pub repl_backlog_size: Option<String>,
    /// Seconds before the backlog is freed when no replicas are connected, if set.
    pub repl_backlog_ttl: Option<u32>,
    /// Whether TCP_NODELAY is disabled on the replication socket, if set.
    pub repl_disable_tcp_nodelay: Option<bool>,
    /// Diskless load policy for replicas, if set.
    pub repl_diskless_load: Option<ReplDisklessLoad>,
    /// Whether the master sends RDB to replicas via diskless transfer, if set.
    pub repl_diskless_sync: Option<bool>,
    /// Delay in seconds before starting a diskless sync, if set.
    pub repl_diskless_sync_delay: Option<u32>,
    /// Maximum number of replicas to wait for before starting a diskless sync, if set.
    pub repl_diskless_sync_max_replicas: Option<u32>,
    /// Interval in seconds between PING commands sent to the master, if set.
    pub repl_ping_replica_period: Option<u32>,
    /// Replication timeout in seconds, if set.
    pub repl_timeout: Option<u32>,
    /// IP address a replica announces to the master, if set.
    pub replica_announce_ip: Option<String>,
    /// Port a replica announces to the master, if set.
    pub replica_announce_port: Option<u16>,
    /// Whether the replica is announced to clients, if set.
    pub replica_announced: Option<bool>,
    /// Buffer limit for full synchronization on replicas (e.g. `"256mb"`), if set.
    pub replica_full_sync_buffer_limit: Option<String>,
    /// Whether replicas ignore disk-write errors, if set.
    pub replica_ignore_disk_write_errors: Option<bool>,
    /// Whether replicas ignore the maxmemory setting, if set.
    pub replica_ignore_maxmemory: Option<bool>,
    /// Whether replicas perform a lazy flush during full sync, if set.
    pub replica_lazy_flush: Option<bool>,
    /// Replica priority for Sentinel promotion, if set.
    pub replica_priority: Option<u32>,
    /// Whether the replica is read-only, if set.
    pub replica_read_only: Option<bool>,
    /// Whether the replica serves stale data while syncing, if set.
    pub replica_serve_stale_data: Option<bool>,
    /// Minimum number of replicas that must acknowledge writes, if set.
    pub min_replicas_to_write: Option<u32>,
    /// Maximum replication lag (in seconds) for a replica to count toward `min-replicas-to-write`, if set.
    pub min_replicas_max_lag: Option<u32>,

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
    /// Path to the cluster config file, if set. Overrides the auto-generated default.
    pub cluster_config_file: Option<PathBuf>,
    /// Whether full hash slot coverage is required for the cluster to accept writes, if set.
    pub cluster_require_full_coverage: Option<bool>,
    /// Whether reads are allowed when the cluster is down, if set.
    pub cluster_allow_reads_when_down: Option<bool>,
    /// Whether pubsub shard channels are allowed when the cluster is down, if set.
    pub cluster_allow_pubsubshard_when_down: Option<bool>,
    /// Whether automatic replica migration is allowed, if set.
    pub cluster_allow_replica_migration: Option<bool>,
    /// Minimum number of replicas a master must have before one can migrate, if set.
    pub cluster_migration_barrier: Option<u32>,
    /// Whether this replica will never attempt a failover, if set.
    pub cluster_replica_no_failover: Option<bool>,
    /// Factor multiplied by node timeout to determine replica validity, if set.
    pub cluster_replica_validity_factor: Option<u32>,
    /// IP address this node announces to the cluster bus, if set.
    pub cluster_announce_ip: Option<String>,
    /// Client port this node announces to the cluster, if set.
    pub cluster_announce_port: Option<u16>,
    /// Cluster bus port this node announces, if set.
    pub cluster_announce_bus_port: Option<u16>,
    /// TLS port this node announces to the cluster, if set.
    pub cluster_announce_tls_port: Option<u16>,
    /// Hostname this node announces to the cluster, if set.
    pub cluster_announce_hostname: Option<String>,
    /// Human-readable node name announced to the cluster, if set.
    pub cluster_announce_human_nodename: Option<String>,
    /// Dedicated cluster bus port, if set (0 = auto, default offset +10000).
    pub cluster_port: Option<u16>,
    /// Preferred endpoint type for cluster redirections, if set (e.g. `"ip"`, `"hostname"`).
    pub cluster_preferred_endpoint_type: Option<String>,
    /// Send buffer limit in bytes for cluster bus links, if set.
    pub cluster_link_sendbuf_limit: Option<u64>,
    /// Compatibility sample ratio percentage, if set.
    pub cluster_compatibility_sample_ratio: Option<u32>,
    /// Maximum lag in bytes before slot migration handoff, if set.
    pub cluster_slot_migration_handoff_max_lag_bytes: Option<u64>,
    /// Write pause timeout in milliseconds during slot migration, if set.
    pub cluster_slot_migration_write_pause_timeout: Option<u64>,
    /// Whether per-slot statistics are enabled, if set.
    pub cluster_slot_stats_enabled: Option<bool>,

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

/// Diskless load policy for replicas.
///
/// Controls how a replica loads the RDB payload received from a master during
/// diskless replication.
#[derive(Debug, Clone, Copy)]
pub enum ReplDisklessLoad {
    /// Never load the RDB directly from the socket (write to disk first).
    Disabled,
    /// Load directly from the socket only when the current dataset is empty.
    OnEmptyDb,
    /// Load directly from the socket, swapping the dataset atomically.
    Swapdb,
}

impl std::fmt::Display for ReplDisklessLoad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplDisklessLoad::Disabled => f.write_str("disabled"),
            ReplDisklessLoad::OnEmptyDb => f.write_str("on-empty-db"),
            ReplDisklessLoad::Swapdb => f.write_str("swapdb"),
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
            tls_key_file_pass: None,
            tls_ca_cert_file: None,
            tls_ca_cert_dir: None,
            tls_auth_clients: None,
            tls_client_cert_file: None,
            tls_client_key_file: None,
            tls_client_key_file_pass: None,
            tls_dh_params_file: None,
            tls_ciphers: None,
            tls_ciphersuites: None,
            tls_protocols: None,
            tls_prefer_server_ciphers: None,
            tls_session_caching: None,
            tls_session_cache_size: None,
            tls_session_cache_timeout: None,
            tls_replication: None,
            tls_cluster: None,
            daemonize: true,
            dir: std::env::temp_dir().join("redis-server-wrapper"),
            logfile: None,
            loglevel: LogLevel::Notice,
            databases: None,
            maxmemory: None,
            maxmemory_policy: None,
            maxmemory_samples: None,
            maxmemory_clients: None,
            maxmemory_eviction_tenacity: None,
            maxclients: None,
            lfu_log_factor: None,
            lfu_decay_time: None,
            active_expire_effort: None,
            lazyfree_lazy_eviction: None,
            lazyfree_lazy_expire: None,
            lazyfree_lazy_server_del: None,
            lazyfree_lazy_user_del: None,
            lazyfree_lazy_user_flush: None,
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
            masteruser: None,
            repl_backlog_size: None,
            repl_backlog_ttl: None,
            repl_disable_tcp_nodelay: None,
            repl_diskless_load: None,
            repl_diskless_sync: None,
            repl_diskless_sync_delay: None,
            repl_diskless_sync_max_replicas: None,
            repl_ping_replica_period: None,
            repl_timeout: None,
            replica_announce_ip: None,
            replica_announce_port: None,
            replica_announced: None,
            replica_full_sync_buffer_limit: None,
            replica_ignore_disk_write_errors: None,
            replica_ignore_maxmemory: None,
            replica_lazy_flush: None,
            replica_priority: None,
            replica_read_only: None,
            replica_serve_stale_data: None,
            min_replicas_to_write: None,
            min_replicas_max_lag: None,
            password: None,
            acl_file: None,
            cluster_enabled: false,
            cluster_node_timeout: None,
            cluster_config_file: None,
            cluster_require_full_coverage: None,
            cluster_allow_reads_when_down: None,
            cluster_allow_pubsubshard_when_down: None,
            cluster_allow_replica_migration: None,
            cluster_migration_barrier: None,
            cluster_replica_no_failover: None,
            cluster_replica_validity_factor: None,
            cluster_announce_ip: None,
            cluster_announce_port: None,
            cluster_announce_bus_port: None,
            cluster_announce_tls_port: None,
            cluster_announce_hostname: None,
            cluster_announce_human_nodename: None,
            cluster_port: None,
            cluster_preferred_endpoint_type: None,
            cluster_link_sendbuf_limit: None,
            cluster_compatibility_sample_ratio: None,
            cluster_slot_migration_handoff_max_lag_bytes: None,
            cluster_slot_migration_write_pause_timeout: None,
            cluster_slot_stats_enabled: None,
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

    /// Set the passphrase for the TLS private key file.
    pub fn tls_key_file_pass(mut self, pass: impl Into<String>) -> Self {
        self.config.tls_key_file_pass = Some(pass.into());
        self
    }

    /// Set the TLS CA certificate directory path.
    pub fn tls_ca_cert_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_ca_cert_dir = Some(path.into());
        self
    }

    /// Set the TLS client certificate file path (for outgoing connections).
    pub fn tls_client_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_client_cert_file = Some(path.into());
        self
    }

    /// Set the TLS client private key file path (for outgoing connections).
    pub fn tls_client_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_client_key_file = Some(path.into());
        self
    }

    /// Set the passphrase for the TLS client private key file.
    pub fn tls_client_key_file_pass(mut self, pass: impl Into<String>) -> Self {
        self.config.tls_client_key_file_pass = Some(pass.into());
        self
    }

    /// Set the DH parameters file path for DHE ciphers.
    pub fn tls_dh_params_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.tls_dh_params_file = Some(path.into());
        self
    }

    /// Set the allowed TLS 1.2 ciphers (OpenSSL cipher list format).
    pub fn tls_ciphers(mut self, ciphers: impl Into<String>) -> Self {
        self.config.tls_ciphers = Some(ciphers.into());
        self
    }

    /// Set the allowed TLS 1.3 ciphersuites (colon-separated).
    pub fn tls_ciphersuites(mut self, suites: impl Into<String>) -> Self {
        self.config.tls_ciphersuites = Some(suites.into());
        self
    }

    /// Set the allowed TLS protocol versions (e.g. `"TLSv1.2 TLSv1.3"`).
    pub fn tls_protocols(mut self, protocols: impl Into<String>) -> Self {
        self.config.tls_protocols = Some(protocols.into());
        self
    }

    /// Prefer the server's cipher order over the client's.
    pub fn tls_prefer_server_ciphers(mut self, prefer: bool) -> Self {
        self.config.tls_prefer_server_ciphers = Some(prefer);
        self
    }

    /// Enable or disable TLS session caching.
    pub fn tls_session_caching(mut self, enable: bool) -> Self {
        self.config.tls_session_caching = Some(enable);
        self
    }

    /// Set the number of entries in the TLS session cache.
    pub fn tls_session_cache_size(mut self, size: u32) -> Self {
        self.config.tls_session_cache_size = Some(size);
        self
    }

    /// Set the timeout in seconds for cached TLS sessions.
    pub fn tls_session_cache_timeout(mut self, seconds: u32) -> Self {
        self.config.tls_session_cache_timeout = Some(seconds);
        self
    }

    /// Enable TLS for replication traffic.
    pub fn tls_replication(mut self, enable: bool) -> Self {
        self.config.tls_replication = Some(enable);
        self
    }

    /// Enable TLS for cluster bus communication.
    pub fn tls_cluster(mut self, enable: bool) -> Self {
        self.config.tls_cluster = Some(enable);
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

    /// Set the number of keys sampled per eviction round (Redis default: 5).
    pub fn maxmemory_samples(mut self, n: u32) -> Self {
        self.config.maxmemory_samples = Some(n);
        self
    }

    /// Set per-client memory limit (e.g. `"0"` to disable).
    pub fn maxmemory_clients(mut self, limit: impl Into<String>) -> Self {
        self.config.maxmemory_clients = Some(limit.into());
        self
    }

    /// Set eviction processing effort (1-100, Redis default: 10).
    pub fn maxmemory_eviction_tenacity(mut self, tenacity: u32) -> Self {
        self.config.maxmemory_eviction_tenacity = Some(tenacity);
        self
    }

    /// Set the maximum number of simultaneous client connections.
    pub fn maxclients(mut self, n: u32) -> Self {
        self.config.maxclients = Some(n);
        self
    }

    /// Set the logarithmic factor for the LFU frequency counter (Redis default: 10).
    pub fn lfu_log_factor(mut self, factor: u32) -> Self {
        self.config.lfu_log_factor = Some(factor);
        self
    }

    /// Set the LFU counter decay time in minutes (Redis default: 1).
    pub fn lfu_decay_time(mut self, minutes: u32) -> Self {
        self.config.lfu_decay_time = Some(minutes);
        self
    }

    /// Set the effort spent on active key expiration (1-100, Redis default: 10).
    pub fn active_expire_effort(mut self, effort: u32) -> Self {
        self.config.active_expire_effort = Some(effort);
        self
    }

    // -- lazyfree --

    /// Enable or disable background deletion during eviction.
    pub fn lazyfree_lazy_eviction(mut self, enable: bool) -> Self {
        self.config.lazyfree_lazy_eviction = Some(enable);
        self
    }

    /// Enable or disable background deletion of expired keys.
    pub fn lazyfree_lazy_expire(mut self, enable: bool) -> Self {
        self.config.lazyfree_lazy_expire = Some(enable);
        self
    }

    /// Enable or disable background deletion for implicit `DEL` (e.g. `RENAME`).
    pub fn lazyfree_lazy_server_del(mut self, enable: bool) -> Self {
        self.config.lazyfree_lazy_server_del = Some(enable);
        self
    }

    /// Make explicit `DEL` behave like `UNLINK` (background deletion).
    pub fn lazyfree_lazy_user_del(mut self, enable: bool) -> Self {
        self.config.lazyfree_lazy_user_del = Some(enable);
        self
    }

    /// Make `FLUSHDB`/`FLUSHALL` default to `ASYNC`.
    pub fn lazyfree_lazy_user_flush(mut self, enable: bool) -> Self {
        self.config.lazyfree_lazy_user_flush = Some(enable);
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

    /// Set the username for authenticating with a master (ACL-based auth).
    pub fn masteruser(mut self, user: impl Into<String>) -> Self {
        self.config.masteruser = Some(user.into());
        self
    }

    /// Set the replication backlog size (e.g. `"1mb"`).
    pub fn repl_backlog_size(mut self, size: impl Into<String>) -> Self {
        self.config.repl_backlog_size = Some(size.into());
        self
    }

    /// Set seconds before the backlog is freed when no replicas are connected.
    pub fn repl_backlog_ttl(mut self, seconds: u32) -> Self {
        self.config.repl_backlog_ttl = Some(seconds);
        self
    }

    /// Disable TCP_NODELAY on the replication socket.
    pub fn repl_disable_tcp_nodelay(mut self, disable: bool) -> Self {
        self.config.repl_disable_tcp_nodelay = Some(disable);
        self
    }

    /// Set the diskless load policy for replicas.
    pub fn repl_diskless_load(mut self, policy: ReplDisklessLoad) -> Self {
        self.config.repl_diskless_load = Some(policy);
        self
    }

    /// Enable or disable diskless sync from master to replicas.
    pub fn repl_diskless_sync(mut self, enable: bool) -> Self {
        self.config.repl_diskless_sync = Some(enable);
        self
    }

    /// Set the delay in seconds before starting a diskless sync.
    pub fn repl_diskless_sync_delay(mut self, seconds: u32) -> Self {
        self.config.repl_diskless_sync_delay = Some(seconds);
        self
    }

    /// Set the maximum number of replicas to wait for before starting a diskless sync.
    pub fn repl_diskless_sync_max_replicas(mut self, n: u32) -> Self {
        self.config.repl_diskless_sync_max_replicas = Some(n);
        self
    }

    /// Set the interval in seconds between PING commands sent to the master.
    pub fn repl_ping_replica_period(mut self, seconds: u32) -> Self {
        self.config.repl_ping_replica_period = Some(seconds);
        self
    }

    /// Set the replication timeout in seconds.
    pub fn repl_timeout(mut self, seconds: u32) -> Self {
        self.config.repl_timeout = Some(seconds);
        self
    }

    /// Set the IP address a replica announces to the master.
    pub fn replica_announce_ip(mut self, ip: impl Into<String>) -> Self {
        self.config.replica_announce_ip = Some(ip.into());
        self
    }

    /// Set the port a replica announces to the master.
    pub fn replica_announce_port(mut self, port: u16) -> Self {
        self.config.replica_announce_port = Some(port);
        self
    }

    /// Control whether the replica is announced to clients.
    pub fn replica_announced(mut self, announced: bool) -> Self {
        self.config.replica_announced = Some(announced);
        self
    }

    /// Set the buffer limit for full synchronization on replicas (e.g. `"256mb"`).
    pub fn replica_full_sync_buffer_limit(mut self, size: impl Into<String>) -> Self {
        self.config.replica_full_sync_buffer_limit = Some(size.into());
        self
    }

    /// Control whether replicas ignore disk-write errors.
    pub fn replica_ignore_disk_write_errors(mut self, ignore: bool) -> Self {
        self.config.replica_ignore_disk_write_errors = Some(ignore);
        self
    }

    /// Control whether replicas ignore the maxmemory setting.
    pub fn replica_ignore_maxmemory(mut self, ignore: bool) -> Self {
        self.config.replica_ignore_maxmemory = Some(ignore);
        self
    }

    /// Enable or disable lazy flush on replicas during full sync.
    pub fn replica_lazy_flush(mut self, enable: bool) -> Self {
        self.config.replica_lazy_flush = Some(enable);
        self
    }

    /// Set the replica priority for Sentinel promotion.
    pub fn replica_priority(mut self, priority: u32) -> Self {
        self.config.replica_priority = Some(priority);
        self
    }

    /// Control whether the replica is read-only.
    pub fn replica_read_only(mut self, read_only: bool) -> Self {
        self.config.replica_read_only = Some(read_only);
        self
    }

    /// Control whether the replica serves stale data while syncing.
    pub fn replica_serve_stale_data(mut self, serve: bool) -> Self {
        self.config.replica_serve_stale_data = Some(serve);
        self
    }

    /// Set the minimum number of replicas that must acknowledge writes.
    pub fn min_replicas_to_write(mut self, n: u32) -> Self {
        self.config.min_replicas_to_write = Some(n);
        self
    }

    /// Set the maximum replication lag (in seconds) for a replica to count toward `min-replicas-to-write`.
    pub fn min_replicas_max_lag(mut self, seconds: u32) -> Self {
        self.config.min_replicas_max_lag = Some(seconds);
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

    /// Set a custom cluster config file path (overrides auto-generated default).
    pub fn cluster_config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.cluster_config_file = Some(path.into());
        self
    }

    /// Require full hash slot coverage for the cluster to accept writes.
    pub fn cluster_require_full_coverage(mut self, require: bool) -> Self {
        self.config.cluster_require_full_coverage = Some(require);
        self
    }

    /// Allow reads when the cluster is down.
    pub fn cluster_allow_reads_when_down(mut self, allow: bool) -> Self {
        self.config.cluster_allow_reads_when_down = Some(allow);
        self
    }

    /// Allow pubsub shard channels when the cluster is down.
    pub fn cluster_allow_pubsubshard_when_down(mut self, allow: bool) -> Self {
        self.config.cluster_allow_pubsubshard_when_down = Some(allow);
        self
    }

    /// Allow automatic replica migration between masters.
    pub fn cluster_allow_replica_migration(mut self, allow: bool) -> Self {
        self.config.cluster_allow_replica_migration = Some(allow);
        self
    }

    /// Set the minimum number of replicas a master must retain before one can migrate.
    pub fn cluster_migration_barrier(mut self, barrier: u32) -> Self {
        self.config.cluster_migration_barrier = Some(barrier);
        self
    }

    /// Prevent this replica from ever attempting a failover.
    pub fn cluster_replica_no_failover(mut self, no_failover: bool) -> Self {
        self.config.cluster_replica_no_failover = Some(no_failover);
        self
    }

    /// Set the replica validity factor (multiplied by node timeout).
    pub fn cluster_replica_validity_factor(mut self, factor: u32) -> Self {
        self.config.cluster_replica_validity_factor = Some(factor);
        self
    }

    /// Set the IP address this node announces to the cluster bus.
    pub fn cluster_announce_ip(mut self, ip: impl Into<String>) -> Self {
        self.config.cluster_announce_ip = Some(ip.into());
        self
    }

    /// Set the client port this node announces to the cluster.
    pub fn cluster_announce_port(mut self, port: u16) -> Self {
        self.config.cluster_announce_port = Some(port);
        self
    }

    /// Set the cluster bus port this node announces.
    pub fn cluster_announce_bus_port(mut self, port: u16) -> Self {
        self.config.cluster_announce_bus_port = Some(port);
        self
    }

    /// Set the TLS port this node announces to the cluster.
    pub fn cluster_announce_tls_port(mut self, port: u16) -> Self {
        self.config.cluster_announce_tls_port = Some(port);
        self
    }

    /// Set the hostname this node announces to the cluster.
    pub fn cluster_announce_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.config.cluster_announce_hostname = Some(hostname.into());
        self
    }

    /// Set the human-readable node name announced to the cluster.
    pub fn cluster_announce_human_nodename(mut self, name: impl Into<String>) -> Self {
        self.config.cluster_announce_human_nodename = Some(name.into());
        self
    }

    /// Set the dedicated cluster bus port (0 = auto with +10000 offset).
    pub fn cluster_port(mut self, port: u16) -> Self {
        self.config.cluster_port = Some(port);
        self
    }

    /// Set the preferred endpoint type for cluster redirections (e.g. `"ip"`, `"hostname"`).
    pub fn cluster_preferred_endpoint_type(mut self, endpoint_type: impl Into<String>) -> Self {
        self.config.cluster_preferred_endpoint_type = Some(endpoint_type.into());
        self
    }

    /// Set the send buffer limit in bytes for cluster bus links.
    pub fn cluster_link_sendbuf_limit(mut self, limit: u64) -> Self {
        self.config.cluster_link_sendbuf_limit = Some(limit);
        self
    }

    /// Set the compatibility sample ratio percentage.
    pub fn cluster_compatibility_sample_ratio(mut self, ratio: u32) -> Self {
        self.config.cluster_compatibility_sample_ratio = Some(ratio);
        self
    }

    /// Set the maximum lag in bytes before slot migration handoff.
    pub fn cluster_slot_migration_handoff_max_lag_bytes(mut self, bytes: u64) -> Self {
        self.config.cluster_slot_migration_handoff_max_lag_bytes = Some(bytes);
        self
    }

    /// Set the write pause timeout in milliseconds during slot migration.
    pub fn cluster_slot_migration_write_pause_timeout(mut self, ms: u64) -> Self {
        self.config.cluster_slot_migration_write_pause_timeout = Some(ms);
        self
    }

    /// Enable per-slot statistics collection.
    pub fn cluster_slot_stats_enabled(mut self, enable: bool) -> Self {
        self.config.cluster_slot_stats_enabled = Some(enable);
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
        if let Some(ref pass) = self.config.tls_key_file_pass {
            conf.push_str(&format!("tls-key-file-pass {pass}\n"));
        }
        if let Some(ref path) = self.config.tls_ca_cert_file {
            conf.push_str(&format!("tls-ca-cert-file {}\n", path.display()));
        }
        if let Some(ref path) = self.config.tls_ca_cert_dir {
            conf.push_str(&format!("tls-ca-cert-dir {}\n", path.display()));
        }
        if let Some(auth) = self.config.tls_auth_clients {
            conf.push_str(&format!("tls-auth-clients {}\n", yn(auth)));
        }
        if let Some(ref path) = self.config.tls_client_cert_file {
            conf.push_str(&format!("tls-client-cert-file {}\n", path.display()));
        }
        if let Some(ref path) = self.config.tls_client_key_file {
            conf.push_str(&format!("tls-client-key-file {}\n", path.display()));
        }
        if let Some(ref pass) = self.config.tls_client_key_file_pass {
            conf.push_str(&format!("tls-client-key-file-pass {pass}\n"));
        }
        if let Some(ref path) = self.config.tls_dh_params_file {
            conf.push_str(&format!("tls-dh-params-file {}\n", path.display()));
        }
        if let Some(ref ciphers) = self.config.tls_ciphers {
            conf.push_str(&format!("tls-ciphers {ciphers}\n"));
        }
        if let Some(ref suites) = self.config.tls_ciphersuites {
            conf.push_str(&format!("tls-ciphersuites {suites}\n"));
        }
        if let Some(ref protocols) = self.config.tls_protocols {
            conf.push_str(&format!("tls-protocols {protocols}\n"));
        }
        if let Some(v) = self.config.tls_prefer_server_ciphers {
            conf.push_str(&format!("tls-prefer-server-ciphers {}\n", yn(v)));
        }
        if let Some(v) = self.config.tls_session_caching {
            conf.push_str(&format!("tls-session-caching {}\n", yn(v)));
        }
        if let Some(size) = self.config.tls_session_cache_size {
            conf.push_str(&format!("tls-session-cache-size {size}\n"));
        }
        if let Some(timeout) = self.config.tls_session_cache_timeout {
            conf.push_str(&format!("tls-session-cache-timeout {timeout}\n"));
        }
        if let Some(v) = self.config.tls_replication {
            conf.push_str(&format!("tls-replication {}\n", yn(v)));
        }
        if let Some(v) = self.config.tls_cluster {
            conf.push_str(&format!("tls-cluster {}\n", yn(v)));
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
        if let Some(n) = self.config.maxmemory_samples {
            conf.push_str(&format!("maxmemory-samples {n}\n"));
        }
        if let Some(ref limit) = self.config.maxmemory_clients {
            conf.push_str(&format!("maxmemory-clients {limit}\n"));
        }
        if let Some(n) = self.config.maxmemory_eviction_tenacity {
            conf.push_str(&format!("maxmemory-eviction-tenacity {n}\n"));
        }
        if let Some(n) = self.config.maxclients {
            conf.push_str(&format!("maxclients {n}\n"));
        }
        if let Some(n) = self.config.lfu_log_factor {
            conf.push_str(&format!("lfu-log-factor {n}\n"));
        }
        if let Some(n) = self.config.lfu_decay_time {
            conf.push_str(&format!("lfu-decay-time {n}\n"));
        }
        if let Some(n) = self.config.active_expire_effort {
            conf.push_str(&format!("active-expire-effort {n}\n"));
        }

        // -- lazyfree --
        if let Some(v) = self.config.lazyfree_lazy_eviction {
            conf.push_str(&format!("lazyfree-lazy-eviction {}\n", yn(v)));
        }
        if let Some(v) = self.config.lazyfree_lazy_expire {
            conf.push_str(&format!("lazyfree-lazy-expire {}\n", yn(v)));
        }
        if let Some(v) = self.config.lazyfree_lazy_server_del {
            conf.push_str(&format!("lazyfree-lazy-server-del {}\n", yn(v)));
        }
        if let Some(v) = self.config.lazyfree_lazy_user_del {
            conf.push_str(&format!("lazyfree-lazy-user-del {}\n", yn(v)));
        }
        if let Some(v) = self.config.lazyfree_lazy_user_flush {
            conf.push_str(&format!("lazyfree-lazy-user-flush {}\n", yn(v)));
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
        if let Some(ref user) = self.config.masteruser {
            conf.push_str(&format!("masteruser {user}\n"));
        }
        if let Some(ref size) = self.config.repl_backlog_size {
            conf.push_str(&format!("repl-backlog-size {size}\n"));
        }
        if let Some(ttl) = self.config.repl_backlog_ttl {
            conf.push_str(&format!("repl-backlog-ttl {ttl}\n"));
        }
        if let Some(v) = self.config.repl_disable_tcp_nodelay {
            conf.push_str(&format!("repl-disable-tcp-nodelay {}\n", yn(v)));
        }
        if let Some(ref policy) = self.config.repl_diskless_load {
            conf.push_str(&format!("repl-diskless-load {policy}\n"));
        }
        if let Some(v) = self.config.repl_diskless_sync {
            conf.push_str(&format!("repl-diskless-sync {}\n", yn(v)));
        }
        if let Some(delay) = self.config.repl_diskless_sync_delay {
            conf.push_str(&format!("repl-diskless-sync-delay {delay}\n"));
        }
        if let Some(n) = self.config.repl_diskless_sync_max_replicas {
            conf.push_str(&format!("repl-diskless-sync-max-replicas {n}\n"));
        }
        if let Some(period) = self.config.repl_ping_replica_period {
            conf.push_str(&format!("repl-ping-replica-period {period}\n"));
        }
        if let Some(t) = self.config.repl_timeout {
            conf.push_str(&format!("repl-timeout {t}\n"));
        }
        if let Some(ref ip) = self.config.replica_announce_ip {
            conf.push_str(&format!("replica-announce-ip {ip}\n"));
        }
        if let Some(port) = self.config.replica_announce_port {
            conf.push_str(&format!("replica-announce-port {port}\n"));
        }
        if let Some(v) = self.config.replica_announced {
            conf.push_str(&format!("replica-announced {}\n", yn(v)));
        }
        if let Some(ref size) = self.config.replica_full_sync_buffer_limit {
            conf.push_str(&format!("replica-full-sync-buffer-limit {size}\n"));
        }
        if let Some(v) = self.config.replica_ignore_disk_write_errors {
            conf.push_str(&format!("replica-ignore-disk-write-errors {}\n", yn(v)));
        }
        if let Some(v) = self.config.replica_ignore_maxmemory {
            conf.push_str(&format!("replica-ignore-maxmemory {}\n", yn(v)));
        }
        if let Some(v) = self.config.replica_lazy_flush {
            conf.push_str(&format!("replica-lazy-flush {}\n", yn(v)));
        }
        if let Some(priority) = self.config.replica_priority {
            conf.push_str(&format!("replica-priority {priority}\n"));
        }
        if let Some(v) = self.config.replica_read_only {
            conf.push_str(&format!("replica-read-only {}\n", yn(v)));
        }
        if let Some(v) = self.config.replica_serve_stale_data {
            conf.push_str(&format!("replica-serve-stale-data {}\n", yn(v)));
        }
        if let Some(n) = self.config.min_replicas_to_write {
            conf.push_str(&format!("min-replicas-to-write {n}\n"));
        }
        if let Some(lag) = self.config.min_replicas_max_lag {
            conf.push_str(&format!("min-replicas-max-lag {lag}\n"));
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
            if let Some(ref path) = self.config.cluster_config_file {
                conf.push_str(&format!("cluster-config-file {}\n", path.display()));
            } else {
                conf.push_str(&format!(
                    "cluster-config-file {}/nodes.conf\n",
                    node_dir.display()
                ));
            }
            if let Some(timeout) = self.config.cluster_node_timeout {
                conf.push_str(&format!("cluster-node-timeout {timeout}\n"));
            }
            if let Some(v) = self.config.cluster_require_full_coverage {
                conf.push_str(&format!("cluster-require-full-coverage {}\n", yn(v)));
            }
            if let Some(v) = self.config.cluster_allow_reads_when_down {
                conf.push_str(&format!("cluster-allow-reads-when-down {}\n", yn(v)));
            }
            if let Some(v) = self.config.cluster_allow_pubsubshard_when_down {
                conf.push_str(&format!("cluster-allow-pubsubshard-when-down {}\n", yn(v)));
            }
            if let Some(v) = self.config.cluster_allow_replica_migration {
                conf.push_str(&format!("cluster-allow-replica-migration {}\n", yn(v)));
            }
            if let Some(barrier) = self.config.cluster_migration_barrier {
                conf.push_str(&format!("cluster-migration-barrier {barrier}\n"));
            }
            if let Some(v) = self.config.cluster_replica_no_failover {
                conf.push_str(&format!("cluster-replica-no-failover {}\n", yn(v)));
            }
            if let Some(factor) = self.config.cluster_replica_validity_factor {
                conf.push_str(&format!("cluster-replica-validity-factor {factor}\n"));
            }
            if let Some(ref ip) = self.config.cluster_announce_ip {
                conf.push_str(&format!("cluster-announce-ip {ip}\n"));
            }
            if let Some(port) = self.config.cluster_announce_port {
                conf.push_str(&format!("cluster-announce-port {port}\n"));
            }
            if let Some(port) = self.config.cluster_announce_bus_port {
                conf.push_str(&format!("cluster-announce-bus-port {port}\n"));
            }
            if let Some(port) = self.config.cluster_announce_tls_port {
                conf.push_str(&format!("cluster-announce-tls-port {port}\n"));
            }
            if let Some(ref hostname) = self.config.cluster_announce_hostname {
                conf.push_str(&format!("cluster-announce-hostname {hostname}\n"));
            }
            if let Some(ref name) = self.config.cluster_announce_human_nodename {
                conf.push_str(&format!("cluster-announce-human-nodename {name}\n"));
            }
            if let Some(port) = self.config.cluster_port {
                conf.push_str(&format!("cluster-port {port}\n"));
            }
            if let Some(ref endpoint_type) = self.config.cluster_preferred_endpoint_type {
                conf.push_str(&format!(
                    "cluster-preferred-endpoint-type {endpoint_type}\n"
                ));
            }
            if let Some(limit) = self.config.cluster_link_sendbuf_limit {
                conf.push_str(&format!("cluster-link-sendbuf-limit {limit}\n"));
            }
            if let Some(ratio) = self.config.cluster_compatibility_sample_ratio {
                conf.push_str(&format!("cluster-compatibility-sample-ratio {ratio}\n"));
            }
            if let Some(bytes) = self.config.cluster_slot_migration_handoff_max_lag_bytes {
                conf.push_str(&format!(
                    "cluster-slot-migration-handoff-max-lag-bytes {bytes}\n"
                ));
            }
            if let Some(ms) = self.config.cluster_slot_migration_write_pause_timeout {
                conf.push_str(&format!(
                    "cluster-slot-migration-write-pause-timeout {ms}\n"
                ));
            }
            if let Some(v) = self.config.cluster_slot_stats_enabled {
                conf.push_str(&format!("cluster-slot-stats-enabled {}\n", yn(v)));
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
    fn memory_eviction_and_lazyfree() {
        let s = RedisServer::new()
            .maxmemory("256mb")
            .maxmemory_policy("allkeys-lfu")
            .maxmemory_samples(10)
            .maxmemory_clients("0")
            .maxmemory_eviction_tenacity(50)
            .lfu_log_factor(10)
            .lfu_decay_time(1)
            .active_expire_effort(25)
            .lazyfree_lazy_eviction(true)
            .lazyfree_lazy_expire(true)
            .lazyfree_lazy_server_del(true)
            .lazyfree_lazy_user_del(false)
            .lazyfree_lazy_user_flush(true);

        assert_eq!(s.config.maxmemory.as_deref(), Some("256mb"));
        assert_eq!(s.config.maxmemory_policy.as_deref(), Some("allkeys-lfu"));
        assert_eq!(s.config.maxmemory_samples, Some(10));
        assert_eq!(s.config.maxmemory_clients.as_deref(), Some("0"));
        assert_eq!(s.config.maxmemory_eviction_tenacity, Some(50));
        assert_eq!(s.config.lfu_log_factor, Some(10));
        assert_eq!(s.config.lfu_decay_time, Some(1));
        assert_eq!(s.config.active_expire_effort, Some(25));
        assert_eq!(s.config.lazyfree_lazy_eviction, Some(true));
        assert_eq!(s.config.lazyfree_lazy_expire, Some(true));
        assert_eq!(s.config.lazyfree_lazy_server_del, Some(true));
        assert_eq!(s.config.lazyfree_lazy_user_del, Some(false));
        assert_eq!(s.config.lazyfree_lazy_user_flush, Some(true));
    }

    #[test]
    fn replication_tuning() {
        let s = RedisServer::new()
            .replicaof("127.0.0.1", 6379)
            .masterauth("secret")
            .masteruser("repl-user")
            .repl_backlog_size("1mb")
            .repl_backlog_ttl(3600)
            .repl_disable_tcp_nodelay(true)
            .repl_diskless_load(ReplDisklessLoad::Swapdb)
            .repl_diskless_sync(true)
            .repl_diskless_sync_delay(5)
            .repl_diskless_sync_max_replicas(3)
            .repl_ping_replica_period(10)
            .repl_timeout(60)
            .replica_announce_ip("10.0.0.1")
            .replica_announce_port(6380)
            .replica_announced(true)
            .replica_full_sync_buffer_limit("256mb")
            .replica_ignore_disk_write_errors(false)
            .replica_ignore_maxmemory(true)
            .replica_lazy_flush(true)
            .replica_priority(100)
            .replica_read_only(true)
            .replica_serve_stale_data(false)
            .min_replicas_to_write(2)
            .min_replicas_max_lag(10);

        assert_eq!(s.config.replicaof, Some(("127.0.0.1".into(), 6379)));
        assert_eq!(s.config.masterauth.as_deref(), Some("secret"));
        assert_eq!(s.config.masteruser.as_deref(), Some("repl-user"));
        assert_eq!(s.config.repl_backlog_size.as_deref(), Some("1mb"));
        assert_eq!(s.config.repl_backlog_ttl, Some(3600));
        assert_eq!(s.config.repl_disable_tcp_nodelay, Some(true));
        assert!(matches!(
            s.config.repl_diskless_load,
            Some(ReplDisklessLoad::Swapdb)
        ));
        assert_eq!(s.config.repl_diskless_sync, Some(true));
        assert_eq!(s.config.repl_diskless_sync_delay, Some(5));
        assert_eq!(s.config.repl_diskless_sync_max_replicas, Some(3));
        assert_eq!(s.config.repl_ping_replica_period, Some(10));
        assert_eq!(s.config.repl_timeout, Some(60));
        assert_eq!(s.config.replica_announce_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(s.config.replica_announce_port, Some(6380));
        assert_eq!(s.config.replica_announced, Some(true));
        assert_eq!(
            s.config.replica_full_sync_buffer_limit.as_deref(),
            Some("256mb")
        );
        assert_eq!(s.config.replica_ignore_disk_write_errors, Some(false));
        assert_eq!(s.config.replica_ignore_maxmemory, Some(true));
        assert_eq!(s.config.replica_lazy_flush, Some(true));
        assert_eq!(s.config.replica_priority, Some(100));
        assert_eq!(s.config.replica_read_only, Some(true));
        assert_eq!(s.config.replica_serve_stale_data, Some(false));
        assert_eq!(s.config.min_replicas_to_write, Some(2));
        assert_eq!(s.config.min_replicas_max_lag, Some(10));
    }

    #[test]
    fn cluster_config() {
        let s = RedisServer::new()
            .port(7000)
            .cluster_enabled(true)
            .cluster_node_timeout(5000)
            .cluster_config_file("/tmp/nodes.conf")
            .cluster_require_full_coverage(false)
            .cluster_allow_reads_when_down(true)
            .cluster_allow_pubsubshard_when_down(true)
            .cluster_allow_replica_migration(true)
            .cluster_migration_barrier(1)
            .cluster_replica_no_failover(false)
            .cluster_replica_validity_factor(10)
            .cluster_announce_ip("10.0.0.1")
            .cluster_announce_port(7000)
            .cluster_announce_bus_port(17000)
            .cluster_announce_tls_port(7100)
            .cluster_announce_hostname("node1.example.com")
            .cluster_announce_human_nodename("node-1")
            .cluster_port(17000)
            .cluster_preferred_endpoint_type("ip")
            .cluster_link_sendbuf_limit(67108864)
            .cluster_compatibility_sample_ratio(50)
            .cluster_slot_migration_handoff_max_lag_bytes(1048576)
            .cluster_slot_migration_write_pause_timeout(5000)
            .cluster_slot_stats_enabled(true);

        assert!(s.config.cluster_enabled);
        assert_eq!(s.config.cluster_node_timeout, Some(5000));
        assert_eq!(
            s.config.cluster_config_file,
            Some(PathBuf::from("/tmp/nodes.conf"))
        );
        assert_eq!(s.config.cluster_require_full_coverage, Some(false));
        assert_eq!(s.config.cluster_allow_reads_when_down, Some(true));
        assert_eq!(s.config.cluster_allow_pubsubshard_when_down, Some(true));
        assert_eq!(s.config.cluster_allow_replica_migration, Some(true));
        assert_eq!(s.config.cluster_migration_barrier, Some(1));
        assert_eq!(s.config.cluster_replica_no_failover, Some(false));
        assert_eq!(s.config.cluster_replica_validity_factor, Some(10));
        assert_eq!(s.config.cluster_announce_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(s.config.cluster_announce_port, Some(7000));
        assert_eq!(s.config.cluster_announce_bus_port, Some(17000));
        assert_eq!(s.config.cluster_announce_tls_port, Some(7100));
        assert_eq!(
            s.config.cluster_announce_hostname.as_deref(),
            Some("node1.example.com")
        );
        assert_eq!(
            s.config.cluster_announce_human_nodename.as_deref(),
            Some("node-1")
        );
        assert_eq!(s.config.cluster_port, Some(17000));
        assert_eq!(
            s.config.cluster_preferred_endpoint_type.as_deref(),
            Some("ip")
        );
        assert_eq!(s.config.cluster_link_sendbuf_limit, Some(67108864));
        assert_eq!(s.config.cluster_compatibility_sample_ratio, Some(50));
        assert_eq!(
            s.config.cluster_slot_migration_handoff_max_lag_bytes,
            Some(1048576)
        );
        assert_eq!(
            s.config.cluster_slot_migration_write_pause_timeout,
            Some(5000)
        );
        assert_eq!(s.config.cluster_slot_stats_enabled, Some(true));
    }

    #[test]
    fn tls_config() {
        let s = RedisServer::new()
            .port(6400)
            .tls_port(6401)
            .tls_cert_file("/etc/tls/redis.crt")
            .tls_key_file("/etc/tls/redis.key")
            .tls_key_file_pass("keypass")
            .tls_ca_cert_file("/etc/tls/ca.crt")
            .tls_ca_cert_dir("/etc/tls/certs")
            .tls_auth_clients(true)
            .tls_client_cert_file("/etc/tls/client.crt")
            .tls_client_key_file("/etc/tls/client.key")
            .tls_client_key_file_pass("clientpass")
            .tls_dh_params_file("/etc/tls/dhparams.pem")
            .tls_ciphers("ECDHE-RSA-AES256-GCM-SHA384")
            .tls_ciphersuites("TLS_AES_256_GCM_SHA384")
            .tls_protocols("TLSv1.2 TLSv1.3")
            .tls_prefer_server_ciphers(true)
            .tls_session_caching(true)
            .tls_session_cache_size(20480)
            .tls_session_cache_timeout(300)
            .tls_replication(true)
            .tls_cluster(true);

        assert_eq!(s.config.tls_port, Some(6401));
        assert_eq!(
            s.config.tls_cert_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/redis.crt"))
        );
        assert_eq!(
            s.config.tls_key_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/redis.key"))
        );
        assert_eq!(s.config.tls_key_file_pass.as_deref(), Some("keypass"));
        assert_eq!(
            s.config.tls_ca_cert_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/ca.crt"))
        );
        assert_eq!(
            s.config.tls_ca_cert_dir.as_deref(),
            Some(std::path::Path::new("/etc/tls/certs"))
        );
        assert_eq!(s.config.tls_auth_clients, Some(true));
        assert_eq!(
            s.config.tls_client_cert_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/client.crt"))
        );
        assert_eq!(
            s.config.tls_client_key_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/client.key"))
        );
        assert_eq!(
            s.config.tls_client_key_file_pass.as_deref(),
            Some("clientpass")
        );
        assert_eq!(
            s.config.tls_dh_params_file.as_deref(),
            Some(std::path::Path::new("/etc/tls/dhparams.pem"))
        );
        assert_eq!(
            s.config.tls_ciphers.as_deref(),
            Some("ECDHE-RSA-AES256-GCM-SHA384")
        );
        assert_eq!(
            s.config.tls_ciphersuites.as_deref(),
            Some("TLS_AES_256_GCM_SHA384")
        );
        assert_eq!(s.config.tls_protocols.as_deref(), Some("TLSv1.2 TLSv1.3"));
        assert_eq!(s.config.tls_prefer_server_ciphers, Some(true));
        assert_eq!(s.config.tls_session_caching, Some(true));
        assert_eq!(s.config.tls_session_cache_size, Some(20480));
        assert_eq!(s.config.tls_session_cache_timeout, Some(300));
        assert_eq!(s.config.tls_replication, Some(true));
        assert_eq!(s.config.tls_cluster, Some(true));
    }
}
