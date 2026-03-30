//! Synchronous (blocking) wrappers for redis-server-wrapper types.
//!
//! Enable with the `blocking` Cargo feature.  Every async operation is driven
//! by a [`tokio::runtime::Runtime`]: handles own a long-lived runtime so that
//! the underlying async handle (and its `Drop` impl) keeps working correctly.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::runtime::Runtime;

use crate::error::Result;
use crate::server::{AppendFsync, LogLevel, ReplDisklessLoad};
use crate::{cli, cluster, sentinel, server};

// ── RedisCli ──────────────────────────────────────────────────────────────────

/// Synchronous wrapper for [`crate::RedisCli`].
///
/// Each method on this type blocks the calling thread by running the
/// corresponding async operation on a temporary [`Runtime`].  For running
/// many commands against a single server, prefer the async API which shares a
/// long-lived runtime.
pub struct RedisCli {
    inner: cli::RedisCli,
}

impl RedisCli {
    /// Create a new `redis-cli` builder with defaults (localhost:6379).
    pub fn new() -> Self {
        Self {
            inner: cli::RedisCli::new(),
        }
    }

    /// Set the `redis-cli` binary path.
    pub fn bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.bin(bin);
        self
    }

    /// Set the host to connect to.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.inner = self.inner.host(host);
        self
    }

    /// Set the port to connect to.
    pub fn port(mut self, port: u16) -> Self {
        self.inner = self.inner.port(port);
        self
    }

    /// Set the password for AUTH.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.password(password);
        self
    }

    /// Set the ACL username for AUTH.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.inner = self.inner.user(user);
        self
    }

    /// Select a database number.
    pub fn db(mut self, db: u32) -> Self {
        self.inner = self.inner.db(db);
        self
    }

    /// Connect via a Unix socket instead of TCP.
    pub fn unixsocket(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.unixsocket(path);
        self
    }

    /// Enable TLS for the connection.
    pub fn tls(mut self, enable: bool) -> Self {
        self.inner = self.inner.tls(enable);
        self
    }

    /// Set the SNI hostname for TLS.
    pub fn sni(mut self, hostname: impl Into<String>) -> Self {
        self.inner = self.inner.sni(hostname);
        self
    }

    /// Set the CA certificate file for TLS verification.
    pub fn cacert(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.cacert(path);
        self
    }

    /// Set the CA certificate directory for TLS verification.
    pub fn cacertdir(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.cacertdir(path);
        self
    }

    /// Set the client certificate file for TLS.
    pub fn cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.cert(path);
        self
    }

    /// Set the client private key file for TLS.
    pub fn key(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.key(path);
        self
    }

    /// Skip TLS certificate verification (`--insecure`).
    pub fn insecure(mut self, enable: bool) -> Self {
        self.inner = self.inner.insecure(enable);
        self
    }

    /// Set the allowed TLS 1.2 ciphers (`--tls-ciphers`).
    pub fn tls_ciphers(mut self, ciphers: impl Into<String>) -> Self {
        self.inner = self.inner.tls_ciphers(ciphers);
        self
    }

    /// Set the allowed TLS 1.3 ciphersuites (`--tls-ciphersuites`).
    pub fn tls_ciphersuites(mut self, ciphersuites: impl Into<String>) -> Self {
        self.inner = self.inner.tls_ciphersuites(ciphersuites);
        self
    }

    /// Set the RESP protocol version.
    pub fn resp(mut self, protocol: cli::RespProtocol) -> Self {
        self.inner = self.inner.resp(protocol);
        self
    }

    /// Enable cluster mode (`-c` flag) for following redirects.
    pub fn cluster_mode(mut self, enable: bool) -> Self {
        self.inner = self.inner.cluster_mode(enable);
        self
    }

    /// Set the output format.
    pub fn output_format(mut self, format: cli::OutputFormat) -> Self {
        self.inner = self.inner.output_format(format);
        self
    }

    /// Suppress the AUTH password warning.
    pub fn no_auth_warning(mut self, suppress: bool) -> Self {
        self.inner = self.inner.no_auth_warning(suppress);
        self
    }

    /// Set the server URI (`-u`), e.g. `redis://user:pass@host:port/db`.
    pub fn uri(mut self, uri: impl Into<String>) -> Self {
        self.inner = self.inner.uri(uri);
        self
    }

    /// Set the connection timeout in seconds (`-t`).
    pub fn timeout(mut self, seconds: f64) -> Self {
        self.inner = self.inner.timeout(seconds);
        self
    }

    /// Prompt for password from stdin (`--askpass`).
    pub fn askpass(mut self, enable: bool) -> Self {
        self.inner = self.inner.askpass(enable);
        self
    }

    /// Set the client connection name (`--name`).
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.client_name(name);
        self
    }

    /// Set IP version preference for connections.
    pub fn ip_preference(mut self, preference: cli::IpPreference) -> Self {
        self.inner = self.inner.ip_preference(preference);
        self
    }

    /// Execute the command N times (`-r`).
    pub fn repeat(mut self, count: u32) -> Self {
        self.inner = self.inner.repeat(count);
        self
    }

    /// Set interval in seconds between repeated commands (`-i`).
    pub fn interval(mut self, seconds: f64) -> Self {
        self.inner = self.inner.interval(seconds);
        self
    }

    /// Run a command and return stdout on success.
    pub fn run(&self, args: &[&str]) -> Result<String> {
        Runtime::new()?.block_on(self.inner.run(args))
    }

    /// Send PING and return true if PONG is received.
    pub fn ping(&self) -> bool {
        Runtime::new()
            .map(|rt| rt.block_on(self.inner.ping()))
            .unwrap_or(false)
    }

    /// Wait until the server responds to PING or timeout expires.
    pub fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        Runtime::new()?.block_on(self.inner.wait_for_ready(timeout))
    }
}

impl Default for RedisCli {
    fn default() -> Self {
        Self::new()
    }
}

// ── RedisServer ───────────────────────────────────────────────────────────────

/// Synchronous builder for a Redis server process.
///
/// All builder methods mirror [`crate::RedisServer`].  Call [`start`] to
/// launch the server; it blocks until the server is ready.
///
/// [`start`]: RedisServer::start
pub struct RedisServer {
    inner: server::RedisServer,
}

impl RedisServer {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            inner: server::RedisServer::new(),
        }
    }

    // -- network --

    /// Set the listening port (default: 6379).
    pub fn port(mut self, port: u16) -> Self {
        self.inner = self.inner.port(port);
        self
    }

    /// Set the bind address (default: `127.0.0.1`).
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.inner = self.inner.bind(bind);
        self
    }

    /// Enable or disable protected mode.
    pub fn protected_mode(mut self, protected: bool) -> Self {
        self.inner = self.inner.protected_mode(protected);
        self
    }

    /// Set the TCP backlog queue length.
    pub fn tcp_backlog(mut self, backlog: u32) -> Self {
        self.inner = self.inner.tcp_backlog(backlog);
        self
    }

    /// Set a Unix socket path for connections.
    pub fn unixsocket(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.unixsocket(path);
        self
    }

    /// Set Unix socket permissions.
    pub fn unixsocketperm(mut self, perm: u32) -> Self {
        self.inner = self.inner.unixsocketperm(perm);
        self
    }

    /// Close idle client connections after this many seconds (0 = disabled).
    pub fn timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.timeout(seconds);
        self
    }

    /// Set TCP keepalive interval in seconds.
    pub fn tcp_keepalive(mut self, seconds: u32) -> Self {
        self.inner = self.inner.tcp_keepalive(seconds);
        self
    }

    // -- tls --

    /// Set TLS listening port.
    pub fn tls_port(mut self, port: u16) -> Self {
        self.inner = self.inner.tls_port(port);
        self
    }

    /// Set the TLS certificate file path.
    pub fn tls_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_cert_file(path);
        self
    }

    /// Set the TLS private key file path.
    pub fn tls_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_key_file(path);
        self
    }

    /// Set the TLS CA certificate file path.
    pub fn tls_ca_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_ca_cert_file(path);
        self
    }

    /// Require TLS client authentication.
    pub fn tls_auth_clients(mut self, require: bool) -> Self {
        self.inner = self.inner.tls_auth_clients(require);
        self
    }

    // -- general --

    /// Set the working directory for data files.
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.dir(dir);
        self
    }

    /// Set the log level.
    pub fn loglevel(mut self, level: LogLevel) -> Self {
        self.inner = self.inner.loglevel(level);
        self
    }

    /// Set the log file path.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.inner = self.inner.logfile(path);
        self
    }

    /// Set the number of databases.
    pub fn databases(mut self, n: u32) -> Self {
        self.inner = self.inner.databases(n);
        self
    }

    // -- memory --

    /// Set the maximum memory limit (e.g. `"256mb"`).
    pub fn maxmemory(mut self, limit: impl Into<String>) -> Self {
        self.inner = self.inner.maxmemory(limit);
        self
    }

    /// Set the eviction policy when maxmemory is reached.
    pub fn maxmemory_policy(mut self, policy: impl Into<String>) -> Self {
        self.inner = self.inner.maxmemory_policy(policy);
        self
    }

    /// Set the maximum number of simultaneous client connections.
    pub fn maxclients(mut self, n: u32) -> Self {
        self.inner = self.inner.maxclients(n);
        self
    }

    // -- persistence --

    /// Enable or disable RDB snapshots.
    pub fn save(mut self, save: bool) -> Self {
        self.inner = self.inner.save(save);
        self
    }

    /// Set a custom RDB save schedule.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.inner = self.inner.save_schedule(schedule);
        self
    }

    /// Enable or disable AOF persistence.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.inner = self.inner.appendonly(appendonly);
        self
    }

    /// Set the AOF fsync policy.
    pub fn appendfsync(mut self, policy: AppendFsync) -> Self {
        self.inner = self.inner.appendfsync(policy);
        self
    }

    /// Set the AOF filename.
    pub fn appendfilename(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.appendfilename(name);
        self
    }

    /// Set the AOF directory name.
    pub fn appenddirname(mut self, name: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.appenddirname(name);
        self
    }

    /// Enable or disable the RDB preamble in AOF files.
    pub fn aof_use_rdb_preamble(mut self, enable: bool) -> Self {
        self.inner = self.inner.aof_use_rdb_preamble(enable);
        self
    }

    /// Control whether truncated AOF files are loaded.
    pub fn aof_load_truncated(mut self, enable: bool) -> Self {
        self.inner = self.inner.aof_load_truncated(enable);
        self
    }

    /// Set the maximum allowed size of a corrupt AOF tail (e.g. `"32mb"`).
    pub fn aof_load_corrupt_tail_max_size(mut self, size: impl Into<String>) -> Self {
        self.inner = self.inner.aof_load_corrupt_tail_max_size(size);
        self
    }

    /// Enable or disable incremental fsync during AOF rewrites.
    pub fn aof_rewrite_incremental_fsync(mut self, enable: bool) -> Self {
        self.inner = self.inner.aof_rewrite_incremental_fsync(enable);
        self
    }

    /// Enable or disable timestamps in the AOF file.
    pub fn aof_timestamp_enabled(mut self, enable: bool) -> Self {
        self.inner = self.inner.aof_timestamp_enabled(enable);
        self
    }

    /// Set the percentage growth that triggers an automatic AOF rewrite.
    pub fn auto_aof_rewrite_percentage(mut self, pct: u32) -> Self {
        self.inner = self.inner.auto_aof_rewrite_percentage(pct);
        self
    }

    /// Set the minimum AOF size before an automatic rewrite is triggered (e.g. `"64mb"`).
    pub fn auto_aof_rewrite_min_size(mut self, size: impl Into<String>) -> Self {
        self.inner = self.inner.auto_aof_rewrite_min_size(size);
        self
    }

    /// Control whether fsync is suppressed during AOF rewrites.
    pub fn no_appendfsync_on_rewrite(mut self, enable: bool) -> Self {
        self.inner = self.inner.no_appendfsync_on_rewrite(enable);
        self
    }

    // -- replication --

    /// Configure this server as a replica of the given master.
    pub fn replicaof(mut self, host: impl Into<String>, port: u16) -> Self {
        self.inner = self.inner.replicaof(host, port);
        self
    }

    /// Set the password for authenticating with a master.
    pub fn masterauth(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.masterauth(password);
        self
    }

    /// Set the username for authenticating with a master (ACL-based auth).
    pub fn masteruser(mut self, user: impl Into<String>) -> Self {
        self.inner = self.inner.masteruser(user);
        self
    }

    /// Set the replication backlog size (e.g. `"1mb"`).
    pub fn repl_backlog_size(mut self, size: impl Into<String>) -> Self {
        self.inner = self.inner.repl_backlog_size(size);
        self
    }

    /// Set seconds before the backlog is freed when no replicas are connected.
    pub fn repl_backlog_ttl(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_backlog_ttl(seconds);
        self
    }

    /// Disable TCP_NODELAY on the replication socket.
    pub fn repl_disable_tcp_nodelay(mut self, disable: bool) -> Self {
        self.inner = self.inner.repl_disable_tcp_nodelay(disable);
        self
    }

    /// Set the diskless load policy for replicas.
    pub fn repl_diskless_load(mut self, policy: ReplDisklessLoad) -> Self {
        self.inner = self.inner.repl_diskless_load(policy);
        self
    }

    /// Enable or disable diskless sync from master to replicas.
    pub fn repl_diskless_sync(mut self, enable: bool) -> Self {
        self.inner = self.inner.repl_diskless_sync(enable);
        self
    }

    /// Set the delay in seconds before starting a diskless sync.
    pub fn repl_diskless_sync_delay(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_diskless_sync_delay(seconds);
        self
    }

    /// Set the maximum number of replicas to wait for before starting a diskless sync.
    pub fn repl_diskless_sync_max_replicas(mut self, n: u32) -> Self {
        self.inner = self.inner.repl_diskless_sync_max_replicas(n);
        self
    }

    /// Set the interval in seconds between PING commands sent to the master.
    pub fn repl_ping_replica_period(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_ping_replica_period(seconds);
        self
    }

    /// Set the replication timeout in seconds.
    pub fn repl_timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_timeout(seconds);
        self
    }

    /// Set the IP address a replica announces to the master.
    pub fn replica_announce_ip(mut self, ip: impl Into<String>) -> Self {
        self.inner = self.inner.replica_announce_ip(ip);
        self
    }

    /// Set the port a replica announces to the master.
    pub fn replica_announce_port(mut self, port: u16) -> Self {
        self.inner = self.inner.replica_announce_port(port);
        self
    }

    /// Control whether the replica is announced to clients.
    pub fn replica_announced(mut self, announced: bool) -> Self {
        self.inner = self.inner.replica_announced(announced);
        self
    }

    /// Set the buffer limit for full synchronization on replicas (e.g. `"256mb"`).
    pub fn replica_full_sync_buffer_limit(mut self, size: impl Into<String>) -> Self {
        self.inner = self.inner.replica_full_sync_buffer_limit(size);
        self
    }

    /// Control whether replicas ignore disk-write errors.
    pub fn replica_ignore_disk_write_errors(mut self, ignore: bool) -> Self {
        self.inner = self.inner.replica_ignore_disk_write_errors(ignore);
        self
    }

    /// Control whether replicas ignore the maxmemory setting.
    pub fn replica_ignore_maxmemory(mut self, ignore: bool) -> Self {
        self.inner = self.inner.replica_ignore_maxmemory(ignore);
        self
    }

    /// Enable or disable lazy flush on replicas during full sync.
    pub fn replica_lazy_flush(mut self, enable: bool) -> Self {
        self.inner = self.inner.replica_lazy_flush(enable);
        self
    }

    /// Set the replica priority for Sentinel promotion.
    pub fn replica_priority(mut self, priority: u32) -> Self {
        self.inner = self.inner.replica_priority(priority);
        self
    }

    /// Control whether the replica is read-only.
    pub fn replica_read_only(mut self, read_only: bool) -> Self {
        self.inner = self.inner.replica_read_only(read_only);
        self
    }

    /// Control whether the replica serves stale data while syncing.
    pub fn replica_serve_stale_data(mut self, serve: bool) -> Self {
        self.inner = self.inner.replica_serve_stale_data(serve);
        self
    }

    /// Set the minimum number of replicas that must acknowledge writes.
    pub fn min_replicas_to_write(mut self, n: u32) -> Self {
        self.inner = self.inner.min_replicas_to_write(n);
        self
    }

    /// Set the maximum replication lag (in seconds) for a replica to count toward `min-replicas-to-write`.
    pub fn min_replicas_max_lag(mut self, seconds: u32) -> Self {
        self.inner = self.inner.min_replicas_max_lag(seconds);
        self
    }

    // -- security --

    /// Set a `requirepass` password for client connections.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.password(password);
        self
    }

    /// Set the path to an ACL file.
    pub fn acl_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.acl_file(path);
        self
    }

    // -- cluster --

    /// Enable Redis Cluster mode.
    pub fn cluster_enabled(mut self, enabled: bool) -> Self {
        self.inner = self.inner.cluster_enabled(enabled);
        self
    }

    /// Set the cluster node timeout in milliseconds.
    pub fn cluster_node_timeout(mut self, ms: u64) -> Self {
        self.inner = self.inner.cluster_node_timeout(ms);
        self
    }

    // -- modules --

    /// Load a Redis module at startup.
    pub fn loadmodule(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.loadmodule(path);
        self
    }

    // -- advanced --

    /// Set the server tick frequency in Hz.
    pub fn hz(mut self, hz: u32) -> Self {
        self.inner = self.inner.hz(hz);
        self
    }

    /// Set the number of I/O threads.
    pub fn io_threads(mut self, n: u32) -> Self {
        self.inner = self.inner.io_threads(n);
        self
    }

    /// Enable I/O threads for reads as well as writes.
    pub fn io_threads_do_reads(mut self, enable: bool) -> Self {
        self.inner = self.inner.io_threads_do_reads(enable);
        self
    }

    /// Set keyspace notification events.
    pub fn notify_keyspace_events(mut self, events: impl Into<String>) -> Self {
        self.inner = self.inner.notify_keyspace_events(events);
        self
    }

    // -- binary paths --

    /// Set a custom `redis-server` binary path.
    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_server_bin(bin);
        self
    }

    /// Set a custom `redis-cli` binary path.
    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_cli_bin(bin);
        self
    }

    /// Set an arbitrary config directive not covered by dedicated methods.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.extra(key, value);
        self
    }

    /// Start the server. Blocks until the server is ready or an error occurs.
    pub fn start(self) -> Result<RedisServerHandle> {
        let rt = Runtime::new()?;
        let inner = rt.block_on(self.inner.start())?;
        Ok(RedisServerHandle { inner, rt })
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a running Redis server. Stops the server on Drop.
pub struct RedisServerHandle {
    inner: server::RedisServerHandle,
    rt: Runtime,
}

impl RedisServerHandle {
    /// The server's address as "host:port".
    pub fn addr(&self) -> String {
        self.inner.addr()
    }

    /// The server's port.
    pub fn port(&self) -> u16 {
        self.inner.port()
    }

    /// The server's bind address.
    pub fn host(&self) -> &str {
        self.inner.host()
    }

    /// The PID of the `redis-server` process.
    pub fn pid(&self) -> u32 {
        self.inner.pid()
    }

    /// Check if the server is alive via PING.
    pub fn is_alive(&self) -> bool {
        self.rt.block_on(self.inner.is_alive())
    }

    /// Run a redis-cli command against this server.
    pub fn run(&self, args: &[&str]) -> Result<String> {
        self.rt.block_on(self.inner.run(args))
    }

    /// Consume the handle without stopping the server.
    pub fn detach(self) {
        self.inner.detach();
    }

    /// Stop the server via SHUTDOWN NOSAVE.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Wait until the server is ready (PING -> PONG).
    pub fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        self.rt.block_on(self.inner.wait_for_ready(timeout))
    }
}

// ── RedisCluster ──────────────────────────────────────────────────────────────

/// Convenience constructor for the synchronous cluster builder.
pub struct RedisCluster;

impl RedisCluster {
    /// Create a new cluster builder with defaults (3 masters, 0 replicas, port 7000).
    pub fn builder() -> RedisClusterBuilder {
        RedisClusterBuilder {
            inner: cluster::RedisCluster::builder(),
        }
    }
}

/// Synchronous builder for a Redis Cluster.
pub struct RedisClusterBuilder {
    inner: cluster::RedisClusterBuilder,
}

impl RedisClusterBuilder {
    /// Set the number of master nodes.
    pub fn masters(mut self, n: u16) -> Self {
        self.inner = self.inner.masters(n);
        self
    }

    /// Set the number of replicas per master.
    pub fn replicas_per_master(mut self, n: u16) -> Self {
        self.inner = self.inner.replicas_per_master(n);
        self
    }

    /// Set the base port for cluster nodes.
    pub fn base_port(mut self, port: u16) -> Self {
        self.inner = self.inner.base_port(port);
        self
    }

    /// Set the bind address.
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.inner = self.inner.bind(bind);
        self
    }

    /// Set a `requirepass` password for all cluster nodes.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.password(password);
        self
    }

    /// Set the log file path for all cluster nodes.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.inner = self.inner.logfile(path);
        self
    }

    /// Set the RDB save policy for all cluster nodes.
    pub fn save(mut self, save: bool) -> Self {
        self.inner = self.inner.save(save);
        self
    }

    /// Set a custom RDB save schedule for all cluster nodes.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.inner = self.inner.save_schedule(schedule);
        self
    }

    /// Enable or disable AOF persistence for all cluster nodes.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.inner = self.inner.appendonly(appendonly);
        self
    }

    /// Set an arbitrary config directive for all cluster nodes.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.extra(key, value);
        self
    }

    /// Set a custom `redis-server` binary path.
    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_server_bin(bin);
        self
    }

    /// Set a custom `redis-cli` binary path.
    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_cli_bin(bin);
        self
    }

    /// Start all nodes and form the cluster. Blocks until the cluster is ready.
    pub fn start(self) -> Result<RedisClusterHandle> {
        let rt = Runtime::new()?;
        let inner = rt.block_on(self.inner.start())?;
        Ok(RedisClusterHandle { inner, rt })
    }
}

/// Handle to a running Redis Cluster. Stops all nodes on Drop.
pub struct RedisClusterHandle {
    inner: cluster::RedisClusterHandle,
    rt: Runtime,
}

impl RedisClusterHandle {
    /// The seed address (first node).
    pub fn addr(&self) -> String {
        self.inner.addr()
    }

    /// All node addresses.
    pub fn node_addrs(&self) -> Vec<String> {
        self.inner.node_addrs()
    }

    /// The PIDs of all `redis-server` processes in the cluster.
    pub fn pids(&self) -> Vec<u32> {
        self.inner.pids()
    }

    /// Check if all nodes are alive.
    pub fn all_alive(&self) -> bool {
        self.rt.block_on(self.inner.all_alive())
    }

    /// Check CLUSTER INFO for state=ok and all slots assigned.
    pub fn is_healthy(&self) -> bool {
        self.rt.block_on(self.inner.is_healthy())
    }

    /// Wait until the cluster is healthy or timeout.
    pub fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        self.rt.block_on(self.inner.wait_for_healthy(timeout))
    }
}

// ── RedisSentinel ─────────────────────────────────────────────────────────────

/// Convenience constructor for the synchronous sentinel builder.
pub struct RedisSentinel;

impl RedisSentinel {
    /// Create a new sentinel builder with defaults.
    pub fn builder() -> RedisSentinelBuilder {
        RedisSentinelBuilder {
            inner: sentinel::RedisSentinel::builder(),
        }
    }
}

/// Synchronous builder for a Redis Sentinel topology.
pub struct RedisSentinelBuilder {
    inner: sentinel::RedisSentinelBuilder,
}

impl RedisSentinelBuilder {
    /// Set the monitored master name.
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.master_name(name);
        self
    }

    /// Set the master port.
    pub fn master_port(mut self, port: u16) -> Self {
        self.inner = self.inner.master_port(port);
        self
    }

    /// Set the number of replicas.
    pub fn replicas(mut self, n: u16) -> Self {
        self.inner = self.inner.replicas(n);
        self
    }

    /// Set the base port for replica nodes.
    pub fn replica_base_port(mut self, port: u16) -> Self {
        self.inner = self.inner.replica_base_port(port);
        self
    }

    /// Set the number of sentinel processes.
    pub fn sentinels(mut self, n: u16) -> Self {
        self.inner = self.inner.sentinels(n);
        self
    }

    /// Set the base port for sentinel processes.
    pub fn sentinel_base_port(mut self, port: u16) -> Self {
        self.inner = self.inner.sentinel_base_port(port);
        self
    }

    /// Set the quorum count.
    pub fn quorum(mut self, q: u16) -> Self {
        self.inner = self.inner.quorum(q);
        self
    }

    /// Set the bind address.
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.inner = self.inner.bind(bind);
        self
    }

    /// Set the log file path for all processes in the topology.
    pub fn logfile(mut self, path: impl Into<String>) -> Self {
        self.inner = self.inner.logfile(path);
        self
    }

    /// Set down-after-milliseconds for the sentinel.
    pub fn down_after_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.down_after_ms(ms);
        self
    }

    /// Set failover-timeout for the sentinel.
    pub fn failover_timeout_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.failover_timeout_ms(ms);
        self
    }

    /// Add an additional master for the sentinels to monitor.
    pub fn monitor(mut self, name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        self.inner = self.inner.monitor(name, host, port);
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
        self.inner = self
            .inner
            .monitor_with_replicas(name, host, port, expected_replicas);
        self
    }

    /// Set the RDB save policy for all data-bearing processes in the topology.
    pub fn save(mut self, save: bool) -> Self {
        self.inner = self.inner.save(save);
        self
    }

    /// Set a custom RDB save schedule for all data-bearing processes in the topology.
    pub fn save_schedule(mut self, schedule: Vec<(u64, u64)>) -> Self {
        self.inner = self.inner.save_schedule(schedule);
        self
    }

    /// Enable or disable AOF persistence for all data-bearing processes in the topology.
    pub fn appendonly(mut self, appendonly: bool) -> Self {
        self.inner = self.inner.appendonly(appendonly);
        self
    }

    /// Set an arbitrary config directive for all processes in the topology.
    pub fn extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.extra(key, value);
        self
    }

    /// Set a custom `redis-server` binary path.
    pub fn redis_server_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_server_bin(bin);
        self
    }

    /// Set a custom `redis-cli` binary path.
    pub fn redis_cli_bin(mut self, bin: impl Into<String>) -> Self {
        self.inner = self.inner.redis_cli_bin(bin);
        self
    }

    /// Start the full topology: master, replicas, sentinels. Blocks until ready.
    pub fn start(self) -> Result<RedisSentinelHandle> {
        let rt = Runtime::new()?;
        let inner = rt.block_on(self.inner.start())?;
        Ok(RedisSentinelHandle { inner, rt })
    }
}

/// Handle to a running Redis Sentinel topology. Stops everything on Drop.
pub struct RedisSentinelHandle {
    inner: sentinel::RedisSentinelHandle,
    rt: Runtime,
}

impl RedisSentinelHandle {
    /// The master's address.
    pub fn master_addr(&self) -> String {
        self.inner.master_addr()
    }

    /// All monitored master names.
    pub fn monitored_master_names(&self) -> Vec<&str> {
        self.inner.monitored_master_names()
    }

    /// All monitored master addresses.
    pub fn monitored_master_addrs(&self) -> Vec<String> {
        self.inner.monitored_master_addrs()
    }

    /// All sentinel addresses.
    pub fn sentinel_addrs(&self) -> Vec<String> {
        self.inner.sentinel_addrs()
    }

    /// The PIDs of all processes in the topology (master, replicas, sentinels).
    pub fn pids(&self) -> Vec<u32> {
        self.inner.pids()
    }

    /// The monitored master name.
    pub fn master_name(&self) -> &str {
        self.inner.master_name()
    }

    /// Query a sentinel for the current master status.
    pub fn poke(&self) -> Result<HashMap<String, String>> {
        self.rt.block_on(self.inner.poke())
    }

    /// Query a sentinel for a specific monitored master status.
    pub fn poke_master(&self, master_name: &str) -> Result<HashMap<String, String>> {
        self.rt.block_on(self.inner.poke_master(master_name))
    }

    /// Check if the topology is healthy.
    pub fn is_healthy(&self) -> bool {
        self.rt.block_on(self.inner.is_healthy())
    }

    /// Wait until the topology is healthy or timeout.
    pub fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        self.rt.block_on(self.inner.wait_for_healthy(timeout))
    }

    /// Stop everything.
    pub fn stop(&self) {
        self.inner.stop();
    }
}
