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

    // -- input/output modifiers --

    /// Read last argument from stdin (`-x`).
    pub fn stdin_last_arg(mut self, enable: bool) -> Self {
        self.inner = self.inner.stdin_last_arg(enable);
        self
    }

    /// Read tag argument from stdin (`-X`).
    pub fn stdin_tag_arg(mut self, enable: bool) -> Self {
        self.inner = self.inner.stdin_tag_arg(enable);
        self
    }

    /// Set the multi-bulk delimiter.
    pub fn multi_bulk_delimiter(mut self, delim: impl Into<String>) -> Self {
        self.inner = self.inner.multi_bulk_delimiter(delim);
        self
    }

    /// Set the output delimiter between responses.
    pub fn output_delimiter(mut self, delim: impl Into<String>) -> Self {
        self.inner = self.inner.output_delimiter(delim);
        self
    }

    /// Return exit error code on server errors.
    pub fn exit_error_code(mut self, enable: bool) -> Self {
        self.inner = self.inner.exit_error_code(enable);
        self
    }

    /// Force formatted output even with pipe.
    pub fn no_raw(mut self, enable: bool) -> Self {
        self.inner = self.inner.no_raw(enable);
        self
    }

    /// Force input to be processed as quoted strings.
    pub fn quoted_input(mut self, enable: bool) -> Self {
        self.inner = self.inner.quoted_input(enable);
        self
    }

    /// Show or hide push messages.
    pub fn show_pushes(mut self, enable: bool) -> Self {
        self.inner = self.inner.show_pushes(enable);
        self
    }

    // -- diagnostic/analysis modes --

    /// Enable continuous stat mode.
    pub fn stat(mut self, enable: bool) -> Self {
        self.inner = self.inner.stat(enable);
        self
    }

    /// Enable latency mode.
    pub fn latency(mut self, enable: bool) -> Self {
        self.inner = self.inner.latency(enable);
        self
    }

    /// Enable latency history mode.
    pub fn latency_history(mut self, enable: bool) -> Self {
        self.inner = self.inner.latency_history(enable);
        self
    }

    /// Enable latency distribution mode.
    pub fn latency_dist(mut self, enable: bool) -> Self {
        self.inner = self.inner.latency_dist(enable);
        self
    }

    /// Scan for big keys.
    pub fn bigkeys(mut self, enable: bool) -> Self {
        self.inner = self.inner.bigkeys(enable);
        self
    }

    /// Scan for keys by memory usage.
    pub fn memkeys(mut self, enable: bool) -> Self {
        self.inner = self.inner.memkeys(enable);
        self
    }

    /// Set the sample count for memkeys.
    pub fn memkeys_samples(mut self, n: u32) -> Self {
        self.inner = self.inner.memkeys_samples(n);
        self
    }

    /// Enable key statistics.
    pub fn keystats(mut self, enable: bool) -> Self {
        self.inner = self.inner.keystats(enable);
        self
    }

    /// Set the sample count for keystats.
    pub fn keystats_samples(mut self, n: u32) -> Self {
        self.inner = self.inner.keystats_samples(n);
        self
    }

    /// Scan for hot keys.
    pub fn hotkeys(mut self, enable: bool) -> Self {
        self.inner = self.inner.hotkeys(enable);
        self
    }

    /// Enable scan mode.
    pub fn scan(mut self, enable: bool) -> Self {
        self.inner = self.inner.scan(enable);
        self
    }

    /// Set a pattern filter for scan.
    pub fn pattern(mut self, pat: impl Into<String>) -> Self {
        self.inner = self.inner.pattern(pat);
        self
    }

    /// Set a count hint for scan.
    pub fn count(mut self, n: u32) -> Self {
        self.inner = self.inner.count(n);
        self
    }

    /// Set a quoted pattern for scan.
    pub fn quoted_pattern(mut self, pat: impl Into<String>) -> Self {
        self.inner = self.inner.quoted_pattern(pat);
        self
    }

    /// Set the starting cursor for scan.
    pub fn cursor(mut self, n: u64) -> Self {
        self.inner = self.inner.cursor(n);
        self
    }

    /// Set the top N for keystats.
    pub fn top(mut self, n: u32) -> Self {
        self.inner = self.inner.top(n);
        self
    }

    /// Measure intrinsic system latency for the given number of seconds.
    pub fn intrinsic_latency(mut self, seconds: u32) -> Self {
        self.inner = self.inner.intrinsic_latency(seconds);
        self
    }

    /// Run LRU simulation test with the given number of keys.
    pub fn lru_test(mut self, keys: u64) -> Self {
        self.inner = self.inner.lru_test(keys);
        self
    }

    /// Enable verbose mode.
    pub fn verbose(mut self, enable: bool) -> Self {
        self.inner = self.inner.verbose(enable);
        self
    }

    // -- scripting --

    /// Evaluate a Lua script file.
    pub fn eval_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.eval_file(path);
        self
    }

    /// Enable Lua debugger.
    pub fn ldb(mut self, enable: bool) -> Self {
        self.inner = self.inner.ldb(enable);
        self
    }

    /// Enable Lua debugger in synchronous mode.
    pub fn ldb_sync_mode(mut self, enable: bool) -> Self {
        self.inner = self.inner.ldb_sync_mode(enable);
        self
    }

    // -- persistence tools --

    /// Enable pipe mode for mass-insert.
    pub fn pipe(mut self, enable: bool) -> Self {
        self.inner = self.inner.pipe(enable);
        self
    }

    /// Set the pipe mode timeout in seconds.
    pub fn pipe_timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.pipe_timeout(seconds);
        self
    }

    /// Transfer an RDB dump to a file.
    pub fn rdb(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.rdb(path);
        self
    }

    /// Transfer a functions-only RDB dump.
    pub fn functions_rdb(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.functions_rdb(path);
        self
    }

    // -- other --

    /// Simulate a replica for replication stream.
    pub fn replica(mut self, enable: bool) -> Self {
        self.inner = self.inner.replica(enable);
        self
    }

    /// Run a `redis-cli --cluster <command>` subcommand.
    pub fn cluster_command(&self, command: &str, args: &[&str]) -> Result<String> {
        Runtime::new()?.block_on(self.inner.cluster_command(command, args))
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

    /// Set the passphrase for the TLS private key file.
    pub fn tls_key_file_pass(mut self, pass: impl Into<String>) -> Self {
        self.inner = self.inner.tls_key_file_pass(pass);
        self
    }

    /// Set the TLS CA certificate directory path.
    pub fn tls_ca_cert_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_ca_cert_dir(path);
        self
    }

    /// Set the TLS client certificate file path (for outgoing connections).
    pub fn tls_client_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_client_cert_file(path);
        self
    }

    /// Set the TLS client private key file path (for outgoing connections).
    pub fn tls_client_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_client_key_file(path);
        self
    }

    /// Set the passphrase for the TLS client private key file.
    pub fn tls_client_key_file_pass(mut self, pass: impl Into<String>) -> Self {
        self.inner = self.inner.tls_client_key_file_pass(pass);
        self
    }

    /// Set the DH parameters file path for DHE ciphers.
    pub fn tls_dh_params_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.tls_dh_params_file(path);
        self
    }

    /// Set the allowed TLS 1.2 ciphers (OpenSSL cipher list format).
    pub fn tls_ciphers(mut self, ciphers: impl Into<String>) -> Self {
        self.inner = self.inner.tls_ciphers(ciphers);
        self
    }

    /// Set the allowed TLS 1.3 ciphersuites (colon-separated).
    pub fn tls_ciphersuites(mut self, suites: impl Into<String>) -> Self {
        self.inner = self.inner.tls_ciphersuites(suites);
        self
    }

    /// Set the allowed TLS protocol versions (e.g. `"TLSv1.2 TLSv1.3"`).
    pub fn tls_protocols(mut self, protocols: impl Into<String>) -> Self {
        self.inner = self.inner.tls_protocols(protocols);
        self
    }

    /// Prefer the server's cipher order over the client's.
    pub fn tls_prefer_server_ciphers(mut self, prefer: bool) -> Self {
        self.inner = self.inner.tls_prefer_server_ciphers(prefer);
        self
    }

    /// Enable or disable TLS session caching.
    pub fn tls_session_caching(mut self, enable: bool) -> Self {
        self.inner = self.inner.tls_session_caching(enable);
        self
    }

    /// Set the number of entries in the TLS session cache.
    pub fn tls_session_cache_size(mut self, size: u32) -> Self {
        self.inner = self.inner.tls_session_cache_size(size);
        self
    }

    /// Set the timeout in seconds for cached TLS sessions.
    pub fn tls_session_cache_timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.tls_session_cache_timeout(seconds);
        self
    }

    /// Enable TLS for replication traffic.
    pub fn tls_replication(mut self, enable: bool) -> Self {
        self.inner = self.inner.tls_replication(enable);
        self
    }

    /// Enable TLS for cluster bus communication.
    pub fn tls_cluster(mut self, enable: bool) -> Self {
        self.inner = self.inner.tls_cluster(enable);
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

    // -- slow log --

    /// Set the slow log threshold in microseconds.
    pub fn slowlog_log_slower_than(mut self, us: i64) -> Self {
        self.inner = self.inner.slowlog_log_slower_than(us);
        self
    }

    /// Set the maximum number of slow log entries.
    pub fn slowlog_max_len(mut self, n: u32) -> Self {
        self.inner = self.inner.slowlog_max_len(n);
        self
    }

    // -- latency tracking --

    /// Set the latency monitor threshold in milliseconds.
    pub fn latency_monitor_threshold(mut self, ms: u64) -> Self {
        self.inner = self.inner.latency_monitor_threshold(ms);
        self
    }

    /// Enable or disable the extended latency tracking system.
    pub fn latency_tracking(mut self, enable: bool) -> Self {
        self.inner = self.inner.latency_tracking(enable);
        self
    }

    /// Set percentiles reported by the latency tracking system.
    pub fn latency_tracking_info_percentiles(mut self, percentiles: impl Into<String>) -> Self {
        self.inner = self.inner.latency_tracking_info_percentiles(percentiles);
        self
    }

    // -- active defragmentation --

    /// Enable or disable active defragmentation.
    pub fn activedefrag(mut self, enable: bool) -> Self {
        self.inner = self.inner.activedefrag(enable);
        self
    }

    /// Set the minimum fragmentation waste to start defragmentation.
    pub fn active_defrag_ignore_bytes(mut self, bytes: impl Into<String>) -> Self {
        self.inner = self.inner.active_defrag_ignore_bytes(bytes);
        self
    }

    /// Set the minimum fragmentation percentage to start defragmentation.
    pub fn active_defrag_threshold_lower(mut self, pct: u32) -> Self {
        self.inner = self.inner.active_defrag_threshold_lower(pct);
        self
    }

    /// Set the fragmentation percentage at which maximum effort is used.
    pub fn active_defrag_threshold_upper(mut self, pct: u32) -> Self {
        self.inner = self.inner.active_defrag_threshold_upper(pct);
        self
    }

    /// Set the minimal CPU effort for defragmentation (percentage).
    pub fn active_defrag_cycle_min(mut self, pct: u32) -> Self {
        self.inner = self.inner.active_defrag_cycle_min(pct);
        self
    }

    /// Set the maximum CPU effort for defragmentation (percentage).
    pub fn active_defrag_cycle_max(mut self, pct: u32) -> Self {
        self.inner = self.inner.active_defrag_cycle_max(pct);
        self
    }

    /// Set the maximum fields processed per defrag scan step.
    pub fn active_defrag_max_scan_fields(mut self, n: u32) -> Self {
        self.inner = self.inner.active_defrag_max_scan_fields(n);
        self
    }

    // -- logging and process --

    /// Enable logging to syslog.
    pub fn syslog_enabled(mut self, enable: bool) -> Self {
        self.inner = self.inner.syslog_enabled(enable);
        self
    }

    /// Set the syslog identity string.
    pub fn syslog_ident(mut self, ident: impl Into<String>) -> Self {
        self.inner = self.inner.syslog_ident(ident);
        self
    }

    /// Set the syslog facility.
    pub fn syslog_facility(mut self, facility: impl Into<String>) -> Self {
        self.inner = self.inner.syslog_facility(facility);
        self
    }

    /// Set the supervision mode.
    pub fn supervised(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.supervised(mode);
        self
    }

    /// Show the Redis logo on startup.
    pub fn always_show_logo(mut self, enable: bool) -> Self {
        self.inner = self.inner.always_show_logo(enable);
        self
    }

    /// Enable setting the process title.
    pub fn set_proc_title(mut self, enable: bool) -> Self {
        self.inner = self.inner.set_proc_title(enable);
        self
    }

    /// Set the process title template.
    pub fn proc_title_template(mut self, template: impl Into<String>) -> Self {
        self.inner = self.inner.proc_title_template(template);
        self
    }

    // -- security and ACL --

    /// Set the default pub/sub ACL permissions.
    pub fn acl_pubsub_default(mut self, default: impl Into<String>) -> Self {
        self.inner = self.inner.acl_pubsub_default(default);
        self
    }

    /// Set the maximum length of the ACL log.
    pub fn acllog_max_len(mut self, n: u32) -> Self {
        self.inner = self.inner.acllog_max_len(n);
        self
    }

    /// Enable the DEBUG command.
    pub fn enable_debug_command(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.enable_debug_command(mode);
        self
    }

    /// Enable the MODULE command.
    pub fn enable_module_command(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.enable_module_command(mode);
        self
    }

    /// Allow CONFIG SET to modify protected configs.
    pub fn enable_protected_configs(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.enable_protected_configs(mode);
        self
    }

    /// Rename a command. Pass an empty new name to disable it.
    pub fn rename_command(
        mut self,
        command: impl Into<String>,
        new_name: impl Into<String>,
    ) -> Self {
        self.inner = self.inner.rename_command(command, new_name);
        self
    }

    /// Set dump payload sanitization mode.
    pub fn sanitize_dump_payload(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.sanitize_dump_payload(mode);
        self
    }

    /// Hide user data from log messages.
    pub fn hide_user_data_from_log(mut self, enable: bool) -> Self {
        self.inner = self.inner.hide_user_data_from_log(enable);
        self
    }

    // -- networking (additional) --

    /// Set the source address for outgoing connections.
    pub fn bind_source_addr(mut self, addr: impl Into<String>) -> Self {
        self.inner = self.inner.bind_source_addr(addr);
        self
    }

    /// Set the busy reply threshold in milliseconds.
    pub fn busy_reply_threshold(mut self, ms: u64) -> Self {
        self.inner = self.inner.busy_reply_threshold(ms);
        self
    }

    /// Add a client output buffer limit.
    pub fn client_output_buffer_limit(mut self, limit: impl Into<String>) -> Self {
        self.inner = self.inner.client_output_buffer_limit(limit);
        self
    }

    /// Set the maximum size of a single client query buffer.
    pub fn client_query_buffer_limit(mut self, limit: impl Into<String>) -> Self {
        self.inner = self.inner.client_query_buffer_limit(limit);
        self
    }

    /// Set the maximum size of a single protocol bulk request.
    pub fn proto_max_bulk_len(mut self, len: impl Into<String>) -> Self {
        self.inner = self.inner.proto_max_bulk_len(len);
        self
    }

    /// Set the maximum number of new connections per event loop cycle.
    pub fn max_new_connections_per_cycle(mut self, n: u32) -> Self {
        self.inner = self.inner.max_new_connections_per_cycle(n);
        self
    }

    /// Set the maximum number of new TLS connections per event loop cycle.
    pub fn max_new_tls_connections_per_cycle(mut self, n: u32) -> Self {
        self.inner = self.inner.max_new_tls_connections_per_cycle(n);
        self
    }

    /// Set the socket mark ID for outgoing connections.
    pub fn socket_mark_id(mut self, id: u32) -> Self {
        self.inner = self.inner.socket_mark_id(id);
        self
    }

    // -- RDB (additional) --

    /// Set the RDB dump filename.
    pub fn dbfilename(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.dbfilename(name);
        self
    }

    /// Enable or disable RDB compression.
    pub fn rdbcompression(mut self, enable: bool) -> Self {
        self.inner = self.inner.rdbcompression(enable);
        self
    }

    /// Enable or disable RDB checksum.
    pub fn rdbchecksum(mut self, enable: bool) -> Self {
        self.inner = self.inner.rdbchecksum(enable);
        self
    }

    /// Enable incremental fsync during RDB save.
    pub fn rdb_save_incremental_fsync(mut self, enable: bool) -> Self {
        self.inner = self.inner.rdb_save_incremental_fsync(enable);
        self
    }

    /// Delete RDB sync files used by diskless replication.
    pub fn rdb_del_sync_files(mut self, enable: bool) -> Self {
        self.inner = self.inner.rdb_del_sync_files(enable);
        self
    }

    /// Stop accepting writes when bgsave fails.
    pub fn stop_writes_on_bgsave_error(mut self, enable: bool) -> Self {
        self.inner = self.inner.stop_writes_on_bgsave_error(enable);
        self
    }

    // -- shutdown --

    /// Set shutdown behavior on SIGINT.
    pub fn shutdown_on_sigint(mut self, behavior: impl Into<String>) -> Self {
        self.inner = self.inner.shutdown_on_sigint(behavior);
        self
    }

    /// Set shutdown behavior on SIGTERM.
    pub fn shutdown_on_sigterm(mut self, behavior: impl Into<String>) -> Self {
        self.inner = self.inner.shutdown_on_sigterm(behavior);
        self
    }

    /// Set the maximum seconds to wait during shutdown for lagging replicas.
    pub fn shutdown_timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.shutdown_timeout(seconds);
        self
    }

    // -- other --

    /// Enable or disable active rehashing.
    pub fn activerehashing(mut self, enable: bool) -> Self {
        self.inner = self.inner.activerehashing(enable);
        self
    }

    /// Enable crash log on crash.
    pub fn crash_log_enabled(mut self, enable: bool) -> Self {
        self.inner = self.inner.crash_log_enabled(enable);
        self
    }

    /// Enable crash memory check on crash.
    pub fn crash_memcheck_enabled(mut self, enable: bool) -> Self {
        self.inner = self.inner.crash_memcheck_enabled(enable);
        self
    }

    /// Disable transparent huge pages.
    pub fn disable_thp(mut self, enable: bool) -> Self {
        self.inner = self.inner.disable_thp(enable);
        self
    }

    /// Enable dynamic Hz adjustment.
    pub fn dynamic_hz(mut self, enable: bool) -> Self {
        self.inner = self.inner.dynamic_hz(enable);
        self
    }

    /// Ignore specific warnings.
    pub fn ignore_warnings(mut self, warning: impl Into<String>) -> Self {
        self.inner = self.inner.ignore_warnings(warning);
        self
    }

    /// Include another config file.
    pub fn include(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.include(path);
        self
    }

    /// Enable or disable jemalloc background thread.
    pub fn jemalloc_bg_thread(mut self, enable: bool) -> Self {
        self.inner = self.inner.jemalloc_bg_thread(enable);
        self
    }

    /// Set the locale collation setting.
    pub fn locale_collate(mut self, locale: impl Into<String>) -> Self {
        self.inner = self.inner.locale_collate(locale);
        self
    }

    /// Set the Lua script time limit in milliseconds.
    pub fn lua_time_limit(mut self, ms: u64) -> Self {
        self.inner = self.inner.lua_time_limit(ms);
        self
    }

    /// Set the OOM score adjustment mode.
    pub fn oom_score_adj(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.oom_score_adj(mode);
        self
    }

    /// Set the OOM score adjustment values.
    pub fn oom_score_adj_values(mut self, values: impl Into<String>) -> Self {
        self.inner = self.inner.oom_score_adj_values(values);
        self
    }

    /// Set the propagation error behavior.
    pub fn propagation_error_behavior(mut self, behavior: impl Into<String>) -> Self {
        self.inner = self.inner.propagation_error_behavior(behavior);
        self
    }

    /// Set the maximum number of keys in the tracking table.
    pub fn tracking_table_max_keys(mut self, n: u64) -> Self {
        self.inner = self.inner.tracking_table_max_keys(n);
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

    // -- cluster directives --

    /// Set the cluster node timeout in milliseconds for all nodes.
    pub fn cluster_node_timeout(mut self, ms: u64) -> Self {
        self.inner = self.inner.cluster_node_timeout(ms);
        self
    }

    /// Require full hash slot coverage for the cluster to accept writes.
    pub fn cluster_require_full_coverage(mut self, require: bool) -> Self {
        self.inner = self.inner.cluster_require_full_coverage(require);
        self
    }

    /// Allow reads when the cluster is down.
    pub fn cluster_allow_reads_when_down(mut self, allow: bool) -> Self {
        self.inner = self.inner.cluster_allow_reads_when_down(allow);
        self
    }

    /// Allow pubsub shard channels when the cluster is down.
    pub fn cluster_allow_pubsubshard_when_down(mut self, allow: bool) -> Self {
        self.inner = self.inner.cluster_allow_pubsubshard_when_down(allow);
        self
    }

    /// Allow automatic replica migration between masters.
    pub fn cluster_allow_replica_migration(mut self, allow: bool) -> Self {
        self.inner = self.inner.cluster_allow_replica_migration(allow);
        self
    }

    /// Set the minimum replicas a master must retain before one can migrate.
    pub fn cluster_migration_barrier(mut self, barrier: u32) -> Self {
        self.inner = self.inner.cluster_migration_barrier(barrier);
        self
    }

    /// Set the hostname each node announces to the cluster.
    pub fn cluster_announce_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.inner = self.inner.cluster_announce_hostname(hostname);
        self
    }

    /// Set a friendly node name broadcast for debugging/admin display.
    pub fn cluster_announce_human_nodename(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.cluster_announce_human_nodename(name);
        self
    }

    /// Set the preferred endpoint type for cluster redirections.
    pub fn cluster_preferred_endpoint_type(mut self, endpoint_type: impl Into<String>) -> Self {
        self.inner = self.inner.cluster_preferred_endpoint_type(endpoint_type);
        self
    }

    /// Prevent replicas from attempting automatic failover.
    pub fn cluster_replica_no_failover(mut self, no_failover: bool) -> Self {
        self.inner = self.inner.cluster_replica_no_failover(no_failover);
        self
    }

    /// Set the replica validity factor for failover eligibility.
    pub fn cluster_replica_validity_factor(mut self, factor: u32) -> Self {
        self.inner = self.inner.cluster_replica_validity_factor(factor);
        self
    }

    /// Set the IP address nodes announce for client redirects.
    pub fn cluster_announce_ip(mut self, ip: impl Into<String>) -> Self {
        self.inner = self.inner.cluster_announce_ip(ip);
        self
    }

    /// Set the client port nodes announce for redirects.
    pub fn cluster_announce_port(mut self, port: u16) -> Self {
        self.inner = self.inner.cluster_announce_port(port);
        self
    }

    /// Set the cluster bus port nodes announce for gossip.
    pub fn cluster_announce_bus_port(mut self, port: u16) -> Self {
        self.inner = self.inner.cluster_announce_bus_port(port);
        self
    }

    /// Set the TLS client port nodes announce for redirects.
    pub fn cluster_announce_tls_port(mut self, port: u16) -> Self {
        self.inner = self.inner.cluster_announce_tls_port(port);
        self
    }

    /// Set a dedicated cluster bus port.
    pub fn cluster_port(mut self, port: u16) -> Self {
        self.inner = self.inner.cluster_port(port);
        self
    }

    /// Set the maximum memory for a cluster bus link's output buffer.
    pub fn cluster_link_sendbuf_limit(mut self, limit: u64) -> Self {
        self.inner = self.inner.cluster_link_sendbuf_limit(limit);
        self
    }

    /// Set the cluster compatibility sample ratio.
    pub fn cluster_compatibility_sample_ratio(mut self, ratio: u32) -> Self {
        self.inner = self.inner.cluster_compatibility_sample_ratio(ratio);
        self
    }

    /// Set the maximum replication lag in bytes before slot migration handoff.
    pub fn cluster_slot_migration_handoff_max_lag_bytes(mut self, bytes: u64) -> Self {
        self.inner = self
            .inner
            .cluster_slot_migration_handoff_max_lag_bytes(bytes);
        self
    }

    /// Set the write pause timeout in milliseconds during slot migration.
    pub fn cluster_slot_migration_write_pause_timeout(mut self, ms: u64) -> Self {
        self.inner = self.inner.cluster_slot_migration_write_pause_timeout(ms);
        self
    }

    /// Enable per-slot statistics tracking.
    pub fn cluster_slot_stats_enabled(mut self, enable: bool) -> Self {
        self.inner = self.inner.cluster_slot_stats_enabled(enable);
        self
    }

    // -- replication directives --

    /// Set the minimum number of connected replicas before the master accepts writes.
    pub fn min_replicas_to_write(mut self, n: u32) -> Self {
        self.inner = self.inner.min_replicas_to_write(n);
        self
    }

    /// Set the maximum replication lag (seconds) before a replica is considered disconnected.
    pub fn min_replicas_max_lag(mut self, seconds: u32) -> Self {
        self.inner = self.inner.min_replicas_max_lag(seconds);
        self
    }

    /// Enable or disable diskless replication sync.
    pub fn repl_diskless_sync(mut self, enable: bool) -> Self {
        self.inner = self.inner.repl_diskless_sync(enable);
        self
    }

    /// Set the delay in seconds before starting a diskless replication transfer.
    pub fn repl_diskless_sync_delay(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_diskless_sync_delay(seconds);
        self
    }

    /// Set how often replicas ping the master (seconds).
    pub fn repl_ping_replica_period(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_ping_replica_period(seconds);
        self
    }

    /// Set the replication timeout in seconds.
    pub fn repl_timeout(mut self, seconds: u32) -> Self {
        self.inner = self.inner.repl_timeout(seconds);
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

    /// The number of master nodes in the cluster.
    pub fn num_masters(&self) -> u16 {
        self.inner.num_masters()
    }

    /// Run a command against a specific node by index.
    pub fn node_run(&self, index: usize, args: &[&str]) -> Result<String> {
        self.rt.block_on(self.inner.node(index).run(args))
    }

    /// Run `CONFIG SET` on all nodes.
    pub fn config_set_all(&self, key: &str, value: &str) -> Result<()> {
        self.rt.block_on(self.inner.config_set_all(key, value))
    }

    /// Run `CONFIG SET` on master nodes only (initial topology).
    pub fn config_set_masters(&self, key: &str, value: &str) -> Result<()> {
        self.rt.block_on(self.inner.config_set_masters(key, value))
    }

    /// Run `CONFIG SET` on replica nodes only (initial topology).
    pub fn config_set_replicas(&self, key: &str, value: &str) -> Result<()> {
        self.rt.block_on(self.inner.config_set_replicas(key, value))
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
