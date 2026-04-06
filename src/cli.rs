//! Type-safe wrapper for the `redis-cli` command.

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use tokio::process::Command as TokioCommand;

use crate::error::{Error, Result};

/// RESP protocol version for client connections.
#[derive(Debug, Clone, Copy)]
pub enum RespProtocol {
    /// RESP2 (default for most Redis versions).
    Resp2,
    /// RESP3.
    Resp3,
}

/// Output format for `redis-cli` commands.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Default Redis protocol output.
    Default,
    /// Raw output (no formatting).
    Raw,
    /// CSV output.
    Csv,
    /// JSON output.
    Json,
    /// Quoted JSON output.
    QuotedJson,
}

/// Builder for executing `redis-cli` commands.
#[derive(Debug, Clone)]
pub struct RedisCli {
    bin: String,
    host: String,
    port: u16,
    password: Option<String>,
    user: Option<String>,
    db: Option<u32>,
    unixsocket: Option<PathBuf>,
    tls: bool,
    sni: Option<String>,
    cacert: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    resp: Option<RespProtocol>,
    cluster_mode: bool,
    output_format: OutputFormat,
    no_auth_warning: bool,

    // -- input/output modifiers (#46) --
    stdin_last_arg: bool,
    stdin_tag_arg: bool,
    multi_bulk_delimiter: Option<String>,
    output_delimiter: Option<String>,
    exit_error_code: bool,
    no_raw: bool,
    quoted_input: bool,
    show_pushes: Option<bool>,

    // -- diagnostic/analysis modes (#45) --
    stat: bool,
    latency: bool,
    latency_history: bool,
    latency_dist: bool,
    bigkeys: bool,
    memkeys: bool,
    memkeys_samples: Option<u32>,
    keystats: bool,
    keystats_samples: Option<u32>,
    hotkeys: bool,
    scan: bool,
    pattern: Option<String>,
    count: Option<u32>,
    quoted_pattern: Option<String>,
    cursor: Option<u64>,
    top: Option<u32>,
    intrinsic_latency: Option<u32>,
    lru_test: Option<u64>,
    verbose: bool,

    // -- scripting (#48) --
    eval_file: Option<PathBuf>,
    ldb: bool,
    ldb_sync_mode: bool,

    // -- persistence tools (#48) --
    pipe: bool,
    pipe_timeout: Option<u32>,
    rdb: Option<PathBuf>,
    functions_rdb: Option<PathBuf>,

    // -- other (#48) --
    replica: bool,
}

impl RedisCli {
    /// Create a new `redis-cli` builder with defaults (localhost:6379).
    pub fn new() -> Self {
        Self {
            bin: "redis-cli".into(),
            host: "127.0.0.1".into(),
            port: 6379,
            password: None,
            user: None,
            db: None,
            unixsocket: None,
            tls: false,
            sni: None,
            cacert: None,
            cert: None,
            key: None,
            resp: None,
            cluster_mode: false,
            output_format: OutputFormat::Default,
            no_auth_warning: false,
            stdin_last_arg: false,
            stdin_tag_arg: false,
            multi_bulk_delimiter: None,
            output_delimiter: None,
            exit_error_code: false,
            no_raw: false,
            quoted_input: false,
            show_pushes: None,
            stat: false,
            latency: false,
            latency_history: false,
            latency_dist: false,
            bigkeys: false,
            memkeys: false,
            memkeys_samples: None,
            keystats: false,
            keystats_samples: None,
            hotkeys: false,
            scan: false,
            pattern: None,
            count: None,
            quoted_pattern: None,
            cursor: None,
            top: None,
            intrinsic_latency: None,
            lru_test: None,
            verbose: false,
            eval_file: None,
            ldb: false,
            ldb_sync_mode: false,
            pipe: false,
            pipe_timeout: None,
            rdb: None,
            functions_rdb: None,
            replica: false,
        }
    }

    /// Set the `redis-cli` binary path.
    pub fn bin(mut self, bin: impl Into<String>) -> Self {
        self.bin = bin.into();
        self
    }

    /// Set the host to connect to.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port to connect to.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the password for AUTH.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the ACL username for AUTH.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Select a database number.
    pub fn db(mut self, db: u32) -> Self {
        self.db = Some(db);
        self
    }

    /// Connect via a Unix socket instead of TCP.
    pub fn unixsocket(mut self, path: impl Into<PathBuf>) -> Self {
        self.unixsocket = Some(path.into());
        self
    }

    /// Enable TLS for the connection.
    pub fn tls(mut self, enable: bool) -> Self {
        self.tls = enable;
        self
    }

    /// Set the SNI hostname for TLS.
    pub fn sni(mut self, hostname: impl Into<String>) -> Self {
        self.sni = Some(hostname.into());
        self
    }

    /// Set the CA certificate file for TLS verification.
    pub fn cacert(mut self, path: impl Into<PathBuf>) -> Self {
        self.cacert = Some(path.into());
        self
    }

    /// Set the client certificate file for TLS.
    pub fn cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert = Some(path.into());
        self
    }

    /// Set the client private key file for TLS.
    pub fn key(mut self, path: impl Into<PathBuf>) -> Self {
        self.key = Some(path.into());
        self
    }

    /// Set the RESP protocol version.
    pub fn resp(mut self, protocol: RespProtocol) -> Self {
        self.resp = Some(protocol);
        self
    }

    /// Enable cluster mode (`-c` flag) for following redirects.
    pub fn cluster_mode(mut self, enable: bool) -> Self {
        self.cluster_mode = enable;
        self
    }

    /// Set the output format.
    pub fn output_format(mut self, format: OutputFormat) -> Self {
        self.output_format = format;
        self
    }

    /// Suppress the AUTH password warning.
    pub fn no_auth_warning(mut self, suppress: bool) -> Self {
        self.no_auth_warning = suppress;
        self
    }

    // -- input/output modifiers --

    /// Read last argument from stdin (`-x`).
    pub fn stdin_last_arg(mut self, enable: bool) -> Self {
        self.stdin_last_arg = enable;
        self
    }

    /// Read tag argument from stdin (`-X`).
    pub fn stdin_tag_arg(mut self, enable: bool) -> Self {
        self.stdin_tag_arg = enable;
        self
    }

    /// Set the multi-bulk delimiter (`-d`).
    pub fn multi_bulk_delimiter(mut self, delim: impl Into<String>) -> Self {
        self.multi_bulk_delimiter = Some(delim.into());
        self
    }

    /// Set the output delimiter between responses (`-D`).
    pub fn output_delimiter(mut self, delim: impl Into<String>) -> Self {
        self.output_delimiter = Some(delim.into());
        self
    }

    /// Return exit error code on server errors (`-e`).
    pub fn exit_error_code(mut self, enable: bool) -> Self {
        self.exit_error_code = enable;
        self
    }

    /// Force formatted output even with pipe (`--no-raw`).
    pub fn no_raw(mut self, enable: bool) -> Self {
        self.no_raw = enable;
        self
    }

    /// Force input to be processed as quoted strings (`--quoted-input`).
    pub fn quoted_input(mut self, enable: bool) -> Self {
        self.quoted_input = enable;
        self
    }

    /// Show or hide push messages (`--show-pushes`).
    pub fn show_pushes(mut self, enable: bool) -> Self {
        self.show_pushes = Some(enable);
        self
    }

    // -- diagnostic/analysis modes --

    /// Enable continuous stat mode (`--stat`).
    pub fn stat(mut self, enable: bool) -> Self {
        self.stat = enable;
        self
    }

    /// Enable latency mode (`--latency`).
    pub fn latency(mut self, enable: bool) -> Self {
        self.latency = enable;
        self
    }

    /// Enable latency history mode (`--latency-history`).
    pub fn latency_history(mut self, enable: bool) -> Self {
        self.latency_history = enable;
        self
    }

    /// Enable latency distribution mode (`--latency-dist`).
    pub fn latency_dist(mut self, enable: bool) -> Self {
        self.latency_dist = enable;
        self
    }

    /// Scan for big keys (`--bigkeys`).
    pub fn bigkeys(mut self, enable: bool) -> Self {
        self.bigkeys = enable;
        self
    }

    /// Scan for keys by memory usage (`--memkeys`).
    pub fn memkeys(mut self, enable: bool) -> Self {
        self.memkeys = enable;
        self
    }

    /// Set the sample count for memkeys (`--memkeys-samples`).
    pub fn memkeys_samples(mut self, n: u32) -> Self {
        self.memkeys_samples = Some(n);
        self
    }

    /// Enable key statistics (`--keystats`).
    pub fn keystats(mut self, enable: bool) -> Self {
        self.keystats = enable;
        self
    }

    /// Set the sample count for keystats (`--keystats-samples`).
    pub fn keystats_samples(mut self, n: u32) -> Self {
        self.keystats_samples = Some(n);
        self
    }

    /// Scan for hot keys (`--hotkeys`).
    pub fn hotkeys(mut self, enable: bool) -> Self {
        self.hotkeys = enable;
        self
    }

    /// Enable scan mode (`--scan`).
    pub fn scan(mut self, enable: bool) -> Self {
        self.scan = enable;
        self
    }

    /// Set a pattern filter for scan (`--pattern`).
    pub fn pattern(mut self, pat: impl Into<String>) -> Self {
        self.pattern = Some(pat.into());
        self
    }

    /// Set a count hint for scan (`--count`).
    pub fn count(mut self, n: u32) -> Self {
        self.count = Some(n);
        self
    }

    /// Set a quoted pattern for scan (`--quoted-pattern`).
    pub fn quoted_pattern(mut self, pat: impl Into<String>) -> Self {
        self.quoted_pattern = Some(pat.into());
        self
    }

    /// Set the starting cursor for scan (`--cursor`).
    pub fn cursor(mut self, n: u64) -> Self {
        self.cursor = Some(n);
        self
    }

    /// Set the top N for keystats (`--top`).
    pub fn top(mut self, n: u32) -> Self {
        self.top = Some(n);
        self
    }

    /// Measure intrinsic system latency for the given number of seconds.
    pub fn intrinsic_latency(mut self, seconds: u32) -> Self {
        self.intrinsic_latency = Some(seconds);
        self
    }

    /// Run LRU simulation test with the given number of keys.
    pub fn lru_test(mut self, keys: u64) -> Self {
        self.lru_test = Some(keys);
        self
    }

    /// Enable verbose mode (`--verbose`).
    pub fn verbose(mut self, enable: bool) -> Self {
        self.verbose = enable;
        self
    }

    // -- scripting --

    /// Evaluate a Lua script file (`--eval`).
    pub fn eval_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.eval_file = Some(path.into());
        self
    }

    /// Enable Lua debugger (`--ldb`).
    pub fn ldb(mut self, enable: bool) -> Self {
        self.ldb = enable;
        self
    }

    /// Enable Lua debugger in synchronous mode (`--ldb-sync-mode`).
    pub fn ldb_sync_mode(mut self, enable: bool) -> Self {
        self.ldb_sync_mode = enable;
        self
    }

    // -- persistence tools --

    /// Enable pipe mode for mass-insert (`--pipe`).
    pub fn pipe(mut self, enable: bool) -> Self {
        self.pipe = enable;
        self
    }

    /// Set the pipe mode timeout in seconds (`--pipe-timeout`).
    pub fn pipe_timeout(mut self, seconds: u32) -> Self {
        self.pipe_timeout = Some(seconds);
        self
    }

    /// Transfer an RDB dump to a file (`--rdb`).
    pub fn rdb(mut self, path: impl Into<PathBuf>) -> Self {
        self.rdb = Some(path.into());
        self
    }

    /// Transfer a functions-only RDB dump (`--functions-rdb`).
    pub fn functions_rdb(mut self, path: impl Into<PathBuf>) -> Self {
        self.functions_rdb = Some(path.into());
        self
    }

    // -- other --

    /// Simulate a replica for replication stream (`--replica`).
    pub fn replica(mut self, enable: bool) -> Self {
        self.replica = enable;
        self
    }

    /// Run a `redis-cli --cluster <command>` subcommand.
    ///
    /// This is a general-purpose method for all cluster subcommands beyond
    /// `create` (which has the dedicated [`cluster_create`](Self::cluster_create) method).
    /// Pass the subcommand and any additional arguments.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_server_wrapper::RedisCli;
    ///
    /// # async fn example() {
    /// let cli = RedisCli::new().host("127.0.0.1").port(7000);
    /// let info = cli.cluster_command("info", &["127.0.0.1:7000"]).await.unwrap();
    /// # }
    /// ```
    pub async fn cluster_command(&self, command: &str, args: &[&str]) -> Result<String> {
        let mut cli_args = self.base_args();
        cli_args.push("--cluster".into());
        cli_args.push(command.into());
        cli_args.extend(args.iter().map(|s| s.to_string()));

        let str_args: Vec<&str> = cli_args.iter().map(|s| s.as_str()).collect();
        let output = TokioCommand::new(&self.bin)
            .args(&str_args)
            .output()
            .await?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(Error::Cli {
                host: self.host.clone(),
                port: self.port,
                detail: stderr.into_owned(),
            })
        }
    }

    /// Run a command and return stdout on success.
    pub async fn run(&self, args: &[&str]) -> Result<String> {
        let output = self.raw_output(args).await?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(Error::Cli {
                host: self.host.clone(),
                port: self.port,
                detail: stderr.into_owned(),
            })
        }
    }

    /// Run a command, ignoring output. Used for fire-and-forget (SHUTDOWN).
    pub fn fire_and_forget(&self, args: &[&str]) {
        let _ = Command::new(&self.bin)
            .args(self.base_args())
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    /// Send PING and return true if PONG is received.
    pub async fn ping(&self) -> bool {
        self.run(&["PING"])
            .await
            .map(|r| r.trim() == "PONG")
            .unwrap_or(false)
    }

    /// Send SHUTDOWN NOSAVE. Best-effort.
    pub fn shutdown(&self) {
        self.fire_and_forget(&["SHUTDOWN", "NOSAVE"]);
    }

    /// Wait until the server responds to PING or timeout expires.
    pub async fn wait_for_ready(&self, timeout: std::time::Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if self.ping().await {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    message: format!(
                        "{}:{} did not respond within {timeout:?}",
                        self.host, self.port
                    ),
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// Run `redis-cli --cluster create ...` to form a cluster.
    pub async fn cluster_create(
        &self,
        node_addrs: &[String],
        replicas_per_master: u16,
    ) -> Result<()> {
        let mut args = self.base_args();
        args.push("--cluster".into());
        args.push("create".into());
        args.extend(node_addrs.iter().cloned());
        if replicas_per_master > 0 {
            args.push("--cluster-replicas".into());
            args.push(replicas_per_master.to_string());
        }
        args.push("--cluster-yes".into());

        let str_args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let output = TokioCommand::new(&self.bin)
            .args(&str_args)
            .output()
            .await?;

        if output.status.success() {
            Ok(())
        } else {
            Err(Error::ClusterCreate {
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        }
    }

    fn base_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        if let Some(ref path) = self.unixsocket {
            args.push("-s".to_string());
            args.push(path.display().to_string());
        } else {
            args.push("-h".to_string());
            args.push(self.host.clone());
            args.push("-p".to_string());
            args.push(self.port.to_string());
        }

        if let Some(ref user) = self.user {
            args.push("--user".to_string());
            args.push(user.clone());
        }
        if let Some(ref pw) = self.password {
            args.push("-a".to_string());
            args.push(pw.clone());
        }
        if let Some(db) = self.db {
            args.push("-n".to_string());
            args.push(db.to_string());
        }

        // TLS
        if self.tls {
            args.push("--tls".to_string());
        }
        if let Some(ref sni) = self.sni {
            args.push("--sni".to_string());
            args.push(sni.clone());
        }
        if let Some(ref path) = self.cacert {
            args.push("--cacert".to_string());
            args.push(path.display().to_string());
        }
        if let Some(ref path) = self.cert {
            args.push("--cert".to_string());
            args.push(path.display().to_string());
        }
        if let Some(ref path) = self.key {
            args.push("--key".to_string());
            args.push(path.display().to_string());
        }

        // Protocol
        if let Some(ref proto) = self.resp {
            match proto {
                RespProtocol::Resp2 => args.push("-2".to_string()),
                RespProtocol::Resp3 => args.push("-3".to_string()),
            }
        }

        // Cluster
        if self.cluster_mode {
            args.push("-c".to_string());
        }

        // Output format
        match self.output_format {
            OutputFormat::Default => {}
            OutputFormat::Raw => args.push("--raw".to_string()),
            OutputFormat::Csv => args.push("--csv".to_string()),
            OutputFormat::Json => args.push("--json".to_string()),
            OutputFormat::QuotedJson => args.push("--quoted-json".to_string()),
        }

        if self.no_auth_warning {
            args.push("--no-auth-warning".to_string());
        }

        // -- input/output modifiers --
        if self.stdin_last_arg {
            args.push("-x".to_string());
        }
        if self.stdin_tag_arg {
            args.push("-X".to_string());
        }
        if let Some(ref delim) = self.multi_bulk_delimiter {
            args.push("-d".to_string());
            args.push(delim.clone());
        }
        if let Some(ref delim) = self.output_delimiter {
            args.push("-D".to_string());
            args.push(delim.clone());
        }
        if self.exit_error_code {
            args.push("-e".to_string());
        }
        if self.no_raw {
            args.push("--no-raw".to_string());
        }
        if self.quoted_input {
            args.push("--quoted-input".to_string());
        }
        if let Some(enable) = self.show_pushes {
            args.push("--show-pushes".to_string());
            args.push(if enable { "yes" } else { "no" }.to_string());
        }

        // -- diagnostic/analysis modes --
        if self.stat {
            args.push("--stat".to_string());
        }
        if self.latency {
            args.push("--latency".to_string());
        }
        if self.latency_history {
            args.push("--latency-history".to_string());
        }
        if self.latency_dist {
            args.push("--latency-dist".to_string());
        }
        if self.bigkeys {
            args.push("--bigkeys".to_string());
        }
        if self.memkeys {
            args.push("--memkeys".to_string());
        }
        if let Some(n) = self.memkeys_samples {
            args.push("--memkeys-samples".to_string());
            args.push(n.to_string());
        }
        if self.keystats {
            args.push("--keystats".to_string());
        }
        if let Some(n) = self.keystats_samples {
            args.push("--keystats-samples".to_string());
            args.push(n.to_string());
        }
        if self.hotkeys {
            args.push("--hotkeys".to_string());
        }
        if self.scan {
            args.push("--scan".to_string());
        }
        if let Some(ref pat) = self.pattern {
            args.push("--pattern".to_string());
            args.push(pat.clone());
        }
        if let Some(n) = self.count {
            args.push("--count".to_string());
            args.push(n.to_string());
        }
        if let Some(ref pat) = self.quoted_pattern {
            args.push("--quoted-pattern".to_string());
            args.push(pat.clone());
        }
        if let Some(n) = self.cursor {
            args.push("--cursor".to_string());
            args.push(n.to_string());
        }
        if let Some(n) = self.top {
            args.push("--top".to_string());
            args.push(n.to_string());
        }
        if let Some(seconds) = self.intrinsic_latency {
            args.push("--intrinsic-latency".to_string());
            args.push(seconds.to_string());
        }
        if let Some(keys) = self.lru_test {
            args.push("--lru-test".to_string());
            args.push(keys.to_string());
        }
        if self.verbose {
            args.push("--verbose".to_string());
        }

        // -- scripting --
        if let Some(ref path) = self.eval_file {
            args.push("--eval".to_string());
            args.push(path.display().to_string());
        }
        if self.ldb {
            args.push("--ldb".to_string());
        }
        if self.ldb_sync_mode {
            args.push("--ldb-sync-mode".to_string());
        }

        // -- persistence tools --
        if self.pipe {
            args.push("--pipe".to_string());
        }
        if let Some(n) = self.pipe_timeout {
            args.push("--pipe-timeout".to_string());
            args.push(n.to_string());
        }
        if let Some(ref path) = self.rdb {
            args.push("--rdb".to_string());
            args.push(path.display().to_string());
        }
        if let Some(ref path) = self.functions_rdb {
            args.push("--functions-rdb".to_string());
            args.push(path.display().to_string());
        }

        // -- other --
        if self.replica {
            args.push("--replica".to_string());
        }

        args
    }

    async fn raw_output(&self, args: &[&str]) -> std::io::Result<Output> {
        TokioCommand::new(&self.bin)
            .args(self.base_args())
            .args(args)
            .output()
            .await
    }
}

impl Default for RedisCli {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cli = RedisCli::new();
        assert_eq!(cli.host, "127.0.0.1");
        assert_eq!(cli.port, 6379);
    }

    #[test]
    fn builder_chain() {
        let cli = RedisCli::new()
            .host("10.0.0.1")
            .port(6380)
            .password("secret")
            .bin("/usr/local/bin/redis-cli");
        assert_eq!(cli.host, "10.0.0.1");
        assert_eq!(cli.port, 6380);
        assert_eq!(cli.password.as_deref(), Some("secret"));
        assert_eq!(cli.bin, "/usr/local/bin/redis-cli");
    }
}
