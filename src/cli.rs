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

/// IP version preference for connections.
#[derive(Debug, Clone, Copy, Default)]
pub enum IpPreference {
    /// Use the system default.
    #[default]
    Default,
    /// Prefer IPv4 (`-4`).
    Ipv4,
    /// Prefer IPv6 (`-6`).
    Ipv6,
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
    cacertdir: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    insecure: bool,
    tls_ciphers: Option<String>,
    tls_ciphersuites: Option<String>,
    resp: Option<RespProtocol>,
    cluster_mode: bool,
    output_format: OutputFormat,
    no_auth_warning: bool,
    uri: Option<String>,
    timeout: Option<f64>,
    askpass: bool,
    client_name: Option<String>,
    ip_preference: IpPreference,
    repeat: Option<u32>,
    interval: Option<f64>,
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
            cacertdir: None,
            cert: None,
            key: None,
            insecure: false,
            tls_ciphers: None,
            tls_ciphersuites: None,
            resp: None,
            cluster_mode: false,
            output_format: OutputFormat::Default,
            no_auth_warning: false,
            uri: None,
            timeout: None,
            askpass: false,
            client_name: None,
            ip_preference: IpPreference::Default,
            repeat: None,
            interval: None,
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

    /// Set the CA certificate directory for TLS verification.
    pub fn cacertdir(mut self, path: impl Into<PathBuf>) -> Self {
        self.cacertdir = Some(path.into());
        self
    }

    /// Skip TLS certificate verification (`--insecure`).
    pub fn insecure(mut self, enable: bool) -> Self {
        self.insecure = enable;
        self
    }

    /// Set the allowed TLS 1.2 ciphers (`--tls-ciphers`).
    pub fn tls_ciphers(mut self, ciphers: impl Into<String>) -> Self {
        self.tls_ciphers = Some(ciphers.into());
        self
    }

    /// Set the allowed TLS 1.3 ciphersuites (`--tls-ciphersuites`).
    pub fn tls_ciphersuites(mut self, ciphersuites: impl Into<String>) -> Self {
        self.tls_ciphersuites = Some(ciphersuites.into());
        self
    }

    /// Set the server URI (`-u`), e.g. `redis://user:pass@host:port/db`.
    pub fn uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    /// Set the connection timeout in seconds (`-t`).
    pub fn timeout(mut self, seconds: f64) -> Self {
        self.timeout = Some(seconds);
        self
    }

    /// Prompt for password from stdin (`--askpass`).
    pub fn askpass(mut self, enable: bool) -> Self {
        self.askpass = enable;
        self
    }

    /// Set the client connection name (`--name`).
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }

    /// Set IP version preference for connections.
    pub fn ip_preference(mut self, preference: IpPreference) -> Self {
        self.ip_preference = preference;
        self
    }

    /// Execute the command N times (`-r`).
    pub fn repeat(mut self, count: u32) -> Self {
        self.repeat = Some(count);
        self
    }

    /// Set interval in seconds between repeated commands (`-i`).
    pub fn interval(mut self, seconds: f64) -> Self {
        self.interval = Some(seconds);
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

        // Connection
        if let Some(ref uri) = self.uri {
            args.push("-u".to_string());
            args.push(uri.clone());
        } else if let Some(ref path) = self.unixsocket {
            args.push("-s".to_string());
            args.push(path.display().to_string());
        } else {
            args.push("-h".to_string());
            args.push(self.host.clone());
            args.push("-p".to_string());
            args.push(self.port.to_string());
        }

        // Auth
        if let Some(ref user) = self.user {
            args.push("--user".to_string());
            args.push(user.clone());
        }
        if let Some(ref pw) = self.password {
            args.push("-a".to_string());
            args.push(pw.clone());
        }
        if self.askpass {
            args.push("--askpass".to_string());
        }
        if let Some(db) = self.db {
            args.push("-n".to_string());
            args.push(db.to_string());
        }

        // Client name
        if let Some(ref name) = self.client_name {
            args.push("--name".to_string());
            args.push(name.clone());
        }

        // IP preference
        match self.ip_preference {
            IpPreference::Default => {}
            IpPreference::Ipv4 => args.push("-4".to_string()),
            IpPreference::Ipv6 => args.push("-6".to_string()),
        }

        // Timeout
        if let Some(t) = self.timeout {
            args.push("-t".to_string());
            args.push(t.to_string());
        }

        // Repeat / interval
        if let Some(r) = self.repeat {
            args.push("-r".to_string());
            args.push(r.to_string());
        }
        if let Some(i) = self.interval {
            args.push("-i".to_string());
            args.push(i.to_string());
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
        if let Some(ref path) = self.cacertdir {
            args.push("--cacertdir".to_string());
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
        if self.insecure {
            args.push("--insecure".to_string());
        }
        if let Some(ref ciphers) = self.tls_ciphers {
            args.push("--tls-ciphers".to_string());
            args.push(ciphers.clone());
        }
        if let Some(ref suites) = self.tls_ciphersuites {
            args.push("--tls-ciphersuites".to_string());
            args.push(suites.clone());
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
