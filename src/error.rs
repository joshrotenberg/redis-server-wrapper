//! Error types for redis-server-wrapper.

use std::io;

/// Errors returned by redis-server-wrapper operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `redis-server` process failed to start.
    #[error("redis-server failed to start on port {port}")]
    ServerStart {
        /// The port on which the server failed to start.
        port: u16,
    },

    /// A sentinel process failed to start.
    #[error("sentinel failed to start on port {port}")]
    SentinelStart {
        /// The port on which the sentinel failed to start.
        port: u16,
    },

    /// `redis-cli --cluster create` failed.
    #[error("cluster create failed:\nstdout: {stdout}\nstderr: {stderr}")]
    ClusterCreate {
        /// Captured stdout from the failed `redis-cli --cluster create` invocation.
        stdout: String,
        /// Captured stderr from the failed `redis-cli --cluster create` invocation.
        stderr: String,
    },

    /// A `redis-cli` command failed.
    #[error("redis-cli {host}:{port} failed: {detail}")]
    Cli {
        /// The host that was targeted.
        host: String,
        /// The port that was targeted.
        port: u16,
        /// Stderr output or other detail from the failed invocation.
        detail: String,
    },

    /// A wait-for-ready or wait-for-healthy call timed out.
    #[error("{message}")]
    Timeout {
        /// Human-readable description of what timed out.
        message: String,
    },

    /// No sentinel was reachable.
    #[error("no reachable sentinel")]
    NoReachableSentinel,

    /// A required binary was not found on PATH.
    #[error("{binary} not found on PATH")]
    BinaryNotFound {
        /// The binary name that could not be found.
        binary: String,
    },

    /// A TLS certificate generation error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
