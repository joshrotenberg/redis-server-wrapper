//! Error types for redis-server-wrapper.

use std::io;

/// Errors returned by redis-server-wrapper operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `redis-server` process failed to start.
    #[error("redis-server failed to start on port {port}")]
    ServerStart { port: u16 },

    /// A sentinel process failed to start.
    #[error("sentinel failed to start on port {port}")]
    SentinelStart { port: u16 },

    /// `redis-cli --cluster create` failed.
    #[error("cluster create failed:\nstdout: {stdout}\nstderr: {stderr}")]
    ClusterCreate { stdout: String, stderr: String },

    /// A `redis-cli` command failed.
    #[error("redis-cli {host}:{port} failed: {detail}")]
    Cli {
        host: String,
        port: u16,
        detail: String,
    },

    /// A wait-for-ready or wait-for-healthy call timed out.
    #[error("{message}")]
    Timeout { message: String },

    /// No sentinel was reachable.
    #[error("no reachable sentinel")]
    NoReachableSentinel,

    /// A required binary was not found on PATH.
    #[error("{binary} not found on PATH")]
    BinaryNotFound { binary: String },

    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
