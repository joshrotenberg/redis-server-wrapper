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

    /// Sending a POSIX signal to a node process failed, either because the
    /// `kill` invocation itself could not be spawned or because it exited
    /// with a non-zero status (e.g. the target PID no longer exists).
    #[error("failed to send signal {signal} to pid {pid}: {detail}")]
    Signal {
        /// The `kill` signal flag that was sent (e.g. `-9`, `-STOP`, `-CONT`).
        signal: String,
        /// The target process ID.
        pid: u32,
        /// Stderr output or other detail from the failed invocation.
        detail: String,
    },

    /// No sentinel was reachable.
    #[error("no reachable sentinel")]
    NoReachableSentinel,

    /// A chaos primitive refused to run because doing so under the current
    /// process privileges would have no effect.
    ///
    /// Currently only returned by [`crate::chaos::break_persistence`]: chmod
    /// permission bits are ignored for the root user, so it refuses to run
    /// as root rather than silently failing to break anything.
    #[error("{message}")]
    PrivilegeRequired {
        /// Human-readable description of the refused operation and why.
        message: String,
    },

    /// A sentinel index passed to a per-sentinel operation was out of range.
    #[error("sentinel index {index} out of range (topology has {len} sentinels)")]
    SentinelIndex {
        /// The requested index.
        index: usize,
        /// The number of sentinels actually running.
        len: usize,
    },

    /// A required binary was not found on PATH.
    #[error("{binary} not found on PATH")]
    BinaryNotFound {
        /// The binary name that could not be found.
        binary: String,
    },

    /// A port the topology needs is already bound by another process.
    ///
    /// Startup fails rather than clearing the port: the wrapper cannot tell a
    /// leftover node of its own from an unrelated Redis, and stopping the
    /// latter would lose data it never owned. Free the port, or point the
    /// builder at a different one.
    #[error("{role} {host}:{port} is already in use")]
    PortInUse {
        /// The host the port was to be bound on.
        host: String,
        /// The occupied port.
        port: u16,
        /// What the port was needed for, e.g. "cluster bus port".
        role: String,
    },

    /// A module expected to be loaded was not.
    ///
    /// A `loadmodule` directive only asks Redis to load a module. A wrong
    /// path, an ABI mismatch, or a module whose `RedisModule_OnLoad` returns
    /// an error all leave the server running without it, so the modules that
    /// are loaded are listed to make a name mismatch obvious.
    #[error(
        "module `{name}` is not loaded on port {port} (loaded: {})",
        if loaded.is_empty() { "none".to_string() } else { loaded.join(", ") }
    )]
    ModuleNotLoaded {
        /// The module name that was expected.
        name: String,
        /// The port of the server that was checked.
        port: u16,
        /// Names of the modules actually loaded there.
        loaded: Vec<String>,
    },

    /// A topology was described in a way that cannot be started.
    ///
    /// Returned before anything starts, so a rejected topology leaves no
    /// processes or directories behind.
    #[error("invalid topology: {message}")]
    InvalidTopology {
        /// What about the topology is unworkable.
        message: String,
    },

    /// An `extra()` directive collided with one the wrapper generates.
    ///
    /// The wrapper reuses these values after startup for readiness probing and
    /// teardown, so overriding them would leave the handle describing a server
    /// that does not exist. Use the dedicated builder method instead.
    #[error(
        "`{key}` is generated by the wrapper and cannot be set via extra(); \
         use the dedicated builder method instead"
    )]
    ReservedDirective {
        /// The directive that was rejected.
        key: String,
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
