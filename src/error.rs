//! Error types for redis-server-wrapper.

use std::io;

/// Errors returned by redis-server-wrapper operations.
///
/// Marked `#[non_exhaustive]`: this enum gains variants as the wrapper learns
/// to report more failures precisely, and every such addition would otherwise
/// break an exhaustive `match` downstream. The 0.5.0 cycle alone added five.
/// Match the variants you handle and end with a catch-all `Err(e)` arm, and
/// use a `..` rest pattern inside a variant so a new field does not break
/// either.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A `redis-server` process failed to start.
    ///
    /// `detail`, when present, carries the last lines of the node's log file
    /// (and, for a spawn-time failure, the daemonizing process's captured
    /// stdout/stderr) so the reason survives even when no tracing subscriber
    /// is installed -- the common case for most test runs, where an event
    /// emitted alongside this error would otherwise be invisible.
    #[error(
        "redis-server failed to start on port {port}{}",
        detail.as_deref().map(|d| format!("\n{d}")).unwrap_or_default()
    )]
    ServerStart {
        /// The port on which the server failed to start.
        port: u16,
        /// Tail of the node's log file plus any captured process output,
        /// when available.
        detail: Option<String>,
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

    /// Automatic port allocation ran out of attempts.
    ///
    /// Every candidate the OS handed out was claimed by another process
    /// before `redis-server` could bind it. On a normal machine this does not
    /// happen: it points at something aggressively grabbing ephemeral ports,
    /// or at an exhausted ephemeral range.
    #[error(
        "could not acquire a free port after {attempts} attempts{}",
        last.as_deref().map(|e| format!(" (last failure: {e})")).unwrap_or_default()
    )]
    PortAllocation {
        /// How many candidates were tried.
        attempts: usize,
        /// The failure from the final attempt, if there was one.
        last: Option<String>,
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
