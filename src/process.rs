//! OS-level process utilities for robust server shutdown and stale process cleanup.
//!
//! This module provides functions for checking process liveness, performing
//! escalating kills (including process-group kills to handle wrapper scripts),
//! and cleaning up stale pidfiles from crashed test runs.
//!
//! All utilities are intentionally synchronous so they can be used from
//! [`Drop`] implementations as well as from async startup paths.

use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

/// Check if a process is alive via `kill -0`.
///
/// Returns `true` if the process exists and is reachable, `false` otherwise.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::process;
///
/// let alive = process::pid_alive(12345);
/// println!("process alive: {alive}");
/// ```
pub fn pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Escalating kill: SIGTERM, wait grace period, then SIGKILL process group and individual PID.
///
/// Strategy:
/// 1. Send SIGTERM to give the process a chance to shut down cleanly.
/// 2. Sleep 500ms.
/// 3. If still alive, SIGKILL the process group (`kill -9 -$pid`) to catch wrapper
///    scripts and any children they spawned (e.g. `redis-stack-server`).
/// 4. SIGKILL the individual PID as a fallback.
///
/// Uses synchronous [`std::process::Command`] so this is safe to call from [`Drop`] impls.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::process;
///
/// process::force_kill(12345);
/// ```
pub fn force_kill(pid: u32) {
    let pid_str = pid.to_string();
    let pgid_str = format!("-{pid}");

    // Step 1: SIGTERM -- graceful shutdown attempt.
    let _ = Command::new("kill").args([&pid_str]).output();

    // Step 2: Grace period.
    thread::sleep(Duration::from_millis(500));

    // Step 3: If still alive, escalate to SIGKILL on process group.
    if pid_alive(pid) {
        // Kill the whole process group to catch wrapper script children.
        let _ = Command::new("kill").args(["-9", &pgid_str]).output();
        // Also kill the individual PID as fallback.
        let _ = Command::new("kill").args(["-9", &pid_str]).output();
    }
}

/// Read a PID from a pidfile.
///
/// Returns `None` if the file does not exist, cannot be read, or its contents
/// cannot be parsed as a `u32`.
pub fn read_pidfile(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
}

/// Kill any process listening on a TCP port via `lsof -ti :$port`.
///
/// Best-effort -- all errors are silently ignored. This is intended as a
/// final safety net to release the port after shutdown, not as a primary
/// kill mechanism.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::process;
///
/// process::kill_by_port(6379);
/// ```
pub fn kill_by_port(port: u16) {
    let port_str = format!(":{port}");
    let Ok(output) = Command::new("lsof").args(["-ti", &port_str]).output() else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let line = line.trim();
        if !line.is_empty() {
            let _ = Command::new("kill").args(["-9", line]).output();
        }
    }
}
