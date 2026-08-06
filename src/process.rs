//! OS-level process utilities for robust server shutdown and stale process cleanup.
//!
//! This module provides functions for checking process liveness, performing
//! escalating kills (including process-group kills to handle wrapper scripts),
//! and cleaning up stale pidfiles from crashed test runs.
//!
//! All utilities are intentionally synchronous so they can be used from
//! [`Drop`] implementations as well as from async startup paths.
//!
//! Unix-only: every function here shells out to `kill` or `lsof`. See the
//! crate-level "Platform Support" docs for details.

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

/// Stop a process this crate started, identified by a pidfile it wrote.
///
/// The pidfile is the wrapper's only claim of ownership. Reading a pid back
/// out of a directory the wrapper created and named is what separates
/// reclaiming a leftover of our own from stopping an unrelated server, which
/// is a distinction the topology builders got wrong before they stopped
/// clearing ports outright.
///
/// Returns the pid that was stopped, or `None` if the file is missing,
/// unreadable, names a process that is already gone, or names one that is no
/// longer Redis.
///
/// # Why the command is checked
///
/// A pidfile only records a number, and the OS reuses numbers. A stale
/// pidfile left by a crashed run can name a pid that now belongs to something
/// else entirely, and [`force_kill`] escalates to killing the whole process
/// group, so acting on a reused pid can take down far more than one process.
/// On a machine running a test suite, the process that inherited the number is
/// quite likely to be part of that suite.
///
/// Confirming the pid still looks like Redis before signalling it turns
/// ownership from "we wrote this number down once" into something checked
/// against the live process.
pub fn reclaim_from_pidfile(path: &Path) -> Option<u32> {
    let pid = read_pidfile(path)?;
    if !pid_alive(pid) {
        return None;
    }
    if !is_redis_process(pid) {
        return None;
    }
    force_kill(pid);
    Some(pid)
}

/// Executable names this crate is willing to signal as its own.
const REDIS_BINARIES: &[&str] = &["redis-server", "redis-sentinel", "redis-stack-server"];

/// Whether a pid belongs to a Redis server or sentinel process.
///
/// Reads the process's command via `ps` and compares the **executable's file
/// name** against a small set of known Redis binaries. A pid that cannot be
/// inspected reads as
/// not-Redis, so an uncertain answer never authorises a kill.
///
/// The file name is compared, not the whole command line. Substring matching
/// is unsafe here in a way that is easy to miss: this crate's own build
/// artifacts live under a directory called `redis-server-wrapper`, so every
/// process in its test suite has `redis-server` somewhere in its command line.
/// A substring check would call the test harness a Redis process and, via
/// [`force_kill`]'s process-group escalation, let a stale pidfile terminate
/// the run. That is not hypothetical: it is what the first version of this
/// function did.
pub fn is_redis_process(pid: u32) -> bool {
    let Ok(output) = Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let command = String::from_utf8_lossy(&output.stdout);

    // The executable is the first field; the rest is the config path and any
    // module arguments, none of which say anything about what is running.
    let Some(argv0) = command.split_whitespace().next() else {
        return false;
    };
    let name = argv0.rsplit('/').next().unwrap_or(argv0);

    REDIS_BINARIES.contains(&name)
}

/// Kill any process **listening** on a TCP port via `lsof`.
///
/// Uses `-sTCP:LISTEN` to restrict matches to server processes, avoiding
/// false positives on client connections to the same port. Also filters
/// out the calling process's own PID as a safeguard.
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
    let Ok(output) = Command::new("lsof")
        .args(["-ti", &port_str, "-sTCP:LISTEN"])
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let my_pid = std::process::id().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let line = line.trim();
        if !line.is_empty() && line != my_pid {
            let _ = Command::new("kill").args(["-9", line]).output();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn our_own_pid_is_not_a_redis_process() {
        // The guard that matters: a reused pid belonging to the test harness
        // must never authorise a kill, because force_kill takes the process
        // group with it.
        assert!(!is_redis_process(std::process::id()));
    }

    #[test]
    fn a_real_redis_process_is_recognised() {
        // The positive case, so the guard cannot be trivially satisfied by
        // always returning false.
        let child = Command::new("redis-server")
            .args(["--port", "16995", "--save", ""])
            .spawn();
        let Ok(mut child) = child else {
            eprintln!("skipping: redis-server not on PATH");
            return;
        };
        thread::sleep(Duration::from_millis(300));
        let pid = child.id();
        assert!(
            is_redis_process(pid),
            "a running redis-server must be recognised"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn an_unused_pid_is_not_a_redis_process() {
        // Very unlikely to be live, and must read as not-Redis either way.
        assert!(!is_redis_process(u32::MAX - 1));
    }

    #[test]
    fn reclaim_ignores_a_missing_pidfile() {
        let path = std::env::temp_dir().join("rsw-nonexistent-pidfile-xyz");
        let _ = std::fs::remove_file(&path);
        assert_eq!(reclaim_from_pidfile(&path), None);
    }

    #[test]
    fn reclaim_ignores_a_pidfile_naming_a_non_redis_process() {
        // Points at this test process. Without the command check this would
        // signal our own process group.
        let path = std::env::temp_dir().join("rsw-self-pidfile-test");
        std::fs::write(&path, std::process::id().to_string()).unwrap();
        assert_eq!(
            reclaim_from_pidfile(&path),
            None,
            "a live but non-Redis pid must not be killed"
        );
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn reclaim_ignores_unparseable_contents() {
        let path = std::env::temp_dir().join("rsw-garbage-pidfile-test");
        std::fs::write(&path, "not a pid").unwrap();
        assert_eq!(reclaim_from_pidfile(&path), None);
        std::fs::remove_file(&path).unwrap();
    }
}
