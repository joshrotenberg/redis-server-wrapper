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
/// 2. Send SIGCONT so a process suspended by `SIGSTOP` (e.g. a frozen node in
///    a chaos test) actually receives that SIGTERM instead of leaving it
///    queued until something else resumes the process.
/// 3. Sleep 500ms.
/// 4. If still alive, SIGKILL the process group (`kill -9 -- -$pid`) to catch wrapper
///    scripts and any children they spawned (e.g. `redis-stack-server`).
/// 5. SIGKILL the individual PID as a fallback.
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
    // 0 as a group target means the caller's own process group, and a value
    // above i32::MAX reaches `kill` as a negative number. Neither can be a
    // server this crate started.
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    let pid_str = pid.to_string();
    let pgid_str = format!("-{pid}");

    // Step 1: SIGTERM -- graceful shutdown attempt.
    let _ = Command::new("kill").args([&pid_str]).output();

    // Step 2: SIGCONT -- a stopped process (SIGSTOP, e.g. via chaos::freeze_node)
    // does not act on SIGTERM until it resumes. Without this, a frozen process
    // sits on the queued SIGTERM through the whole grace period below and this
    // function relies entirely on SIGKILL's stop-independent delivery to make
    // any progress at all. Waking it here lets the SIGTERM actually run first.
    let _ = Command::new("kill").args(["-CONT", &pid_str]).output();

    // Step 3: Grace period.
    thread::sleep(Duration::from_millis(500));

    // Step 4: If still alive, escalate to SIGKILL on process group.
    if pid_alive(pid) {
        // Kill the whole process group to catch wrapper script children.
        //
        // The `--` is required. Without it, procps-ng 4.0.4's `kill` (Ubuntu
        // 24.04) does not read `-9 -<pid>` as a process group: it signals
        // every process the user can reach, which on a CI runner includes
        // the runner itself.
        let _ = Command::new("kill").args(["-9", "--", &pgid_str]).output();
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

#[cfg(feature = "tokio")]
/// Upper bound on the descriptors [`cloexec_inherited_fds`] marks when the
/// open descriptors cannot be listed.
///
/// `RLIMIT_NOFILE` can be very large or unlimited; descriptors above this
/// bound are left as they are.
const MAX_FD_SWEEP: libc::c_int = 65_536;

#[cfg(feature = "tokio")]
/// Descriptors past the highest one open at spawn time that
/// [`cloexec_inherited_fds`] also marks.
///
/// Covers descriptors other threads open between the listing and the fork.
/// A new descriptor takes the lowest free number, so it lands at or just
/// above the highest one already open.
const FD_SWEEP_MARGIN: libc::c_int = 256;

#[cfg(feature = "tokio")]
/// A `pre_exec` hook that marks every descriptor above stderr close-on-exec.
///
/// On macOS std creates a child's pipes with `pipe()` then
/// `fcntl(FD_CLOEXEC)`, which is not atomic. A process forked by another
/// thread in between inherits the write end, and a daemonized `redis-server`
/// would hold it for its lifetime, so the thread reading that pipe never sees
/// EOF (#202). Running this in the child after fork and before exec drops any
/// such descriptor at exec.
///
/// The descriptors are marked close-on-exec rather than closed so std's own
/// exec-error pipe stays open until exec and a failed exec is still reported
/// as a spawn error. Stdio has already been set up by the time the hook runs.
///
/// The bound is computed here, in the parent: the highest descriptor listed
/// in `/dev/fd` plus [`FD_SWEEP_MARGIN`], or the soft `RLIMIT_NOFILE` capped
/// at [`MAX_FD_SWEEP`] when the listing fails. The sweep is kept short
/// because between fork and exec the child holds a copy of every descriptor
/// the parent has, so a socket the parent closes in that window stays
/// connected until the child execs. The returned closure only calls `fcntl`,
/// which is async-signal-safe, and does not allocate.
pub(crate) fn cloexec_inherited_fds() -> impl FnMut() -> std::io::Result<()> + Send + Sync + 'static
{
    let max_fd = sweep_bound();
    move || {
        for fd in 3..max_fd {
            // SAFETY: fcntl on an arbitrary integer is defined; a closed
            // descriptor returns EBADF, which is ignored.
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "tokio")]
/// One past the last descriptor [`cloexec_inherited_fds`] marks.
fn sweep_bound() -> libc::c_int {
    let limit = open_file_limit();
    match highest_open_fd() {
        Some(fd) => fd.saturating_add(1 + FD_SWEEP_MARGIN).min(limit),
        None => limit,
    }
}

#[cfg(feature = "tokio")]
/// The highest descriptor listed in `/dev/fd`, if it can be read.
fn highest_open_fd() -> Option<libc::c_int> {
    std::fs::read_dir("/dev/fd")
        .ok()?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse::<libc::c_int>().ok())
        .max()
}

#[cfg(feature = "tokio")]
/// `RLIMIT_NOFILE`'s soft limit, capped at [`MAX_FD_SWEEP`].
fn open_file_limit() -> libc::c_int {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `rl` is a valid, writable rlimit.
    let ok = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } == 0;
    if !ok || rl.rlim_cur == libc::RLIM_INFINITY {
        return MAX_FD_SWEEP;
    }
    rl.rlim_cur.min(MAX_FD_SWEEP as libc::rlim_t) as libc::c_int
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether a child spawned with or without the hook can see `fd`.
    #[cfg(feature = "tokio")]
    fn child_sees_fd(fd: libc::c_int, hook: bool) -> bool {
        use std::os::unix::process::CommandExt;
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &format!("test -e /dev/fd/{fd}")]);
        if hook {
            // SAFETY: the hook only calls fcntl.
            unsafe {
                cmd.pre_exec(cloexec_inherited_fds());
            }
        }
        cmd.status().expect("sh should run").success()
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn cloexec_hook_keeps_a_leaked_descriptor_out_of_the_child() {
        // A pipe created without close-on-exec stands in for one another
        // thread was midway through creating when the fork happened.
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `fds` is a valid two-element array.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let write_end = fds[1];

        assert!(
            child_sees_fd(write_end, false),
            "without the hook the child should inherit the descriptor"
        );
        assert!(
            !child_sees_fd(write_end, true),
            "with the hook the child must not inherit the descriptor"
        );

        // SAFETY: both descriptors were opened above and are closed once.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn cloexec_hook_still_reports_a_failed_exec() {
        // The hook marks descriptors close-on-exec instead of closing them, so
        // std's exec-error pipe survives and a missing binary is still an
        // error from spawn rather than a child that exits non-zero.
        use std::os::unix::process::CommandExt;
        let mut cmd = Command::new("/nonexistent/redis-server-wrapper-test-bin");
        // SAFETY: the hook only calls fcntl.
        unsafe {
            cmd.pre_exec(cloexec_inherited_fds());
        }
        let err = cmd
            .spawn()
            .expect_err("spawning a missing binary must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

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

    /// Spawn `args` as the leader of a new process group.
    fn spawn_in_own_group(args: &[&str]) -> std::process::Child {
        use std::os::unix::process::CommandExt;
        Command::new(args[0])
            .args(&args[1..])
            .process_group(0)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn should succeed")
    }

    /// Whether `pid` is a running process. A zombie counts as gone: it has
    /// exited and only waits for a parent to reap it.
    fn running(pid: u32) -> bool {
        Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .map(|o| {
                let stat = String::from_utf8_lossy(&o.stdout);
                let stat = stat.trim();
                !stat.is_empty() && !stat.starts_with('Z')
            })
            .unwrap_or(false)
    }

    #[test]
    fn force_kill_signals_the_whole_target_group_and_nothing_else() {
        // The target ignores SIGTERM, so force_kill has to escalate, and it
        // has a child in its process group that only the group SIGKILL
        // reaches. A bystander in another group must survive.
        //
        // Without `--`, procps-ng 4.0.4 fails this either way: `-<pgid>`
        // starting with 1 reads as `-1` and kills every process the user
        // owns, including this test; any other leading digit signals
        // nothing, and the child survives.
        let mut target =
            spawn_in_own_group(&["sh", "-c", "trap '' TERM; sleep 30 & echo $!; wait"]);
        let mut bystander = spawn_in_own_group(&["sleep", "30"]);

        let mut line = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(target.stdout.take().expect("stdout")),
            &mut line,
        )
        .expect("child pid should be printed");
        let child: u32 = line.trim().parse().expect("child pid");
        assert!(running(child));

        force_kill(target.id());
        let _ = target.wait();
        thread::sleep(Duration::from_millis(200));

        let child_survived = running(child);
        let bystander_survived = bystander.try_wait().expect("try_wait").is_none();
        if child_survived {
            let _ = Command::new("kill")
                .args(["-9", &child.to_string()])
                .output();
        }
        let _ = bystander.kill();
        let _ = bystander.wait();

        assert!(
            !child_survived,
            "the group kill must reach the target's child"
        );
        assert!(
            bystander_survived,
            "a process outside the target's group must not be signalled"
        );
    }

    #[test]
    fn force_kill_refuses_pids_that_are_not_a_single_process() {
        // Returns without signalling. Were pid 0 passed through, the group
        // kill would target this test's own process group.
        force_kill(0);
        force_kill(u32::MAX);
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
