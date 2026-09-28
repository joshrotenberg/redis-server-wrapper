//! Owner lock for a standalone server's node directory.
//!
//! The default `dir` is `std::env::temp_dir().join("redis-server-wrapper")`,
//! shared by every process on the machine that starts a server without an
//! explicit `dir()`. Two starts on the same port therefore land in the same
//! `node-<port>` directory and, without something to tell them apart, the
//! second one has no way to know whether the pidfile it finds there names a
//! server it is free to reclaim or one another process is still using.
//!
//! This lock answers that question with an advisory `flock` on
//! `<node_dir>/owner`, held for the lifetime of the handle rather than
//! recorded by the file's mere existence. The kernel drops the lock the
//! instant the holding process exits -- crashed or clean -- so liveness needs
//! no pid check of its own, and the file is never deleted: a starter always
//! opens (or creates) the same inode and contends for its lock, rather than
//! racing another starter over which of them gets to create a fresh one.
//! (An earlier create-and-delete design had exactly that race: two starters
//! could each see a dead owner, and one's `remove_file` could land after the
//! other's `create_new`, leaving both believing they owned the directory.)
//!
//! The file's contents, read only after the flock is held, tell a live
//! [`detach`](crate::server::RedisServerHandle::detach)ed server from a
//! crashed wrapper process: either this process's id, a `detached <pid>`
//! marker naming the redis-server pid a detached handle left running, or
//! nothing at all (a clean release leaves the file empty rather than
//! removing it). A pid on its own is not enough -- the OS reuses them -- so a
//! takeover only happens after confirming the recorded owner (or, for a
//! detached lock, its redis process) is actually gone.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::preflight::PortRole;
use crate::process;

/// Prefixes a detached lock's content: the line becomes `detached <pid>`,
/// naming the redis-server pid the handle left running rather than a wrapper
/// process id.
const DETACHED_PREFIX: &str = "detached ";

/// RAII guard for a node directory's `owner` file and the flock held on it.
///
/// Dropped without [`release`](Self::release) or
/// [`mark_detached`](Self::mark_detached) -- the crash path, where a start
/// failed after taking the lock -- this only unlocks. The contents are left
/// exactly as they were: whatever the next acquire finds there is exactly
/// what this holder wrote (or, on a fresh takeover, whatever the previous
/// holder left).
///
/// # Why dropping unlocks explicitly
///
/// Closing the fd is not enough to release a flock promptly. The lock
/// belongs to the open file description, and `fork` gives the child a
/// duplicate of every fd in the process, whichever thread forked. The fd is
/// `O_CLOEXEC`, but that only closes it at `exec`, so for the moment between
/// another thread's `fork` and its `exec` the child holds a second reference
/// to the same open file description. If this guard merely closed its fd in
/// that window, the lock would stay held until the child exec'd, and a start
/// right after a stop could see a spurious `PortInUse`. Test binaries run
/// tests on parallel threads that fork `kill`, `ps`, and `redis-server`
/// constantly, so the window is hit in practice. `LOCK_UN` releases the lock
/// on the open file description itself, which covers every duplicate.
pub(crate) struct OwnerLock {
    file: File,
}

impl OwnerLock {
    /// Take ownership of `node_dir`, reclaiming a stale lock left by a
    /// crashed run when doing so is safe.
    ///
    /// Creates `node_dir` first. Returns [`Error::PortInUse`] naming `host`
    /// and `port` with [`PortRole::Server`] when the flock is already held by
    /// a live owner -- another process, or another handle in this same
    /// process, since `flock` conflicts across separate open file
    /// descriptions even within one process -- or when a detached lock names
    /// a redis-server pid that is still alive and still Redis.
    pub(crate) fn acquire(node_dir: &Path, host: &str, port: u16) -> Result<Self> {
        crate::secure_file::create_dir_all(node_dir)?;
        let path = node_dir.join("owner");

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        // No `truncate`: the previous holder's contents must survive until
        // the flock is held and they've been read, or a torn write could be
        // mistaken for garbage by a racing reader.
        let file = opts.open(&path)?;

        // Rust's `OpenOptions` sets `O_CLOEXEC` by default, so the
        // redis-server child this lock goes on to spawn never inherits this
        // fd. If it did, the flock would stay held for as long as the
        // server ran even after the wrapper process that took it exited,
        // which would make every crash look like a live owner forever.
        if !try_flock_exclusive(&file)? {
            return Err(Error::PortInUse {
                host: host.to_string(),
                port,
                role: PortRole::Server.to_string(),
            });
        }
        // Wrap it at once so every early return below unlocks through Drop.
        let mut lock = Self { file };

        // The flock is ours now, so nothing else can be mid-write: every
        // writer takes this same lock first. What we read here is exactly
        // what the previous holder left, never a torn write.
        let mut contents = String::new();
        lock.file.read_to_string(&mut contents)?;
        let contents = contents.trim();

        if let Some(rest) = contents.strip_prefix(DETACHED_PREFIX) {
            if let Ok(redis_pid) = rest.trim().parse::<u32>()
                && process::pid_alive(redis_pid)
                && process::is_redis_process(redis_pid)
            {
                // A live detached server. Returning drops `lock`, which
                // unlocks and leaves the contents untouched for the next
                // reader.
                return Err(Error::PortInUse {
                    host: host.to_string(),
                    port,
                    role: PortRole::Server.to_string(),
                });
            }
            // Dead, not Redis, or unparseable: the detached server is gone.
            // Fall through to take over the lock.
        } else if contents.parse::<u32>().is_ok() {
            // A bare pid with no `detached` prefix means the previous holder
            // exited while still holding the lock -- a clean release always
            // empties the file first, so this can only be a crash. The
            // redis-server it spawned may still be running as an orphan;
            // reclaim_from_pidfile confirms the pidfile's pid is actually
            // Redis before signalling it, so a reused pid is never touched.
            process::reclaim_from_pidfile(&node_dir.join("redis.pid"));
        }
        // Empty (a clean release) or unparseable (garbage): signal nothing,
        // just take over the lock below.

        lock.file.set_len(0)?;
        lock.file.seek(SeekFrom::Start(0))?;
        write!(lock.file, "{}", std::process::id())?;

        Ok(lock)
    }

    /// Empty the file, marking a clean release, then unlock.
    ///
    /// The file is never removed: deleting a flocked file would let a
    /// starter that already opened the old inode and one that creates a new
    /// one both take the lock, reintroducing the race this design exists to
    /// avoid.
    pub(crate) fn release(self) {
        let _ = self.file.set_len(0);
    }

    /// Rewrite the lock as a detached marker naming `redis_pid`, then unlock.
    ///
    /// A later start treats a detached lock as reclaimable only once
    /// `redis_pid` is gone (or has stopped looking like Redis), never merely
    /// because the wrapper process that started it has exited -- that
    /// process exiting is exactly what `detach` says is fine.
    pub(crate) fn mark_detached(mut self, redis_pid: u32) {
        let _ = self.file.set_len(0);
        let _ = self.file.seek(SeekFrom::Start(0));
        let _ = write!(self.file, "{DETACHED_PREFIX}{redis_pid}");
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        unlock(&self.file);
    }
}

/// Try to take an exclusive, non-blocking advisory lock on `file`.
///
/// Returns `Ok(true)` if the lock was taken, `Ok(false)` if it is already
/// held elsewhere, and `Err` for any other failure.
#[cfg(unix)]
fn try_flock_exclusive(file: &File) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd;

    // SAFETY: `file` is a valid, open file owned by this function's caller
    // for the duration of this call, so its raw fd is valid for exactly as
    // long as `flock` needs it. `LOCK_NB` makes the call return immediately
    // rather than blocking, so there is no risk of holding the fd across
    // anything else.
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
        return Ok(false);
    }
    Err(err)
}

/// Release the flock on `file`'s open file description.
///
/// Best-effort: the only failure `flock(LOCK_UN)` can report for a valid fd
/// is an interrupted call, and closing the fd right after releases the lock
/// anyway once no duplicate is left.
#[cfg(unix)]
fn unlock(file: &File) {
    use std::os::unix::io::AsRawFd;

    // SAFETY: as in `try_flock_exclusive`, `file` owns a valid, open fd for
    // the duration of this call.
    let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

/// Process lifecycle management is Unix-only (see the crate-level "Platform
/// Support" docs), so there is nothing to contend with on other targets.
#[cfg(not(unix))]
fn try_flock_exclusive(_file: &File) -> std::io::Result<bool> {
    Ok(true)
}

#[cfg(not(unix))]
fn unlock(_file: &File) {}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rsw-owner-lock-unit-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// An acquire that must succeed. No retries: `Drop` releases with
    /// `LOCK_UN`, so a fork on another test thread cannot hold a released
    /// lock open, and a failure here is a real bug.
    fn acquire_now(dir: &Path, host: &str, port: u16) -> OwnerLock {
        OwnerLock::acquire(dir, host, port).expect("acquire should succeed")
    }

    #[test]
    fn acquire_creates_the_lock_with_our_pid() {
        let dir = scratch("acquire");
        let lock = OwnerLock::acquire(&dir, "127.0.0.1", 1).expect("first acquire should win");
        let contents = std::fs::read_to_string(dir.join("owner")).unwrap();
        assert_eq!(contents, std::process::id().to_string());
        lock.release();
    }

    #[test]
    fn a_second_acquire_while_the_first_is_live_fails_with_port_in_use() {
        let dir = scratch("live-conflict");
        let _lock = OwnerLock::acquire(&dir, "127.0.0.1", 2).expect("first acquire should win");
        let result = OwnerLock::acquire(&dir, "127.0.0.1", 2);
        assert!(matches!(result, Err(Error::PortInUse { port: 2, .. })));
    }

    #[test]
    fn release_leaves_an_empty_file_and_a_new_acquire_succeeds() {
        let dir = scratch("release-then-reacquire");
        let lock = OwnerLock::acquire(&dir, "127.0.0.1", 3).expect("first acquire should win");
        lock.release();

        let owner_path = dir.join("owner");
        assert!(owner_path.exists(), "release must not remove the file");
        assert_eq!(std::fs::read_to_string(&owner_path).unwrap(), "");

        let second = acquire_now(&dir, "127.0.0.1", 3);
        let contents = std::fs::read_to_string(&owner_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());
        second.release();
    }

    #[test]
    fn a_lock_naming_a_dead_pid_is_taken_over() {
        let dir = scratch("dead-owner");
        std::fs::create_dir_all(&dir).unwrap();
        // `true` exits immediately, so its pid is dead well before we get here.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();

        std::fs::write(dir.join("owner"), dead_pid.to_string()).unwrap();

        let lock = acquire_now(&dir, "127.0.0.1", 4);
        let contents = std::fs::read_to_string(dir.join("owner")).unwrap();
        assert_eq!(contents, std::process::id().to_string());
        lock.release();
    }

    #[test]
    fn garbage_contents_are_taken_over_without_signalling_anything() {
        let dir = scratch("garbage");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("owner"), "not a pid").unwrap();

        let lock = acquire_now(&dir, "127.0.0.1", 5);
        lock.release();
    }

    #[test]
    fn mark_detached_survives_drop_and_blocks_a_new_acquire_while_alive() {
        let dir = scratch("detached-alive");
        let lock = OwnerLock::acquire(&dir, "127.0.0.1", 6).unwrap();
        // Stand in for a redis-server pid with our own, very much alive, pid.
        lock.mark_detached(std::process::id());

        let contents = std::fs::read_to_string(dir.join("owner")).unwrap();
        assert_eq!(contents, format!("{DETACHED_PREFIX}{}", std::process::id()));

        // Our own pid is alive but is_redis_process(pid) is false for it, so
        // this exercises the "not actually redis" half of the detached
        // check: it must be taken over rather than reported as in use.
        let second = acquire_now(&dir, "127.0.0.1", 6);
        second.release();
    }

    #[test]
    fn drop_without_release_or_detach_frees_the_lock() {
        let dir = scratch("drop-frees-lock");
        {
            let _lock = OwnerLock::acquire(&dir, "127.0.0.1", 7).unwrap();
            assert!(dir.join("owner").exists());
        }
        let second = acquire_now(&dir, "127.0.0.1", 7);
        second.release();
    }

    #[test]
    fn exactly_one_of_many_racing_acquires_succeeds() {
        let dir = scratch("race");
        std::fs::create_dir_all(&dir).unwrap();

        const THREADS: usize = 8;
        let barrier = Arc::new(Barrier::new(THREADS));
        let dir = Arc::new(dir);

        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                let dir = Arc::clone(&dir);
                std::thread::spawn(move || {
                    barrier.wait();
                    OwnerLock::acquire(&dir, "127.0.0.1", 8000 + i as u16)
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let successes = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(successes, 1, "exactly one racing acquire should win");
    }
}
