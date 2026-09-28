//! The owner lock that guards a standalone server's node directory.
//!
//! The default `dir` is shared across every process on the machine, so two
//! starts on the same port and directory have no way to tell a live owner
//! from a crashed run's leftover without something recorded on disk. The
//! lock is an advisory flock held for the handle's lifetime, on a file that
//! is never deleted: a live owner blocks a second start, a crashed one is
//! taken over, a bogus pidfile with no lock behind it is never signalled,
//! and stopping, dropping, or failing a start frees the lock without ever
//! removing the file.

use std::process::Command;
use std::time::Duration;

use redis_server_wrapper::{Error, RedisCli, RedisServer, process};

/// A fresh, uniquely named scratch directory for one test.
fn scratch_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rsw-owner-lock-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn node_dir(dir: &std::path::Path, port: u16) -> std::path::PathBuf {
    dir.join(format!("node-{port}"))
}

/// Spawn `sleep 1000` as the leader of its own process group, so a
/// `force_kill` aimed at it cannot be mistaken for a signal against the test
/// harness itself. Alive for the life of the test; never Redis, per
/// [`process::is_redis_process`].
fn spawn_long_lived_non_redis_process() -> std::process::Child {
    use std::os::unix::process::CommandExt;
    Command::new("sleep")
        .arg("1000")
        .process_group(0)
        .spawn()
        .expect("failed to spawn sleep")
}

/// A pid that is guaranteed to be dead: `true` exits immediately, and we wait
/// for it before handing the pid back.
fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().expect("failed to spawn true");
    let pid = child.id();
    child.wait().expect("failed to wait for true");
    pid
}

/// A start that must succeed on the first try once the lock is free.
///
/// No retries. Other tests in this binary fork constantly, and a fork can
/// hold a duplicate of a just-released lock fd until it execs; the lock is
/// released with `LOCK_UN` rather than by closing the fd precisely so that
/// window cannot produce a spurious `PortInUse`. A failure here is a bug.
async fn start_now(build: impl Fn() -> RedisServer) -> redis_server_wrapper::RedisServerHandle {
    build()
        .start()
        .await
        .expect("start should succeed once the lock is free")
}

#[tokio::test]
async fn second_start_on_a_live_port_and_dir_returns_port_in_use() {
    let dir = scratch_dir("two-handles");

    let first = RedisServer::new()
        .port(16600)
        .dir(&dir)
        .start()
        .await
        .expect("first start should succeed");
    first
        .run(&["SET", "precious", "data"])
        .await
        .expect("seed failed");

    let result = RedisServer::new().port(16600).dir(&dir).start().await;

    match result {
        Err(Error::PortInUse { port: 16600, .. }) => {}
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("a second start on a live port and dir must fail"),
    }

    assert!(first.is_alive().await, "the first server must survive");
    let value = first
        .run(&["GET", "precious"])
        .await
        .expect("GET should still work");
    assert_eq!(value.trim(), "data");
}

#[tokio::test]
async fn a_pidfile_naming_a_live_non_redis_process_is_never_signalled() {
    let dir = scratch_dir("non-redis-pidfile");
    let node = node_dir(&dir, 16601);
    std::fs::create_dir_all(&node).expect("failed to create node dir");

    let mut bystander = spawn_long_lived_non_redis_process();
    std::fs::write(node.join("redis.pid"), bystander.id().to_string())
        .expect("failed to write pidfile");

    let result = RedisServer::new()
        .port(16601)
        .dir(&dir)
        .start()
        .await
        .expect("the port is free; a bogus pidfile with no lock must not block the start");

    assert!(
        process::pid_alive(bystander.id()),
        "a live non-redis process named only by a pidfile must survive"
    );

    result.stop();
    let _ = bystander.kill();
    let _ = bystander.wait();
}

#[tokio::test]
async fn a_pidfile_naming_a_live_non_redis_process_survives_a_stale_owner_file() {
    let dir = scratch_dir("non-redis-pidfile-stale-owner");
    let node = node_dir(&dir, 16602);
    std::fs::create_dir_all(&node).expect("failed to create node dir");

    let mut bystander = spawn_long_lived_non_redis_process();
    std::fs::write(node.join("redis.pid"), bystander.id().to_string())
        .expect("failed to write pidfile");
    std::fs::write(node.join("owner"), dead_pid().to_string()).expect("failed to write owner");

    let result = RedisServer::new()
        .port(16602)
        .dir(&dir)
        .start()
        .await
        .expect("a stale owner file must reclaim the lock, not the pidfile's process");

    assert!(
        process::pid_alive(bystander.id()),
        "the non-redis process must survive even once the stale lock is reclaimed"
    );

    result.stop();
    let _ = bystander.kill();
    let _ = bystander.wait();
}

#[tokio::test]
async fn a_stale_owner_with_a_live_orphaned_redis_is_reclaimed() {
    let dir = scratch_dir("stale-owner-live-orphan");

    // Start a server and detach it, simulating a redis-server left running by
    // a process that is about to disappear without stopping it.
    let orphan = RedisServer::new()
        .port(16603)
        .dir(&dir)
        .start()
        .await
        .expect("orphan should start");
    let orphan_pid = orphan.pid();
    orphan.detach();

    // Overwrite the detached marker `detach` just wrote with a plain dead
    // pid, simulating a wrapper process that crashed before it ever called
    // detach: the owner file names a wrapper pid, not a redis pid, and that
    // wrapper is gone.
    std::fs::write(node_dir(&dir, 16603).join("owner"), dead_pid().to_string())
        .expect("failed to overwrite owner file");

    let second = start_now(|| RedisServer::new().port(16603).dir(&dir)).await;

    assert!(
        !process::pid_alive(orphan_pid),
        "the crashed owner's orphaned redis-server should have been reclaimed"
    );
    assert_ne!(second.pid(), orphan_pid);
    assert!(second.is_alive().await);
}

#[tokio::test]
async fn a_detached_handle_blocks_a_new_start_until_shut_down() {
    let dir = scratch_dir("detached-blocks");

    let handle = RedisServer::new()
        .port(16604)
        .dir(&dir)
        .start()
        .await
        .expect("first start should succeed");
    handle.detach();

    let result = RedisServer::new().port(16604).dir(&dir).start().await;
    match result {
        Err(Error::PortInUse { port: 16604, .. }) => {}
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => {
            panic!("a detached server that is still alive and still redis must block a new start")
        }
    }

    RedisCli::new().port(16604).shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let restarted = start_now(|| RedisServer::new().port(16604).dir(&dir)).await;
    assert!(restarted.is_alive().await);
}

#[tokio::test]
async fn lock_free_after_stop() {
    let dir = scratch_dir("free-after-stop");
    let owner_path = node_dir(&dir, 16605).join("owner");

    let handle = RedisServer::new()
        .port(16605)
        .dir(&dir)
        .start()
        .await
        .expect("start should succeed");
    assert!(
        owner_path.exists(),
        "the lock file should exist while running"
    );

    handle.stop();

    // The file is never removed, only unlocked and emptied; what proves the
    // lock is free is that a fresh start on the same port and dir succeeds.
    assert!(owner_path.exists(), "stop must not remove the lock file");
    let restarted = start_now(|| RedisServer::new().port(16605).dir(&dir)).await;
    assert!(restarted.is_alive().await);
}

#[tokio::test]
async fn lock_free_after_drop() {
    let dir = scratch_dir("free-after-drop");
    let owner_path = node_dir(&dir, 16606).join("owner");

    {
        let _handle = RedisServer::new()
            .port(16606)
            .dir(&dir)
            .start()
            .await
            .expect("start should succeed");
        assert!(
            owner_path.exists(),
            "the lock file should exist while running"
        );
    }

    assert!(owner_path.exists(), "drop must not remove the lock file");
    let restarted = start_now(|| RedisServer::new().port(16606).dir(&dir)).await;
    assert!(restarted.is_alive().await);
}

#[tokio::test]
async fn lock_free_after_a_failed_start() {
    let dir = scratch_dir("free-after-failed-start");

    let result = RedisServer::new()
        .port(16607)
        .dir(&dir)
        .extra("thisisnotarealdirective", "yes")
        .start()
        .await;

    assert!(
        result.is_err(),
        "redis-server should refuse an unrecognized directive"
    );

    let started = start_now(|| RedisServer::new().port(16607).dir(&dir)).await;
    assert!(started.is_alive().await);
}
