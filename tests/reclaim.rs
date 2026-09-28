//! Reclaiming a topology left behind by a crashed run.
//!
//! Startup fails closed on an occupied port rather than clearing it, because
//! the wrapper cannot tell an unrelated Redis from one of its own. The cost
//! used to be that a crashed run left orphans blocking every later start.
//!
//! These tests simulate a crash with `std::mem::forget`, which drops the
//! handle's ownership without running the teardown in its `Drop`, leaving the
//! processes running exactly as an aborted run would.

use redis_server_wrapper::{Error, RedisCluster, RedisSentinel, RedisServer};
use redis_server_wrapper::{preflight, process};

#[tokio::test]
async fn a_cluster_orphaned_by_a_crash_can_be_restarted() {
    let first = RedisCluster::builder()
        .masters(3)
        .base_port(19400)
        .start()
        .await
        .expect("first cluster should start");

    let addrs = first.node_addrs();
    assert_eq!(addrs.len(), 3);

    // Abandon the handle without stopping anything: the nodes stay up holding
    // their ports, as they would after a panic or a kill -9 of the runner.
    std::mem::forget(first);

    // The same topology starts again rather than failing on its own leftovers.
    let second = RedisCluster::builder()
        .masters(3)
        .base_port(19400)
        .start()
        .await
        .expect("the same topology should reclaim its own orphans and restart");

    assert!(second.is_healthy().await);
    assert_eq!(second.node_addrs().len(), 3);
}

#[tokio::test]
async fn a_sentinel_topology_orphaned_by_a_crash_can_be_restarted() {
    let first = RedisSentinel::builder()
        .master_port(19410)
        .replica_base_port(19411)
        .sentinel_base_port(29410)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("first topology should start");
    std::mem::forget(first);

    let second = RedisSentinel::builder()
        .master_port(19410)
        .replica_base_port(19411)
        .sentinel_base_port(29410)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("the same topology should reclaim its own orphans and restart");

    assert!(second.is_healthy().await);
}

#[tokio::test]
async fn reclaim_still_refuses_to_touch_an_unowned_server() {
    // The guarantee reclaim must not weaken. A server the wrapper did not
    // start is not covered by any pidfile it wrote, so it is left alone and
    // the port preflight rejects the topology.
    let bystander = RedisServer::new()
        .port(19421)
        .start()
        .await
        .expect("bystander should start");
    bystander
        .run(&["SET", "precious", "data"])
        .await
        .expect("seed failed");

    let result = RedisCluster::builder()
        .masters(3)
        .base_port(19420) // covers 19420, 19421, 19422
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port, .. }) => assert_eq!(port, 19421),
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("an unowned server must still fail the preflight"),
    }

    assert!(bystander.is_alive().await);
    let value = bystander
        .run(&["GET", "precious"])
        .await
        .expect("bystander should still answer");
    assert_eq!(value.trim(), "data");
}

#[tokio::test]
async fn restarting_a_cluster_does_not_inherit_the_previous_run_data() {
    // A stable directory keeps the previous run's nodes.conf and RDB, which
    // would both leave the node non-empty and make CLUSTER CREATE refuse it.
    // Reclaim clears the state it owns, so a restart is genuinely fresh.
    let first = RedisCluster::builder()
        .masters(3)
        .base_port(19430)
        .start()
        .await
        .expect("first cluster should start");

    // Write through a cluster-aware client so the key lands on whichever node
    // owns its slot, rather than assuming a hash.
    first
        .cli()
        .cluster_mode(true)
        .run(&["SET", "leftover", "value"])
        .await
        .expect("write should succeed");

    let before: u64 = {
        let mut total = 0;
        for node in first.nodes() {
            total += node
                .run(&["DBSIZE"])
                .await
                .expect("DBSIZE should succeed")
                .trim()
                .parse::<u64>()
                .unwrap_or(0);
        }
        total
    };
    assert_eq!(before, 1, "the key should be somewhere in the cluster");

    std::mem::forget(first);

    let second = RedisCluster::builder()
        .masters(3)
        .base_port(19430)
        .start()
        .await
        .expect("restart should succeed");

    let mut after = 0;
    for node in second.nodes() {
        after += node
            .run(&["DBSIZE"])
            .await
            .expect("DBSIZE should succeed")
            .trim()
            .parse::<u64>()
            .unwrap_or(0);
    }
    assert_eq!(
        after, 0,
        "a reclaimed cluster must not carry the previous run's keys"
    );
}

#[tokio::test]
async fn a_failed_sentinel_start_stops_the_sentinel_it_already_started() {
    // A sentinel that fails partway through must not leave the sentinels it
    // already started running as orphaned daemons (#184).
    let dir = std::env::temp_dir().join(format!("rsw-sentinel-partial-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("failed to create the test dir");

    // A regular file at the second sentinel's directory path makes its
    // `create_dir_all` fail after the first sentinel is already running.
    // `reclaim_owned_processes`'s cleanup pass only removes directories it
    // owns -- `remove_dir_all` fails silently on a file -- so this survives
    // the reclaim `start_inner` runs before anything is spawned.
    std::fs::write(dir.join("sentinel-28151"), b"not a directory")
        .expect("failed to seed the blocking file");

    let result = RedisSentinel::builder()
        .dir(dir.clone())
        .master_port(18150)
        .replica_base_port(18151)
        .sentinel_base_port(28150)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await;

    match result {
        Err(_) => {}
        Ok(_) => panic!("sentinel start should have failed on the blocked sentinel directory"),
    }

    // The first sentinel was spawned and daemonized before the second one's
    // directory failed to create; it must not still be running.
    assert!(
        preflight::port_available("127.0.0.1", 28150),
        "the first sentinel's port must be free after a failed start"
    );
    let pidfile = dir.join("sentinel-28150").join("sentinel.pid");
    if let Some(pid) = process::read_pidfile(&pidfile) {
        assert!(
            !process::pid_alive(pid),
            "the first sentinel's pid must not still be alive"
        );
    }

    // The master and replica are stopped by their handles' Drop; cheap to
    // check as well.
    assert!(preflight::port_available("127.0.0.1", 18150));
    assert!(preflight::port_available("127.0.0.1", 18151));

    let _ = std::fs::remove_dir_all(&dir);
}
