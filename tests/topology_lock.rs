//! A topology's owner lock (#194) stops a second start of the same shape,
//! in the same process, from reclaiming (and killing) the nodes a live copy
//! of it already started.
//!
//! `src/owner_lock.rs` gave this guarantee to a standalone server (#186);
//! these tests cover the same guarantee for `RedisCluster` and
//! `RedisSentinel`, whose reclaim scans the whole topology's stable
//! directory rather than a single node's.

use redis_server_wrapper::{Error, RedisCluster, RedisSentinel};

#[tokio::test]
async fn a_second_start_of_the_same_live_cluster_fails_without_touching_it() {
    let first = RedisCluster::builder()
        .masters(3)
        .base_port(19700)
        .start()
        .await
        .expect("first cluster should start");
    assert!(first.is_healthy().await);

    let result = RedisCluster::builder()
        .masters(3)
        .base_port(19700)
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port: 19700, .. }) => {}
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("a second start of the same live cluster must not reclaim its nodes"),
    }

    // The lock is checked before reclaim ever runs, so the first cluster's
    // nodes must be completely untouched by the second start's failure.
    assert!(first.is_healthy().await);
    assert_eq!(first.node_addrs().len(), 3);

    drop(first);

    // Once the first topology has stopped, the lock is released and a start
    // of the same shape succeeds again.
    let third = RedisCluster::builder()
        .masters(3)
        .base_port(19700)
        .start()
        .await
        .expect("a start after the first topology stopped should succeed");
    assert!(third.is_healthy().await);
}

#[tokio::test]
async fn a_second_start_of_the_same_live_sentinel_topology_fails_without_touching_it() {
    let first = RedisSentinel::builder()
        .master_port(19710)
        .replica_base_port(19711)
        .sentinel_base_port(29710)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("first sentinel topology should start");
    assert!(first.is_healthy().await);

    let result = RedisSentinel::builder()
        .master_port(19710)
        .replica_base_port(19711)
        .sentinel_base_port(29710)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port: 19710, .. }) => {}
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("a second start of the same live sentinel topology must not reclaim it"),
    }

    assert!(first.is_healthy().await);

    drop(first);

    let third = RedisSentinel::builder()
        .master_port(19710)
        .replica_base_port(19711)
        .sentinel_base_port(29710)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("a start after the first topology stopped should succeed");
    assert!(third.is_healthy().await);
}
