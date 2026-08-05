//! Startup must never stop a Redis process the wrapper does not own.
//!
//! The cluster and Sentinel builders used to clear the ports they wanted by
//! sending `SHUTDOWN` to whatever was listening. These tests stand an
//! unrelated server on one of those ports and assert that startup fails
//! instead, with the intruder still running afterwards.

use redis_server_wrapper::{Error, RedisCluster, RedisSentinel, RedisServer};

#[tokio::test]
async fn cluster_start_does_not_shut_down_an_unowned_server() {
    // A server that has nothing to do with the cluster, sitting on a port the
    // cluster is about to want.
    let bystander = RedisServer::new()
        .port(18201)
        .start()
        .await
        .expect("failed to start the bystander server");
    bystander
        .run(&["SET", "precious", "data"])
        .await
        .expect("failed to seed the bystander");

    let result = RedisCluster::builder()
        .masters(3)
        .base_port(18200) // covers 18200, 18201, 18202
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port, ref role, .. }) => {
            assert_eq!(port, 18201);
            assert_eq!(role, "cluster node port");
        }
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("cluster started despite an occupied port"),
    }

    // The bystander is still running and still has its data.
    assert!(bystander.is_alive().await);
    let value = bystander
        .run(&["GET", "precious"])
        .await
        .expect("bystander should still answer");
    assert_eq!(value.trim(), "data");
}

#[tokio::test]
async fn cluster_start_checks_bus_ports_too() {
    // Redis derives the bus port as client port + 10000. A cluster whose bus
    // range is occupied cannot form, so that must fail before startup too.
    let bystander = RedisServer::new()
        .port(18311) // bus port for client port 8311, and 18310+1
        .start()
        .await
        .expect("failed to start the bystander server");

    let result = RedisCluster::builder()
        .masters(3)
        .base_port(8310) // bus ports 18310, 18311, 18312
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port, ref role, .. }) => {
            assert_eq!(port, 18311);
            assert_eq!(role, "cluster bus port");
        }
        Err(other) => panic!("expected PortInUse for a bus port, got: {other}"),
        Ok(_) => panic!("cluster started despite an occupied bus port"),
    }

    assert!(bystander.is_alive().await);
}

#[tokio::test]
async fn sentinel_start_does_not_shut_down_an_unowned_server() {
    let bystander = RedisServer::new()
        .port(18410)
        .start()
        .await
        .expect("failed to start the bystander server");
    bystander
        .run(&["SET", "precious", "data"])
        .await
        .expect("failed to seed the bystander");

    let result = RedisSentinel::builder()
        .master_port(18410)
        .replica_base_port(18420)
        .sentinel_base_port(18430)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port, ref role, .. }) => {
            assert_eq!(port, 18410);
            assert_eq!(role, "sentinel master port");
        }
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("sentinel topology started despite an occupied port"),
    }

    assert!(bystander.is_alive().await);
    let value = bystander
        .run(&["GET", "precious"])
        .await
        .expect("bystander should still answer");
    assert_eq!(value.trim(), "data");
}

#[tokio::test]
async fn sentinel_start_checks_sentinel_ports() {
    let bystander = RedisServer::new()
        .port(18531)
        .start()
        .await
        .expect("failed to start the bystander server");

    let result = RedisSentinel::builder()
        .master_port(18510)
        .replica_base_port(18520)
        .sentinel_base_port(18530)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .start()
        .await;

    match result {
        Err(Error::PortInUse { port, ref role, .. }) => {
            assert_eq!(port, 18531);
            assert_eq!(role, "sentinel port");
        }
        Err(other) => panic!("expected PortInUse, got: {other}"),
        Ok(_) => panic!("sentinel topology started despite an occupied port"),
    }

    assert!(bystander.is_alive().await);
}

#[tokio::test]
async fn a_free_topology_still_starts() {
    // The preflight check must not reject a topology whose ports are free.
    let cluster = RedisCluster::builder()
        .masters(3)
        .base_port(18600)
        .start()
        .await
        .expect("a cluster on free ports must still start");

    assert!(cluster.is_healthy().await);
}
