#![cfg(feature = "blocking")]

use redis_server_wrapper::blocking::{RedisCluster, RedisSentinel, RedisServer};

#[test]
fn blocking_server_start_and_ping() {
    let server = RedisServer::new()
        .port(16500)
        .bind("127.0.0.1")
        .start()
        .expect("failed to start redis-server");

    assert!(server.is_alive());
    assert_eq!(server.port(), 16500);
    assert_eq!(server.host(), "127.0.0.1");
    assert_eq!(server.addr(), "127.0.0.1:16500");
}

#[test]
fn blocking_server_set_and_get() {
    let server = RedisServer::new()
        .port(16501)
        .start()
        .expect("failed to start redis-server");

    server.run(&["SET", "hello", "world"]).unwrap();
    let val = server.run(&["GET", "hello"]).unwrap();
    assert_eq!(val.trim(), "world");
}

#[test]
fn blocking_server_stop_and_verify() {
    let server = RedisServer::new()
        .port(16502)
        .start()
        .expect("failed to start redis-server");

    assert!(server.is_alive());
    server.stop();

    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!server.is_alive());
}

#[test]
fn blocking_cluster_start_and_health() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17100)
        .start()
        .expect("failed to start redis cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .expect("cluster did not become healthy");
    assert!(cluster.is_healthy());
    assert_eq!(cluster.node_addrs().len(), 3);
    assert_eq!(cluster.addr(), "127.0.0.1:17100");
}

#[test]
fn blocking_sentinel_start_and_health() {
    let sentinel = RedisSentinel::builder()
        .master_port(16590)
        .replicas(1)
        .replica_base_port(16591)
        .sentinels(3)
        .sentinel_base_port(26590)
        .start()
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .expect("sentinel topology did not become healthy");
    assert!(sentinel.is_healthy());
    assert_eq!(sentinel.master_name(), "mymaster");
    assert_eq!(sentinel.master_addr(), "127.0.0.1:16590");
    assert_eq!(sentinel.sentinel_addrs().len(), 3);
}
