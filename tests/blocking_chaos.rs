#![cfg(feature = "blocking")]

use redis_server_wrapper::blocking::{RedisCluster, RedisServer, chaos};
use std::time::Duration;

#[cfg_attr(not(unix), ignore)]
#[test]
fn freeze_and_resume_node() {
    let server = RedisServer::new()
        .port(17800)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());

    chaos::freeze_node(&server).expect("freeze_node failed");
    std::thread::sleep(Duration::from_millis(200));

    chaos::resume_node(&server).expect("resume_node failed");
    std::thread::sleep(Duration::from_millis(200));

    let pong = server.run(&["PING"]).expect("PING failed after resume");
    assert_eq!(pong.trim(), "PONG");
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn pause_node_resumes_automatically() {
    let server = RedisServer::new()
        .port(17801)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());

    chaos::pause_node(&server, Duration::from_millis(300)).expect("pause_node failed");

    std::thread::sleep(Duration::from_millis(600));

    assert!(server.is_alive());
}

#[test]
fn fill_memory_writes_keys() {
    let server = RedisServer::new()
        .port(17802)
        .start()
        .expect("failed to start server");

    chaos::fill_memory(&server, "k:", 50).expect("fill_memory failed");

    let dbsize = server.run(&["DBSIZE"]).expect("DBSIZE failed");
    let dbsize: u64 = dbsize
        .trim()
        .parse()
        .expect("DBSIZE did not return an integer");
    assert_eq!(dbsize, 50);
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn partition_and_recover_cluster() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17820)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let frozen = chaos::partition(&cluster, &[0]).expect("partition failed");
    assert!(!frozen.is_empty());
    assert_eq!(frozen.len(), cluster.node_addrs().len() - 1);

    chaos::recover(&cluster).expect("recover failed");

    let pong = cluster
        .node_run(0, &["PING"])
        .expect("node 0 did not respond after recover");
    assert_eq!(pong.trim(), "PONG");
}
