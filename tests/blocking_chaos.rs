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

#[test]
fn cluster_wait_for_all_healthy_and_cluster_info() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17950)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");
    cluster
        .wait_for_all_healthy(Duration::from_secs(30))
        .expect("cluster did not converge to all-nodes-healthy");

    for index in 0..cluster.node_addrs().len() {
        let info = cluster.cluster_info(index).expect("cluster_info failed");
        assert_eq!(info.get("cluster_state").map(String::as_str), Some("ok"));
    }
}

#[test]
fn chaos_slot_helpers_and_key_count() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17960)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "count-key").expect("keyslot failed");
    assert!(slot < 16384);

    let owner_port = chaos::slot_owner(&cluster, slot).expect("slot_owner failed");
    assert!(
        cluster
            .node_addrs()
            .iter()
            .any(|a| a.ends_with(&format!(":{owner_port}")))
    );

    cluster
        .node_run(
            cluster
                .node_addrs()
                .iter()
                .position(|a| a.ends_with(&format!(":{owner_port}")))
                .expect("owner port not found among cluster nodes"),
            &["SET", "count-key", "count-value"],
        )
        .expect("SET on slot owner failed");

    let count = chaos::count_keys_in_slot(&cluster, slot).expect("count_keys_in_slot failed");
    assert_eq!(count, 1);
}

#[test]
fn chaos_wait_for_slot_owner_change_after_failover() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17970)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "failover-slot-key").expect("keyslot failed");
    let old_owner_port = chaos::slot_owner(&cluster, slot).expect("slot_owner failed");

    let masters = cluster.num_masters() as usize;
    let total = cluster.node_addrs().len();
    let mut replica_index = None;
    for index in masters..total {
        let repl_info = cluster
            .node_run(index, &["INFO", "replication"])
            .expect("INFO replication failed");
        let master_port_line = repl_info
            .lines()
            .find(|l| l.starts_with("master_port:"))
            .map(|l| l.trim_end_matches('\r'));
        if master_port_line == Some(&format!("master_port:{old_owner_port}")) {
            replica_index = Some(index);
            break;
        }
    }
    let replica_index = replica_index.expect("no replica found for the slot's current owner");

    let replica_addr = &cluster.node_addrs()[replica_index];
    let replica_port: u16 = replica_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("could not parse replica port");

    cluster
        .node_run(replica_index, &["CLUSTER", "FAILOVER"])
        .expect("trigger_failover failed");

    let new_owner_port =
        chaos::wait_for_slot_owner_change(&cluster, slot, old_owner_port, Duration::from_secs(30))
            .expect("wait_for_slot_owner_change failed");

    assert_ne!(new_owner_port, old_owner_port);
    assert_eq!(new_owner_port, replica_port);
}
