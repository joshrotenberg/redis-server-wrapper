use redis_server_wrapper::{RedisCluster, RedisServer, chaos};
use std::time::Duration;

#[cfg_attr(not(unix), ignore)]
#[tokio::test]
async fn freeze_and_resume_node() {
    let server = RedisServer::new()
        .port(17700)
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);

    chaos::freeze_node(&server).expect("freeze_node failed");

    // SIGSTOP suspends the process but the kernel still completes the TCP
    // handshake, so a PING can hang forever waiting for a reply that will
    // never come. Bound the check with a timeout.
    let frozen_ping = tokio::time::timeout(Duration::from_secs(2), server.is_alive()).await;
    assert!(frozen_ping.is_err() || frozen_ping == Ok(false));

    chaos::resume_node(&server).expect("resume_node failed");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.is_alive().await);
}

#[tokio::test]
async fn kill_node_terminates_process() {
    let server = RedisServer::new()
        .port(17701)
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);

    chaos::kill_node(&server).expect("kill_node failed");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!server.is_alive().await);
}

#[tokio::test]
async fn slow_down_returns_ok() {
    let server = RedisServer::new()
        .port(17702)
        .start()
        .await
        .expect("failed to start server");

    let result = chaos::slow_down(&server, 100).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn trigger_save_and_flushall() {
    let server = RedisServer::new()
        .port(17703)
        .start()
        .await
        .expect("failed to start server");

    server.run(&["SET", "k", "v"]).await.expect("SET failed");

    chaos::trigger_save(&server).await.expect("bgsave failed");
    chaos::flushall(&server).await.expect("flushall failed");

    let val = server.run(&["GET", "k"]).await.expect("GET failed");
    assert!(val.trim().is_empty() || val.trim() == "(nil)");
}

#[tokio::test]
async fn failover_and_recover_cluster() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17710)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let replica = &cluster.replica_nodes()[0];
    chaos::trigger_failover(replica)
        .await
        .expect("trigger_failover failed");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not recover after failover");

    chaos::recover(&cluster).expect("recover failed");
    assert!(cluster.all_alive().await);
}

#[cfg_attr(not(unix), ignore)]
#[tokio::test]
async fn pause_node_resumes_automatically() {
    let server = RedisServer::new()
        .port(17704)
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);

    chaos::pause_node(&server, Duration::from_millis(300)).expect("pause_node failed");

    tokio::time::sleep(Duration::from_millis(600)).await;

    let resumed_ping = tokio::time::timeout(Duration::from_secs(2), server.is_alive()).await;
    assert_eq!(resumed_ping, Ok(true));
}

#[tokio::test]
async fn fill_memory_writes_keys() {
    let server = RedisServer::new()
        .port(17705)
        .start()
        .await
        .expect("failed to start server");

    chaos::fill_memory(&server, "k:", 50)
        .await
        .expect("fill_memory failed");

    let dbsize = server.run(&["DBSIZE"]).await.expect("DBSIZE failed");
    let dbsize: u64 = dbsize
        .trim()
        .parse()
        .expect("DBSIZE did not return an integer");
    assert_eq!(dbsize, 50);
}

#[cfg_attr(not(unix), ignore)]
#[tokio::test]
async fn partition_freezes_unreachable_nodes() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17740)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let frozen = chaos::partition(&cluster, &[0]).expect("partition failed");
    assert!(!frozen.is_empty());
    assert_eq!(frozen.len(), cluster.nodes().len() - 1);

    chaos::recover(&cluster).expect("recover failed");

    let node0 = &cluster.nodes()[0];
    assert!(node0.run(&["PING"]).await.is_ok());
}

#[tokio::test]
async fn kill_master_by_slot_removes_node() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17730)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let killed_port = chaos::kill_master_by_slot(&cluster, 0)
        .await
        .expect("kill_master_by_slot failed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let killed_node = cluster
        .nodes()
        .iter()
        .find(|n| n.port() == killed_port)
        .expect("killed node not found among cluster nodes");
    assert!(!killed_node.is_alive().await);
}

#[tokio::test]
async fn public_slot_helpers_and_key_count() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17920)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "count-key")
        .await
        .expect("keyslot failed");
    assert!(slot < 16384);

    let owner_port = chaos::slot_owner(&cluster, slot)
        .await
        .expect("slot_owner failed");
    let owner = cluster
        .nodes()
        .iter()
        .find(|n| n.port() == owner_port)
        .expect("slot owner port did not match any cluster node");
    owner
        .run(&["SET", "count-key", "count-value"])
        .await
        .expect("SET on slot owner failed");

    let count = chaos::count_keys_in_slot(&cluster, slot)
        .await
        .expect("count_keys_in_slot failed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn wait_for_slot_owner_change_after_failover() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17930)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "failover-slot-key")
        .await
        .expect("keyslot failed");
    let old_owner_port = chaos::slot_owner(&cluster, slot)
        .await
        .expect("slot_owner failed");

    // Find the replica that replicates the slot's current owner, via its
    // own INFO replication view rather than assuming an index pairing --
    // `redis-cli --cluster create` doesn't guarantee replica i replicates
    // master i.
    let mut target_replica = None;
    for replica in cluster.replica_nodes() {
        let repl_info = replica
            .info(Some("replication"))
            .await
            .expect("INFO replication failed");
        let master_port: u16 = repl_info
            .get("master_port")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if master_port == old_owner_port {
            target_replica = Some(replica);
            break;
        }
    }
    let replica = target_replica.expect("no replica found for the slot's current owner");

    chaos::trigger_failover(replica)
        .await
        .expect("trigger_failover failed");

    let new_owner_port =
        chaos::wait_for_slot_owner_change(&cluster, slot, old_owner_port, Duration::from_secs(30))
            .await
            .expect("wait_for_slot_owner_change failed");

    assert_ne!(new_owner_port, old_owner_port);
    assert_eq!(new_owner_port, replica.port());
}
