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

    chaos::freeze_node(&server);

    // SIGSTOP suspends the process but the kernel still completes the TCP
    // handshake, so a PING can hang forever waiting for a reply that will
    // never come. Bound the check with a timeout.
    let frozen_ping = tokio::time::timeout(Duration::from_secs(2), server.is_alive()).await;
    assert!(frozen_ping.is_err() || frozen_ping == Ok(false));

    chaos::resume_node(&server);
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

    chaos::kill_node(&server);
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

    chaos::recover(&cluster);
    assert!(cluster.all_alive().await);
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
