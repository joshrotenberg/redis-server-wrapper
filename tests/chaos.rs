use redis_server_wrapper::chaos::{ClientKillFilter, ClientType};
use redis_server_wrapper::{RedisCluster, RedisServer, chaos, server};
use std::time::Duration;
use tokio::io::AsyncReadExt;

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

#[cfg_attr(not(unix), ignore)]
#[tokio::test]
async fn restart_node_after_kill() {
    let mut server = RedisServer::new()
        .port(18000)
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);
    let old_pid = server.pid();

    chaos::kill_node(&server).expect("kill_node failed");

    // Poll until the process is confirmed dead before restarting.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server.is_alive().await {
        assert!(
            std::time::Instant::now() < deadline,
            "killed node never stopped responding"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    chaos::restart_node(&mut server, Duration::from_secs(10))
        .await
        .expect("restart_node failed");

    assert!(server.is_alive().await);
    assert_ne!(server.pid(), old_pid);
}

#[tokio::test]
async fn kill_client_connections_by_type() {
    let server = RedisServer::new()
        .port(18010)
        .start()
        .await
        .expect("failed to start server");

    // A raw connection with no command sent still registers as a `normal`
    // client the moment Redis accepts it.
    let mut extra = tokio::net::TcpStream::connect((server.host(), server.port()))
        .await
        .expect("connect failed");

    let killed =
        chaos::kill_client_connections(&server, ClientKillFilter::of_type(ClientType::Normal))
            .await
            .expect("kill_client_connections failed");
    assert!(killed >= 1, "expected at least one client killed");

    // The killed connection observes the close (EOF) on its next read --
    // bounded so a wrong-filter bug can't hang the test.
    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_secs(2), extra.read(&mut buf))
        .await
        .expect("read timed out")
        .expect("read failed");
    assert_eq!(n, 0, "expected the killed connection to observe EOF");
}

#[tokio::test]
async fn kill_client_connections_by_addr() {
    let server = RedisServer::new()
        .port(18011)
        .start()
        .await
        .expect("failed to start server");

    let mut extra = tokio::net::TcpStream::connect((server.host(), server.port()))
        .await
        .expect("connect failed");
    let local_addr = extra.local_addr().expect("local_addr failed");

    let killed =
        chaos::kill_client_connections(&server, ClientKillFilter::by_addr(local_addr.to_string()))
            .await
            .expect("kill_client_connections failed");
    assert_eq!(killed, 1);

    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_secs(2), extra.read(&mut buf))
        .await
        .expect("read timed out")
        .expect("read failed");
    assert_eq!(n, 0, "expected the killed connection to observe EOF");
}

#[tokio::test]
async fn block_event_loop_blocks_then_recovers() {
    let server = RedisServer::new()
        .port(18020)
        .enable_debug_command("yes")
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);

    chaos::block_event_loop(&server, Duration::from_secs(1)).expect("block_event_loop failed");

    // Give the spawned task a moment to actually issue DEBUG SLEEP, then
    // confirm the server is unresponsive while the sleep is in effect: a
    // short bounded PING should time out rather than return quickly.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let blocked = tokio::time::timeout(Duration::from_millis(200), server.run(&["PING"])).await;
    assert!(
        blocked.is_err(),
        "expected PING to be blocked while DEBUG SLEEP is in effect"
    );

    // Poll until the server recovers -- bounded so a real failure doesn't
    // hang the suite.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if server.is_alive().await {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server did not recover after block_event_loop"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn force_full_resync_triggers_replica_full_sync() {
    let master = RedisServer::new()
        .port(18030)
        .enable_debug_command("yes")
        .start()
        .await
        .expect("failed to start master");

    let replica = RedisServer::new()
        .port(18031)
        .replicaof("127.0.0.1", 18030)
        .start()
        .await
        .expect("failed to start replica");

    replica
        .wait_until_role("slave", Duration::from_secs(10))
        .await
        .expect("replica did not report role slave");

    server::wait_for_replica_sync(&replica, &master, Duration::from_secs(10))
        .await
        .expect("replica did not catch up initially");

    // `sync_full` lives in the `# Stats` section of `INFO`, not `#
    // Replication`.
    let sync_full_before: u64 = master
        .info(Some("stats"))
        .await
        .expect("INFO stats failed")
        .get("sync_full")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    chaos::force_full_resync(&master)
        .await
        .expect("force_full_resync failed");

    // Poll until the master counts a new full resync -- proves the replica
    // reconnected via a full SYNC rather than a partial one.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let sync_full: u64 = master
            .info(Some("stats"))
            .await
            .expect("INFO stats failed")
            .get("sync_full")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if sync_full > sync_full_before {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "master did not record a new full resync after force_full_resync"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The replica should be healthy again afterward.
    master
        .run(&["SET", "resync-key", "resync-value"])
        .await
        .expect("SET on master failed");
    server::wait_for_replica_sync(&replica, &master, Duration::from_secs(10))
        .await
        .expect("replica did not resync after force_full_resync");
    let val = replica
        .run(&["GET", "resync-key"])
        .await
        .expect("GET on replica failed");
    assert_eq!(val.trim(), "resync-value");
}

#[tokio::test]
async fn break_persistence_rejects_writes_then_restores() {
    let server = RedisServer::new()
        .port(18040)
        .save(true)
        .start()
        .await
        .expect("failed to start server");

    let guard = match chaos::break_persistence(&server, Duration::from_secs(10)).await {
        Ok(guard) => guard,
        Err(redis_server_wrapper::Error::PrivilegeRequired { message }) => {
            eprintln!("skipping break_persistence_rejects_writes_then_restores: {message}");
            return;
        }
        Err(e) => panic!("break_persistence failed: {e}"),
    };

    let result = server.run(&["SET", "k", "v"]).await;
    let saw_miscount = match &result {
        Err(e) => e.to_string().to_uppercase().contains("MISCONF"),
        Ok(s) => s.to_uppercase().contains("MISCONF"),
    };
    assert!(saw_miscount, "expected a MISCONF error, got: {result:?}");

    guard
        .restore(Duration::from_secs(10))
        .await
        .expect("restore failed");

    server
        .run(&["SET", "k", "v"])
        .await
        .expect("SET after restore failed");
}

#[tokio::test]
async fn exhaust_maxclients_rejects_new_connections() {
    let server = RedisServer::new()
        .port(18050)
        .start()
        .await
        .expect("failed to start server");

    let guard = chaos::exhaust_maxclients(&server, Some(5))
        .await
        .expect("exhaust_maxclients failed");

    // While exhausted, a fresh connection should be rejected immediately.
    let mut extra = tokio::net::TcpStream::connect((server.host(), server.port()))
        .await
        .expect("connect failed");
    let mut buf = [0u8; 128];
    let n = tokio::time::timeout(Duration::from_secs(2), extra.read(&mut buf))
        .await
        .expect("read timed out")
        .expect("read failed");
    assert!(
        String::from_utf8_lossy(&buf[..n]).contains("max number of clients reached"),
        "expected a max-clients rejection"
    );

    guard.release().await.expect("release failed");

    // After release, the server accepts and answers commands again.
    let pong = server
        .run(&["PING"])
        .await
        .expect("PING failed after release");
    assert_eq!(pong.trim(), "PONG");
}

#[cfg_attr(not(unix), ignore)]
#[tokio::test]
async fn flap_node_cycles_and_leaves_running_on_drop() {
    let server = RedisServer::new()
        .port(18060)
        .start()
        .await
        .expect("failed to start server");

    assert!(server.is_alive().await);

    let guard = chaos::flap_node(
        &server,
        Duration::from_millis(150),
        Duration::from_millis(150),
    )
    .expect("flap_node failed");

    // Poll-based: expect to observe at least one "down" window within a few
    // cycles.
    let mut saw_down = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let ping = tokio::time::timeout(Duration::from_millis(80), server.is_alive()).await;
        if !matches!(ping, Ok(true)) {
            saw_down = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        saw_down,
        "expected to observe at least one down window while flapping"
    );

    drop(guard);

    // Poll until the node is confirmed running again after the guard drops.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if server.is_alive().await {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node did not resume running after dropping FlapGuard"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
