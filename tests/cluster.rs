use redis_server_wrapper::RedisCluster;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn cluster_start_and_health() {
    let log_path = std::env::temp_dir().join(format!(
        "redis-cluster-{}.log",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    ));
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17000)
        .logfile(log_path.display().to_string())
        .extra("maxmemory", "10mb")
        .start()
        .await
        .expect("failed to start redis cluster");

    assert!(cluster.all_alive().await);
    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");
    assert!(cluster.is_healthy().await);
    let maxmemory = cluster
        .cli()
        .run(&["CONFIG", "GET", "maxmemory"])
        .await
        .expect("failed to query cluster config");
    assert!(maxmemory.contains("10485760") || maxmemory.contains("10mb"));
    let logfile = cluster
        .cli()
        .run(&["CONFIG", "GET", "logfile"])
        .await
        .expect("failed to query cluster logfile");
    assert!(logfile.contains(&log_path.display().to_string()));
    assert_eq!(cluster.node_addrs().len(), 3);
    assert_eq!(cluster.addr(), "127.0.0.1:17000");
}

#[tokio::test]
async fn cluster_password_auth() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17010)
        .password("testpass")
        .start()
        .await
        .expect("failed to start password-protected redis cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("password-protected cluster did not become healthy");

    let pong = cluster.cli().run(&["PING"]).await.unwrap();
    assert_eq!(pong.trim(), "PONG");
}

#[tokio::test]
async fn cluster_node_access_and_config_set() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17020)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    // Verify topology counts.
    assert_eq!(cluster.nodes().len(), 6);
    assert_eq!(cluster.num_masters(), 3);
    assert_eq!(cluster.master_nodes().len(), 3);
    assert_eq!(cluster.replica_nodes().len(), 3);

    // Access individual node by index.
    let node0 = cluster.node(0);
    assert!(node0.is_alive().await);

    // CONFIG SET on all nodes.
    cluster
        .config_set_all("hz", "20")
        .await
        .expect("config_set_all failed");
    for node in cluster.nodes() {
        let val = node.run(&["CONFIG", "GET", "hz"]).await.unwrap();
        assert!(val.contains("20"));
    }

    // CONFIG SET on masters only.
    cluster
        .config_set_masters("slowlog-log-slower-than", "5000")
        .await
        .expect("config_set_masters failed");
    for node in cluster.master_nodes() {
        let val = node
            .run(&["CONFIG", "GET", "slowlog-log-slower-than"])
            .await
            .unwrap();
        assert!(val.contains("5000"));
    }

    // CONFIG SET on replicas only.
    cluster
        .config_set_replicas("slowlog-max-len", "256")
        .await
        .expect("config_set_replicas failed");
    for node in cluster.replica_nodes() {
        let val = node
            .run(&["CONFIG", "GET", "slowlog-max-len"])
            .await
            .unwrap();
        assert!(val.contains("256"));
    }
}

#[tokio::test]
async fn cluster_with_node_config() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17030)
        .with_node_config(|ctx| {
            let is_master = ctx.is_master();
            let index = ctx.index;
            let mut server = ctx.server;
            if is_master {
                server = server.maxmemory("20mb");
            } else {
                server = server.maxmemory("10mb");
            }
            if index == 0 {
                server = server.slowlog_max_len(512);
            }
            server
        })
        .start()
        .await
        .expect("failed to start cluster with node config");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    // Verify master nodes got 20mb.
    for port in [17030, 17031, 17032] {
        let cli = redis_server_wrapper::RedisCli::new().port(port);
        let val = cli.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
        assert!(
            val.contains("20971520") || val.contains("20mb"),
            "master on port {port} should have 20mb, got: {val}"
        );
    }

    // Verify replica nodes got 10mb.
    for port in [17033, 17034, 17035] {
        let cli = redis_server_wrapper::RedisCli::new().port(port);
        let val = cli.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
        assert!(
            val.contains("10485760") || val.contains("10mb"),
            "replica on port {port} should have 10mb, got: {val}"
        );
    }

    // Verify node 0 got the custom slowlog setting.
    let cli = redis_server_wrapper::RedisCli::new().port(17030);
    let val = cli
        .run(&["CONFIG", "GET", "slowlog-max-len"])
        .await
        .unwrap();
    assert!(
        val.contains("512"),
        "node 0 should have slowlog-max-len 512, got: {val}"
    );
}

#[tokio::test]
async fn cluster_enable_module_command() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17050)
        .enable_module_command("yes")
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    for node in cluster.nodes() {
        node.run(&["MODULE", "LIST"])
            .await
            .expect("MODULE LIST should succeed on every node");
    }
}

#[tokio::test]
async fn cluster_config_directives() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17060)
        .maxmemory("64mb")
        .maxmemory_policy("allkeys-lru")
        .notify_keyspace_events("Ex")
        .enable_debug_command("yes")
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    for node in cluster.nodes() {
        let maxmemory = node
            .run(&["CONFIG", "GET", "maxmemory"])
            .await
            .expect("CONFIG GET maxmemory should succeed on every node");
        assert!(
            maxmemory.contains("67108864") || maxmemory.contains("64mb"),
            "node should have maxmemory 64mb, got: {maxmemory}"
        );

        let policy = node
            .run(&["CONFIG", "GET", "maxmemory-policy"])
            .await
            .expect("CONFIG GET maxmemory-policy should succeed on every node");
        assert!(
            policy.contains("allkeys-lru"),
            "node should have maxmemory-policy allkeys-lru, got: {policy}"
        );

        let notify = node
            .run(&["CONFIG", "GET", "notify-keyspace-events"])
            .await
            .expect("CONFIG GET notify-keyspace-events should succeed on every node");
        // Redis normalizes the flag order in its response (e.g. "Ex" becomes "xE"),
        // so check that both configured flags are present rather than exact order.
        assert!(
            notify.contains('E') && notify.contains('x'),
            "node should have notify-keyspace-events with E and x flags, got: {notify}"
        );

        let debug = node
            .run(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"])
            .await
            .expect("DEBUG command should be enabled on every node");
        assert!(
            debug.to_uppercase().contains("OK"),
            "DEBUG SET-ACTIVE-EXPIRE should succeed, got: {debug}"
        );
    }
}

#[tokio::test]
async fn wait_for_all_healthy_and_cluster_info() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17910)
        .start()
        .await
        .expect("failed to start cluster");

    // wait_for_healthy only requires one node to agree; wait_for_all_healthy
    // additionally requires every node to independently converge.
    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");
    cluster
        .wait_for_all_healthy(Duration::from_secs(30))
        .await
        .expect("cluster did not converge to all-nodes-healthy");

    for index in 0..cluster.nodes().len() {
        let info = cluster
            .cluster_info(index)
            .await
            .expect("cluster_info failed");
        assert_eq!(info.get("cluster_state").map(String::as_str), Some("ok"));
        assert_eq!(
            info.get("cluster_slots_assigned").map(String::as_str),
            Some("16384")
        );
        assert_eq!(
            info.get("cluster_slots_ok").map(String::as_str),
            Some("16384")
        );
    }
}
