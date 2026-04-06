use redis_server_wrapper::RedisCluster;
use std::time::{SystemTime, UNIX_EPOCH};

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
async fn cluster_with_node_config() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17020)
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
    for port in [17020, 17021, 17022] {
        let cli = redis_server_wrapper::RedisCli::new().port(port);
        let val = cli.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
        assert!(
            val.contains("20971520") || val.contains("20mb"),
            "master on port {port} should have 20mb, got: {val}"
        );
    }

    // Verify replica nodes got 10mb.
    for port in [17023, 17024, 17025] {
        let cli = redis_server_wrapper::RedisCli::new().port(port);
        let val = cli.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
        assert!(
            val.contains("10485760") || val.contains("10mb"),
            "replica on port {port} should have 10mb, got: {val}"
        );
    }

    // Verify node 0 got the custom slowlog setting.
    let cli = redis_server_wrapper::RedisCli::new().port(17020);
    let val = cli
        .run(&["CONFIG", "GET", "slowlog-max-len"])
        .await
        .unwrap();
    assert!(
        val.contains("512"),
        "node 0 should have slowlog-max-len 512, got: {val}"
    );
}
