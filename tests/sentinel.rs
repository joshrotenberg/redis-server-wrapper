use redis_server_wrapper::{RedisCli, RedisSentinel, RedisServer};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn sentinel_start_and_health() {
    let log_path = std::env::temp_dir().join(format!(
        "redis-sentinel-{}.log",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    ));
    let sentinel = RedisSentinel::builder()
        .master_port(16390)
        .replicas(1)
        .replica_base_port(16391)
        .sentinels(3)
        .sentinel_base_port(26490)
        .logfile(log_path.display().to_string())
        .extra("maxmemory", "10mb")
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");
    let master_cli = RedisCli::new().host("127.0.0.1").port(16390);
    let maxmemory = master_cli
        .run(&["CONFIG", "GET", "maxmemory"])
        .await
        .expect("failed to query master config");
    assert!(maxmemory.contains("10485760") || maxmemory.contains("10mb"));
    let logfile = master_cli
        .run(&["CONFIG", "GET", "logfile"])
        .await
        .expect("failed to query master logfile");
    assert!(logfile.contains(&log_path.display().to_string()));
    assert!(sentinel.is_healthy().await);
    assert_eq!(sentinel.master_name(), "mymaster");
    assert_eq!(sentinel.master_addr(), "127.0.0.1:16390");
    assert_eq!(sentinel.sentinel_addrs().len(), 3);
}

#[tokio::test]
async fn sentinel_monitors_multiple_masters() {
    let external_master = RedisServer::new()
        .port(16395)
        .bind("127.0.0.1")
        .start()
        .await
        .expect("failed to start external master");

    let sentinel = RedisSentinel::builder()
        .master_port(16396)
        .replicas(1)
        .replica_base_port(16397)
        .sentinels(3)
        .sentinel_base_port(26496)
        .monitor("backup", "127.0.0.1", 16395)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    assert!(sentinel.is_healthy().await);
    assert_eq!(
        sentinel.monitored_master_names(),
        vec!["mymaster", "backup"]
    );
    assert_eq!(
        sentinel.monitored_master_addrs(),
        vec!["127.0.0.1:16396".to_string(), "127.0.0.1:16395".to_string()]
    );

    let backup = sentinel
        .poke_master("backup")
        .await
        .expect("failed to query backup master");
    assert_eq!(backup.get("name").map(String::as_str), Some("backup"));
    assert_eq!(backup.get("ip").map(String::as_str), Some("127.0.0.1"));
    assert_eq!(backup.get("port").map(String::as_str), Some("16395"));

    drop(sentinel);
    drop(external_master);
}
