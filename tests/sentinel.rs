use redis_server_wrapper::{RedisCli, RedisSentinel};
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
