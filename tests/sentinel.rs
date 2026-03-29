use redis_server_wrapper::RedisSentinel;

#[tokio::test]
async fn sentinel_start_and_health() {
    let sentinel = RedisSentinel::builder()
        .master_port(16390)
        .replicas(1)
        .replica_base_port(16391)
        .sentinels(3)
        .sentinel_base_port(26490)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");
    assert!(sentinel.is_healthy().await);
    assert_eq!(sentinel.master_name(), "mymaster");
    assert_eq!(sentinel.master_addr(), "127.0.0.1:16390");
    assert_eq!(sentinel.sentinel_addrs().len(), 3);
}
