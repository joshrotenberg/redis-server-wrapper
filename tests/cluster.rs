use redis_server_wrapper::RedisCluster;

#[tokio::test]
async fn cluster_start_and_health() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17000)
        .start()
        .await
        .expect("failed to start redis cluster");

    assert!(cluster.all_alive().await);
    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");
    assert!(cluster.is_healthy().await);
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
