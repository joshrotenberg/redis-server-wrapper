use redis_server_wrapper::{RedisCli, RedisServer};

#[tokio::test]
async fn cli_ping_running_server() {
    let server = RedisServer::new()
        .port(16410)
        .start()
        .await
        .expect("failed to start redis-server");

    let cli = RedisCli::new().host("127.0.0.1").port(16410);
    assert!(cli.ping().await);

    drop(server);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!cli.ping().await);
}

#[tokio::test]
async fn cli_run_command() {
    let _server = RedisServer::new()
        .port(16411)
        .start()
        .await
        .expect("failed to start redis-server");

    let cli = RedisCli::new().port(16411);
    let result = cli.run(&["SET", "foo", "bar"]).await.unwrap();
    assert_eq!(result.trim(), "OK");

    let result = cli.run(&["GET", "foo"]).await.unwrap();
    assert_eq!(result.trim(), "bar");
}

#[tokio::test]
async fn cli_wait_for_ready_timeout() {
    let cli = RedisCli::new().port(16412);
    let result = cli
        .wait_for_ready(std::time::Duration::from_millis(500))
        .await;
    assert!(result.is_err());
}
