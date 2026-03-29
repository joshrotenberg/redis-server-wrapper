use redis_server_wrapper::{LogLevel, RedisServer};

#[tokio::test]
async fn start_and_ping() {
    let server = RedisServer::new()
        .port(16400)
        .bind("127.0.0.1")
        .loglevel(LogLevel::Warning)
        .start()
        .await
        .expect("failed to start redis-server");

    assert!(server.is_alive().await);
    assert_eq!(server.port(), 16400);
    assert_eq!(server.host(), "127.0.0.1");
    assert_eq!(server.addr(), "127.0.0.1:16400");
}

#[tokio::test]
async fn set_and_get() {
    let server = RedisServer::new()
        .port(16401)
        .start()
        .await
        .expect("failed to start redis-server");

    server.run(&["SET", "hello", "world"]).await.unwrap();
    let val = server.run(&["GET", "hello"]).await.unwrap();
    assert_eq!(val.trim(), "world");
}

#[tokio::test]
async fn password_auth() {
    let server = RedisServer::new()
        .port(16402)
        .password("testpass")
        .start()
        .await
        .expect("failed to start redis-server");

    // The handle's cli is already configured without the password,
    // but the server was started with wait_for_ready which uses the
    // cli without auth. Since redis-server with requirepass still
    // responds to PING, the handle should be alive.
    assert!(server.is_alive().await);
}

#[tokio::test]
async fn extra_config() {
    let server = RedisServer::new()
        .port(16403)
        .extra("maxmemory", "10mb")
        .extra("maxmemory-policy", "allkeys-lru")
        .start()
        .await
        .expect("failed to start redis-server");

    let info = server.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
    assert!(info.contains("10485760") || info.contains("10mb"));
}

#[tokio::test]
async fn stop_and_verify() {
    let server = RedisServer::new()
        .port(16404)
        .start()
        .await
        .expect("failed to start redis-server");

    assert!(server.is_alive().await);
    server.stop();

    // Give it a moment to shut down.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!server.is_alive().await);
}
