use redis_server_wrapper::{Error, LogLevel, RedisServer};
use std::fs;

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

#[tokio::test]
async fn detach_leaves_server_running() {
    let server = RedisServer::new()
        .port(16405)
        .start()
        .await
        .expect("failed to start redis-server");

    let cli = server.cli().clone();
    server.detach();

    cli.wait_for_ready(std::time::Duration::from_secs(2))
        .await
        .expect("detached server should still be reachable");
    assert!(cli.ping().await);

    cli.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!cli.ping().await);
}

#[tokio::test]
async fn dir_with_spaces() {
    // Verify that a working directory whose path contains a space does not
    // cause redis-server to fail parsing the generated config.
    let base = std::env::temp_dir().join("redis wrapper test");
    fs::create_dir_all(&base).expect("failed to create temp dir with space");

    let server = RedisServer::new()
        .port(16406)
        .dir(&base)
        .loglevel(LogLevel::Warning)
        .start()
        .await
        .expect("server with space in dir should start cleanly");

    assert!(server.is_alive().await);
}

#[tokio::test]
async fn bad_server_binary_returns_binary_not_found() {
    let result = RedisServer::new()
        .port(16407)
        .redis_server_bin("/nonexistent/redis-server")
        .start()
        .await;

    assert!(matches!(
        result,
        Err(Error::BinaryNotFound { binary }) if binary == "/nonexistent/redis-server"
    ));
}

#[tokio::test]
async fn bad_cli_binary_returns_binary_not_found() {
    let result = RedisServer::new()
        .port(16408)
        .redis_cli_bin("/nonexistent/redis-cli")
        .start()
        .await;

    assert!(matches!(
        result,
        Err(Error::BinaryNotFound { binary }) if binary == "/nonexistent/redis-cli"
    ));
}

#[tokio::test]
async fn port_already_in_use_returns_server_start_error() {
    let _first = RedisServer::new()
        .port(16409)
        .dir(std::env::temp_dir().join("rsw-port-conflict-a"))
        .start()
        .await
        .expect("first server should start");

    // A daemonizing redis-server forks and its parent exits 0 before the
    // child even attempts to bind, so a second daemonized start on the same
    // port would falsely report success. Run the second attempt in the
    // foreground so the bind failure surfaces synchronously.
    let result = RedisServer::new()
        .port(16409)
        .dir(std::env::temp_dir().join("rsw-port-conflict-b"))
        .extra("daemonize", "no")
        .start()
        .await;

    assert!(matches!(result, Err(Error::ServerStart { port: 16409 })));
}
