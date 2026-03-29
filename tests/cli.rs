use redis_server_wrapper::{RedisCli, RedisServer};

#[test]
fn cli_ping_running_server() {
    let server = RedisServer::new()
        .port(16410)
        .start()
        .expect("failed to start redis-server");

    let cli = RedisCli::new().host("127.0.0.1").port(16410);
    assert!(cli.ping());

    drop(server);
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!cli.ping());
}

#[test]
fn cli_run_command() {
    let _server = RedisServer::new()
        .port(16411)
        .start()
        .expect("failed to start redis-server");

    let cli = RedisCli::new().port(16411);
    let result = cli.run(&["SET", "foo", "bar"]).unwrap();
    assert_eq!(result.trim(), "OK");

    let result = cli.run(&["GET", "foo"]).unwrap();
    assert_eq!(result.trim(), "bar");
}

#[test]
fn cli_wait_for_ready_timeout() {
    let cli = RedisCli::new().port(16412);
    let result = cli.wait_for_ready(std::time::Duration::from_millis(500));
    assert!(result.is_err());
}
