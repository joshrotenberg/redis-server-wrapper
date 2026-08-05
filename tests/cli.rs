use redis_server_wrapper::{Error, RedisCli, RedisServer};

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

// -- Redis error replies (#143) --
//
// redis-cli exits 0 whenever it successfully round-trips with the server,
// whether the reply was a value or an error. `-e` (on by default, see
// `RedisCli::exit_error_code`) makes error replies exit non-zero so `run` can
// tell them apart from data.

#[tokio::test]
async fn error_reply_is_an_error() {
    let server = RedisServer::new()
        .port(16920)
        .password("secret")
        .start()
        .await
        .expect("failed to start redis-server");

    let unauthed = RedisCli::new().host(server.host()).port(server.port());
    let result = unauthed.run(&["GET", "k"]).await;

    match result {
        Err(Error::Cli { detail, port, .. }) => {
            assert!(detail.contains("NOAUTH"), "detail lost the reply: {detail}");
            assert_eq!(port, 16920);
        }
        other => panic!("expected Error::Cli for a NOAUTH reply, got: {other:?}"),
    }
}

#[tokio::test]
async fn wrongtype_reply_is_an_error() {
    let server = RedisServer::new()
        .port(16921)
        .start()
        .await
        .expect("failed to start redis-server");

    server.run(&["LPUSH", "list", "a"]).await.unwrap();
    let result = server.run(&["GET", "list"]).await;

    assert!(
        matches!(result, Err(Error::Cli { ref detail, .. }) if detail.contains("WRONGTYPE")),
        "expected WRONGTYPE to be an error, got: {result:?}"
    );
}

#[tokio::test]
async fn missing_key_is_not_an_error() {
    // A nil reply is absence, not failure. This is the case that would break
    // if error detection keyed on an empty reply rather than the exit code.
    let server = RedisServer::new()
        .port(16922)
        .start()
        .await
        .expect("failed to start redis-server");

    let value = server
        .run(&["GET", "never-set"])
        .await
        .expect("a missing key must not be an error");
    assert!(value.trim().is_empty(), "unexpected value: {value:?}");
}

#[tokio::test]
async fn successful_command_still_returns_its_value() {
    let server = RedisServer::new()
        .port(16923)
        .start()
        .await
        .expect("failed to start redis-server");

    server.run(&["SET", "k", "v"]).await.unwrap();
    let value = server.run(&["GET", "k"]).await.unwrap();
    assert_eq!(value.trim(), "v");
}

#[tokio::test]
async fn ping_is_false_when_unauthenticated() {
    let server = RedisServer::new()
        .port(16924)
        .password("secret")
        .start()
        .await
        .expect("failed to start redis-server");

    let unauthed = RedisCli::new().host(server.host()).port(server.port());
    assert!(!unauthed.ping().await);

    let authed = RedisCli::new()
        .host(server.host())
        .port(server.port())
        .password("secret");
    assert!(authed.ping().await);
}

#[tokio::test]
async fn exit_error_code_can_be_disabled() {
    let server = RedisServer::new()
        .port(16925)
        .password("secret")
        .start()
        .await
        .expect("failed to start redis-server");

    let lenient = RedisCli::new()
        .host(server.host())
        .port(server.port())
        .exit_error_code(false);

    let reply = lenient
        .run(&["GET", "k"])
        .await
        .expect("with -e disabled the error reply comes back as a value");
    assert!(reply.contains("NOAUTH"), "unexpected reply: {reply:?}");
}
