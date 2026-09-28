use redis_server_wrapper::{Error, LogLevel, RedisServer, chaos, process, server};
use std::fs;
use std::time::Duration;

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

    // `start` applies the password to the handle's cli before it waits for
    // readiness, so wait_for_ready, is_alive, and run all speak to the server
    // over an authenticated connection. An unauthenticated cli would not get
    // this far: redis-server under requirepass answers PING with NOAUTH rather
    // than PONG, which `ping_is_false_when_unauthenticated` in tests/cli.rs
    // asserts directly.
    assert!(server.is_alive().await);

    server
        .run(&["SET", "auth-key", "auth-value"])
        .await
        .expect("an authenticated SET should succeed");
    let value = server
        .run(&["GET", "auth-key"])
        .await
        .expect("an authenticated GET should succeed");
    assert_eq!(value.trim(), "auth-value");

    // The round trip above would pass just as well against a server with no
    // password at all, so confirm requirepass is what the connection
    // authenticated against.
    let requirepass = server
        .run(&["CONFIG", "GET", "requirepass"])
        .await
        .expect("CONFIG GET requirepass should succeed");
    assert!(
        requirepass.contains("testpass"),
        "requirepass is not set on the server: {requirepass:?}"
    );
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
    let first = RedisServer::new()
        .port(16409)
        .dir(std::env::temp_dir().join("rsw-port-conflict-a"))
        .start()
        .await
        .expect("first server should start");

    // The preflight check catches this before redis-server is spawned, so the
    // daemonize workaround this test used to need no longer applies: a
    // daemonizing start would have forked and exited 0 before the child even
    // attempted to bind.
    let result = RedisServer::new()
        .port(16409)
        .dir(std::env::temp_dir().join("rsw-port-conflict-b"))
        .start()
        .await;

    assert!(
        matches!(result, Err(Error::PortInUse { port: 16409, .. })),
        "expected PortInUse for the occupied port"
    );

    // The server that already held the port is untouched.
    assert!(first.is_alive().await);
}

#[tokio::test]
async fn info_and_role_report_replication_fields() {
    let server = RedisServer::new()
        .port(17900)
        .start()
        .await
        .expect("failed to start redis-server");

    let full = server.info(None).await.expect("INFO failed");
    assert!(full.contains_key("redis_version"));

    let repl = server
        .info(Some("replication"))
        .await
        .expect("INFO replication failed");
    assert_eq!(repl.get("role").map(String::as_str), Some("master"));

    assert_eq!(server.role().await.expect("role failed"), "master");
}

#[tokio::test]
async fn wait_until_role_master_immediately() {
    let server = RedisServer::new()
        .port(17901)
        .start()
        .await
        .expect("failed to start redis-server");

    server
        .wait_until_role("master", Duration::from_secs(5))
        .await
        .expect("a freshly started standalone server should already be master");
}

#[tokio::test]
async fn wait_for_replica_sync_after_write() {
    let master = RedisServer::new()
        .port(17902)
        .start()
        .await
        .expect("failed to start master");

    let replica = RedisServer::new()
        .port(17903)
        .replicaof("127.0.0.1", 17902)
        .start()
        .await
        .expect("failed to start replica");

    replica
        .wait_until_role("slave", Duration::from_secs(10))
        .await
        .expect("replica did not report role slave");

    master
        .run(&["SET", "sync-key", "sync-value"])
        .await
        .expect("SET on master failed");

    server::wait_for_replica_sync(&replica, &master, Duration::from_secs(10))
        .await
        .expect("replica did not catch up to master");

    let val = replica
        .run(&["GET", "sync-key"])
        .await
        .expect("GET on replica failed");
    assert_eq!(val.trim(), "sync-value");
}

#[tokio::test]
async fn dbsize_exact_count() {
    let server = RedisServer::new()
        .port(17904)
        .start()
        .await
        .expect("failed to start redis-server");

    // fill_memory writes exactly `count` keys; DBSIZE should match exactly,
    // not just contain the count as a substring (150 also contains "50").
    chaos::fill_memory(&server, "k:", 150)
        .await
        .expect("fill_memory failed");

    let size = server.dbsize().await.expect("dbsize failed");
    assert_eq!(size, 150);
}

#[tokio::test]
async fn wait_for_log_after_bgsave() {
    let server = RedisServer::new()
        .port(17905)
        .loglevel(LogLevel::Notice)
        .start()
        .await
        .expect("failed to start redis-server");

    let from = server.log_len().expect("log_len failed");

    chaos::trigger_save(&server).await.expect("BGSAVE failed");

    let line = server
        .wait_for_log(
            "Background saving terminated with success",
            from,
            Duration::from_secs(10),
        )
        .await
        .expect("wait_for_log did not find the bgsave completion line");
    assert!(line.contains("Background saving terminated with success"));
}

/// An unrecognized directive makes `redis-server` reject the config file and
/// exit non-zero before it daemonizes or opens the logfile -- exactly the
/// case the switch from `.status()` to `.output()` exists for: the failure
/// reason lives only in the daemonizing process's own stderr, not in the
/// (nonexistent) log file, and it must still reach the returned error.
#[tokio::test]
async fn start_failure_surfaces_log_tail_in_error() {
    let result = RedisServer::new()
        .port(17906)
        .extra("thisisnotarealdirective", "yes")
        .start()
        .await;

    let err = result
        .err()
        .expect("redis-server should have aborted on the bad directive");
    let message = err.to_string();
    assert!(
        message.contains("thisisnotarealdirective") || message.contains("Unresolved"),
        "expected the log tail in the error message, got: {message}"
    );
}

// -- daemonize(false) (#181) --

/// `daemonize(false)` keeps `redis-server` as a foreground child instead of
/// letting it fork, so `start` has to race its own readiness probe against
/// the child exiting rather than reading a daemonizing parent's exit status.
/// Wrapped in a bounded timeout: before this fix, a healthy foreground start
/// never returned at all.
#[tokio::test]
async fn daemonize_false_starts_and_stops() {
    let port: u16 = 17907;
    tokio::time::timeout(Duration::from_secs(20), async move {
        let server = RedisServer::new()
            .port(port)
            .daemonize(false)
            .start()
            .await
            .expect("a healthy foreground start should return a working handle");

        assert!(server.is_alive().await);
        assert!(server.run(&["PING"]).await.unwrap().contains("PONG"));
        let pid = server.pid();

        server.stop();
        assert!(server.is_stopped());
        assert!(
            !process::pid_alive(pid),
            "the foreground process should be gone after stop"
        );
    })
    .await
    .expect("daemonize(false) start/stop should not hang");

    std::net::TcpListener::bind(("127.0.0.1", port)).expect("the port should be free after stop");
}

/// A `daemonize(false)` start that redis rejects must fail promptly with the
/// server's own output in the error, the foreground counterpart of
/// `start_failure_surfaces_log_tail_in_error`, rather than hang the way it
/// did before this fix.
#[tokio::test]
async fn daemonize_false_rejects_a_bad_directive_promptly() {
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        RedisServer::new()
            .port(17908)
            .daemonize(false)
            .extra("maxmemory-policy", "not-a-policy")
            .start(),
    )
    .await
    .expect("a rejected config should fail promptly rather than hang");

    let err = result
        .err()
        .expect("redis-server should have refused the invalid maxmemory-policy value");
    match err {
        Error::ServerStart {
            detail: Some(detail),
            ..
        } => {
            assert!(
                detail.contains("maxmemory-policy") || detail.contains("not-a-policy"),
                "expected the bad directive in the error detail, got: {detail}"
            );
        }
        other => panic!("expected Error::ServerStart {{ detail: Some(_), .. }}, got: {other:?}"),
    }
}

/// Dropping a foreground handle without an explicit `stop` must still stop
/// the server: `Drop` is the only teardown most callers rely on.
#[tokio::test]
async fn dropping_a_foreground_handle_stops_the_server() {
    let port: u16 = 17909;
    tokio::time::timeout(Duration::from_secs(20), async move {
        let server = RedisServer::new()
            .port(port)
            .daemonize(false)
            .start()
            .await
            .expect("a healthy foreground start should return a working handle");
        assert!(server.is_alive().await);
        drop(server);
    })
    .await
    .expect("dropping a foreground handle should not hang");

    std::net::TcpListener::bind(("127.0.0.1", port))
        .expect("the port should be free once the handle is dropped");
}

// -- stop idempotence (#165) --

#[tokio::test]
async fn a_stopped_handle_does_not_kill_the_ports_next_occupant() {
    // The reported bug. A handle stopped explicitly used to stop again on
    // drop, and by then the port could belong to an unrelated server.
    let first = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("first server should start");
    let port = first.port();
    first.stop();
    assert!(first.is_stopped());

    let second = RedisServer::new()
        .port(port)
        .dir(std::env::temp_dir().join("rsw-stop-idempotence"))
        .start()
        .await
        .expect("a second server should be able to take the freed port");
    second
        .run(&["SET", "survivor", "yes"])
        .await
        .expect("seed failed");

    // The dangerous moment: this used to run the whole stop sequence again,
    // addressed at a port the first handle no longer owned.
    drop(first);

    assert!(
        second.is_alive().await,
        "dropping a stopped handle killed the port's new occupant"
    );
    let value = second
        .run(&["GET", "survivor"])
        .await
        .expect("the second server should still answer");
    assert_eq!(value.trim(), "yes");
}

#[tokio::test]
async fn stopping_twice_is_harmless() {
    let server = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("failed to start");
    assert!(!server.is_stopped());

    server.stop();
    server.stop();
    server.stop();

    assert!(server.is_stopped());
    assert!(!server.is_alive().await);
}

#[tokio::test]
async fn a_detached_handle_reports_that_it_stopped_nothing() {
    let server = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("failed to start");
    let port = server.port();
    assert!(!server.is_stopped());
    server.detach();

    // Still running, because detach suppressed the teardown.
    let cli = redis_server_wrapper::RedisCli::new().port(port);
    assert!(cli.ping().await);
    cli.shutdown();
}

#[tokio::test]
async fn stopping_a_frozen_server_cannot_hang() {
    // The failure that wedged CI. A SIGSTOPped server still completes the TCP
    // handshake from the kernel's accept queue, so redis-cli connects and then
    // waits for a reply that never comes. Reached from Drop, an unbounded wait
    // there means the test binary never exits and the run hangs until the job
    // is killed.
    let server = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("failed to start");
    let port = server.port();

    chaos::freeze_node(&server).expect("freeze failed");

    // Bounded generously: the point is that this returns at all, not that it
    // is quick. Unbounded, it never returns.
    let stopped = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::task::spawn_blocking(move || {
            server.stop();
            server
        }),
    )
    .await;

    let server = stopped
        .expect("stopping a frozen server hung")
        .expect("stop panicked");

    assert!(server.is_stopped());
    assert!(
        !redis_server_wrapper::RedisCli::new()
            .port(port)
            .ping()
            .await,
        "the frozen server should be gone after stop"
    );
}
