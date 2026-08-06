//! Automatic port allocation for fixtures that run in parallel.
//!
//! The pattern this replaces is binding `127.0.0.1:0`, reading the assigned
//! port, dropping the listener, and passing the number to the builder. That
//! leaves a window where another process can take the port before Redis binds
//! it, and the caller has no way to recover. `auto_port` keeps the same
//! reservation trick but owns the retry.

use redis_server_wrapper::RedisServer;

#[tokio::test]
async fn auto_port_starts_on_a_usable_port() {
    let server = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("a server with an automatic port should start");

    let port = server.port();
    assert_ne!(port, 0, "the handle must report the port actually chosen");
    assert_eq!(server.addr(), format!("127.0.0.1:{port}"));

    // The reported port is the one serving, not just a number that was free.
    server.run(&["SET", "k", "v"]).await.expect("SET failed");
    let value = server.run(&["GET", "k"]).await.expect("GET failed");
    assert_eq!(value.trim(), "v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fixtures_get_distinct_ports() {
    // The case the issue was filed for: independent tests each owning a Redis
    // lifecycle, with no coordinated port range between them. Started on
    // separate tasks so the reservations genuinely overlap.
    const FIXTURES: usize = 12;

    let mut tasks = Vec::with_capacity(FIXTURES);
    for _ in 0..FIXTURES {
        tasks.push(tokio::spawn(async {
            RedisServer::new()
                .auto_port()
                .start()
                .await
                .expect("fixture should start")
        }));
    }

    let mut servers = Vec::with_capacity(FIXTURES);
    for task in tasks {
        servers.push(task.await.expect("fixture task panicked"));
    }

    let mut ports: Vec<u16> = servers.iter().map(|s| s.port()).collect();
    let before = ports.len();
    assert_eq!(before, FIXTURES);

    ports.sort_unstable();
    ports.dedup();
    assert_eq!(ports.len(), before, "every fixture must get its own port");

    // All of them are still up: allocation did not stop an earlier fixture.
    for server in &servers {
        assert!(server.is_alive().await, "fixture on {} died", server.port());
    }
}

#[tokio::test]
async fn allocation_never_disturbs_the_process_holding_a_port() {
    // Stand a server on a fixed port, then allocate many automatic ones. The
    // fixed server must be untouched: automatic allocation abandons a
    // contested candidate, it never clears one.
    let bystander = RedisServer::new()
        .port(19300)
        .start()
        .await
        .expect("bystander should start");
    bystander
        .run(&["SET", "precious", "data"])
        .await
        .expect("seed failed");

    let mut allocated = Vec::new();
    for _ in 0..8 {
        allocated.push(
            RedisServer::new()
                .auto_port()
                .start()
                .await
                .expect("automatic allocation should succeed"),
        );
    }

    for server in &allocated {
        assert_ne!(
            server.port(),
            19300,
            "allocation handed out a port that was already serving"
        );
    }

    assert!(bystander.is_alive().await);
    let value = bystander
        .run(&["GET", "precious"])
        .await
        .expect("bystander should still answer");
    assert_eq!(value.trim(), "data");
}

#[tokio::test]
async fn explicit_port_zero_is_not_automatic_allocation() {
    // port(0) keeps its Redis meaning of disabling the plaintext listener, so
    // it must not be quietly reinterpreted as a request for a free port. With
    // no TLS configured that is a server with no listener at all, which fails
    // to become ready rather than silently coming up on some other port.
    let result = RedisServer::new().port(0).start().await;

    match result {
        Ok(handle) => panic!(
            "port(0) should not have produced a reachable server, got port {}",
            handle.port()
        ),
        Err(e) => {
            let text = e.to_string();
            assert!(
                !text.contains("could not acquire a free port"),
                "port(0) must not have been treated as automatic allocation: {text}"
            );
        }
    }
}

#[tokio::test]
async fn auto_port_overrides_a_configured_port() {
    let server = RedisServer::new()
        .port(19301)
        .auto_port()
        .start()
        .await
        .expect("failed to start");

    assert_ne!(
        server.port(),
        19301,
        "auto_port should take precedence over an explicit port"
    );
    assert!(server.is_alive().await);
}
