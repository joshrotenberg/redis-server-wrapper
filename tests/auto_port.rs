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

    // Distinct directories, not just distinct ports. Two attempts that drew the
    // same candidate would otherwise share a node directory and its pidfile,
    // and the loser would read the winner's and report success: two handles,
    // one server, and a teardown that stops someone else's.
    let mut dirs: Vec<_> = servers.iter().map(|s| s.node_dir()).collect();
    let dir_count = dirs.len();
    dirs.sort();
    dirs.dedup();
    assert_eq!(
        dirs.len(),
        dir_count,
        "every fixture must own its own directory"
    );

    // All of them are still up: allocation did not stop an earlier fixture.
    for server in &servers {
        assert!(server.is_alive().await, "fixture on {} died", server.port());
    }

    // And each is genuinely its own server, not several handles onto one.
    for (i, server) in servers.iter().enumerate() {
        server
            .run(&["SET", "owner", &i.to_string()])
            .await
            .expect("write should succeed");
    }
    for (i, server) in servers.iter().enumerate() {
        let owner = server.run(&["GET", "owner"]).await.expect("read failed");
        assert_eq!(
            owner.trim(),
            i.to_string(),
            "fixture {i} on port {} is not a distinct server",
            server.port()
        );
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

#[tokio::test]
async fn config_get_dir_identifies_the_server_behind_a_port() {
    // The invariant the automatic-port ownership check rests on. Neither the
    // spawn's exit status nor the readiness probe can tell our server from
    // someone else's on the same port, so the retry loop asks the server which
    // directory it is running from. If Redis ever stopped reporting that, the
    // check would reject every attempt and allocation would fail rather than
    // silently hand back a foreign server, but pin it either way.
    let server = RedisServer::new()
        .auto_port()
        .start()
        .await
        .expect("failed to start");

    let reply = server
        .run(&["CONFIG", "GET", "dir"])
        .await
        .expect("CONFIG GET dir should succeed");

    let node_dir = server.node_dir().display().to_string();
    assert!(
        reply.contains(&node_dir),
        "CONFIG GET dir returned {reply:?}, which does not contain {node_dir}"
    );
}

/// List the `auto-*` attempt directories directly under `dir`.
fn auto_dirs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .expect("test dir should exist")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("auto-"))
        })
        .collect()
}

#[tokio::test]
async fn dropping_every_handle_removes_its_directory() {
    // #185: nothing ever removed the auto-* directory an automatic-port
    // attempt created, so a test suite using auto_port grew the temp dir
    // without bound. A dropped handle's directory, credentials and all, must
    // be gone.
    let dir = std::env::temp_dir().join(format!("rsw-auto-dir-{}-drop", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create test dir");

    {
        let mut servers = Vec::with_capacity(3);
        for _ in 0..3 {
            servers.push(
                RedisServer::new()
                    .auto_port()
                    .dir(&dir)
                    .password("secret")
                    .start()
                    .await
                    .expect("server should start"),
            );
        }
        // All three drop here, at the end of this scope.
    }

    let leftover = auto_dirs(&dir);
    assert!(
        leftover.is_empty(),
        "dropping every auto_port handle must leave no auto-* directory behind, found {leftover:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn explicit_stop_removes_the_directory_and_a_later_drop_is_harmless() {
    let dir = std::env::temp_dir().join(format!("rsw-auto-dir-{}-stop", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create test dir");

    let server = RedisServer::new()
        .auto_port()
        .dir(&dir)
        .password("secret")
        .start()
        .await
        .expect("server should start");

    server.stop();
    assert!(
        auto_dirs(&dir).is_empty(),
        "an explicit stop() must remove the auto-* directory"
    );

    // Dropping an already-stopped handle must not error or try to remove the
    // directory again.
    drop(server);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_detached_handle_keeps_its_directory() {
    let dir = std::env::temp_dir().join(format!("rsw-auto-dir-{}-detach", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create test dir");

    let server = RedisServer::new()
        .auto_port()
        .dir(&dir)
        .password("secret")
        .start()
        .await
        .expect("server should start");

    let node_dir = server.node_dir();
    let cli = server.cli().clone();
    server.detach();

    let leftover = auto_dirs(&dir);
    assert!(
        !leftover.is_empty(),
        "a detached handle must keep its auto-* directory"
    );
    assert!(
        node_dir.exists(),
        "the detached server's node directory must still be on disk"
    );

    // Shut the now-unmanaged process down ourselves, the way
    // `detach_leaves_server_running` in tests/server.rs does, so the process
    // does not outlive the test.
    cli.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!cli.ping().await);

    let _ = std::fs::remove_dir_all(&dir);
}
