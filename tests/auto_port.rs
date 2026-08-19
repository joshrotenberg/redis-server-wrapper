//! Automatic port allocation for fixtures that run in parallel.
//!
//! The pattern this replaces is binding `127.0.0.1:0`, reading the assigned
//! port, dropping the listener, and passing the number to the builder. That
//! leaves a window where another process can take the port before Redis binds
//! it, and the caller has no way to recover. `auto_port` keeps the same
//! reservation trick but owns the retry.

use redis_server_wrapper::{RedisCluster, RedisServer};

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

// -- cluster ranges (#166) --

/// The window automatic cluster ranges are drawn from, mirrored from
/// `preflight` so a change to either side has to be deliberate.
const AUTO_BASE_MIN: u16 = 10000;
const AUTO_CEILING: u16 = 32768;

#[tokio::test]
async fn a_cluster_can_take_a_wrapper_chosen_range() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .auto_port()
        .start()
        .await
        .expect("a cluster with an automatic range should start");

    let base = cluster.base_port();
    assert!(
        base >= AUTO_BASE_MIN,
        "base {base} is below the automatic window"
    );

    // Every node sits in the chosen range, and every bus port it derives fits
    // under the ceiling. A range that satisfied the client ports but ran the
    // bus ports into the OS pool would look fine until the gossip failed.
    let ports: Vec<u16> = cluster.nodes().iter().map(|n| n.port()).collect();
    assert_eq!(ports.len(), 3);
    for port in &ports {
        assert!(
            *port >= base && *port < base + 3,
            "node port {port} is outside the range starting at {base}"
        );
        let bus = port + 10000;
        assert!(
            bus < AUTO_CEILING,
            "bus port {bus} derived from {port} reaches the ephemeral pool"
        );
    }

    assert!(cluster.is_healthy().await, "the cluster should have formed");
}

#[tokio::test]
async fn an_automatic_cluster_really_owns_its_bus_ports() {
    // The half of the allocation a client-port-only check would miss. Redis
    // only opens a bus port when the node is cluster-enabled and it bound
    // successfully, so a listener there is proof the derived port was free and
    // is now ours.
    let cluster = RedisCluster::builder()
        .masters(3)
        .auto_port()
        .start()
        .await
        .expect("cluster should start");

    for node in cluster.nodes() {
        let bus = node.port() + 10000;
        assert!(
            !redis_server_wrapper::preflight::port_available("127.0.0.1", bus),
            "nothing is listening on bus port {bus} for node {}",
            node.port()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clusters_get_non_overlapping_ranges() {
    // The case #166 was filed for: parallel cluster fixtures with no agreed
    // range between them. Overlapping ranges would not merely collide on a
    // client port, they would cross the two clusters' gossip.
    const FIXTURES: usize = 3;
    const NODES: u16 = 3;

    let mut tasks = Vec::with_capacity(FIXTURES);
    for _ in 0..FIXTURES {
        tasks.push(tokio::spawn(async {
            RedisCluster::builder()
                .masters(NODES)
                .auto_port()
                .start()
                .await
                .expect("cluster fixture should start")
        }));
    }

    let mut clusters = Vec::with_capacity(FIXTURES);
    for task in tasks {
        clusters.push(task.await.expect("cluster fixture panicked"));
    }

    // No client port is shared, and no client range reaches into another's.
    let mut all_ports: Vec<u16> = clusters
        .iter()
        .flat_map(|c| c.nodes().iter().map(|n| n.port()))
        .collect();
    let total = all_ports.len();
    assert_eq!(total, FIXTURES * NODES as usize);
    all_ports.sort_unstable();
    all_ports.dedup();
    assert_eq!(all_ports.len(), total, "two clusters shared a client port");

    // Nor a bus port, which is the collision that would let one cluster's
    // gossip reach another's.
    let mut buses: Vec<u16> = all_ports.iter().map(|p| p + 10000).collect();
    let bus_total = buses.len();
    buses.sort_unstable();
    buses.dedup();
    assert_eq!(buses.len(), bus_total, "two clusters shared a bus port");

    // Every one still formed and is still up: allocating a range never
    // disturbed a cluster that already had one.
    for cluster in &clusters {
        assert!(
            cluster.is_healthy().await,
            "cluster at {} did not survive its neighbours",
            cluster.base_port()
        );
    }
}

#[tokio::test]
async fn a_range_is_all_or_nothing() {
    // A partial-range collision. Stand a plain listener in the middle of a
    // range and confirm the allocator steps over the whole range rather than
    // starting the nodes that would have fitted around it.
    //
    // The candidate is whatever the allocator picks, so this cannot force the
    // collision onto a chosen range. What it does pin is that a range holding
    // an occupied port is rejected outright, which is the property a cluster
    // depends on: three nodes minus one is not a cluster.
    let squatter = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind failed");
    let held = squatter.local_addr().unwrap().port();

    assert!(
        !redis_server_wrapper::preflight::port_range_available("127.0.0.1", held - 1, 3, false),
        "a range containing the occupied port {held} must be rejected whole"
    );

    // And the allocator still finds the cluster a home despite it.
    let cluster = RedisCluster::builder()
        .masters(3)
        .auto_port()
        .start()
        .await
        .expect("allocation should step over the occupied port");

    for node in cluster.nodes() {
        assert_ne!(
            node.port(),
            held,
            "allocation handed out a port that was already taken"
        );
    }

    // Untouched: the allocator abandons a contested range, it never clears it.
    drop(squatter);
}
