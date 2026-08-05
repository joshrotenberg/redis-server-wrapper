use redis_server_wrapper::chaos::SentinelCrashPoint;
use redis_server_wrapper::{RedisCli, RedisSentinel, RedisServer, chaos};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn sentinel_start_and_health() {
    let log_path = std::env::temp_dir().join(format!(
        "redis-sentinel-{}.log",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    ));
    let sentinel = RedisSentinel::builder()
        .master_port(16390)
        .replicas(1)
        .replica_base_port(16391)
        .sentinels(3)
        .sentinel_base_port(26490)
        .logfile(log_path.display().to_string())
        .extra("maxmemory", "10mb")
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");
    let master_cli = RedisCli::new().host("127.0.0.1").port(16390);
    let maxmemory = master_cli
        .run(&["CONFIG", "GET", "maxmemory"])
        .await
        .expect("failed to query master config");
    assert!(maxmemory.contains("10485760") || maxmemory.contains("10mb"));
    let logfile = master_cli
        .run(&["CONFIG", "GET", "logfile"])
        .await
        .expect("failed to query master logfile");
    assert!(logfile.contains(&log_path.display().to_string()));
    assert!(sentinel.is_healthy().await);
    assert_eq!(sentinel.master_name(), "mymaster");
    assert_eq!(sentinel.master_addr(), "127.0.0.1:16390");
    assert_eq!(sentinel.sentinel_addrs().len(), 3);
}

#[tokio::test]
async fn sentinel_monitors_multiple_masters() {
    let external_master = RedisServer::new()
        .port(16395)
        .bind("127.0.0.1")
        .start()
        .await
        .expect("failed to start external master");

    let sentinel = RedisSentinel::builder()
        .master_port(16396)
        .replicas(1)
        .replica_base_port(16397)
        .sentinels(3)
        .sentinel_base_port(26496)
        .monitor("backup", "127.0.0.1", 16395)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    assert!(sentinel.is_healthy().await);
    assert_eq!(
        sentinel.monitored_master_names(),
        vec!["mymaster", "backup"]
    );
    assert_eq!(
        sentinel.monitored_master_addrs(),
        vec!["127.0.0.1:16396".to_string(), "127.0.0.1:16395".to_string()]
    );

    let backup = sentinel
        .poke_master("backup")
        .await
        .expect("failed to query backup master");
    assert_eq!(backup.get("name").map(String::as_str), Some("backup"));
    assert_eq!(backup.get("ip").map(String::as_str), Some("127.0.0.1"));
    assert_eq!(backup.get("port").map(String::as_str), Some("16395"));

    drop(sentinel);
    drop(external_master);
}

#[tokio::test]
async fn sentinel_password_auth() {
    let sentinel = RedisSentinel::builder()
        .master_port(17400)
        .replicas(1)
        .replica_base_port(17401)
        .sentinels(3)
        .sentinel_base_port(27400)
        .password("testpass")
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    // is_healthy() still works: it only ever talks to the (unauthenticated)
    // sentinel processes, never the password-protected data nodes.
    assert!(sentinel.is_healthy().await);

    // A sentinel still discovers the master via the handle's existing
    // discovery path.
    let master_info = sentinel
        .poke()
        .await
        .expect("failed to query master via sentinel");
    assert_eq!(master_info.get("port").map(String::as_str), Some("17400"));

    // The master rejects an unauthenticated command...
    let unauthed = RedisCli::new().host("127.0.0.1").port(17400);
    let unauthed_reply = unauthed
        .run(&["GET", "foo"])
        .await
        .expect("redis-cli itself should not fail even on a NOAUTH reply");
    assert!(
        unauthed_reply.contains("NOAUTH"),
        "expected a NOAUTH reply, got: {unauthed_reply}"
    );

    // ...and accepts the same command once authenticated.
    let authed = RedisCli::new()
        .host("127.0.0.1")
        .port(17400)
        .password("testpass");
    authed
        .run(&["SET", "foo", "bar"])
        .await
        .expect("authenticated SET should succeed");
    let value = authed
        .run(&["GET", "foo"])
        .await
        .expect("authenticated GET should succeed");
    assert_eq!(value.trim(), "bar");
}

#[tokio::test]
async fn sentinel_enable_module_command() {
    let sentinel = RedisSentinel::builder()
        .master_port(17410)
        .replicas(1)
        .replica_base_port(17411)
        .sentinels(3)
        .sentinel_base_port(27410)
        .enable_module_command("yes")
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    let master_cli = RedisCli::new().host("127.0.0.1").port(17410);
    master_cli
        .run(&["MODULE", "LIST"])
        .await
        .expect("MODULE LIST should succeed on the master");
}

#[tokio::test]
async fn sentinel_node_access() {
    let sentinel = RedisSentinel::builder()
        .master_port(18100)
        .replicas(2)
        .replica_base_port(18101)
        .sentinels(3)
        .sentinel_base_port(28100)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    let master = sentinel.master();
    assert_eq!(master.port(), 18100);
    assert!(
        master.is_alive().await,
        "master should respond to PING through its handle"
    );

    let replicas = sentinel.replicas();
    assert_eq!(replicas.len(), 2, "expected exactly 2 replica handles");
    for replica in replicas {
        assert!(
            replica.is_alive().await,
            "each replica should respond to PING through its handle"
        );
    }
}

#[tokio::test]
async fn sentinel_kill_master_triggers_promotion() {
    let sentinel = RedisSentinel::builder()
        .master_port(18110)
        .replicas(1)
        .replica_base_port(18111)
        .sentinels(3)
        .sentinel_base_port(28110)
        .down_after_ms(2000)
        .failover_timeout_ms(10000)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    let old_addr = sentinel.master_addr();

    // Kill the master directly via its exposed handle -- this is the whole
    // point of `master()`: chaos functions that only ever took
    // `&RedisServerHandle` now reach sentinel-managed data nodes.
    chaos::kill_node(sentinel.master()).expect("failed to send SIGKILL to the master");

    let new_addr = sentinel
        .wait_for_new_master(&old_addr, Duration::from_secs(30))
        .await
        .expect("sentinel did not promote a new master in time");

    assert_ne!(
        new_addr, old_addr,
        "sentinel should have promoted a different node as master"
    );
}

#[tokio::test]
async fn sentinel_simulate_failure_crash_after_election() {
    let sentinel = RedisSentinel::builder()
        .master_port(18120)
        .replicas(1)
        .replica_base_port(18121)
        .sentinels(3)
        .sentinel_base_port(28120)
        .start()
        .await
        .expect("failed to start sentinel topology");

    sentinel
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("sentinel topology did not become healthy");

    // Arm sentinel index 0 (port 28120) to crash right after it wins the
    // failover election, then trigger the failover on that same sentinel.
    // Both commands reply +OK before the process actually dies, so success
    // here means the crash was armed and the failover requested.
    chaos::crash_sentinel_during_failover(&sentinel, 0, SentinelCrashPoint::AfterElection)
        .await
        .expect("SENTINEL SIMULATE-FAILURE + FAILOVER should succeed");

    // Optionally confirm the effect: the armed sentinel should stop
    // responding once it actually crashes. Bounded, poll-based -- not a
    // fixed sleep -- so it can't hang if the crash is slow to land.
    let armed_sentinel = RedisCli::new().host("127.0.0.1").port(28120);
    redis_server_wrapper::wait::wait_for(
        || async { !armed_sentinel.ping().await },
        Duration::from_secs(30),
        Duration::from_millis(250),
        "armed sentinel did not crash after simulated failure in time",
    )
    .await
    .expect("armed sentinel should stop responding after crashing");
}
