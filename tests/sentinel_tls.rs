#![cfg(feature = "test-tls")]

use redis_server_wrapper::tls::generate_test_certs;
use redis_server_wrapper::{RedisCli, RedisSentinel};
use std::time::Duration;

/// A TLS Sentinel topology, with 1 replica and 3 sentinels, starts and
/// converges: every process serves TLS on its own port, the master accepts a
/// write over TLS, the replica links up, and every sentinel answers
/// `SENTINEL get-master-addr-by-name` over TLS with the master's port.
#[tokio::test]
async fn sentinel_topology_over_tls_starts_and_is_healthy() {
    let dir = std::env::temp_dir().join("rsw-sentinel-tls-certs");
    let certs = generate_test_certs(&dir).expect("cert generation failed");

    let sentinel = RedisSentinel::builder()
        .master_port(16700)
        .replicas(1)
        .replica_base_port(16701)
        .sentinels(3)
        .sentinel_base_port(16710)
        .tls_cert_file(&certs.cert_file)
        .tls_key_file(&certs.key_file)
        .tls_ca_cert_file(&certs.ca_cert_file)
        .tls_auth_clients(false)
        .start()
        .await
        .expect("failed to start TLS sentinel topology");

    sentinel
        .wait_for_healthy(Duration::from_secs(30))
        .await
        .expect("TLS sentinel topology did not become healthy");
    assert!(sentinel.is_healthy().await);

    // The master's own address still reports its real (TLS) port.
    assert_eq!(sentinel.master_addr(), "127.0.0.1:16700");

    // A write on the master over TLS, through the handle's own CLI.
    sentinel
        .master()
        .run(&["SET", "sentinel-tls-key", "sentinel-tls-value"])
        .await
        .expect("SET on TLS master failed");
    let value = sentinel
        .master()
        .run(&["GET", "sentinel-tls-key"])
        .await
        .expect("GET on TLS master failed");
    assert_eq!(value.trim(), "sentinel-tls-value");

    // The master reports the replica connected over its TLS listener.
    let info = sentinel
        .master()
        .info(Some("replication"))
        .await
        .expect("INFO replication on TLS master failed");
    let connected_slaves: u32 = info
        .get("connected_slaves")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        connected_slaves >= 1,
        "expected at least 1 connected replica, info: {info:?}"
    );

    // Every sentinel answers SENTINEL get-master-addr-by-name over TLS with
    // the master's (TLS) port.
    for addr in sentinel.sentinel_addrs() {
        let (host, port) = addr.split_once(':').expect("sentinel addr has no port");
        let cli = RedisCli::new()
            .host(host)
            .port(port.parse().expect("sentinel port is not numeric"))
            .tls(true)
            .cacert(&certs.ca_cert_file);
        let reply = cli
            .run(&["SENTINEL", "get-master-addr-by-name", "mymaster"])
            .await
            .expect("SENTINEL get-master-addr-by-name over TLS failed");
        assert!(
            reply.contains("16700"),
            "expected the master's port 16700 in the sentinel reply, got: {reply}"
        );
    }
}
