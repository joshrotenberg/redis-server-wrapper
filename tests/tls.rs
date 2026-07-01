#![cfg(feature = "test-tls")]

use redis_server_wrapper::tls::generate_test_certs;
use redis_server_wrapper::{RedisCli, RedisServer};

#[tokio::test]
async fn generate_test_certs_writes_files() {
    let dir = std::env::temp_dir().join("rsw-tls-test-certs");
    let certs = generate_test_certs(&dir).expect("cert generation failed");

    assert!(certs.ca_cert_file.is_file(), "ca.crt not found");
    assert!(certs.cert_file.is_file(), "server.crt not found");
    assert!(certs.key_file.is_file(), "server.key not found");
    assert!(std::fs::metadata(&certs.ca_cert_file).unwrap().len() > 0);
    assert!(std::fs::metadata(&certs.cert_file).unwrap().len() > 0);
    assert!(std::fs::metadata(&certs.key_file).unwrap().len() > 0);
}

#[tokio::test]
async fn tls_server_roundtrip() {
    let dir = std::env::temp_dir().join("rsw-tls-roundtrip");
    let certs = generate_test_certs(&dir).expect("cert generation failed");

    let server = RedisServer::new()
        .port(16800)
        .tls_port(16801)
        .tls_cert_file(&certs.cert_file)
        .tls_key_file(&certs.key_file)
        .tls_ca_cert_file(&certs.ca_cert_file)
        .tls_auth_clients(false)
        .start()
        .await
        .expect("failed to start TLS-enabled server");

    assert!(server.is_alive().await);

    let reply = RedisCli::new()
        .host("127.0.0.1")
        .port(16801)
        .tls(true)
        .cacert(&certs.ca_cert_file)
        .run(&["PING"])
        .await
        .expect("PING over TLS failed");
    assert_eq!(reply.trim(), "PONG");
}

#[tokio::test]
async fn tls_auth_clients_rejects_missing_client_cert() {
    let dir = std::env::temp_dir().join("rsw-tls-auth-clients");
    let certs = generate_test_certs(&dir).expect("cert generation failed");

    let server = RedisServer::new()
        .port(16802)
        .tls_port(16803)
        .tls_cert_file(&certs.cert_file)
        .tls_key_file(&certs.key_file)
        .tls_ca_cert_file(&certs.ca_cert_file)
        .tls_auth_clients(true)
        .start()
        .await
        .expect("failed to start TLS-enabled server");

    assert!(server.is_alive().await);

    // tls_auth_clients(true) requires mutual TLS, so a client that presents
    // no certificate of its own must be rejected.
    let result = RedisCli::new()
        .host("127.0.0.1")
        .port(16803)
        .tls(true)
        .cacert(&certs.ca_cert_file)
        .run(&["PING"])
        .await;

    assert!(
        result.is_err(),
        "expected PING without a client cert to fail under tls_auth_clients(true)"
    );
}
