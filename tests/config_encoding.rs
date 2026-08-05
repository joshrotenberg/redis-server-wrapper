//! End-to-end checks that config values survive the trip through the
//! generated config file and into a running Redis.
//!
//! The unit tests in `src/config_token.rs` cover the encoder in isolation.
//! These start a real `redis-server` and confirm that Redis parses back what
//! the encoder wrote, which is the part a hand-rolled quoting scheme gets
//! wrong.

use redis_server_wrapper::{Error, RedisServer};

/// Passwords containing every character class that `sdssplitargs` treats
/// specially. Each must round-trip: the server must require it, and it must
/// authenticate.
const AWKWARD_PASSWORDS: &[(&str, u16)] = &[
    ("with space", 16900),
    ("with#hash", 16901),
    ("with\"quote", 16902),
    ("with\\backslash", 16903),
    ("with'single", 16904),
    ("  leading-and-trailing  ", 16905),
    ("semi;colon and $dollar", 16906),
];

#[tokio::test]
async fn awkward_passwords_round_trip() {
    for (password, port) in AWKWARD_PASSWORDS {
        let server = RedisServer::new()
            .port(*port)
            .password(*password)
            .start()
            .await
            .unwrap_or_else(|e| panic!("failed to start with password {password:?}: {e}"));

        // The password is actually in force: an unauthenticated client is
        // rejected. Without this the next assertion would pass on a server
        // that simply has no password set.
        let unauthenticated = redis_server_wrapper::RedisCli::new()
            .host(server.host())
            .port(server.port())
            .run(&["PING"])
            .await;
        assert!(
            matches!(unauthenticated, Err(Error::Cli { ref detail, .. }) if detail.contains("NOAUTH")),
            "password {password:?} was not enforced, got: {unauthenticated:?}"
        );

        // And the exact password authenticates.
        let pong = server
            .run(&["PING"])
            .await
            .unwrap_or_else(|e| panic!("auth failed for password {password:?}: {e}"));
        assert_eq!(pong.trim(), "PONG", "password {password:?}");
    }
}

#[tokio::test]
async fn password_cannot_inject_a_directive() {
    // If the newline were written through unescaped, Redis would parse the
    // tail as its own directive and set maxmemory. The encoder must keep it
    // inside the quoted token.
    let server = RedisServer::new()
        .port(16907)
        .password("pw\nmaxmemory 11111")
        .start()
        .await
        .expect("server with a newline in the password must still start");

    let maxmemory = server.run(&["CONFIG", "GET", "maxmemory"]).await.unwrap();
    assert!(
        !maxmemory.contains("11111"),
        "newline in password injected a directive: {maxmemory}"
    );
}

#[tokio::test]
async fn extra_value_with_spaces_reaches_redis_intact() {
    let server = RedisServer::new()
        .port(16908)
        .extra("proc-title-template", "{title} {listen-addr} custom")
        .start()
        .await
        .expect("failed to start");

    let template = server
        .run(&["CONFIG", "GET", "proc-title-template"])
        .await
        .unwrap();
    assert!(
        template.contains("{title} {listen-addr} custom"),
        "template was mangled: {template}"
    );
}

#[tokio::test]
async fn extra_rejects_a_wrapper_owned_directive() {
    let err = RedisServer::new()
        .port(16909)
        .extra("port", "16999")
        .start()
        .await
        .err()
        .expect("extra(\"port\") must be rejected rather than starting a server");

    assert!(
        matches!(err, Error::ReservedDirective { ref key } if key == "port"),
        "unexpected error: {err}"
    );

    // Nothing was started on either port.
    assert!(
        !redis_server_wrapper::RedisCli::new()
            .port(16999)
            .ping()
            .await
    );
    assert!(
        !redis_server_wrapper::RedisCli::new()
            .port(16909)
            .ping()
            .await
    );
}

#[tokio::test]
async fn generated_config_is_not_world_readable() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let server = RedisServer::new()
            .port(16910)
            .password("secret")
            .start()
            .await
            .expect("failed to start");

        let conf = server.config_path();
        let mode = std::fs::metadata(conf)
            .expect("config file must exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "config carrying requirepass is readable beyond its owner"
        );
    }
}
