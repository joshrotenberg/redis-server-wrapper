#![cfg(feature = "blocking")]

use redis_server_wrapper::blocking::{CloseKind, Direction, FaultProxy, RedisServer};
use std::io::{Read, Write};
use std::net::TcpStream;

#[test]
fn passthrough_ping_roundtrip() {
    let server = RedisServer::new()
        .port(17850)
        .start()
        .expect("failed to start server");

    let proxy = FaultProxy::spawn(server.addr()).expect("failed to spawn proxy");

    let mut client = TcpStream::connect(proxy.addr()).expect("failed to connect to proxy");
    client.write_all(b"PING\r\n").expect("write failed");

    let mut buf = [0u8; 7];
    client.read_exact(&mut buf).expect("read failed");
    assert_eq!(&buf, b"+PONG\r\n");
}

#[test]
fn close_after_drops_connection_mid_frame() {
    let server = RedisServer::new()
        .port(17851)
        .start()
        .expect("failed to start server");

    let proxy = FaultProxy::spawn(server.addr()).expect("failed to spawn proxy");
    proxy.close_after(Direction::UpstreamToClient, 3);

    let mut client = TcpStream::connect(proxy.addr()).expect("failed to connect to proxy");
    client.write_all(b"PING\r\n").expect("write failed");

    let mut received = Vec::new();
    client
        .read_to_end(&mut received)
        .expect("read_to_end failed");

    assert_eq!(received, b"+PO");
}

#[test]
fn stats_and_reject_next_smoke_test() {
    let server = RedisServer::new()
        .port(17852)
        .start()
        .expect("failed to start server");

    let proxy = FaultProxy::spawn(server.addr()).expect("failed to spawn proxy");
    proxy.reject_next(1, CloseKind::Fin);

    // The first connection attempt is rejected: accepted, then closed
    // immediately without any upstream connection or data.
    {
        let mut client = TcpStream::connect(proxy.addr()).expect("failed to connect to proxy");
        let mut buf = [0u8; 8];
        let n = client.read(&mut buf).expect("read failed");
        assert_eq!(
            n, 0,
            "expected an immediate close for a rejected connection"
        );
    }

    // The second connection attempt passes through normally.
    let mut client = TcpStream::connect(proxy.addr()).expect("failed to connect to proxy");
    client.write_all(b"PING\r\n").expect("write failed");
    let mut buf = [0u8; 7];
    client.read_exact(&mut buf).expect("read failed");
    assert_eq!(&buf, b"+PONG\r\n");

    // The byte counters are updated by the proxy's forwarding task, which
    // races the client's read: the reply can reach the client before the
    // counter increment lands. Poll instead of asserting immediately.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let stats = loop {
        let stats = proxy.stats();
        if stats.bytes_upstream_to_client > 0 || std::time::Instant::now() > deadline {
            break stats;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert_eq!(stats.connections_accepted, 2);
    assert_eq!(stats.connections_rejected, 1);
    assert!(stats.bytes_upstream_to_client > 0);
}
