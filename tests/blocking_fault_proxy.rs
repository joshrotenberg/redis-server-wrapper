#![cfg(feature = "blocking")]

use redis_server_wrapper::blocking::{Direction, FaultProxy, RedisServer};
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
