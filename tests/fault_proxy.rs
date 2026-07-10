use redis_server_wrapper::{Delay, Direction, FaultProxy};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Start a bare TCP echo server: every connection reads whatever the peer
/// sends and writes it straight back, in a single `write_all` per read.
async fn spawn_echo_upstream() -> std::net::SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// Start a bare TCP server that writes `payload` to every connection as soon
/// as it's accepted, without waiting to read anything first.
async fn spawn_greeting_upstream(payload: &'static [u8]) -> std::net::SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = stream.write_all(payload).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn passthrough_forwards_bytes_both_ways() {
    let upstream = spawn_echo_upstream().await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    client.write_all(b"hello world").await.unwrap();

    let mut buf = [0u8; 11];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello world");
}

#[tokio::test]
async fn close_after_drops_connection_mid_frame() {
    let upstream = spawn_greeting_upstream(b"+PONGPONGPONG\r\n").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.close_after(Direction::UpstreamToClient, 8);

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let mut received = Vec::new();
    client.read_to_end(&mut received).await.unwrap();

    assert_eq!(received, b"+PONGPON");
}

#[tokio::test]
async fn chunked_writes_preserve_data_integrity() {
    let payload = b"the quick brown fox jumps over the lazy dog";
    let upstream = spawn_greeting_upstream(payload).await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.set_chunk_size(1);

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let mut received = Vec::new();
    client.read_to_end(&mut received).await.unwrap();

    assert_eq!(received, payload);
}

#[tokio::test]
async fn drop_all_holds_connection_open_without_data() {
    let upstream = spawn_greeting_upstream(b"should never arrive").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.set_drop_all(true);

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    client.write_all(b"ping").await.unwrap();

    let mut buf = [0u8; 16];
    let result = tokio::time::timeout(Duration::from_millis(200), client.read(&mut buf)).await;
    assert!(
        result.is_err(),
        "expected read to time out on a black-holed connection, got {result:?}"
    );
}

#[tokio::test]
async fn reset_restores_clean_passthrough() {
    let upstream = spawn_echo_upstream().await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.close_after(Direction::UpstreamToClient, 1);
    proxy.set_drop_all(true);
    proxy.reset();

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    client.write_all(b"still works").await.unwrap();

    let mut buf = [0u8; 11];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"still works");
}

#[tokio::test]
async fn fixed_delay_holds_up_the_response() {
    let upstream = spawn_greeting_upstream(b"delayed").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.set_delay(
        Direction::UpstreamToClient,
        Delay::Fixed(Duration::from_millis(150)),
    );

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 7];
    client.read_exact(&mut buf).await.unwrap();

    assert!(start.elapsed() >= Duration::from_millis(150));
    assert_eq!(&buf, b"delayed");
}

#[tokio::test]
async fn dropping_proxy_stops_accepting_new_connections() {
    let upstream = spawn_echo_upstream().await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    let addr = proxy.addr();
    drop(proxy);

    // Give the accept task a moment to actually stop, then confirm the
    // listening socket is gone -- new connections must be refused.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(TcpStream::connect(addr).await.is_err());
}
