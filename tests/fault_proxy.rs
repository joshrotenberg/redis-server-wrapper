use redis_server_wrapper::{CloseKind, Delay, Direction, FaultProxy};
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

#[tokio::test]
async fn stats_counts_a_fired_fault() {
    let upstream = spawn_greeting_upstream(b"delayed").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.set_delay(
        Direction::UpstreamToClient,
        Delay::Fixed(Duration::from_millis(20)),
    );

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let mut buf = [0u8; 7];
    client.read_exact(&mut buf).await.unwrap();

    let stats = proxy.stats();
    assert_eq!(&buf, b"delayed");
    assert!(stats.connections_accepted >= 1);
    assert!(
        stats.delays_upstream_to_client > 0,
        "expected the configured delay to have fired at least once, got {stats:?}"
    );
    assert!(stats.bytes_upstream_to_client >= 7);
}

#[tokio::test]
async fn stall_hangs_in_flight_read_then_deadline_closes() {
    let upstream = spawn_greeting_upstream(b"should be held back").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.stall_then_close(Duration::from_millis(200));

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let mut buf = [0u8; 32];

    // The response is held back by the stall: a short read attempt must not
    // complete.
    let quick = tokio::time::timeout(Duration::from_millis(80), client.read(&mut buf)).await;
    assert!(
        quick.is_err(),
        "expected the stalled read to still be pending, got {quick:?}"
    );

    // After the deadline, the connection is severed and the pending read
    // resolves one way or another (EOF or an error).
    let eventual = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf)).await;
    assert!(
        eventual.is_ok(),
        "expected the stalled connection to be closed after the deadline"
    );
}

#[tokio::test]
async fn rst_close_produces_econnreset() {
    let upstream = spawn_greeting_upstream(b"RESETRESETRESET").await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.close_after_with(Direction::UpstreamToClient, 4, CloseKind::Rst);

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut buf = [0u8; 64];
        loop {
            match client.read(&mut buf).await {
                Ok(0) => panic!("expected ECONNRESET, got a clean EOF instead"),
                Ok(_) => continue,
                Err(e) => return e,
            }
        }
    })
    .await
    .expect("timed out waiting for the connection reset");

    assert_eq!(result.kind(), std::io::ErrorKind::ConnectionReset);
}

#[tokio::test]
async fn disable_refuses_connections_then_enable_restores_passthrough() {
    let upstream = spawn_echo_upstream().await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    let addr = proxy.addr();

    proxy.disable().await.unwrap();

    let err = TcpStream::connect(addr)
        .await
        .expect_err("expected connect to fail while the proxy is disabled");
    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);

    proxy.enable().await.unwrap();
    assert_eq!(
        proxy.addr(),
        addr,
        "addr() must stay stable across disable/enable"
    );

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"still works").await.unwrap();
    let mut buf = [0u8; 11];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"still works");
}

#[tokio::test]
async fn chunk_delay_yields_multiple_distinct_reads() {
    let payload = b"ABC";
    let upstream = spawn_greeting_upstream(payload).await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.set_chunk_size(1);
    proxy.set_chunk_delay(Duration::from_millis(80));

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    let mut reads: Vec<(Instant, Vec<u8>)> = Vec::new();
    let mut buf = [0u8; 16];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => reads.push((Instant::now(), buf[..n].to_vec())),
            Ok(Err(_)) => break,
        }
    }

    assert!(
        reads.len() >= 2,
        "expected multiple distinct reads for a delayed chunked write, got {}",
        reads.len()
    );
    let gap = reads[1].0.duration_since(reads[0].0);
    assert!(
        gap >= Duration::from_millis(40),
        "expected a measurable inter-chunk delay, got {gap:?}"
    );

    let received: Vec<u8> = reads.into_iter().flat_map(|(_, bytes)| bytes).collect();
    assert_eq!(received, payload);
}

#[tokio::test]
async fn reject_next_fails_exactly_twice_then_succeeds() {
    let upstream = spawn_echo_upstream().await;
    let proxy = FaultProxy::spawn(upstream).await.unwrap();
    proxy.reject_next(2, CloseKind::Fin);

    for attempt in 0..2 {
        let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
        let mut buf = [0u8; 8];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("attempt {attempt} timed out waiting for close"))
            .unwrap();
        assert_eq!(
            n, 0,
            "expected an immediate close for a rejected connection"
        );
    }

    let mut client = TcpStream::connect(proxy.addr()).await.unwrap();
    client.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    assert_eq!(proxy.stats().connections_rejected, 2);
}
