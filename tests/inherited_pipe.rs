//! A server started on one thread must not keep another thread's pipes open.
//!
//! On macOS, std creates a child's stdio pipes with `pipe()` followed by
//! `fcntl(FD_CLOEXEC)`, which is not atomic. A `redis-server` forked by
//! another thread in that window inherits the write end, and a daemonized
//! server keeps it for its lifetime, so the `redis-cli` reader waiting on
//! EOF never sees one (#202).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis_server_wrapper::RedisServer;

/// Rounds of concurrent starts. Each round starts and stops `STARTS` servers.
const ROUNDS: usize = 6;
const STARTS: usize = 8;
/// Tasks running `redis-cli` commands in a loop throughout.
const CLI_LOOPS: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn server_starts_do_not_hold_concurrent_cli_pipes() {
    let target = Arc::new(
        RedisServer::new()
            .auto_port()
            .start()
            .await
            .expect("target server should start"),
    );

    let run = async {
        let stop = Arc::new(AtomicBool::new(false));

        let mut loops = Vec::with_capacity(CLI_LOOPS);
        for i in 0..CLI_LOOPS {
            let target = Arc::clone(&target);
            let stop = Arc::clone(&stop);
            loops.push(tokio::spawn(async move {
                let key = format!("k{i}");
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    target
                        .run(&["SET", &key, &n.to_string()])
                        .await
                        .expect("SET failed");
                    let got = target.run(&["GET", &key]).await.expect("GET failed");
                    assert_eq!(got.trim(), n.to_string());
                    n += 1;
                }
                n
            }));
        }

        for _ in 0..ROUNDS {
            let mut starts = Vec::with_capacity(STARTS);
            for _ in 0..STARTS {
                starts.push(tokio::spawn(async {
                    RedisServer::new()
                        .auto_port()
                        .start()
                        .await
                        .expect("server should start")
                }));
            }
            let mut servers = Vec::with_capacity(STARTS);
            for start in starts {
                servers.push(start.await.expect("start task panicked"));
            }
            for server in &servers {
                server.run(&["PING"]).await.expect("PING failed");
            }
            // Stopped on a blocking thread: Drop shells out synchronously.
            tokio::task::spawn_blocking(move || drop(servers))
                .await
                .expect("stop task panicked");
        }

        stop.store(true, Ordering::Relaxed);
        let mut total = 0;
        for l in loops {
            total += l.await.expect("cli loop panicked");
        }
        assert!(total > 0, "the cli loops never ran a command");
    };

    tokio::time::timeout(Duration::from_secs(120), run)
        .await
        .expect("a redis-cli call hung: a concurrently started server holds its pipe");
}
