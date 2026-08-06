# redis-server-wrapper

[![Crates.io](https://img.shields.io/crates/v/redis-server-wrapper)](https://crates.io/crates/redis-server-wrapper)
[![docs.rs](https://img.shields.io/docsrs/redis-server-wrapper)](https://docs.rs/redis-server-wrapper)
[![CI](https://github.com/joshrotenberg/redis-server-wrapper/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/redis-server-wrapper/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/redis-server-wrapper)](https://github.com/joshrotenberg/redis-server-wrapper#license)

Type-safe Rust wrapper for `redis-server` and `redis-cli` with builder pattern APIs.

Manage Redis server processes for testing, development, and CI without Docker --
just `redis-server` and `redis-cli` on PATH.

## Features

- **Single server** -- start/stop with builder pattern, auto-cleanup on drop
- **Cluster** -- spin up N-master clusters with optional replicas
- **Sentinel** -- full sentinel topology (master + replicas + sentinels)
- **Custom binaries** -- point to any `redis-server`/`redis-cli` path
- **Arbitrary config** -- pass any Redis directive via `.extra(key, value)`
- **Fault injection** -- process-level chaos (freeze, kill, partition) via the `chaos` module,
  and byte-level TCP fault injection (delay, drop, chunking) via `FaultProxy`
- **Parallel fixtures** -- `.auto_port()` picks a free port and retries a lost race
- **Module verification** -- assert a module actually loaded, not just that it was configured
- **Safe by default** -- never stops a Redis process it did not start; reclaims only its own
- **Tracing** -- spans and events across every lifecycle path, visible via `RUST_LOG`

## Prerequisites

`redis-server` and `redis-cli` must be available on your PATH (or specify custom paths with
`.redis_server_bin()` and `.redis_cli_bin()` on any builder).

Minimum supported Rust version (MSRV): 1.88, enforced in CI.

## Redis compatibility

These release lines are tested in CI against pinned builds, on every pull request that touches
the crate's Redis-facing surface and weekly:

| Line | Pinned build under test |
|------|-------------------------|
| 7.2  | 7.2.14                  |
| 7.4  | 7.4.9                   |
| 8.2  | 8.2.6                   |
| 8.x  | 8.8.0                   |

Each is built from source with `BUILD_TLS=yes` and runs the full suite, so the TLS paths are
exercised rather than skipped. Other versions may work; these are the ones a regression would
be caught on.

Every pull request additionally sees both ends of that range: the Ubuntu job pins 7.2.14, the
oldest supported line, and the macOS job uses whatever Homebrew currently provides, which
tracks the newest. The lines in between are covered by the matrix above.

## Platform support

Unix-like platforms only (Linux, macOS, BSD). Process lifecycle management relies on POSIX
signals (`SIGTERM`/`SIGKILL`/`SIGSTOP`/`SIGCONT`) and Unix utilities (`kill`, `lsof`) that have
no equivalent on Windows. Building on a non-Unix target fails at compile time.

## Installation

Add to `Cargo.toml` for async use (the default):

```toml
[dev-dependencies]
redis-server-wrapper = "0.5"
```

The `tokio` feature is enabled by default. To use the synchronous blocking API instead,
disable default features and enable `blocking`:

```toml
[dev-dependencies]
redis-server-wrapper = { version = "0.5", default-features = false, features = ["blocking"] }
```

To use both async and blocking APIs together:

```toml
[dev-dependencies]
redis-server-wrapper = { version = "0.5", features = ["blocking"] }
```

## Usage

The async API requires [tokio](https://tokio.rs). See the [Blocking API](#blocking-api) section
for synchronous use.

Examples below are written as plain functions rather than annotated tests. Every one is
compiled as a doc test on each build, which keeps them from drifting from the API; a
`#[tokio::test]` attribute would make the compiler skip the body and defeat that. Add the
attribute when you copy one into your own suite.

### Single Server

```rust
use redis_server_wrapper::RedisServer;

async fn test_server() {
    let server = RedisServer::new()
        .port(6400)
        .bind("127.0.0.1")
        .start()
        .await
        .unwrap();

    assert!(server.is_alive().await);
    // Stopped automatically on drop.
}
```

The server process is stopped via `SHUTDOWN NOSAVE` when the handle is dropped.
Call `server.detach()` to consume the handle without stopping the process.

### Configuration

Common options have dedicated builder methods. Anything else can be passed as
a raw Redis directive with `.extra(key, value)`. Directives the wrapper
generates itself (`port`, `dir`, `requirepass`, `loadmodule`, and similar) are
rejected: the wrapper reuses those values for readiness probing and teardown, so
overriding one would leave the handle describing a server that does not exist.
Use the dedicated method instead.

```rust
use redis_server_wrapper::{LogLevel, RedisServer};

async fn test_server_config() {
    let server = RedisServer::new()
        .port(6400)
        .bind("127.0.0.1")
        .password("secret")
        .loglevel(LogLevel::Warning)
        .appendonly(true)
        .extra("maxmemory", "256mb")
        .extra("maxmemory-policy", "allkeys-lru")
        .start()
        .await
        .unwrap();
}
```

Redis modules can be loaded at startup with `.loadmodule()`, optionally passing load-time
arguments with `.loadmodule_with_args()`:

```rust
use redis_server_wrapper::RedisServer;

async fn test_loadmodule() {
    let server = RedisServer::new()
        .port(6400)
        .loadmodule("/path/to/module.so")
        .loadmodule_with_args("/path/to/other_module.so", ["arg1", "value1"])
        .start()
        .await
        .unwrap();
}
```

### Parallel Fixtures

Tests that each own a Redis lifecycle can let the wrapper pick a free port
instead of coordinating a fixed range between them:

```rust
use redis_server_wrapper::RedisServer;

async fn test_auto_port() {
    let server = RedisServer::new().auto_port().start().await.unwrap();

    // The chosen port is on the handle.
    println!("started on {}", server.addr());
}
```

Asking the OS for a free port and having Redis bind it are two steps, so another
process can take the port in between. The builder retries with a new candidate
rather than failing, and never stops whatever won the race.

`port(0)` is unrelated: it keeps its Redis meaning of disabling the plaintext
listener for a TLS-only server.

### Verifying Loaded Modules

A `loadmodule` directive only asks Redis to load a module. A wrong path, an ABI
mismatch, or a module whose `RedisModule_OnLoad` fails all leave the server
running without it. Check against the live server:

```rust
use redis_server_wrapper::RedisServer;

async fn test_module_is_loaded() {
    let server = RedisServer::new()
        .port(6400)
        .loadmodule("/path/to/module.so")
        .start()
        .await
        .unwrap();

    // Errors listing what is loaded instead, since the module's registered
    // name often differs from its filename.
    server.require_module("my_module").await.unwrap();

    for module in server.modules().await.unwrap() {
        println!("{} v{:?}", module.name, module.version);
    }
}
```

Clusters and Sentinel topologies propagate modules to every node, so they have
`require_module_on_all_nodes` and `require_module_on_data_nodes` respectively.

### Ports and Cleanup

Startup never stops a Redis process it did not start. If a port a topology needs
is already serving, startup fails with `Error::PortInUse` naming the port and
leaves that process alone.

A crashed run is reclaimed rather than blocking the next one. Cluster and
Sentinel topologies keep a working directory derived from their shape, and on
startup they stop only processes recorded in pidfiles they wrote, after
confirming the pid still belongs to a Redis binary. Point that directory
somewhere specific with `.dir()` on either builder.

### Logging

The crate emits [`tracing`](https://docs.rs/tracing) spans and events across the
lifecycle paths: server, cluster, and Sentinel startup and shutdown, every chaos
operation, and the fault proxy. With no subscriber installed these surface as
`log` records, so `env_logger` or `test-log` users get output with no setup:

```console
RUST_LOG=redis_server_wrapper=debug cargo test
```

`tracing-subscriber` users get the same targets as structured spans.

### Running Commands

The handle exposes a `RedisCli` you can use to run arbitrary commands against the server:

```rust
use redis_server_wrapper::RedisServer;

async fn test_run_commands() {
    let server = RedisServer::new().port(6400).start().await.unwrap();

    server.run(&["SET", "key", "value"]).await.unwrap();
    let val = server.run(&["GET", "key"]).await.unwrap();
    assert_eq!(val.trim(), "value");
}
```

You can also get a `RedisCli` instance directly from the handle:

```rust
use redis_server_wrapper::RedisServer;

async fn test_cli() {
    let server = RedisServer::new().port(6400).start().await.unwrap();
    let cli = server.cli();
    let pong = cli.ping().await;
    assert!(pong);
}
```

### Cluster

```rust
use redis_server_wrapper::RedisCluster;

async fn test_cluster() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(7000)
        .start()
        .await
        .unwrap();

    assert!(cluster.is_healthy().await);
}
```

### Sentinel

```rust
use redis_server_wrapper::RedisSentinel;

async fn test_sentinel() {
    let sentinel = RedisSentinel::builder()
        .master_port(6390)
        .replicas(2)
        .sentinels(3)
        .start()
        .await
        .unwrap();

    assert!(sentinel.is_healthy().await);
}
```

### Chaos and Fault Injection

The `chaos` module simulates process-level failures -- freezing, killing, and partitioning
nodes with POSIX signals -- for testing how clients handle timeouts and failovers:

```rust
use redis_server_wrapper::{RedisServer, chaos};
use std::time::Duration;

async fn test_chaos_pause() {
    let server = RedisServer::new().port(6400).start().await.unwrap();

    // Freeze the node for 2 seconds, then resume it automatically.
    chaos::pause_node(&server, Duration::from_secs(2)).unwrap();

    // ... test client behavior while the node is frozen ...
}
```

`chaos::partition` and `chaos::recover` simulate a network partition across a cluster by
freezing every node outside a reachable set, and `chaos::fill_memory` writes a bounded number
of fixed-size keys for exercising `maxmemory` and eviction behavior. `chaos::slow_down` and
`chaos::slow_down_writes` use `CLIENT PAUSE` to delay all commands or writes only.

Signal-sending functions (`kill_node`, `freeze_node`, `resume_node`, `pause_node`, `partition`,
`recover`, and the cluster `*_by_slot`/`*_by_key` variants) return `Result` -- a failed `kill`
invocation or a non-zero exit status is surfaced as an error instead of being silently ignored.

`FaultProxy` operates at the byte level instead of the process level, injecting faults into
the TCP connection itself -- delay, mid-frame drops, chunked writes -- without touching the
server process:

```rust
use redis_server_wrapper::{Direction, FaultProxy, RedisServer};

async fn test_fault_proxy() {
    let server = RedisServer::new().port(6400).start().await.unwrap();
    let proxy = FaultProxy::spawn(server.addr()).await.unwrap();

    // Point clients at proxy.addr() instead of server.addr().
    proxy.close_after(Direction::UpstreamToClient, 8);

    // ... assert the client sees a clean mid-frame connection error ...
}
```

## Error Handling

All fallible operations return `Result<T, Error>`. The `Error` enum covers server
start failures, timeouts, CLI errors, port and topology problems, and underlying
I/O errors:

```rust
use redis_server_wrapper::{Error, RedisServer};

async fn test_error_handling() {
    match RedisServer::new().port(6400).start().await {
        Ok(server) => println!("running on {}", server.addr()),
        // `detail` carries the tail of the node's log when one was written,
        // which usually says why Redis refused to start.
        Err(Error::ServerStart { port, detail }) => {
            eprintln!("could not start on port {port}: {}", detail.unwrap_or_default())
        }
        Err(Error::PortInUse { port, role, .. }) => {
            eprintln!("{role} {port} is already serving; startup will not clear it")
        }
        Err(Error::BinaryNotFound { binary }) => eprintln!("{binary} not found on PATH"),
        Err(e) => eprintln!("unexpected: {e}"),
    }
}
```

`Error` is `#[non_exhaustive]`, so a catch-all `Err(e)` arm is required. Use a
`..` rest pattern inside a variant as well, since variants gain fields between
releases the same way the enum gains variants.

## Blocking API

Enable the `blocking` feature for synchronous wrappers that require no async runtime:

```toml
[dev-dependencies]
redis-server-wrapper = { version = "0.5", features = ["blocking"] }
```

The `blocking` module mirrors the async API. Every operation blocks the calling thread
until it completes. Handles own a long-lived `tokio::runtime::Runtime` so that the
underlying async `Drop` implementation keeps working correctly.

```rust
use redis_server_wrapper::blocking::RedisServer;

fn test_blocking_server() {
    let server = RedisServer::new()
        .port(6400)
        .bind("127.0.0.1")
        .start()
        .unwrap();

    assert!(server.is_alive());
    // Stopped automatically on drop.
}
```

Use `server.detach()` in the blocking API for the same keep-running behavior.

Cluster and Sentinel work the same way:

```rust
use redis_server_wrapper::blocking::{RedisCluster, RedisSentinel};

fn test_blocking_topologies() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .base_port(7000)
        .start()
        .unwrap();

    assert!(cluster.is_healthy());

    let sentinel = RedisSentinel::builder()
        .master_port(6390)
        .replicas(2)
        .sentinels(3)
        .start()
        .unwrap();

    assert!(sentinel.is_healthy());
}
```

## Examples

The crate ships a runnable example that demonstrates various server configurations:

```sh
cargo run --example redis-run
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
