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

## Prerequisites

`redis-server` and `redis-cli` must be available on your PATH (or specify custom paths).

## Usage

The API is async-first and requires [tokio](https://tokio.rs). Enable the `blocking` feature
for synchronous wrappers; see the [Blocking API](#blocking-api) section below.

### Single Server

```rust
use redis_server_wrapper::RedisServer;

let server = RedisServer::new()
    .port(6400)
    .bind("127.0.0.1")
    .start()
    .await
    .unwrap();

assert!(server.is_alive().await);
// Stopped automatically on drop.
```

### Cluster

```rust
use redis_server_wrapper::RedisCluster;

let cluster = RedisCluster::builder()
    .masters(3)
    .replicas_per_master(1)
    .base_port(7000)
    .start()
    .await
    .unwrap();

assert!(cluster.is_healthy().await);
```

### Sentinel

```rust
use redis_server_wrapper::RedisSentinel;

let sentinel = RedisSentinel::builder()
    .master_port(6390)
    .replicas(2)
    .sentinels(3)
    .start()
    .await
    .unwrap();

assert!(sentinel.is_healthy().await);
```

## Blocking API

Enable the `blocking` feature for synchronous wrappers that require no async runtime:

```toml
[dev-dependencies]
redis-server-wrapper = { version = "...", features = ["blocking"] }
```

The `blocking` module mirrors the async API. Every operation blocks the calling thread
until it completes:

```rust
use redis_server_wrapper::blocking::RedisServer;

let server = RedisServer::new()
    .port(6400)
    .bind("127.0.0.1")
    .start()
    .unwrap();

assert!(server.is_alive());
// Stopped automatically on drop.
```

Cluster and Sentinel work the same way:

```rust
use redis_server_wrapper::blocking::{RedisCluster, RedisSentinel};

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
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
