# redis-server-wrapper

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

### Single Server

```rust
use redis_server_wrapper::RedisServer;

let server = RedisServer::new()
    .port(6400)
    .bind("127.0.0.1")
    .start()
    .unwrap();

assert!(server.is_alive());
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
    .unwrap();

assert!(cluster.is_healthy());
```

### Sentinel

```rust
use redis_server_wrapper::RedisSentinel;

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
