//! Type-safe wrapper for `redis-server` and `redis-cli` with builder pattern APIs.
//!
//! Manage Redis server processes for testing, development, and CI.
//! No Docker required -- just `redis-server` and `redis-cli` on PATH.
//!
//! # Quick Start
//!
//! ```no_run
//! use redis_server_wrapper::RedisServer;
//!
//! let server = RedisServer::new()
//!     .port(6400)
//!     .bind("127.0.0.1")
//!     .start()
//!     .unwrap();
//!
//! assert!(server.is_alive());
//! // Stopped automatically on Drop.
//! ```
//!
//! # Cluster
//!
//! ```no_run
//! use redis_server_wrapper::RedisCluster;
//!
//! let cluster = RedisCluster::builder()
//!     .masters(3)
//!     .replicas_per_master(1)
//!     .base_port(7000)
//!     .start()
//!     .unwrap();
//!
//! assert!(cluster.is_healthy());
//! ```
//!
//! # Sentinel
//!
//! ```no_run
//! use redis_server_wrapper::RedisSentinel;
//!
//! let sentinel = RedisSentinel::builder()
//!     .master_port(6390)
//!     .replicas(2)
//!     .sentinels(3)
//!     .start()
//!     .unwrap();
//!
//! assert!(sentinel.is_healthy());
//! ```

pub mod cli;
pub mod cluster;
pub mod sentinel;
pub mod server;

pub use cli::RedisCli;
pub use cluster::{RedisCluster, RedisClusterBuilder, RedisClusterHandle};
pub use sentinel::{RedisSentinel, RedisSentinelBuilder, RedisSentinelHandle};
pub use server::{LogLevel, RedisServer, RedisServerConfig, RedisServerHandle};
