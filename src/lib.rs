//! Type-safe wrapper for `redis-server` and `redis-cli` with builder pattern APIs.
//!
//! Manage Redis server processes for testing, development, and CI.
//! No Docker required -- just `redis-server` and `redis-cli` on PATH.
//!
//! # Overview
//!
//! This crate provides Rust builders that launch real Redis processes and manage
//! their lifecycle. Servers are started on [`RedisServer::start`] and
//! automatically stopped when the returned handle is dropped. Three topologies
//! are supported:
//!
//! | Topology | Builder | Handle |
//! |----------|---------|--------|
//! | Standalone | [`RedisServer`] | [`RedisServerHandle`] |
//! | Cluster | [`RedisClusterBuilder`] | [`RedisClusterHandle`] |
//! | Sentinel | [`RedisSentinelBuilder`] | [`RedisSentinelHandle`] |
//!
//! # Prerequisites
//!
//! `redis-server` and `redis-cli` must be on your `PATH`, or you can point to
//! custom binaries with `.redis_server_bin()` and `.redis_cli_bin()` on any
//! builder.
//!
//! # Quick Start
//!
//! ```no_run
//! use redis_server_wrapper::RedisServer;
//!
//! # async fn example() {
//! let server = RedisServer::new()
//!     .port(6400)
//!     .bind("127.0.0.1")
//!     .start()
//!     .await
//!     .unwrap();
//!
//! assert!(server.is_alive().await);
//! // Stopped automatically on Drop.
//! # }
//! ```
//!
//! # Configuration
//!
//! Every Redis configuration directive can be passed through the builder.
//! Common options have dedicated methods; anything else goes through
//! [`RedisServer::extra`]:
//!
//! ```no_run
//! use redis_server_wrapper::{LogLevel, RedisServer};
//!
//! # async fn example() {
//! let server = RedisServer::new()
//!     .port(6400)
//!     .bind("127.0.0.1")
//!     .password("secret")
//!     .loglevel(LogLevel::Warning)
//!     .appendonly(true)
//!     .extra("maxmemory", "256mb")
//!     .extra("maxmemory-policy", "allkeys-lru")
//!     .start()
//!     .await
//!     .unwrap();
//! # }
//! ```
//!
//! # Running Commands
//!
//! The handle exposes a [`RedisCli`] that you can use to run arbitrary
//! commands against the server:
//!
//! ```no_run
//! use redis_server_wrapper::RedisServer;
//!
//! # async fn example() {
//! let server = RedisServer::new().port(6400).start().await.unwrap();
//!
//! server.run(&["SET", "key", "value"]).await.unwrap();
//! let val = server.run(&["GET", "key"]).await.unwrap();
//! assert_eq!(val.trim(), "value");
//! # }
//! ```
//!
//! # Cluster
//!
//! Spin up a Redis Cluster with automatic slot assignment. The builder starts
//! each node, then calls `redis-cli --cluster create` to form the cluster:
//!
//! ```no_run
//! use redis_server_wrapper::RedisCluster;
//!
//! # async fn example() {
//! let cluster = RedisCluster::builder()
//!     .masters(3)
//!     .replicas_per_master(1)
//!     .base_port(7000)
//!     .start()
//!     .await
//!     .unwrap();
//!
//! assert!(cluster.is_healthy().await);
//! assert_eq!(cluster.node_addrs().len(), 6);
//! # }
//! ```
//!
//! # Sentinel
//!
//! Start a full Sentinel topology -- master, replicas, and sentinel processes:
//!
//! ```no_run
//! use redis_server_wrapper::RedisSentinel;
//!
//! # async fn example() {
//! let sentinel = RedisSentinel::builder()
//!     .master_port(6390)
//!     .replicas(2)
//!     .sentinels(3)
//!     .quorum(2)
//!     .start()
//!     .await
//!     .unwrap();
//!
//! assert!(sentinel.is_healthy().await);
//! assert_eq!(sentinel.master_name(), "mymaster");
//! # }
//! ```
//!
//! # Error Handling
//!
//! All fallible operations return [`Result<T>`], which uses the crate's
//! [`Error`] type. Variants cover server start failures, timeouts, CLI errors,
//! and the underlying I/O errors:
//!
//! ```no_run
//! use redis_server_wrapper::{Error, RedisServer};
//!
//! # async fn example() {
//! match RedisServer::new().port(6400).start().await {
//!     Ok(server) => println!("running on {}", server.addr()),
//!     Err(Error::ServerStart { port }) => eprintln!("could not start on {port}"),
//!     Err(e) => eprintln!("unexpected: {e}"),
//! }
//! # }
//! ```
//!
//! # Lifecycle
//!
//! All handles implement [`Drop`]. When a handle goes out of scope, it sends
//! `SHUTDOWN NOSAVE` to the corresponding Redis process. For sentinel
//! topologies, sentinels are shut down first, then replicas and master (via
//! their own handle drops).
//!
//! You can also call `.stop()` explicitly on any handle to shut down early.

#[cfg(feature = "tokio")]
pub mod cli;
#[cfg(feature = "tokio")]
pub mod cluster;
pub mod error;
#[cfg(feature = "tokio")]
pub mod sentinel;
#[cfg(feature = "tokio")]
pub mod server;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "tokio")]
pub use cli::{OutputFormat, RedisCli, RespProtocol};
#[cfg(feature = "tokio")]
pub use cluster::{RedisCluster, RedisClusterBuilder, RedisClusterHandle};
pub use error::{Error, Result};
#[cfg(feature = "tokio")]
pub use sentinel::{RedisSentinel, RedisSentinelBuilder, RedisSentinelHandle};
#[cfg(feature = "tokio")]
pub use server::{LogLevel, RedisServer, RedisServerConfig, RedisServerHandle};
