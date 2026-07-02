//! Byte-level TCP fault-injection proxy for testing Redis client resilience.
//!
//! [`FaultProxy`] sits between a test client and an upstream Redis node,
//! forwarding bytes over TCP while allowing tests to inject faults: per-
//! direction delay, mid-frame connection drops, chunked writes, and a
//! black-hole mode. Unlike [`crate::chaos`], which operates on the server
//! process, this module operates purely on the wire and needs no Docker or
//! root privileges.
