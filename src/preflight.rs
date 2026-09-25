//! Port availability checks run before a topology starts anything.
//!
//! The cluster and Sentinel builders used to clear the ports they wanted by
//! sending `SHUTDOWN` to whatever was listening. Neither could distinguish a
//! leftover node of its own from an unrelated Redis holding the port, so the
//! cleanup could stop a server the wrapper never owned.
//!
//! Both builders create a uniquely named temp directory per start, so they
//! never have a prior process of their own to reclaim: an occupied port always
//! belongs to something else. These helpers check first and fail with the port
//! named, leaving whatever holds it alone.

use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};

/// How long to wait for a connect probe before treating the port as free.
///
/// Probes are against loopback in the common case, where a live listener
/// answers in well under a millisecond and a free port is refused just as
/// fast. The bound only matters for an address that silently drops packets.
const PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// What a port was needed for, used to make [`Error::PortInUse`] specific
/// enough to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortRole {
    /// A standalone server's client port.
    Server,
    /// A cluster node's client port.
    ClusterNode,
    /// A cluster node's bus port (client port + 10000).
    ClusterBus,
    /// A Sentinel topology's master port.
    SentinelMaster,
    /// A Sentinel topology's replica port.
    SentinelReplica,
    /// A sentinel process's own port.
    Sentinel,
}

impl std::fmt::Display for PortRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Server => "server port",
            Self::ClusterNode => "cluster node port",
            Self::ClusterBus => "cluster bus port",
            Self::SentinelMaster => "sentinel master port",
            Self::SentinelReplica => "sentinel replica port",
            Self::Sentinel => "sentinel port",
        };
        f.write_str(s)
    }
}

/// Whether a port is free of a live listener on `host`.
///
/// Probes by connecting, not by binding. The question this needs to answer is
/// "would starting here disturb a server that is already running", and only a
/// process actually accepting connections can be disturbed.
///
/// Binding is the obvious implementation and is wrong for this. A port whose
/// previous listener has just exited can still refuse a bind with
/// `AddrInUse` while the kernel tears the socket down, and sockets left in
/// `TIME_WAIT` have no owning process at all. A bind probe reads both as
/// occupied, which turns the normal case of reusing a port moments after
/// stopping a server into a spurious failure. `SO_REUSEADDR` does not close
/// this gap portably: Linux lets a listener bind over `TIME_WAIT`, BSD and
/// macOS do not.
///
/// A refused connection means nothing is serving, so the port is available
/// even if the kernel is still holding remnants: `redis-server` sets
/// `SO_REUSEADDR` itself and binds it fine.
///
/// Anything other than a successful connection reads as available. If the
/// address cannot be reached, this cannot disturb a server there either, and
/// a genuine bind failure is reported by the start that follows.
pub fn port_available(host: &str, port: u16) -> bool {
    let Ok(addrs) = (host, port).to_socket_addrs() else {
        return true;
    };
    let addrs: Vec<SocketAddr> = addrs.collect();

    for addr in addrs {
        if TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).is_ok() {
            return false;
        }
    }
    true
}

/// Return an error naming the first occupied port, if any.
///
/// Checked in the order given, so the error points at the first conflict a
/// reader would look for.
pub fn ensure_ports_available(
    host: &str,
    ports: impl IntoIterator<Item = (u16, PortRole)>,
) -> Result<()> {
    for (port, role) in ports {
        if !port_available(host, port) {
            return Err(Error::PortInUse {
                host: host.to_string(),
                port,
                role: role.to_string(),
            });
        }
    }
    Ok(())
}

/// Ask the OS for a port that is free right now.
///
/// Binds `127.0.0.1:0`, reads back what the kernel assigned, and releases it.
/// The port is free at the moment it is returned and nothing holds it
/// afterwards, so another process can claim it before the caller does.
///
/// That window is unavoidable: reserving a port and handing it to a separate
/// process that must bind it itself cannot be atomic. Callers are expected to
/// treat the result as a candidate and retry on a lost race, which is what
/// [`crate::server::RedisServer::auto_port`] does.
pub fn reserve_ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(Error::Io)?;
    let port = listener.local_addr().map_err(Error::Io)?.port();
    drop(listener);
    Ok(port)
}

/// The cluster bus port Redis derives from a client port.
///
/// Redis uses client port + 10000 unless `cluster-port` overrides it. Returns
/// `None` when that would exceed the port space, which is a topology error
/// rather than something to discover at startup.
pub fn bus_port(client_port: u16) -> Option<u16> {
    client_port.checked_add(10000)
}

/// The gap Redis leaves between a client port and its bus port.
const BUS_OFFSET: u16 = 10000;

/// The lowest base port an automatic cluster range is drawn from.
///
/// Above the well-known range, and clear of the ports a person is likely to
/// have picked by hand: 6379 for a standalone server and the 7000 block that
/// every cluster tutorial uses.
const AUTO_BASE_MIN: u16 = 10000;

/// One past the highest port an automatic cluster range may touch.
///
/// A cluster needs two ranges, the client ports and the bus ports 10000
/// above them, and neither may sit in the pool the OS hands out for outbound
/// connections. Drawing from that pool would put the allocator in a race with
/// every connection the machine makes, which is the race automatic allocation
/// exists to avoid.
///
/// Linux's default pool starts at 32768 and macOS's at 49152, so keeping the
/// whole bus range below 32768 stays clear on either without asking the OS
/// what its pool is.
const AUTO_CEILING: u16 = 32768;

/// Candidate base ports for a cluster of `count` nodes, in random order.
///
/// A cluster cannot use the bind-to-zero trick a standalone server uses: it
/// needs `count` consecutive client ports and the matching bus ports, and the
/// OS will not hand out a contiguous run. So the range is chosen rather than
/// requested, and the caller probes it.
///
/// Random rather than sequential so two processes starting at the same moment
/// do not walk the same candidates in the same order and collide on every
/// one. Each candidate is a base port whose client range
/// `[base, base + count)` and bus range `[base + 10000, base + 10000 + count)`
/// both fit under [`AUTO_CEILING`].
///
/// Yields nothing when `count` is too large to place: either the two ranges
/// no longer fit under [`AUTO_CEILING`], or the run is long enough to overlap
/// its own bus range at any base. [`crate::cluster::RedisClusterBuilder`]
/// rejects both as topology errors before ever asking.
pub fn candidate_base_ports(count: u16, attempts: usize) -> impl Iterator<Item = u16> {
    // A run longer than the bus offset overlaps its own bus range wherever it
    // is placed: the client ports reach past `base + 10000`, which is where
    // the bus ports start. No base fixes that, so yield nothing.
    let placeable = count <= BUS_OFFSET;

    // The highest base whose bus range still ends below the ceiling.
    let highest = AUTO_CEILING
        .checked_sub(BUS_OFFSET)
        .and_then(|p| p.checked_sub(count));

    let span = if placeable {
        highest
            .and_then(|h| h.checked_sub(AUTO_BASE_MIN))
            .unwrap_or(0)
    } else {
        0
    };

    let mut seed = entropy();
    (0..attempts).filter_map(move |_| {
        if span == 0 {
            return None;
        }
        // xorshift, so successive candidates are unrelated. The quality bar
        // here is "does not repeat itself", not cryptographic.
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        Some(AUTO_BASE_MIN + (seed % span as u64) as u16)
    })
}

/// A seed that differs between processes and between calls.
///
/// `RandomState` is seeded by the OS once per process and then perturbed per
/// instance, which is enough to keep two concurrently starting test binaries
/// from drawing the same sequence. Avoids a dependency on `rand` for a
/// non-cryptographic use.
fn entropy() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    let seed = hasher.finish();
    // A zero seed is a fixed point for xorshift and would yield one number
    // forever.
    if seed == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        seed
    }
}

/// Whether every client port in `[base, base + count)` and its bus port is
/// free on `host`.
///
/// The whole range is one allocation decision: a cluster that gets most of
/// its ports is not partially started, it is rejected. Bus ports are skipped
/// when `check_bus` is false, which is the case when `cluster-port` overrides
/// the derived bus port.
pub fn port_range_available(host: &str, base: u16, count: u16, check_bus: bool) -> bool {
    for offset in 0..count {
        let Some(port) = base.checked_add(offset) else {
            return false;
        };
        if !port_available(host, port) {
            return false;
        }
        if check_bus {
            let Some(bus) = bus_port(port) else {
                return false;
            };
            if !port_available(host, bus) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};

    /// Run `check` against a port that was free the instant it was handed
    /// over, retrying if a concurrent test claimed it first.
    ///
    /// Every assertion about an unoccupied port races the rest of this
    /// process: tests here, in `auto_port`, and anything else asking the OS
    /// for an ephemeral port draw from the same pool, and a port released to
    /// make an assertion about it can be taken before the assertion runs. A
    /// genuine failure fails every attempt; a lost race does not.
    fn with_free_port(mut check: impl FnMut(u16) -> bool, what: &str) {
        const ATTEMPTS: usize = 8;
        for attempt in 0..ATTEMPTS {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);

            if check(port) {
                return;
            }
            assert!(attempt < ATTEMPTS - 1, "{what}");
        }
    }

    #[test]
    fn unbound_port_is_available() {
        with_free_port(
            |port| port_available("127.0.0.1", port),
            "a port with nothing listening must read as available",
        );
    }

    #[test]
    fn listening_port_is_not_available() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(!port_available("127.0.0.1", port));
        drop(listener);
    }

    #[test]
    fn port_with_lingering_time_wait_is_available() {
        // The regression this probe design exists for. A listener that has
        // handled a connection and then exited can leave the port refusing a
        // bind while nothing is serving on it. Starting there is fine, so it
        // must not read as occupied.
        //
        // Retried because the port is released before it is checked, and any
        // other test in this process asking for an ephemeral port can take it
        // in between. A genuine failure fails every attempt.
        for attempt in 0..8 {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();

            // Drive a real connection through so the socket has something to
            // linger over, rather than closing an untouched listener.
            let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let (mut server, _) = listener.accept().unwrap();
            client.write_all(b"ping").unwrap();
            let mut buf = [0u8; 4];
            server.read_exact(&mut buf).unwrap();

            drop(client);
            drop(server);
            drop(listener);

            if port_available("127.0.0.1", port) {
                return;
            }
            assert!(
                attempt < 7,
                "a port with no live listener must be available even while \
                 the kernel still holds socket remnants"
            );
        }
    }

    #[test]
    fn candidates_keep_both_ranges_clear_of_the_ephemeral_pool() {
        // The property that makes automatic allocation worth anything: if the
        // bus range reached into the pool the OS draws outbound connections
        // from, the allocator would be racing every connection on the machine.
        for count in [3u16, 6, 30, 300] {
            let mut yielded = 0;
            for base in candidate_base_ports(count, 64) {
                yielded += 1;
                assert!(base >= AUTO_BASE_MIN, "base {base} below the floor");
                let highest_client = base + count - 1;
                let highest_bus = bus_port(highest_client)
                    .unwrap_or_else(|| panic!("base {base} has no bus port"));
                assert!(
                    highest_bus < AUTO_CEILING,
                    "count {count} at base {base} puts bus port {highest_bus} \
                     in the ephemeral pool"
                );
            }
            assert_eq!(yielded, 64, "every attempt should yield a candidate");
        }
    }

    #[test]
    fn candidates_do_not_repeat_in_order() {
        // Sequential candidates would make two processes starting together
        // walk the same numbers in the same order and collide on every one.
        let candidates: Vec<u16> = candidate_base_ports(6, 32).collect();
        let unique: std::collections::HashSet<u16> = candidates.iter().copied().collect();
        assert!(
            unique.len() > candidates.len() / 2,
            "candidates should be spread out, got {candidates:?}"
        );
        assert!(
            candidates.windows(2).any(|w| w[1] != w[0] + 1),
            "candidates should not be a sequential walk"
        );
    }

    #[test]
    fn a_run_that_would_overlap_its_own_bus_range_yields_no_candidates() {
        // Past the bus offset the client ports reach into where the bus ports
        // start, and moving the base does not help.
        assert_eq!(candidate_base_ports(BUS_OFFSET + 1, 8).count(), 0);
    }

    #[test]
    fn every_candidate_range_is_disjoint_from_its_bus_range() {
        for count in [1u16, 3, 500, BUS_OFFSET] {
            for base in candidate_base_ports(count, 16) {
                let last_client = base + count - 1;
                let first_bus = base + BUS_OFFSET;
                assert!(
                    last_client < first_bus,
                    "count {count} at base {base}: client range reaches {last_client}, \
                     bus range starts at {first_bus}"
                );
            }
        }
    }

    #[test]
    fn a_cluster_too_large_to_place_yields_no_candidates() {
        // Larger than the window between the floor and the ceiling. Better to
        // yield nothing than to hand back a range that runs into the
        // ephemeral pool.
        let count = AUTO_CEILING - BUS_OFFSET - AUTO_BASE_MIN + 1;
        assert_eq!(candidate_base_ports(count, 8).count(), 0);
    }

    #[test]
    fn an_occupied_port_anywhere_in_the_range_rejects_the_whole_range() {
        // One allocation decision, not a partial start: a cluster that can
        // only get some of its ports has not got its ports.
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let occupied = listener.local_addr().unwrap().port();

        // The occupied port sits in the middle of the requested range.
        let base = occupied - 2;
        assert!(
            !port_range_available("127.0.0.1", base, 5, false),
            "a live listener at {occupied} must reject the range from {base}"
        );
        drop(listener);
    }

    #[test]
    fn an_occupied_bus_port_rejects_the_range_too() {
        // The failure that a client-only check would miss. The client ports
        // are all free; the bus port one of them derives is not.
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let occupied_bus = listener.local_addr().unwrap().port();

        let Some(base) = occupied_bus.checked_sub(BUS_OFFSET) else {
            // No client port derives this bus port, so there is nothing to
            // assert. Ephemeral ports are well above the offset in practice.
            drop(listener);
            return;
        };

        assert!(
            !port_range_available("127.0.0.1", base, 1, true),
            "a live listener on bus port {occupied_bus} must reject base {base}"
        );

        // The same range is fine when bus ports are not derived, which is what
        // an explicit cluster-port means. Conditional because `base` is
        // wherever the OS put the listener minus 10000, and this makes no
        // claim about what else on the machine might be sitting there.
        if port_available("127.0.0.1", base) {
            assert!(
                port_range_available("127.0.0.1", base, 1, false),
                "with bus ports out of the picture, the free client port \
                 {base} should read as available"
            );
        }
        drop(listener);
    }

    #[test]
    fn a_free_range_reads_as_available() {
        with_free_port(
            |port| port_range_available("127.0.0.1", port, 1, false),
            "a range of one free port must read as available",
        );
    }

    #[test]
    fn ensure_reports_the_occupied_port_and_role() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

        let err = ensure_ports_available("127.0.0.1", [(port, PortRole::ClusterNode)])
            .expect_err("a port with a live listener must be reported");

        match err {
            Error::PortInUse {
                port: reported,
                ref role,
                ..
            } => {
                assert_eq!(reported, port);
                assert_eq!(role, "cluster node port");
            }
            other => panic!("unexpected error: {other}"),
        }
        drop(listener);
    }

    #[test]
    fn ensure_passes_when_every_port_is_free() {
        with_free_port(
            |port| ensure_ports_available("127.0.0.1", [(port, PortRole::Server)]).is_ok(),
            "a free port must not be reported as in use",
        );
    }

    #[test]
    fn ensure_reports_the_first_conflict_in_order() {
        let a = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let b = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let (pa, pb) = (
            a.local_addr().unwrap().port(),
            b.local_addr().unwrap().port(),
        );

        let err = ensure_ports_available(
            "127.0.0.1",
            [(pa, PortRole::ClusterNode), (pb, PortRole::ClusterBus)],
        )
        .expect_err("must report a conflict");
        assert!(matches!(err, Error::PortInUse { port, .. } if port == pa));

        drop(a);
        drop(b);
    }

    #[test]
    fn reserved_port_is_free_when_returned() {
        // The reservation must be released rather than held, and the released
        // port must be bindable, which is what the caller's process needs.
        for attempt in 0..8 {
            let port = reserve_ephemeral_port().expect("the OS should hand out a port");
            assert_ne!(port, 0, "a reserved port must be concrete");

            if port_available("127.0.0.1", port) && TcpListener::bind(("127.0.0.1", port)).is_ok() {
                return;
            }
            assert!(attempt < 7, "the reservation must be released, not held");
        }
    }

    #[test]
    fn a_held_port_is_not_handed_out_again() {
        // Hold the listener rather than reserving and re-binding. Reserving a
        // port, releasing it, and binding it again races every other test in
        // this process that is also asking the OS for ephemeral ports, which
        // is exactly the race `reserve_ephemeral_port` documents.
        let held = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let held_port = held.local_addr().unwrap().port();

        for _ in 0..16 {
            assert_ne!(
                reserve_ephemeral_port().unwrap(),
                held_port,
                "a port with a live listener must not be offered as free"
            );
        }
    }

    #[test]
    fn bus_port_is_client_port_plus_ten_thousand() {
        assert_eq!(bus_port(7000), Some(17000));
        assert_eq!(bus_port(6379), Some(16379));
    }

    #[test]
    fn bus_port_rejects_overflow() {
        assert_eq!(bus_port(60000), None);
        assert_eq!(bus_port(u16::MAX), None);
    }
}
