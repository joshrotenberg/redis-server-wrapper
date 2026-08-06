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
