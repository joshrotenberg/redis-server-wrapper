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

use std::net::TcpListener;

use crate::error::{Error, Result};

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

/// Whether a port can be bound on `host`.
///
/// Rust sets `SO_REUSEADDR` on Unix, so a socket left in `TIME_WAIT` by a
/// process that has already exited does not read as occupied, while a live
/// listener does. That is the distinction we want: a port is "in use" only if
/// something is actually accepting on it.
///
/// A host that does not resolve reads as available. Binding is not this
/// function's job to diagnose, and the subsequent start will report the real
/// failure.
pub fn port_available(host: &str, port: u16) -> bool {
    match TcpListener::bind((host, port)) {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => false,
        Err(_) => true,
    }
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

    #[test]
    fn unbound_port_is_available() {
        // Bind and release to get a port nothing is listening on.
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(port_available("127.0.0.1", port));
    }

    #[test]
    fn bound_port_is_not_available() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(!port_available("127.0.0.1", port));
        drop(listener);
    }

    #[test]
    fn ensure_reports_the_occupied_port_and_role() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

        let err = ensure_ports_available("127.0.0.1", [(port, PortRole::ClusterNode)])
            .expect_err("an occupied port must be reported");

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
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(ensure_ports_available("127.0.0.1", [(port, PortRole::Server)]).is_ok());
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
