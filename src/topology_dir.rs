//! Where a cluster or Sentinel topology keeps its working directory.
//!
//! The directory name has to be stable across runs, because it is the only
//! place a previous run's pidfiles can be found. Both builders used to name it
//! with a pid and a timestamp, which guaranteed a fresh directory every time
//! and so guaranteed the wrapper could never recognise anything it had started
//! before. That is why the port cleanup those builders once did could not be
//! ownership-aware, and why removing it in the fail-closed change left
//! orphaned nodes with no way to be reclaimed.
//!
//! Stable does not mean shared. The name encodes the topology's shape, so two
//! topologies that could actually run at the same time get different
//! directories, while the same topology started twice gets the same one.

use std::path::PathBuf;

/// Build a stable directory name for a topology.
///
/// `kind` separates clusters from Sentinel topologies. `bind` and `parts`
/// carry the shape: any two topologies that differ in address or ports get
/// different directories, and two that agree on both cannot run concurrently
/// anyway, since they would want the same ports.
pub(crate) fn topology_dir(kind: &str, bind: &str, parts: &[u16]) -> PathBuf {
    let mut name = format!("redis-{kind}-wrapper-{}", sanitize(bind));
    for part in parts {
        name.push('-');
        name.push_str(&part.to_string());
    }
    std::env::temp_dir().join(name)
}

/// Reduce a bind address to characters that are safe in a path component.
///
/// IPv6 addresses carry colons and brackets, and a hostname could carry
/// anything a resolver accepts. Collapsing everything outside a small safe set
/// keeps the name a single path component on every platform.
fn sanitize(bind: &str) -> String {
    let mapped: String = bind
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    // Collapse runs so `[::1]` does not become a row of separators.
    let mut out = String::with_capacity(mapped.len());
    let mut last_dash = false;
    for c in mapped.chars() {
        if c == '-' {
            if !last_dash {
                out.push(c);
            }
            last_dash = true;
        } else {
            out.push(c);
            last_dash = false;
        }
    }
    let trimmed = out.trim_matches('-');
    // A bind of "::" sanitizes to nothing, which would leave the name with a
    // hole in it rather than a component.
    if trimmed.is_empty() {
        "any".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_topology_maps_to_the_same_directory() {
        // The property the whole reclaim path depends on.
        let a = topology_dir("cluster", "127.0.0.1", &[7000, 3]);
        let b = topology_dir("cluster", "127.0.0.1", &[7000, 3]);
        assert_eq!(a, b);
    }

    #[test]
    fn different_ports_map_to_different_directories() {
        let a = topology_dir("cluster", "127.0.0.1", &[7000, 3]);
        let b = topology_dir("cluster", "127.0.0.1", &[7100, 3]);
        assert_ne!(a, b);
    }

    #[test]
    fn different_shapes_on_the_same_base_port_differ() {
        let a = topology_dir("cluster", "127.0.0.1", &[7000, 3]);
        let b = topology_dir("cluster", "127.0.0.1", &[7000, 6]);
        assert_ne!(a, b);
    }

    #[test]
    fn different_binds_map_to_different_directories() {
        let a = topology_dir("cluster", "127.0.0.1", &[7000, 3]);
        let b = topology_dir("cluster", "0.0.0.0", &[7000, 3]);
        assert_ne!(a, b);
    }

    #[test]
    fn clusters_and_sentinels_do_not_share_a_directory() {
        let a = topology_dir("cluster", "127.0.0.1", &[7000]);
        let b = topology_dir("sentinel", "127.0.0.1", &[7000]);
        assert_ne!(a, b);
    }

    #[test]
    fn the_name_is_a_single_path_component() {
        for bind in ["127.0.0.1", "::1", "[::1]", "localhost", "0.0.0.0"] {
            let dir = topology_dir("cluster", bind, &[7000, 3]);
            let name = dir.file_name().expect("must have a file name");
            assert_eq!(
                PathBuf::from(name).components().count(),
                1,
                "{bind} produced a name that is not one component"
            );
        }
    }

    #[test]
    fn ipv6_binds_do_not_collapse_into_each_other() {
        // `::1` and `[::1]` are the same address written differently, so
        // sharing a directory is correct. A different address must not.
        assert_eq!(
            topology_dir("cluster", "::1", &[7000]),
            topology_dir("cluster", "[::1]", &[7000])
        );
        assert_ne!(
            topology_dir("cluster", "::1", &[7000]),
            topology_dir("cluster", "::2", &[7000])
        );
    }

    #[test]
    fn sanitize_collapses_runs_and_trims() {
        assert_eq!(sanitize("127.0.0.1"), "127-0-0-1");
        assert_eq!(sanitize("[::1]"), "1");
        assert_eq!(sanitize("localhost"), "localhost");
        assert_eq!(
            sanitize("::"),
            "any",
            "an all-separator bind still needs a name"
        );
    }
}
