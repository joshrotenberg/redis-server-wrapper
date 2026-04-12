//! Redis Stack server detection and module loading.
//!
//! This module handles auto-detection of the `redis-stack-server` binary and
//! the Stack modules (RedisJSON, RediSearch, etc.) bundled with it.

use std::path::Path;

/// Detect the best redis-server binary.
///
/// Prefers the real `redis-server` binary inside a redis-stack-server
/// Homebrew cask (not the wrapper script which overrides `--dir`), then
/// falls back to `redis-server` on PATH.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::stack::detect_server_bin;
///
/// let bin = detect_server_bin();
/// println!("using redis-server binary: {bin}");
/// ```
pub fn detect_server_bin() -> String {
    let patterns = [
        "/opt/homebrew/Caskroom/redis-stack-server/*/bin/redis-server",
        "/usr/local/Caskroom/redis-stack-server/*/bin/redis-server",
    ];

    for pattern in &patterns {
        if let Ok(mut paths) = glob::glob(pattern)
            && let Some(Ok(path)) = paths.next()
            && let Some(s) = path.to_str()
        {
            return s.to_string();
        }
    }

    "redis-server".to_string()
}

/// Detect Redis Stack modules next to the given binary.
///
/// If the binary lives inside a redis-stack installation (a sibling `lib/`
/// directory exists), returns `--loadmodule` arguments for each discovered
/// module. Returns an empty vec if no modules are found.
///
/// Modules are checked in this order:
/// - `rediscompat.so`
/// - `redisearch.so` (with `MAXSEARCHRESULTS 10000 MAXAGGREGATERESULTS 10000`)
/// - `redistimeseries.so`
/// - `rejson.so`
/// - `redisbloom.so`
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::stack::{detect_server_bin, detect_stack_modules};
///
/// let bin = detect_server_bin();
/// let module_args = detect_stack_modules(&bin);
/// println!("module args: {module_args:?}");
/// ```
pub fn detect_stack_modules(server_bin: &str) -> Vec<String> {
    let bin_path = Path::new(server_bin);

    let lib_dir = match bin_path.parent().and_then(|p| p.parent()) {
        Some(base) => base.join("lib"),
        None => return Vec::new(),
    };

    if !lib_dir.is_dir() {
        return Vec::new();
    }

    // (filename, extra args)
    let modules: &[(&str, &[&str])] = &[
        ("rediscompat.so", &[]),
        (
            "redisearch.so",
            &["MAXSEARCHRESULTS", "10000", "MAXAGGREGATERESULTS", "10000"],
        ),
        ("redistimeseries.so", &[]),
        ("rejson.so", &[]),
        ("redisbloom.so", &[]),
    ];

    let mut args: Vec<String> = Vec::new();

    for (filename, extra) in modules {
        let module_path = lib_dir.join(filename);
        if module_path.is_file() {
            args.push("--loadmodule".to_string());
            args.push(module_path.to_string_lossy().into_owned());
            for arg in *extra {
                args.push(arg.to_string());
            }
        }
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_server_bin_fallback() {
        // Without a real redis-stack installation the function must fall back
        // to the plain binary name.
        let bin = detect_server_bin();
        // Either the fallback or a real path -- just make sure it's non-empty.
        assert!(!bin.is_empty());
    }

    #[test]
    fn detect_stack_modules_missing_lib() {
        // A binary that has no sibling lib/ dir should produce no module args.
        let args = detect_stack_modules("redis-server");
        assert!(args.is_empty());
    }

    #[test]
    fn detect_stack_modules_nonexistent_path() {
        let args = detect_stack_modules("/nonexistent/bin/redis-server");
        assert!(args.is_empty());
    }
}
