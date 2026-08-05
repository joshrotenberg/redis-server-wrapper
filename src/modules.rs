//! Inspecting the Redis modules a running server has loaded.
//!
//! Loading a module through the builder only puts a `loadmodule` directive in
//! the config. Whether Redis actually loaded it is a separate question: a
//! wrong path, an ABI mismatch, or a module whose `RedisModule_OnLoad`
//! returns an error all leave a server running with the module absent. These
//! helpers answer that question against the live server.

use std::collections::HashMap;

/// A module reported by `MODULE LIST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleInfo {
    /// The name the module registered with `RedisModule_Init`, which is what
    /// `MODULE UNLOAD` and these helpers match on. It is not derived from the
    /// filename and often differs from it.
    pub name: String,
    /// The module's own version number, or `None` if Redis did not report one.
    pub version: Option<u64>,
    /// Path Redis loaded the module from. Empty for modules built into the
    /// server rather than loaded from disk.
    pub path: String,
    /// Load-time arguments the module was given.
    pub args: Vec<String>,
}

/// Parse the reply to `MODULE LIST` as redis-cli renders it by default.
///
/// redis-cli flattens the nested reply to one value per line, so the structure
/// has to be recovered from the key order Redis emits: `name`, `ver`, `path`,
/// then `args`. A record begins at each `name`, and because `args` is last and
/// variable length, its values run until the next `name` or the end of input.
///
/// Unknown keys are skipped, so a Redis version that reports more fields than
/// these still parses. Redis 6 reported only `name` and `ver`, which parses
/// too: the missing fields stay empty.
///
/// Two inputs would confuse this, both vanishingly unlikely and neither worth
/// a JSON dependency to rule out: a module argument whose value is literally
/// `name`, and one containing a newline.
pub fn parse_module_list(raw: &str) -> Vec<ModuleInfo> {
    let lines: Vec<&str> = raw.lines().collect();
    let mut modules: Vec<ModuleInfo> = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        match lines[i].trim() {
            "name" if i + 1 < lines.len() => {
                modules.push(ModuleInfo {
                    name: lines[i + 1].trim().to_string(),
                    version: None,
                    path: String::new(),
                    args: Vec::new(),
                });
                i += 2;
            }
            "ver" if i + 1 < lines.len() => {
                if let Some(m) = modules.last_mut() {
                    m.version = lines[i + 1].trim().parse().ok();
                }
                i += 2;
            }
            "path" if i + 1 < lines.len() => {
                if let Some(m) = modules.last_mut() {
                    m.path = lines[i + 1].trim().to_string();
                }
                i += 2;
            }
            "args" => {
                i += 1;
                let mut args = Vec::new();
                while i < lines.len() && lines[i].trim() != "name" {
                    let value = lines[i].trim();
                    // The empty line Redis emits for an empty argument vector
                    // is structure, not an argument.
                    if !value.is_empty() {
                        args.push(value.to_string());
                    }
                    i += 1;
                }
                if let Some(m) = modules.last_mut() {
                    m.args = args;
                }
            }
            _ => i += 1,
        }
    }

    modules
}

/// Index parsed modules by name, for callers checking several at once.
pub fn by_name(modules: Vec<ModuleInfo>) -> HashMap<String, ModuleInfo> {
    modules.into_iter().map(|m| (m.name.clone(), m)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured from `redis-cli MODULE LIST` against Redis 8.8.1 with one
    /// loaded module and one built in.
    const TWO_MODULES: &str = "name\n\
                               rsw_test_module\n\
                               ver\n\
                               1\n\
                               path\n\
                               /tmp/rsw_test_module.so\n\
                               args\n\
                               \n\
                               name\n\
                               vectorset\n\
                               ver\n\
                               1\n\
                               path\n\
                               \n\
                               args\n";

    /// Same, with the module loaded as `loadmodule <path> alpha "two words"`.
    const WITH_ARGS: &str = "name\n\
                             rsw_test_module\n\
                             ver\n\
                             1\n\
                             path\n\
                             /tmp/rsw_test_module.so\n\
                             args\n\
                             alpha\n\
                             two words\n\
                             name\n\
                             vectorset\n\
                             ver\n\
                             1\n\
                             path\n\
                             \n\
                             args\n";

    #[test]
    fn empty_input_yields_no_modules() {
        assert!(parse_module_list("").is_empty());
        assert!(parse_module_list("\n\n").is_empty());
    }

    #[test]
    fn parses_name_version_and_path() {
        let mods = parse_module_list(TWO_MODULES);
        assert_eq!(mods.len(), 2);

        assert_eq!(mods[0].name, "rsw_test_module");
        assert_eq!(mods[0].version, Some(1));
        assert_eq!(mods[0].path, "/tmp/rsw_test_module.so");
        assert!(mods[0].args.is_empty());
    }

    #[test]
    fn builtin_module_has_an_empty_path() {
        let mods = parse_module_list(TWO_MODULES);
        assert_eq!(mods[1].name, "vectorset");
        assert_eq!(mods[1].path, "");
        assert!(mods[1].args.is_empty());
    }

    #[test]
    fn parses_multi_valued_args_without_swallowing_the_next_module() {
        let mods = parse_module_list(WITH_ARGS);
        assert_eq!(mods.len(), 2, "args must not consume the next record");
        assert_eq!(mods[0].args, vec!["alpha", "two words"]);
        assert_eq!(mods[1].name, "vectorset");
    }

    #[test]
    fn tolerates_a_redis_6_reply_without_path_or_args() {
        let raw = "name\nold_module\nver\n2\n";
        let mods = parse_module_list(raw);
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].name, "old_module");
        assert_eq!(mods[0].version, Some(2));
        assert_eq!(mods[0].path, "");
    }

    #[test]
    fn skips_unknown_keys() {
        let raw = "name\nm\nver\n1\nfuture_field\nsomething\npath\n/p.so\nargs\n";
        let mods = parse_module_list(raw);
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].path, "/p.so");
    }

    #[test]
    fn indexes_by_name() {
        let index = by_name(parse_module_list(TWO_MODULES));
        assert!(index.contains_key("rsw_test_module"));
        assert!(index.contains_key("vectorset"));
        assert_eq!(index["rsw_test_module"].version, Some(1));
    }
}
