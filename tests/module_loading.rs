//! Module loading against a real compiled Redis module.
//!
//! The builder surface for `loadmodule` was only ever covered by config-render
//! assertions, which prove a directive was written and nothing about whether
//! Redis acted on it. These tests load an actual module and check `MODULE
//! LIST` on every node of every topology.
//!
//! They need a compiled module, which is not something `cargo test` can
//! produce on its own, so they skip when `REDIS_TEST_MODULE` is unset. Build
//! the fixture in `tests/support/rsw_test_module.c` and point the variable at
//! it:
//!
//! ```console
//! cc -fPIC -shared -I<redis-src>/src tests/support/rsw_test_module.c \
//!    -o /tmp/rsw_test_module.so
//! REDIS_TEST_MODULE=/tmp/rsw_test_module.so cargo test --all-features
//! ```
//!
//! CI builds it against each Redis version in the compatibility matrix.

use redis_server_wrapper::{Error, RedisCluster, RedisSentinel, RedisServer};

/// The name the fixture registers with `RedisModule_Init`. Deliberately
/// unlike its filename, since that mismatch is the usual reason a module check
/// fails.
const MODULE_NAME: &str = "rsw_test_module";

/// Path to the compiled fixture, or `None` when the suite should skip.
fn module_path() -> Option<String> {
    match std::env::var("REDIS_TEST_MODULE") {
        Ok(path) if !path.trim().is_empty() => {
            assert!(
                std::path::Path::new(&path).exists(),
                "REDIS_TEST_MODULE points at {path}, which does not exist"
            );
            Some(path)
        }
        _ => {
            eprintln!("skipping: REDIS_TEST_MODULE is not set");
            None
        }
    }
}

#[tokio::test]
async fn standalone_loads_a_module() {
    let Some(module) = module_path() else { return };

    let server = RedisServer::new()
        .port(19100)
        .loadmodule(&module)
        .start()
        .await
        .expect("server with a module should start");

    server
        .require_module(MODULE_NAME)
        .await
        .expect("module should be loaded");

    // Listed is not the same as working: prove the module is actually serving.
    let echoed = server
        .run(&["RSW.ECHO", "hello"])
        .await
        .expect("the module's command should be callable");
    assert_eq!(echoed.trim(), "hello");
}

#[tokio::test]
async fn module_metadata_is_reported() {
    let Some(module) = module_path() else { return };

    let server = RedisServer::new()
        .port(19101)
        .loadmodule(&module)
        .start()
        .await
        .expect("failed to start");

    let modules = server.modules().await.expect("MODULE LIST failed");
    let found = modules
        .iter()
        .find(|m| m.name == MODULE_NAME)
        .expect("fixture module missing from MODULE LIST");

    assert_eq!(found.version, Some(1));
    assert_eq!(found.path, module, "path should be what Redis loaded");
}

#[tokio::test]
async fn module_load_time_arguments_reach_the_module() {
    let Some(module) = module_path() else { return };

    // An argument containing a space is the case that silently became two
    // arguments before config values were encoded as single tokens.
    let server = RedisServer::new()
        .port(19102)
        .loadmodule_with_args(&module, ["alpha", "two words"])
        .start()
        .await
        .expect("failed to start with module arguments");

    let modules = server.modules().await.expect("MODULE LIST failed");
    let found = modules
        .iter()
        .find(|m| m.name == MODULE_NAME)
        .expect("fixture module missing");

    assert_eq!(
        found.args,
        vec!["alpha", "two words"],
        "an argument with a space must arrive as one argument"
    );
}

#[tokio::test]
async fn missing_module_reports_what_is_loaded() {
    let Some(module) = module_path() else { return };

    let server = RedisServer::new()
        .port(19103)
        .loadmodule(&module)
        .start()
        .await
        .expect("failed to start");

    let err = server
        .require_module("not_a_real_module")
        .await
        .expect_err("a module that is not loaded must be an error");

    match err {
        Error::ModuleNotLoaded { name, loaded, .. } => {
            assert_eq!(name, "not_a_real_module");
            assert!(
                loaded.iter().any(|m| m == MODULE_NAME),
                "the error should list what is loaded, got: {loaded:?}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn every_cluster_node_loads_the_module() {
    let Some(module) = module_path() else { return };

    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(19110)
        .loadmodule(&module)
        .start()
        .await
        .expect("cluster with a module should start");

    cluster
        .require_module_on_all_nodes(MODULE_NAME)
        .await
        .expect("every node should have the module");

    assert_eq!(cluster.nodes().len(), 6);
}

#[tokio::test]
async fn every_sentinel_data_node_loads_the_module() {
    let Some(module) = module_path() else { return };

    let sentinel = RedisSentinel::builder()
        .master_port(19130)
        .replica_base_port(19131)
        .sentinel_base_port(29130)
        .replicas(2)
        .sentinels(3)
        .quorum(2)
        .loadmodule(&module)
        .start()
        .await
        .expect("sentinel topology with a module should start");

    sentinel
        .require_module_on_data_nodes(MODULE_NAME)
        .await
        .expect("master and every replica should have the module");
}
