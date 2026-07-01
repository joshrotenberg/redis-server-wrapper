use redis_server_wrapper::RedisCluster;
use redis_server_wrapper::chaos::migrate_slot;

#[tokio::test]
async fn migrate_slot_moves_keys_and_ownership() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17040)
        .start()
        .await
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .await
        .expect("cluster did not become healthy");

    let key = "reshard-test-key";
    let slot: u16 = cluster
        .cli()
        .run(&["CLUSTER", "KEYSLOT", key])
        .await
        .expect("CLUSTER KEYSLOT failed")
        .trim()
        .parse()
        .expect("could not parse CLUSTER KEYSLOT response");

    cluster
        .cli()
        .cluster_mode(true)
        .run(&["SET", key, "reshard-test-value"])
        .await
        .expect("SET failed");

    // Find which master currently owns the slot.
    let mut from_index = None;
    for (i, node) in cluster.master_nodes().iter().enumerate() {
        let count = node
            .run(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()])
            .await
            .expect("CLUSTER COUNTKEYSINSLOT failed");
        if count.trim() == "1" {
            from_index = Some(i);
            break;
        }
    }
    let from_index = from_index.expect("no master owns the test key's slot");
    let to_index = (from_index + 1) % cluster.master_nodes().len();

    let from = cluster.node(from_index);
    let to = cluster.node(to_index);

    let moved = migrate_slot(&cluster, slot, from, to)
        .await
        .expect("migrate_slot failed");
    assert_eq!(moved, 1);

    // The key data landed on the target node.
    let value = to
        .run(&["GET", key])
        .await
        .expect("GET on target node failed");
    assert!(value.contains("reshard-test-value"));

    // Slot ownership moved: the source no longer counts it, the target does.
    let from_count = from
        .run(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()])
        .await
        .expect("CLUSTER COUNTKEYSINSLOT on source failed");
    assert_eq!(from_count.trim(), "0");

    // A cluster-mode client following redirects reads the value from its new home.
    let redirected = cluster
        .cli()
        .cluster_mode(true)
        .run(&["GET", key])
        .await
        .expect("GET through cluster-mode client failed");
    assert!(redirected.contains("reshard-test-value"));
}
