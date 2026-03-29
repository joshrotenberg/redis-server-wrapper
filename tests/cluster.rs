use redis_server_wrapper::RedisCluster;

#[test]
fn cluster_start_and_health() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17000)
        .start()
        .expect("failed to start redis cluster");

    assert!(cluster.all_alive());
    cluster
        .wait_for_healthy(std::time::Duration::from_secs(30))
        .expect("cluster did not become healthy");
    assert!(cluster.is_healthy());
    assert_eq!(cluster.node_addrs().len(), 3);
    assert_eq!(cluster.addr(), "127.0.0.1:17000");
}
