#![cfg(feature = "blocking")]

use redis_server_wrapper::blocking::chaos::{ClientKillFilter, ClientType};
use redis_server_wrapper::blocking::{RedisCluster, RedisServer, chaos};
use std::time::Duration;

#[cfg_attr(not(unix), ignore)]
#[test]
fn freeze_and_resume_node() {
    let server = RedisServer::new()
        .port(17800)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());

    chaos::freeze_node(&server).expect("freeze_node failed");
    std::thread::sleep(Duration::from_millis(200));

    chaos::resume_node(&server).expect("resume_node failed");
    std::thread::sleep(Duration::from_millis(200));

    let pong = server.run(&["PING"]).expect("PING failed after resume");
    assert_eq!(pong.trim(), "PONG");
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn pause_node_resumes_automatically() {
    let server = RedisServer::new()
        .port(17801)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());

    chaos::pause_node(&server, Duration::from_millis(300)).expect("pause_node failed");

    std::thread::sleep(Duration::from_millis(600));

    assert!(server.is_alive());
}

#[test]
fn fill_memory_writes_keys() {
    let server = RedisServer::new()
        .port(17802)
        .start()
        .expect("failed to start server");

    chaos::fill_memory(&server, "k:", 50).expect("fill_memory failed");

    let dbsize = server.run(&["DBSIZE"]).expect("DBSIZE failed");
    let dbsize: u64 = dbsize
        .trim()
        .parse()
        .expect("DBSIZE did not return an integer");
    assert_eq!(dbsize, 50);
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn partition_and_recover_cluster() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17820)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let frozen = chaos::partition(&cluster, &[0]).expect("partition failed");
    assert!(!frozen.is_empty());
    assert_eq!(frozen.len(), cluster.node_addrs().len() - 1);

    chaos::recover(&cluster).expect("recover failed");

    let pong = cluster
        .node_run(0, &["PING"])
        .expect("node 0 did not respond after recover");
    assert_eq!(pong.trim(), "PONG");
}

#[test]
fn cluster_wait_for_all_healthy_and_cluster_info() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17950)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");
    cluster
        .wait_for_all_healthy(Duration::from_secs(30))
        .expect("cluster did not converge to all-nodes-healthy");

    for index in 0..cluster.node_addrs().len() {
        let info = cluster.cluster_info(index).expect("cluster_info failed");
        assert_eq!(info.get("cluster_state").map(String::as_str), Some("ok"));
    }
}

#[test]
fn chaos_slot_helpers_and_key_count() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17960)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "count-key").expect("keyslot failed");
    assert!(slot < 16384);

    let owner_port = chaos::slot_owner(&cluster, slot).expect("slot_owner failed");
    assert!(
        cluster
            .node_addrs()
            .iter()
            .any(|a| a.ends_with(&format!(":{owner_port}")))
    );

    cluster
        .node_run(
            cluster
                .node_addrs()
                .iter()
                .position(|a| a.ends_with(&format!(":{owner_port}")))
                .expect("owner port not found among cluster nodes"),
            &["SET", "count-key", "count-value"],
        )
        .expect("SET on slot owner failed");

    let count = chaos::count_keys_in_slot(&cluster, slot).expect("count_keys_in_slot failed");
    assert_eq!(count, 1);
}

#[test]
fn chaos_wait_for_slot_owner_change_after_failover() {
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17970)
        .start()
        .expect("failed to start cluster");

    cluster
        .wait_for_healthy(Duration::from_secs(30))
        .expect("cluster did not become healthy");

    let slot = chaos::keyslot(&cluster, "failover-slot-key").expect("keyslot failed");
    let old_owner_port = chaos::slot_owner(&cluster, slot).expect("slot_owner failed");

    let masters = cluster.num_masters() as usize;
    let total = cluster.node_addrs().len();
    let mut replica_index = None;
    for index in masters..total {
        let repl_info = cluster
            .node_run(index, &["INFO", "replication"])
            .expect("INFO replication failed");
        let master_port_line = repl_info
            .lines()
            .find(|l| l.starts_with("master_port:"))
            .map(|l| l.trim_end_matches('\r'));
        if master_port_line == Some(&format!("master_port:{old_owner_port}")) {
            replica_index = Some(index);
            break;
        }
    }
    let replica_index = replica_index.expect("no replica found for the slot's current owner");

    let replica_addr = &cluster.node_addrs()[replica_index];
    let replica_port: u16 = replica_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("could not parse replica port");

    cluster
        .node_run(replica_index, &["CLUSTER", "FAILOVER"])
        .expect("trigger_failover failed");

    let new_owner_port =
        chaos::wait_for_slot_owner_change(&cluster, slot, old_owner_port, Duration::from_secs(30))
            .expect("wait_for_slot_owner_change failed");

    assert_ne!(new_owner_port, old_owner_port);
    assert_eq!(new_owner_port, replica_port);
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn restart_node_after_kill() {
    let mut server = RedisServer::new()
        .port(18100)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());
    let old_pid = server.pid();

    chaos::kill_node(&server).expect("kill_node failed");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server.is_alive() {
        assert!(
            std::time::Instant::now() < deadline,
            "killed node never stopped responding"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    chaos::restart_node(&mut server, Duration::from_secs(10)).expect("restart_node failed");

    assert!(server.is_alive());
    assert_ne!(server.pid(), old_pid);
}

#[test]
fn kill_client_connections_by_type() {
    use std::io::Read;

    let server = RedisServer::new()
        .port(18110)
        .start()
        .expect("failed to start server");

    let mut extra =
        std::net::TcpStream::connect((server.host(), server.port())).expect("connect failed");
    extra
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set_read_timeout failed");

    let killed =
        chaos::kill_client_connections(&server, ClientKillFilter::of_type(ClientType::Normal))
            .expect("kill_client_connections failed");
    assert!(killed >= 1, "expected at least one client killed");

    let mut buf = [0u8; 8];
    let n = extra.read(&mut buf).expect("read failed");
    assert_eq!(n, 0, "expected the killed connection to observe EOF");
}

#[test]
fn break_persistence_rejects_writes_then_restores() {
    let server = RedisServer::new()
        .port(18120)
        .save(true)
        .start()
        .expect("failed to start server");

    let guard = match chaos::break_persistence(&server, Duration::from_secs(10)) {
        Ok(guard) => guard,
        Err(redis_server_wrapper::Error::PrivilegeRequired { message }) => {
            eprintln!("skipping break_persistence_rejects_writes_then_restores: {message}");
            return;
        }
        Err(e) => panic!("break_persistence failed: {e}"),
    };

    let result = server.run(&["SET", "k", "v"]);
    let saw_miscount = match &result {
        Err(e) => e.to_string().to_uppercase().contains("MISCONF"),
        Ok(s) => s.to_uppercase().contains("MISCONF"),
    };
    assert!(saw_miscount, "expected a MISCONF error, got: {result:?}");

    guard
        .restore(Duration::from_secs(10))
        .expect("restore failed");

    server
        .run(&["SET", "k", "v"])
        .expect("SET after restore failed");
}

#[cfg_attr(not(unix), ignore)]
#[test]
fn flap_node_cycles_and_leaves_running_on_drop() {
    let server = RedisServer::new()
        .port(18130)
        .start()
        .expect("failed to start server");

    assert!(server.is_alive());

    let down = Duration::from_millis(300);
    let up = Duration::from_millis(150);
    let guard = chaos::flap_node(&server, down, up).expect("flap_node failed");

    // `run` has no internal timeout (unlike `is_alive`), so a call that
    // starts while the node is `SIGSTOP`ped simply waits in the kernel
    // accept queue until the node resumes and answers: its elapsed time
    // reveals whether it caught a down window. Repeat until one call takes
    // a large enough chunk of the down window to prove it did.
    let mut saw_down = false;
    let test_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < test_deadline {
        let start = std::time::Instant::now();
        let _ = server.run(&["PING"]);
        if start.elapsed() >= down / 2 {
            saw_down = true;
            break;
        }
    }
    assert!(
        saw_down,
        "expected to observe at least one down window while flapping"
    );

    drop(guard);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if server.is_alive() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node did not resume running after dropping FlapGuard"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
