use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::{Parser, Subcommand};
use redis_server_wrapper::{RedisCluster, RedisSentinel, RedisServer};

#[derive(Parser)]
#[command(
    name = "redis-run",
    about = "Run Redis standalone, cluster, or sentinel topologies"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a standalone Redis server
    Standalone {
        /// Port to listen on
        #[arg(short, long, default_value_t = 6379)]
        port: u16,

        /// Address to bind to
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,
    },

    /// Start a Redis Cluster
    Cluster {
        /// Number of master nodes
        #[arg(short, long, default_value_t = 3)]
        masters: u16,

        /// Number of replicas per master
        #[arg(short, long, default_value_t = 0)]
        replicas_per_master: u16,

        /// Starting port for cluster nodes
        #[arg(long, default_value_t = 7000)]
        base_port: u16,

        /// Address to bind to
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,
    },

    /// Start a Redis Sentinel topology
    Sentinel {
        /// Name for the monitored master
        #[arg(long, default_value = "mymaster")]
        master_name: String,

        /// Port for the master node
        #[arg(long, default_value_t = 6390)]
        master_port: u16,

        /// Number of replica nodes
        #[arg(short, long, default_value_t = 2)]
        replicas: u16,

        /// Starting port for replicas
        #[arg(long, default_value_t = 6391)]
        replica_base_port: u16,

        /// Number of sentinel processes
        #[arg(short, long, default_value_t = 3)]
        sentinels: u16,

        /// Starting port for sentinels
        #[arg(long, default_value_t = 26389)]
        sentinel_base_port: u16,

        /// Quorum count for failover decisions
        #[arg(short, long, default_value_t = 2)]
        quorum: u16,

        /// Address to bind to
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,
    },
}

fn wait_for_ctrl_c() {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("failed to set ctrl-c handler");

    println!("Press Ctrl-C to shut down...");
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(100));
    }
    println!("Shutting down...");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Standalone { port, bind } => {
            let server = RedisServer::new().port(port).bind(&bind).start().await?;
            server.wait_for_ready(Duration::from_secs(10)).await?;
            println!("Standalone Redis server running at {}", server.addr());
            wait_for_ctrl_c();
            drop(server);
        }

        Command::Cluster {
            masters,
            replicas_per_master,
            base_port,
            bind,
        } => {
            let cluster = RedisCluster::builder()
                .masters(masters)
                .replicas_per_master(replicas_per_master)
                .base_port(base_port)
                .bind(&bind)
                .start()
                .await?;
            cluster.wait_for_healthy(Duration::from_secs(30)).await?;
            println!("Redis Cluster running:");
            for addr in cluster.node_addrs() {
                println!("  - {addr}");
            }
            wait_for_ctrl_c();
            drop(cluster);
        }

        Command::Sentinel {
            master_name,
            master_port,
            replicas,
            replica_base_port,
            sentinels,
            sentinel_base_port,
            quorum,
            bind,
        } => {
            let sentinel = RedisSentinel::builder()
                .master_name(&master_name)
                .master_port(master_port)
                .replicas(replicas)
                .replica_base_port(replica_base_port)
                .sentinels(sentinels)
                .sentinel_base_port(sentinel_base_port)
                .quorum(quorum)
                .bind(&bind)
                .start()
                .await?;
            sentinel.wait_for_healthy(Duration::from_secs(30)).await?;
            println!(
                "Redis Sentinel topology running (master: {} at {})",
                master_name,
                sentinel.master_addr()
            );
            println!("Sentinels:");
            for addr in sentinel.sentinel_addrs() {
                println!("  - {addr}");
            }
            wait_for_ctrl_c();
            drop(sentinel);
        }
    }

    Ok(())
}
