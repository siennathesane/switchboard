//! Switchboard: a multi-master AMQP 0-9-1 broker built on OpenRaft and
//! RocksDB. See `docs/architecture.md` for the cluster design.
//!
//! Coverage: `main` is process-entry glue (argument parsing + spawn) that
//! cannot execute in-process under llvm-cov; it is excluded from coverage
//! via `#[coverage(off)]` (active only on nightly coverage builds). All
//! library code behind it is covered by the integration suites.
//!
//! Coverage note: `main` is process-entry glue (CLI parsing + spawn) that
//! cannot execute in-process under llvm-cov; the integration suites cover
//! everything behind it.

// Coverage instrumentation of process-entry glue is meaningless: `main`
// cannot execute in-process under llvm-cov. The attribute activates only
// on nightly coverage builds (`--cfg=coverage_nightly`, set by
// cargo-llvm-cov).
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

use std::path::PathBuf;

use clap::Parser;
use switchboard_cluster::NodeConfig;
use switchboard_server::ConnectionLimits;

/// Command-line interface.
#[derive(Debug, Parser)]
#[command(name = "switchboard", version, about = "Multi-master AMQP 0-9-1 broker")]
pub struct Cli {
    /// Unique node id within the cluster (1..).
    #[arg(long, env = "SWITCHBOARD_NODE_ID", default_value_t = 1)]
    pub node_id: u64,

    /// Client-facing AMQP listen address.
    #[arg(long, env = "SWITCHBOARD_LISTEN", default_value = "0.0.0.0:5672")]
    pub listen: String,

    /// Internal cluster listen address (raft + forwarding).
    #[arg(long, env = "SWITCHBOARD_INTERNAL", default_value = "0.0.0.0:5673")]
    pub internal: String,

    /// The address this node advertises to peers for internal traffic.
    #[arg(long, env = "SWITCHBOARD_ADVERTISE")]
    pub advertise: Option<String>,

    /// Data directory (RocksDB).
    #[arg(long, env = "SWITCHBOARD_DATA", default_value = "./data")]
    pub data: PathBuf,

    /// Internal addresses of existing nodes to join through. Empty forms a
    /// new cluster (first node only).
    #[arg(long, env = "SWITCHBOARD_SEEDS", value_delimiter = ',')]
    pub seeds: Vec<String>,

    /// Form a brand-new cluster as its first node.
    #[arg(long, env = "SWITCHBOARD_BOOTSTRAP", default_value_t = false)]
    pub bootstrap: bool,

    /// Total nodes expected at formation (drives shard-group layout).
    #[arg(long, env = "SWITCHBOARD_EXPECTED_NODES", default_value_t = 1)]
    pub expected_nodes: u64,

    /// Formation peer: `--peer <id>=<internal-addr>` (repeatable).
    #[arg(long, env = "SWITCHBOARD_PEER", value_delimiter = ',')]
    pub peer: Vec<String>,

    /// TLS certificate chain (PEM). Enables TLS on the client listener.
    #[arg(long, env = "SWITCHBOARD_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// TLS private key (PEM).
    #[arg(long, env = "SWITCHBOARD_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Default user created at bootstrap.
    #[arg(long, env = "SWITCHBOARD_USER", default_value = "guest")]
    pub user: String,

    /// Password for the default user.
    #[arg(long, env = "SWITCHBOARD_PASSWORD", default_value = "guest")]
    pub password: String,

    /// Log filter (tracing env syntax).
    #[arg(long, env = "SWITCHBOARD_LOG", default_value = "info")]
    pub log: String,

    /// Allow the well-known `guest` user from non-loopback addresses
    /// (production should create real users instead).
    #[arg(long, env = "SWITCHBOARD_ALLOW_REMOTE_GUEST", default_value_t = false)]
    pub allow_remote_guest: bool,

    /// Seconds to spend announcing departure on SIGTERM before exiting
    /// anyway. The controller's dead-node reaper heals membership
    /// regardless, so a leaderless meta group must not park the pod in
    /// Terminating.
    #[arg(long, env = "SWITCHBOARD_LEAVE_TIMEOUT_SECS", default_value_t = 10)]
    pub leave_timeout_secs: u64,

    /// Client protocols on the gateway port, comma-separated:
    /// amqp,amqp1,mqtt,stomp,ws,http (default: all; `amqp` cannot be off).
    #[arg(long, env = "SWITCHBOARD_PROTOCOLS", default_value = "amqp,amqp1,mqtt,stomp,ws,http")]
    pub protocols: String,

    /// DNS seed hostname whose A/AAAA records list cluster members
    /// (`host[:port]`, default port 5673). Repeatable / comma-separated.
    #[arg(long, env = "SWITCHBOARD_DNS_SEEDS", value_delimiter = ',')]
    pub dns_seed: Vec<String>,

    /// Domain for `_switchboard._tcp.<domain>` SRV discovery. Repeatable.
    #[arg(long, env = "SWITCHBOARD_DNS_SRV", value_delimiter = ',')]
    pub dns_srv: Vec<String>,

    /// Advertise and discover peers on the local network over mDNS
    /// (`_switchboard._tcp.local.`).
    #[arg(long, env = "SWITCHBOARD_MDNS", default_value_t = false)]
    pub mdns: bool,

    /// mDNS instance name (defaults to `switchboard-node-<id>`).
    #[arg(long, env = "SWITCHBOARD_MDNS_INSTANCE")]
    pub mdns_instance: Option<String>,

    /// Discovery re-resolution interval in seconds.
    #[arg(long, env = "SWITCHBOARD_DISCOVERY_INTERVAL", default_value_t = 30)]
    pub discovery_interval: u64,

    // ---- Timeouts: every wait in the broker, all configurable. Defaults
    // bound any single operation's worst case to five seconds. ----
    /// Retry budget for one logical write (elections, forwards): ms.
    #[arg(long, env = "SWITCHBOARD_WRITE_BUDGET_MS", default_value_t = 5000)]
    pub write_budget_ms: u64,

    /// Retry budget for one fanout publish (leader resolution + legs): ms.
    #[arg(long, env = "SWITCHBOARD_FANOUT_BUDGET_MS", default_value_t = 5000)]
    pub fanout_budget_ms: u64,

    /// One internal RPC's reply wait (raft, forwards, admin): ms.
    #[arg(long, env = "SWITCHBOARD_RPC_REPLY_BUDGET_MS", default_value_t = 5000)]
    pub rpc_reply_budget_ms: u64,

    /// Retry budget for a membership reconfiguration: ms.
    #[arg(long, env = "SWITCHBOARD_RECONFIGURE_BUDGET_MS", default_value_t = 5000)]
    pub reconfigure_budget_ms: u64,

    /// Retry budget for joining an existing cluster at startup: ms.
    #[arg(long, env = "SWITCHBOARD_JOIN_BUDGET_MS", default_value_t = 5000)]
    pub join_budget_ms: u64,

    /// Raft heartbeat interval: ms.
    #[arg(long, env = "SWITCHBOARD_RAFT_HEARTBEAT_MS", default_value_t = 100)]
    pub raft_heartbeat_ms: u64,

    /// Raft election timeout lower bound: ms (clamped to ≥ 2× heartbeat).
    #[arg(long, env = "SWITCHBOARD_RAFT_ELECTION_MIN_MS", default_value_t = 300)]
    pub raft_election_min_ms: u64,

    /// Raft election timeout upper bound: ms (clamped to ≥ 2× min).
    #[arg(long, env = "SWITCHBOARD_RAFT_ELECTION_MAX_MS", default_value_t = 600)]
    pub raft_election_max_ms: u64,

    /// Reconciliation (topology refresh) tick: ms.
    #[arg(long, env = "SWITCHBOARD_RECONCILE_INTERVAL_MS", default_value_t = 500)]
    pub reconcile_interval_ms: u64,

    /// Liveness-probe (consumer janitor) tick: ms.
    #[arg(long, env = "SWITCHBOARD_JANITOR_INTERVAL_MS", default_value_t = 1000)]
    pub janitor_interval_ms: u64,

    /// Consecutive refused liveness probes that declare a peer dead.
    #[arg(long, env = "SWITCHBOARD_JANITOR_DEAD_AFTER", default_value_t = 3)]
    pub janitor_dead_after: u32,

    /// Consecutive refused controller probes before a registered node is
    /// reaped from the directory (≈400 ms per probe).
    #[arg(long, env = "SWITCHBOARD_REAP_DEAD_AFTER", default_value_t = 25)]
    pub reap_dead_after: u32,

    /// Retries for a failed off-node delivery's Release write.
    #[arg(long, env = "SWITCHBOARD_DELIVER_RELEASE_RETRIES", default_value_t = 10)]
    pub deliver_release_retries: u32,

    /// Pause between failed off-node delivery Release retries: ms.
    #[arg(long, env = "SWITCHBOARD_DELIVER_RELEASE_INTERVAL_MS", default_value_t = 200)]
    pub deliver_release_interval_ms: u64,

    /// AMQP heartbeat offered to clients, in seconds (0 disables).
    #[arg(long, env = "SWITCHBOARD_HEARTBEAT", default_value_t = 60)]
    pub heartbeat: u16,
}

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_new(&cli.log).unwrap_or_default())
        .init();

    let advertise = cli.advertise.clone().unwrap_or_else(|| cli.internal.clone());
    // Formation peers: `--peer id=addr` (repeatable) seeds the directory so
    // the initial voters can reach each other before anything is registered.
    let mut peers = Vec::new();
    for spec in &cli.peer {
        if let Some((id, addr)) = spec.split_once('=') {
            peers.push((id.trim().parse::<u64>().expect("peer id must be u64"), addr.trim().to_string()));
        }
    }
    let timeouts = switchboard_cluster::node::Timeouts {
        write_budget: std::time::Duration::from_millis(cli.write_budget_ms),
        fanout_budget: std::time::Duration::from_millis(cli.fanout_budget_ms),
        rpc_reply_budget: std::time::Duration::from_millis(cli.rpc_reply_budget_ms),
        reconfigure_budget: std::time::Duration::from_millis(cli.reconfigure_budget_ms),
        join_budget: std::time::Duration::from_millis(cli.join_budget_ms),
        raft_heartbeat: std::time::Duration::from_millis(cli.raft_heartbeat_ms),
        raft_election_min: std::time::Duration::from_millis(cli.raft_election_min_ms),
        raft_election_max: std::time::Duration::from_millis(cli.raft_election_max_ms),
        reconcile_interval: std::time::Duration::from_millis(cli.reconcile_interval_ms),
        janitor_interval: std::time::Duration::from_millis(cli.janitor_interval_ms),
        janitor_dead_after: cli.janitor_dead_after,
        reap_dead_after: cli.reap_dead_after,
        deliver_release_retries: cli.deliver_release_retries,
        deliver_release_interval: std::time::Duration::from_millis(cli.deliver_release_interval_ms),
    };
    let cfg = NodeConfig {
        id: cli.node_id,
        data_dir: cli.data.clone(),
        client_addr: cli.listen.clone(),
        internal_addr: advertise.clone(),
        seeds: cli.seeds.clone(),
        bootstrap: cli.bootstrap,
        expected_nodes: cli.expected_nodes,
        peers,
        timeouts,
    };

    let node = switchboard_cluster::ClusterNode::start(cfg)
        .await
        .map_err(|e| anyhow::anyhow!("cluster start failed: {e}"))?;

    // Graceful departure on SIGTERM/SIGINT: announce ForgetNode through
    // meta so the directory, meta voter set, and group member lists
    // converge immediately instead of waiting out the dead-node reaper.
    // Orchestrators (k8s, systemd, docker stop) get a bounded window;
    // if the process is SIGKILLed anyway, the controller's reaper heals
    // the membership on its own.
    {
        let node2 = node.clone();
        let leave_budget = std::time::Duration::from_secs(cli.leave_timeout_secs);
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::signal;
                use tokio::signal::unix::SignalKind;
                let mut term = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let mut int = match signal(SignalKind::interrupt()) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                tokio::select! {
                    _ = term.recv() => tracing::info!("SIGTERM: leaving cluster"),
                    _ = int.recv() => tracing::info!("SIGINT: leaving cluster"),
                }
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await.ok();
            // Bounded graceful leave: announcing ForgetNode needs a meta
            // quorum, and a node that IS part of that quorum leaving
            // simultaneously with its peers can wait out the entire
            // election budget. The controller's dead-node reaper heals
            // membership regardless, so cap the ceremony and exit.
            match tokio::time::timeout(leave_budget, node2.leave()).await {
                Ok(Ok(())) => tracing::info!("left the cluster cleanly"),
                Ok(Err(e)) => tracing::warn!("leave errored: {e}; exiting anyway"),
                Err(_) => tracing::warn!(
                    "leave exceeded {leave_budget:?} (meta quorum unreachable?); exiting anyway"
                ),
            }
            std::process::exit(0);
        });
    }


    // Self-discovery: DNS seeds / SRV records / mDNS all feed the join
    // protocol, so clusters assemble without listing every peer by hand.
    let discovery_cfg = switchboard_cluster::discovery::DiscoveryConfig {
        dns_seeds: cli.dns_seed.clone(),
        dns_srv_domains: cli.dns_srv.clone(),
        mdns: cli.mdns,
        mdns_instance: cli.mdns_instance.clone(),
        interval: std::time::Duration::from_secs(cli.discovery_interval.max(1)),
    };
    {
        let node2 = node.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            // The sender MUST live as long as discovery: a dropped
            // sender resolves `changed()` immediately, which ended the
            // discovery loop on its first poll — every --dns-seed /
            // mDNS deployment silently never discovered anything.
            let _keep_alive = shutdown_tx;
            switchboard_cluster::discovery::run(node2, discovery_cfg, shutdown_rx).await;
        });
    }

    let limits = ConnectionLimits {
        heartbeat: cli.heartbeat,
        allow_remote_guest: cli.allow_remote_guest,
        ..ConnectionLimits::default()
    };
    let protocols = switchboard_server::ProtocolConfig::from_list(&cli.protocols)
        .map_err(|e| anyhow::anyhow!("bad --protocols: {e}"))?;

    let tls = match (cli.tls_cert.clone(), cli.tls_key.clone()) {
        (Some(cert), Some(key)) => {
            let id = switchboard_server::tls::TlsIdentity {
                cert_pem: std::fs::read(&cert)?,
                key_pem: std::fs::read(&key)?,
            };
            Some(id)
        }
        _ => None,
    };

    tracing::info!(node = cli.node_id, listen = %cli.listen, "switchboard up");
    switchboard_server::listener::run(node, &cli.listen, limits, tls, protocols).await?;
    Ok(())
}
