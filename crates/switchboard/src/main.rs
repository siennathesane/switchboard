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
}

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
    let cfg = NodeConfig {
        id: cli.node_id,
        data_dir: cli.data.clone(),
        client_addr: cli.listen.clone(),
        internal_addr: advertise.clone(),
        seeds: cli.seeds.clone(),
        bootstrap: cli.bootstrap,
        expected_nodes: cli.expected_nodes,
        peers,
    };

    let node = switchboard_cluster::ClusterNode::start(cfg)
        .await
        .map_err(|e| anyhow::anyhow!("cluster start failed: {e}"))?;

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
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            switchboard_cluster::discovery::run(node2, discovery_cfg, shutdown_rx).await;
        });
    }

    let limits = ConnectionLimits::default();
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
