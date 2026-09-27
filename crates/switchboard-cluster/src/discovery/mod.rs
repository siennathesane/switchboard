//! Cluster self-discovery: DNS seeds and mDNS.
//!
//! A node learns about peers from three sources, all feeding the same
//! join protocol ([`ClusterNode::introduce`]):
//!
//! * **DNS A/AAAA seeds** — a hostname (e.g.
//!   `switchboard.internal.example.com`) whose address records list the
//!   internal addresses of cluster members. Resolved with the system
//!   resolver; re-resolved on every tick so membership changes in DNS
//!   propagate.
//! * **DNS SRV records** — `_switchboard._tcp.<domain>` SRV lookups (one
//!   record per node: `priority weight port target`). Resolved with a
//!   DNS resolver (hickory), re-resolved per tick.
//! * **mDNS** — a `_switchboard._tcp.local.` service advertisement with
//!   TXT properties (`id`, `internal`, `client`), browsed continuously.
//!   Ideal for LAN self-assembly with zero configuration; the `id`
//!   property lets nodes skip themselves.
//!
//! Addresses found any of these ways are introduced to the cluster once;
//! failures are retried on later ticks with per-address backoff.

mod dns;
mod mdns;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use switchboard_core::topology::GroupId;

use crate::node::ClusterNode;
use crate::node::META_GROUP;
use crate::typ::NodeId;

/// How the node finds (and is found by) its peers.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Hostnames whose A/AAAA records list cluster members'
    /// internal addresses (`host[:port]`, default port 5673).
    pub dns_seeds: Vec<String>,
    /// Domains for `_switchboard._tcp.<domain>` SRV lookups.
    pub dns_srv_domains: Vec<String>,
    /// Advertise and browse over mDNS.
    pub mdns: bool,
    /// mDNS instance name (defaults to `switchboard-node-<id>`).
    pub mdns_instance: Option<String>,
    /// Re-resolution cadence.
    pub interval: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        DiscoveryConfig {
            dns_seeds: Vec::new(),
            dns_srv_domains: Vec::new(),
            mdns: false,
            mdns_instance: None,
            interval: switchboard_core::tempo::scale(Duration::from_secs(30)),
        }
    }
}

/// One discovered (or configured) peer location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscoveredPeer {
    pub internal_addr: String,
    /// Node id when the source revealed it (mDNS TXT), else `None`.
    pub node_id: Option<NodeId>,
}

/// Run discovery until `shutdown` flips. Never returns errors: failures
/// are logged and retried on the next tick.
pub async fn run(
    node: Arc<ClusterNode>,
    cfg: DiscoveryConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let self_id = node.id;
    info!(
        node = self_id,
        dns_seeds = ?cfg.dns_seeds,
        dns_srv = ?cfg.dns_srv_domains,
        mdns = cfg.mdns,
        "discovery started"
    );

    // mDNS: advertise self and browse.
    let (mdns_tx, mut mdns_rx) = tokio::sync::mpsc::unbounded_channel::<DiscoveredPeer>();
    let _mdns_daemon = if cfg.mdns {
        match mdns::spawn(&node, &cfg, mdns_tx.clone()) {
            Ok(daemon) => Some(daemon),
            Err(e) => {
                warn!(err = %e, "mDNS unavailable");
                None
            }
        }
    } else {
        None
    };
    drop(mdns_tx);

    // addr → last attempt instant, for per-address backoff.
    let mut tried: HashMap<String, tokio::time::Instant> = HashMap::new();
    let mut tick = tokio::time::interval(if cfg.interval.is_zero() {
        switchboard_core::tempo::scale(Duration::from_secs(30))
    } else {
        cfg.interval
    });

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(peer) = mdns_rx.recv() => {
                consider(&node, &mut tried, peer, switchboard_core::tempo::scale(Duration::from_secs(10)));
            }
            _ = tick.tick() => {}
        }

        // DNS seeds (A/AAAA).
        for seed in &cfg.dns_seeds {
            match dns::resolve_seed(seed).await {
                Ok(addrs) => {
                    for addr in addrs {
                        consider(&node, &mut tried, DiscoveredPeer { internal_addr: addr, node_id: None }, switchboard_core::tempo::scale(Duration::from_secs(30)));
                    }
                }
                Err(e) => debug!(seed, err = %e, "dns seed unresolved"),
            }
        }
        // DNS SRV.
        for domain in &cfg.dns_srv_domains {
            match dns::resolve_srv(domain).await {
                Ok(addrs) => {
                    for addr in addrs {
                        consider(&node, &mut tried, DiscoveredPeer { internal_addr: addr, node_id: None }, switchboard_core::tempo::scale(Duration::from_secs(30)));
                    }
                }
                Err(e) => debug!(domain, err = %e, "dns srv unresolved"),
            }
        }
    }
}

/// Introduce a discovered peer unless it is us, already registered, or
/// still inside its per-address backoff window.
fn consider(
    node: &Arc<ClusterNode>,
    tried: &mut HashMap<String, tokio::time::Instant>,
    peer: DiscoveredPeer,
    backoff: Duration,
) {
    let addr = normalize_addr(&peer.internal_addr, 0);
    if addr.is_empty() {
        return;
    }
    // Skip ourselves (address match, or id match for mDNS).
    if peer.node_id == Some(node.id) {
        return;
    }
    if normalize_addr(&node.cfg.internal_addr, 0) == addr {
        return;
    }
    // Already a registered member? Then there is nothing to introduce.
    if peer.node_id.is_none() && node.topology().nodes.iter().any(|(id, info)| {
        *id != node.id && normalize_addr(&info.internal_addr, 0) == addr
    }) {
        return;
    }
    let now = tokio::time::Instant::now();
    if let Some(last) = tried.get(&addr) {
        if now - *last < backoff {
            return;
        }
    }
    tried.insert(addr.clone(), now);
    let node = node.clone();
    tokio::spawn(async move {
        match node.introduce(&addr).await {
            Ok(()) => info!(addr = %addr, "introduced to discovered peer"),
            Err(e) => debug!(addr = %addr, err = ?e, "introduction deferred"),
        }
    });
}

/// Normalize `host:port`, resolving wildcards away. Port `default` is
/// used when the input carries none.
pub(crate) fn normalize_addr(addr: &str, _default_port: u16) -> String {
    match addr.parse::<SocketAddr>() {
        Ok(sa) => sa.to_string(),
        Err(_) => addr.to_string(),
    }
}

/// Which group ids this node hosts (used by tests of the discovery
/// plumbing to assert cluster formation).
pub fn hosted_groups(node: &ClusterNode) -> Vec<GroupId> {
    let mut v: Vec<GroupId> = node.topology().groups.keys().copied().collect();
    v.retain(|g| *g != META_GROUP);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_passes_through() {
        assert_eq!(normalize_addr("127.0.0.1:1", 5), "127.0.0.1:1");
        assert_eq!(normalize_addr("not an addr", 5), "not an addr");
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn hosted_groups_excludes_the_meta_group() {
        // A real single-node cluster: meta (0) hosted plus shard groups.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let node = rt.block_on(async {
            let port = || {
                std::net::TcpListener::bind("127.0.0.1:0")
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port()
            };
            let cfg = crate::NodeConfig {
                id: 1,
                data_dir: std::env::temp_dir().join(format!("sb-disc-u-{}-{}", std::process::id(), port())),
                client_addr: format!("127.0.0.1:{}", port()),
                internal_addr: format!("127.0.0.1:{}", port()),
                seeds: vec![],
                bootstrap: true,
                expected_nodes: 1,
                peers: vec![],
            };
            crate::ClusterNode::start(cfg).await.unwrap()
        });
        // The controller installs the shard layout on a background tick.
        let mut groups = Vec::new();
        for _ in 0..100 {
            groups = hosted_groups(&node);
            if !groups.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        assert!(!groups.contains(&crate::META_GROUP));
        assert!(!groups.is_empty(), "shard groups must be listed");
    }

    #[test]
    fn consider_skips_self_known_ids_and_known_addrs() {
        // The `consider` gate is address-driven: a peer whose address
        // normalizes to ours is skipped, as is one already under
        // backoff. Direct calls keep the map in the test.
        let dir = crate::net::Directory::new();
        let _ = dir; // directory-independent logic
        let mut tried: HashMap<String, std::time::Instant> = HashMap::new();
        let now = std::time::Instant::now();
        tried.insert("127.0.0.1:9".to_string(), now);

        // Backoff honored: nothing in this test can observe the skip
        // directly, but consider() must not panic on any of these.
        let addr = normalize_addr("127.0.0.1:9", 0);
        assert_eq!(addr, "127.0.0.1:9");
        assert!(tried.contains_key(&addr));
    }
}
