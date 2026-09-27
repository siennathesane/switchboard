//! mDNS advertisement and browsing via `mdns-sd`.
//!
//! The node registers `_switchboard._tcp.local.` with TXT properties
//! * `id` — the node id (so peers can skip themselves),
//! * `internal` — the internal (raft/forwarding) address,
//! * `client` — the client-facing address,
//! and browses the same service type, forwarding every resolved peer to
//! the join loop.

use std::sync::Arc;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::{debug, info, warn};

use crate::node::ClusterNode;

use super::DiscoveryConfig;
use super::DiscoveredPeer;

pub const SERVICE_TYPE: &str = "_switchboard._tcp.local.";

/// The registered daemon handle; dropping it unregisters the service.
pub struct MdnsHandle {
    _daemon: ServiceDaemon,
}

/// Pick a non-loopback IPv4 for advertisement (falling back to loopback).
fn advertise_ip() -> std::net::IpAddr {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .find_map(|i| {
            let ip = i.ip();
            if !ip.is_loopback() {
                Some(ip)
            } else {
                None
            }
        })
        .unwrap_or_else(|| std::net::IpAddr::from([127, 0, 0, 1]))
}

fn instance_name(cfg: &DiscoveryConfig, node_id: u64) -> String {
    cfg.mdns_instance
        .clone()
        .unwrap_or_else(|| format!("switchboard-node-{node_id}"))
}

fn internal_port(addr: &str) -> u16 {
    addr.rsplit(':').next().and_then(|p| p.parse().ok()).unwrap_or(5673)
}

fn host_name(cfg: &DiscoveryConfig, node_id: u64) -> String {
    format!("{}-{}.local.", instance_name(cfg, node_id), node_id)
}

/// Register our service and start browsing; discovered peers (excluding
/// ourselves by id) are sent on `tx`.
pub fn spawn(
    node: &Arc<ClusterNode>,
    cfg: &DiscoveryConfig,
    tx: tokio::sync::mpsc::UnboundedSender<DiscoveredPeer>,
) -> Result<MdnsHandle, String> {
    let daemon = ServiceDaemon::new().map_err(|e| format!("mdns daemon: {e}"))?;

    let id = node.id;
    let instance = instance_name(cfg, id);
    let ip = advertise_ip();
    let port = internal_port(&node.cfg.internal_addr);
    let service = ServiceInfo::new(
        SERVICE_TYPE,
        &instance,
        &host_name(cfg, id),
        ip,
        port,
        Some(
            [
                ("id".to_string(), id.to_string()),
                ("internal".to_string(), node.cfg.internal_addr.clone()),
                ("client".to_string(), node.cfg.client_addr.clone()),
            ]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
        ),
    )
    .map_err(|e| format!("service info: {e}"))?;
    daemon
        .register(service)
        .map_err(|e| format!("mdns register: {e}"))?;
    info!(node = id, instance = %instance, ip = %ip, port, "mDNS service registered");

    let self_id = id;
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .map_err(|e| format!("mdns browse: {e}"))?;
    tokio::spawn(async move {
        while let Ok(event) = receiver.recv_async().await {
            if let ServiceEvent::ServiceResolved(info) = event {
                let peer_id = info
                    .get_property_val_str("id")
                    .and_then(|v| v.parse::<u64>().ok());
                if peer_id == Some(self_id) {
                    continue; // our own advertisement echoed back
                }
                let addr = match info.get_property_val_str("internal") {
                    Some(a) => a.to_string(),
                    None => {
                        // Fall back to the resolved host/port.
                        let host = info.get_hostname().trim_end_matches('.').to_string();
                        format!("{host}:{}", info.get_port())
                    }
                };
                debug!(%addr, ?peer_id, "mdns peer discovered");
                let _ = tx.send(DiscoveredPeer { internal_addr: addr, node_id: peer_id });
            }
        }
        warn!("mdns browse stream ended");
    });

    Ok(MdnsHandle { _daemon: daemon })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_and_ports() {
        let cfg = DiscoveryConfig::default();
        assert_eq!(instance_name(&cfg, 7), "switchboard-node-7");
        assert_eq!(internal_port("10.0.0.5:5679"), 5679);
        assert_eq!(internal_port("10.0.0.5"), 5673);
        assert_eq!(host_name(&cfg, 7), "switchboard-node-7-7.local.");
    }

    #[test]
    fn service_type_is_stable() {
        assert_eq!(SERVICE_TYPE, "_switchboard._tcp.local.");
    }
}
