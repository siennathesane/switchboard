//! Client-facing listeners. One port carries every supported protocol
//! (AMQP 0-9-1, AMQP 1.0, MQTT, STOMP, WebSocket-wrapped variants, and an
//! HTTP health probe), selected by sniffing the first bytes — with or
//! without TLS in front.

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;
use tracing::warn;

use switchboard_cluster::ClusterNode;

use crate::channel::ConnectionLimits;
use crate::protocols;
use crate::tls::TlsIdentity;

/// Run a client-facing listener until the process exits.
pub async fn run(
    node: Arc<ClusterNode>,
    addr: &str,
    limits: ConnectionLimits,
    tls: Option<TlsIdentity>,
    protocols: protocols::ProtocolConfig,
) -> std::io::Result<()> {
    let acceptor = match &tls {
        Some(id) => match crate::tls::acceptor(id) {
            Ok(a) => Some(a),
            Err(e) => {
                warn!(err = %e, "tls disabled: bad identity");
                return Err(std::io::Error::other(e));
            }
        },
        None => None,
    };
    let listener = TcpListener::bind(addr).await?;
    info!(addr, tls = acceptor.is_some(), ?protocols, "gateway listener up");
    loop {
        let Ok((socket, _peer)) = listener.accept().await else {
            continue;
        };
        // Per-frame writes (a Deliver method frame followed by its body)
        // must not sit in Nagle's single-unacked-small-segment queue: with
        // a delayed-ACKing client each pair of frames stalls ~40 ms. Set
        // before the gateway splits the socket into protocol halves.
        socket.set_nodelay(true).ok();
        let node = node.clone();
        let limits = limits.clone();
        let protocols = protocols.clone();
        match &acceptor {
            Some(a) => {
                let a = a.clone();
                tokio::spawn(async move {
                    match a.accept(socket).await {
                        Ok(tls) => {
                            let _ =
                                protocols::serve_client(tls, node, limits, protocols).await;
                        }
                        Err(e) => warn!(err = ?e, "tls handshake failed"),
                    }
                });
            }
            None => {
                tokio::spawn(async move {
                    let _ = protocols::serve_client(socket, node, limits, protocols).await;
                });
            }
        }
    }
}
