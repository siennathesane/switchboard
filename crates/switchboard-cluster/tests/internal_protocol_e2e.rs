//! The internal management protocol, driven black-box over a real
//! socket: liveness, admin errors, consumer delivery for an unknown
//! subscription, and malformed frames — the wire behaviors every peer
//! and every forwarded write relies on.

use std::time::Duration;

use switchboard_cluster::proto::{
    AdminRequest, AdminResponse, Envelope, InternalMessage, InternalRequest, InternalResponse,
};
use switchboard_cluster::{ClusterNode, NodeConfig};
use switchboard_core::model::{StoredMessage, SubscriptionId};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn bootstrap_node(tag: &str, id: u64) -> std::sync::Arc<ClusterNode> {
    let cfg = NodeConfig {
        id,
        data_dir: std::env::temp_dir().join(format!("sb-ip-{tag}-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![],
        bootstrap: true,
        expected_nodes: 1,
        peers: vec![],
        timeouts: Default::default(),
    };
    ClusterNode::start(cfg).await.unwrap()
}

/// Exchange one envelope with the node's internal listener.
async fn roundtrip(
    conn: &mut switchboard_cluster::transport::PeerConn,
    env: &Envelope,
) -> InternalResponse {
    conn.send(&bincode::serialize(env).unwrap()).await.unwrap();
    let back = conn.recv().await.unwrap();
    let InternalMessage::Response(resp) = bincode::deserialize(&back).unwrap() else {
        panic!("expected a response");
    };
    resp
}

#[tokio::test(flavor = "multi_thread")]
async fn internal_protocol_admin_and_error_arms() {
    let node = bootstrap_node("proto", 1).await;
    let mut conn = switchboard_cluster::transport::PeerChannel::new(None, "sb".into())
        .connect(&node.cfg.internal_addr)
        .await
        .unwrap();

    // Liveness.
    let resp = roundtrip(
        &mut conn,
        &Envelope { from: 9, message: InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping)) },
    )
    .await;
    assert!(matches!(resp, InternalResponse::Admin(AdminResponse::Pong)));

    // A Response where the peer expected a Request is a protocol error.
    let resp = roundtrip(
        &mut conn,
        &Envelope { from: 9, message: InternalMessage::Response(InternalResponse::Admin(AdminResponse::Pong)) },
    )
    .await;
    assert!(matches!(resp, InternalResponse::Error(ref e) if e.contains("expected request")));

    // A frame that does not decode as bincode is answered with the
    // codec error (the connection stays usable for well-formed frames).
    conn.send(b"\x00\x01\x02\x03 definitely not bincode").await.unwrap();
    let back = conn.recv().await.unwrap();
    let InternalMessage::Response(InternalResponse::Error(_)) = bincode::deserialize(&back).unwrap() else {
        panic!("codec error expected");
    };

    // Deliver for a subscription nobody hosts: NotFound, and the
    // connection survives.
    let resp = roundtrip(
        &mut conn,
        &Envelope {
            from: 9,
            message: InternalMessage::Request(InternalRequest::Admin(AdminRequest::Deliver {
                sub: SubscriptionId { node: 9, sub: 77 },
                queue: "ghost".into(),
                seq: 1,
                message: StoredMessage {
                    properties: switchboard_wire::BasicProperties::new(),
                    body: b"x".to_vec(),
                    exchange: String::new(),
                    routing_key: String::new(),
                    persistent: false,
                },
                redelivered: false,
                deleted: false,
            })),
        },
    )
    .await;
    assert!(matches!(
        resp,
        InternalResponse::Admin(AdminResponse::NotFound)
    ));

    let _ = tokio::time::timeout(Duration::from_secs(1), node.shutdown());
}
