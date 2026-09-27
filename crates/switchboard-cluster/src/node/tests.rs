//! Tests for `node`.

use super::*;

fn ids(n: u64) -> Vec<NodeId> {
    (1..=n).collect()
}

#[test]
fn nine_nodes_nine_groups_of_three() {
    let w = sliding_windows(&ids(9));
    assert_eq!(w.len(), 9);
    for (g, members) in &w {
        assert_eq!(members.len(), 3, "group {g}");
        assert_eq!(members.iter().collect::<std::collections::HashSet<_>>().len(), members.len(), "no duplicate voters in {g}");
    }
    // Every node participates in exactly 3 groups.
    let mut count = HashMap::new();
    for (_, members) in &w {
        for m in members {
            *count.entry(*m).or_insert(0) += 1;
        }
    }
    assert!(count.values().all(|c| *c == 3), "{count:?}");
    // Consecutive windows.
    assert_eq!(w[0], (1, vec![1, 2, 3]));
    assert_eq!(w[1], (2, vec![2, 3, 4]));
    assert_eq!(w[8], (9, vec![9, 1, 2]));
}

#[test]
fn small_clusters_form_one_group() {
    assert_eq!(sliding_windows(&[]), vec![]);
    assert_eq!(sliding_windows(&[7]), vec![(1, vec![7])]);
    assert_eq!(sliding_windows(&[5, 9]), vec![(1, vec![5, 9])]);
}

#[test]
fn three_nodes_three_rotating_windows() {
    // With exactly 3 nodes the sliding windows are rotations; groups 1
    // and 3 share members, which is harmless (a queue lands in exactly
    // one group by name hash).
    let w = sliding_windows(&ids(3));
    assert_eq!(
        w,
        vec![(1, vec![1, 2, 3]), (2, vec![2, 3, 1]), (3, vec![3, 1, 2])]
    );
}

#[test]
fn every_group_stays_within_the_voter_cap() {
    for n in [3u64, 4, 5, 6, 7, 8, 9, 12] {
        for (_, members) in sliding_windows(&ids(n)) {
            assert!(members.len() <= MAX_VOTERS, "N={n}");
        }
    }
}

// ---------------------------------------------------------------------------
// Node-level behavior: watchers, admin arms, join refusal, forward mapping.
// ---------------------------------------------------------------------------

use crate::proto::{
    AdminRequest, AdminResponse, Envelope, ForwardError, InternalMessage, InternalRequest,
    InternalResponse,
};
use switchboard_core::error::BrokerError;
use switchboard_core::model::{ConnectionId, QueueOptions, StoredMessage, SubscriptionId};
use switchboard_core::shard::{QueuePolicy, ShardCmd};
use switchboard_core::topology::{MetaCmd, NodeInfo};
use switchboard_core::topology::MetaReply;
use switchboard_wire::BasicProperties;


fn set_test_tempo(scale: &str) {
    unsafe { std::env::set_var("SB_TIME_SCALE", scale) };
}

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
        data_dir: std::env::temp_dir().join(format!("sb-nt-{tag}-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![],
        bootstrap: true,
        expected_nodes: 1,
        peers: vec![],
    };
    ClusterNode::start(cfg).await.unwrap()
}

fn test_info() -> NodeInfo {
    NodeInfo {
        client_addr: "127.0.0.1:1".into(),
        internal_addr: "127.0.0.1:2".into(),
    }
}

/// Wait until the node's meta group is applying and its view includes
/// the default vhost (fresh nodes need a beat to install the layout).
async fn wait_meta_ready(node: &std::sync::Arc<ClusterNode>) {
    wait_for("meta ready", 30, || {
        !node.topology().groups.is_empty() && !node.topology().vhosts.is_empty()
    })
    .await;
}

/// Await a future with a deadline (replaces the old sync busy-poll,
/// which parked forever on a noop waker whenever the future returned
/// Pending).
async fn within<F: std::future::Future>(secs: u64, f: F) -> F::Output {
    match tokio::time::timeout(std::time::Duration::from_secs(secs), f).await {
        Ok(v) => v,
        Err(_) => panic!("future did not settle within {secs}s"),
    }
}


/// Wait until `node` observes `leader` leading the meta group.
async fn wait_leader_seen(
    node: &std::sync::Arc<ClusterNode>,
    leader: NodeId,
    secs: u64,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if node.leader_hint_of(crate::META_GROUP).await == Some(leader) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("node {} never saw node {leader} lead meta within {secs}s", node.id);
}

async fn wait_for(what: &str, secs: u64, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_flag_and_topology_watcher() {
    set_test_tempo("10");
    let node = bootstrap_node("watch", 1).await;
    assert!(!node.shutting_down());
    // A topology-affecting write, then the watcher must observe the new
    // state on subscribe.
    let cmd = BrokerCommand::Meta(MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "watched".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: ConnectionId { node: 1, conn: 0 },
    });
    wait_meta_ready(&node).await;
    node.write(crate::META_GROUP, cmd).await.unwrap();
    node.refresh_topology().await;
    let mut w = node.topology_watcher();
    w.borrow_and_update();
    assert!(w.borrow().vhosts["/"].queues.contains_key("watched"));
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_request_admin_arms() {
    set_test_tempo("10");
    let node = bootstrap_node("admin", 1).await;
    // A Response where a Request was expected.
    let resp = node
        .handle_request(InternalMessage::Response(InternalResponse::Admin(AdminResponse::Pong)))
        .await;
    assert!(matches!(resp, InternalResponse::Error(e) if e.contains("expected request")));
    // Liveness.
    let resp = node
        .handle_request(InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping)))
        .await;
    assert!(matches!(resp, InternalResponse::Admin(AdminResponse::Pong)));
}

/// A seed that answers the join handshake with something other than
/// `Joined` must be reported as a refusal (and the join loop keeps
/// retrying other seeds / the deadline).
#[tokio::test(flavor = "multi_thread")]
async fn join_one_seed_refuses_non_join_reply() {
    set_test_tempo("10");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut conn = crate::transport::accept(tcp, None).await.unwrap();
        let frame = conn.read_frame_full().await.unwrap();
        let _req: Envelope = bincode::deserialize(&frame).unwrap();
        let reply = InternalMessage::Response(InternalResponse::Admin(AdminResponse::Pong));
        conn.write_frame(&bincode::serialize(&reply).unwrap()).await.unwrap();
    });
    let node = bootstrap_node("refuse", 1).await;
    let err = node.join_one_seed(&addr, &test_info()).await.unwrap_err();
    match err {
        ClusterError::Codec(m) => assert!(m.contains("refused"), "{m}"),
        other => panic!("expected refusal, got {other:?}"),
    }
}

/// A fake peer serving the scripted reply on every connection. The raft
/// fabric dials peers constantly while a leader looks unreachable, so the
/// admin relay's connection must be served too — a one-shot accept would
/// leave the real request hanging in the listener backlog.
async fn fake_peer(reply: InternalMessage) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let bytes = bincode::serialize(&reply).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let Ok(mut conn) = crate::transport::accept(tcp, None).await else {
                    return;
                };
                if conn.read_frame_full().await.is_err() {
                    return;
                }
                let _ = conn.write_frame(&bytes).await;
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn forward_write_maps_every_failure_shape() {
    set_test_tempo("10");
    let node = bootstrap_node("fwd", 1).await;
    let cmd = BrokerCommand::Meta(MetaCmd::RegisterNode { node: 42, info: test_info() });

    // A broker-level rejection from the peer stays a broker error.
    let peer = fake_peer(InternalMessage::Response(InternalResponse::Forward(BrokerReply::Error(
        BrokerError::not_found("nope"),
    ))))
    .await;
    node.reroute_peer(42, peer).await;
    match node.forward_write(42, crate::META_GROUP, cmd.clone()).await {
        Err(ClusterError::Broker(e)) => assert!(e.to_string().contains("nope")),
        other => panic!("expected broker error, got {other:?}"),
    }

    // Typed forward failures route to their ClusterError shapes.
    for (fe, expect) in [
        (ForwardError::Transient, "transient"),
        (ForwardError::Unreachable, "unreachable"),
        (ForwardError::NotJoined, "not joined"),
        (ForwardError::Broker(BrokerError::not_found("nf")), "broker"),
        (ForwardError::Other("odd".into()), "other"),
    ] {
        let peer = fake_peer(InternalMessage::Response(InternalResponse::ForwardFailed(fe.clone()))).await;
        node.reroute_peer(42, peer).await;
        let r = node.forward_write(42, crate::META_GROUP, cmd.clone()).await;
        let msg = format!("{:?}", r.as_ref().err());
        match expect {
            "transient" => assert!(matches!(r, Err(ClusterError::Transient)), "{msg}"),
            "unreachable" => assert!(matches!(r, Err(ClusterError::Unreachable { .. })), "{msg}"),
            "not joined" => assert!(matches!(r, Err(ClusterError::NotJoined)), "{msg}"),
            "broker" => assert!(matches!(r, Err(ClusterError::Broker(_))), "{msg}"),
            _ => assert!(matches!(r, Err(ClusterError::Codec(m)) if m.contains("odd")), "{msg}"),
        }
    }

    // A bare error string and a wrong reply type both decode as codec
    // errors.
    let peer = fake_peer(InternalMessage::Response(InternalResponse::Error("boom".into()))).await;
    node.reroute_peer(42, peer).await;
    assert!(matches!(
        node.forward_write(42, crate::META_GROUP, cmd.clone()).await,
        Err(ClusterError::Codec(m)) if m.contains("boom")
    ));

    let peer = fake_peer(InternalMessage::Response(InternalResponse::Admin(AdminResponse::Pong))).await;
    node.reroute_peer(42, peer).await;
    assert!(matches!(
        node.forward_write(42, crate::META_GROUP, cmd).await,
        Err(ClusterError::Codec(m)) if m.contains("unexpected forward reply")
    ));
}

/// A node that never joined has no leader to write through: the write
/// retries until its deadline, then reports the group unreachable.
#[tokio::test(flavor = "multi_thread")]
async fn unjoined_write_reports_unreachable() {
    set_test_tempo("10");
    let cfg = NodeConfig {
        id: 7,
        data_dir: std::env::temp_dir().join(format!("sb-nt-pending-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node = ClusterNode::start(cfg).await.unwrap();
    let cmd = BrokerCommand::Meta(MetaCmd::RegisterNode { node: 42, info: test_info() });
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        node.write(crate::META_GROUP, cmd),
    )
    .await
    .expect("write must fail, not hang");
    assert!(matches!(r, Err(ClusterError::Unreachable { group: crate::META_GROUP })));
}

/// Direct inter-node consumer data path: a consumer registered on node 2
/// receives deliveries published through node 1, and cancellation is
/// announced to the sink.
#[tokio::test(flavor = "multi_thread")]
async fn deliver_and_cancel_cross_nodes() {
    set_test_tempo("10");
    let node1 = bootstrap_node("x1", 1).await;
    let internal1 = node1.cfg.internal_addr.clone();
    let cfg2 = NodeConfig {
        id: 2,
        data_dir: std::env::temp_dir().join(format!("sb-nt-x2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![internal1],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node2 = ClusterNode::start(cfg2).await.unwrap();
    wait_for("node2 joins", 60, || node1.topology().nodes.contains_key(&2)).await;
    wait_meta_ready(&node1).await;
    wait_meta_ready(&node2).await;

    // Declare a queue through meta; both nodes learn the shard mapping.
    let cmd = BrokerCommand::Meta(MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "cross".into(),
        passive: false,
        options: QueueOptions::default(),
        owner: ConnectionId { node: 1, conn: 0 },
    });
    let reply = node1.write(crate::META_GROUP, cmd).await.unwrap();
    let shard = match reply {
        BrokerReply::Meta(MetaReply::QueueDeclared { shard, .. }) => shard,
        other => panic!("unexpected declare reply {other:?}"),
    };
    node1
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::CreateQueueData {
                queue: "cross".into(),
                policy: QueuePolicy::default(),
            }),
        )
        .await
        .unwrap();

    // Consumer on node 2.
    let sub = SubscriptionId { node: 2, sub: 1 };
    let (dtx, mut drx) = tokio::sync::mpsc::unbounded_channel();
    let (ctx_tx, mut crx) = tokio::sync::mpsc::unbounded_channel();
    node2
        .attach_consumer(sub, ConsumerSink { deliveries: dtx, cancelled: ctx_tx })
        .await;
    node2
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::RegisterSubscription {
                sub,
                queue: "cross".into(),
                node: 2,
                consumer_tag: "ctag".into(),
                no_ack: true,
                exclusive: false,
                conn: ConnectionId { node: 2, conn: 1 },
                byte_limit: 0,
            }),
        )
        .await
        .unwrap();
    node2
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::Credit {
                sub,
                count: 10,
            }),
        )
        .await
        .unwrap();

    // Publish through node 1: the shard effect must be forwarded to the
    // consumer's home node.
    node1
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::Enqueue {
                queue: "cross".into(),
                message: StoredMessage {
                    properties: BasicProperties::new(),
                    body: b"payload".to_vec(),
                    exchange: String::new(),
                    routing_key: "cross".into(),
                    persistent: false,
                },
                at_ms: crate::now_ms(),
            }),
        )
        .await
        .unwrap();
    wait_for("cross-node delivery", 30, || !drx.is_empty()).await;
    let d = drx.recv().await.unwrap();
    assert_eq!(d.queue, "cross");

    // Deleting the queue data cancels every consumer of the queue; the
    // group leader's pump either notifies a local sink or forwards the
    // cancellation to the consumer's home node.
    node2
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::DeleteQueueData { queue: "cross".into() }),
        )
        .await
        .unwrap();
    let tag = tokio::time::timeout(std::time::Duration::from_secs(30), crx.recv())
        .await
        .expect("cancellation must arrive")
        .expect("sink alive");
    assert_eq!(tag, "ctag");
}

// ---------------------------------------------------------------------------
// Fault injection: handle removal, dead peers, raft-payload relays.
// ---------------------------------------------------------------------------

/// Enable a tracing subscriber so `info!`/`warn!` field code executes in
/// unit tests (line coverage of event fields).
fn init_log() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt().try_init();
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_node_logs_and_write_times_out() {
    set_test_tempo("10");
    init_log();
    let cfg = NodeConfig {
        id: 8,
        data_dir: std::env::temp_dir().join(format!("sb-nt-pend2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node = ClusterNode::start(cfg).await.unwrap();

    // Calls to unknown peers are unreachable; a request-shaped reply is a
    // codec error.
    assert!(matches!(
        node.call(42, InternalRequest::Admin(AdminRequest::Ping)).await,
        Err(ClusterError::Unreachable { group: crate::META_GROUP })
    ));
    let writes_to_unknown_group = node
        .write(999, BrokerCommand::Meta(MetaCmd::RegisterNode { node: 42, info: test_info() }))
        .await;
    assert!(matches!(
        writes_to_unknown_group,
        Err(ClusterError::Unreachable { group: 999 })
    ));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn call_rejects_a_request_shaped_reply() {
    set_test_tempo("10");
    let node = bootstrap_node("call-req", 1).await;
    // A peer that answers a request with another REQUEST.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut conn = crate::transport::accept(tcp, None).await.unwrap();
        let _frame = conn.read_frame_full().await.unwrap();
        let ping = InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping));
        conn.write_frame(&bincode::serialize(&ping).unwrap()).await.unwrap();
    });
    node.reroute_peer(30, addr).await;
    assert!(matches!(
        node.call(30, InternalRequest::Admin(AdminRequest::Ping)).await,
        Err(ClusterError::Codec(m)) if m.contains("unexpected")
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn owner_node_of_unknown_subscription_is_none() {
    set_test_tempo("10");
    let node = bootstrap_node("owner", 1).await;
    let ghost = SubscriptionId { node: 9, sub: 9 };
    assert_eq!(node.owner_node_of(ghost), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn ensure_group_reuses_cached_handles() {
    set_test_tempo("10");
    let node = bootstrap_node("ensure", 1).await;
    wait_meta_ready(&node).await;
    // Fast path: the handle is already in the map.
    let h1 = node.ensure_group(crate::META_GROUP).await.unwrap();
    let h2 = node.ensure_group(crate::META_GROUP).await.unwrap();
    assert!(Arc::ptr_eq(&h1, &h2));
    // Weak-cache path: evict from the map; the process-global LIVE cache
    // still holds the live handle and re-installs it.
    node.groups.write().expect("groups lock").remove(&crate::META_GROUP);
    let h3 = node.ensure_group(crate::META_GROUP).await.unwrap();
    assert!(Arc::ptr_eq(&h1, &h3), "LIVE cache must resurrect the handle");
    assert!(node.groups.read().expect("groups lock").contains_key(&crate::META_GROUP));
}

#[tokio::test(flavor = "multi_thread")]
async fn controller_reinstalls_evicted_group_handles() {
    set_test_tempo("10");
    let node = bootstrap_node("ctrl-evict", 1).await;
    wait_meta_ready(&node).await;
    // Evict a shard group handle: the controller (400 ms tick) drives
    // convergence, which re-runs ensure_group for every meta group.
    node.groups.write().expect("groups lock").remove(&1);
    wait_for(
        "controller reinstalls the evicted handle",
        15,
        || node.groups.read().expect("groups lock").contains_key(&1),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raft_payload_relays_and_reconfigure_relay_errors() {
    set_test_tempo("10");
    let node = bootstrap_node("relay", 1).await;
    wait_meta_ready(&node).await;

    // An unknown group gets a fresh (empty) raft core, which grants the
    // vote from a clean slate. (The "not available" error arm requires a
    // failing raft startup — a storage-level fault.)
    let vote = crate::proto::RaftPayload::Vote(Box::new(openraft::raft::VoteRequest::new(
        openraft::Vote::new(1, 1),
        None,
    )));
    let resp = node
        .handle_request(InternalMessage::Request(InternalRequest::Raft { group: 999, payload: vote }))
        .await;
    assert!(
        matches!(resp, InternalResponse::Raft(crate::proto::RaftResponse::Vote(_))),
        "{resp:?}"
    );

    // AttachConsumer is accepted for identity bookkeeping.
    let resp = node
        .handle_request(InternalMessage::Request(InternalRequest::Admin(
            AdminRequest::AttachConsumer {
                sub: SubscriptionId { node: 1, sub: 5 },
                conn: switchboard_core::model::ConnectionId { node: 1, conn: 5 },
                reply: false,
            },
        )))
        .await;
    assert!(matches!(resp, InternalResponse::Admin(AdminResponse::Attached)));

    // Reconfigure of a group this node cannot host fails without a hint.
    let resp = node
        .handle_request(InternalMessage::Request(InternalRequest::Admin(
            AdminRequest::ReconfigureGroup { group: 999, voters: vec![1], forwarded: false },
        )))
        .await;
    assert!(
        matches!(resp, InternalResponse::Error(ref e) if e.contains("reconfigure 999")),
        "{resp:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconfigure_relay_maps_leader_reply_errors() {
    set_test_tempo("1");
    // node1 leads; node2 fails its local reconfigure and hops to the
    // leader, whose address we point at fake peers.
    let node1 = bootstrap_node("rr1", 1).await;
    let internal1 = node1.cfg.internal_addr.clone();
    let cfg2 = NodeConfig {
        id: 2,
        data_dir: std::env::temp_dir().join(format!("sb-nt-rr2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![internal1],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node2 = ClusterNode::start(cfg2).await.unwrap();
    wait_for("node2 joins", 60, || node1.topology().nodes.contains_key(&2)).await;
    wait_meta_ready(&node2).await;
    wait_leader_seen(&node2, 1, 30).await;

    // Group 0 is hosted on node2, but node2 is the follower: driving the
    // membership toward a voter set that excludes the leader fails
    // locally with ForwardToLeader, which triggers the one-hop relay.
    let req = |forwarded| {
        InternalMessage::Request(InternalRequest::Admin(AdminRequest::ReconfigureGroup {
            group: crate::META_GROUP,
            voters: vec![9],
            forwarded,
        }))
    };

    // Leader peer replies with a bare error → surfaced verbatim.
    let err_peer = fake_peer(InternalMessage::Response(InternalResponse::Error("nope".into()))).await;
    node2.reroute_peer(1, err_peer).await;
    let resp = node2.handle_request(req(false)).await;
    assert!(matches!(resp, InternalResponse::Error(ref e) if e.contains("nope")), "{resp:?}");

    // Leader peer replies with the wrong admin kind → bad-reply error.
    let pong_peer = fake_peer(InternalMessage::Response(InternalResponse::Admin(
        crate::proto::AdminResponse::Pong,
    )))
    .await;
    node2.reroute_peer(1, pong_peer).await;
    let resp = node2.handle_request(req(false)).await;
    assert!(
        matches!(resp, InternalResponse::Error(ref e) if e.contains("bad reconfigure reply")),
        "{resp:?}"
    );

    // Leader peer unreachable → the dial error is stringified.
    node2.reroute_peer(1, "127.0.0.1:1".into()).await;
    let resp = node2.handle_request(req(false)).await;
    assert!(matches!(resp, InternalResponse::Error(_)), "{resp:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn install_snapshot_relay_reaches_the_local_raft() {
    set_test_tempo("10");
    let node = bootstrap_node("snap", 1).await;
    wait_meta_ready(&node).await;
    let req = openraft::raft::InstallSnapshotRequest {
        vote: openraft::Vote::new(1, 1),
        meta: openraft::SnapshotMeta::default(),
        offset: 0,
        data: Vec::new(),
        done: false,
    };
    let resp = node
        .handle_request(InternalMessage::Request(InternalRequest::Raft {
            group: crate::META_GROUP,
            payload: crate::proto::RaftPayload::InstallSnapshot(Box::new(req)),
        }))
        .await;
    match resp {
        InternalResponse::Raft(crate::proto::RaftResponse::InstallSnapshot(_)) => {}
        InternalResponse::Error(_) => {} // raft rejected the chunk; either way the arm ran
        other => panic!("unexpected reply {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_write_with_poisoned_leader_hint_times_out() {
    // Tempo 1: the reconcile loop re-asserts every directory address from
    // meta every tick, and at high tempo that tick (50ms) can undo the
    // poison mid-write. At identity tempo the tick is a full 500ms — the
    // single-shot attempt always observes the poison.
    set_test_tempo("1");
    // A follower whose leader hint points at a poisoned address falls
    // back to local attempts, follows the hint loop until exhaustion,
    // and the outer deadline reports the group unreachable.
    let node1 = bootstrap_node("pw1", 1).await;
    let internal1 = node1.cfg.internal_addr.clone();
    let cfg2 = NodeConfig {
        id: 2,
        data_dir: std::env::temp_dir().join(format!("sb-nt-pw2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![internal1],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node2 = ClusterNode::start(cfg2).await.unwrap();
    wait_for("node2 joins", 60, || node1.topology().nodes.contains_key(&2)).await;
    wait_meta_ready(&node2).await;
    // Make sure node2 is the FOLLOWER (its writes must hop to node1).
    loop {
        if tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if node2.leader_hint_of(crate::META_GROUP).await == Some(1) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok()
        {
            break;
        }
    }
    // Poison node1's address in node2's directory: every forward dials a
    // dead socket. (One poisoned entry is enough for a single-shot
    // attempt; long-running loops would refresh the directory from meta.)
    node2.reroute_peer(1, "127.0.0.1:1".into()).await;
    let cmd = BrokerCommand::Meta(MetaCmd::DeclareQueue {
        vhost: "/".into(),
        name: "poisoned".into(),
        passive: false,
        options: switchboard_core::model::QueueOptions::default(),
        owner: ConnectionId { node: 2, conn: 0 },
    });
    // One write attempt: the follower cannot land the write locally, the
    // leader forward fails (dead address), and the stale-hint fallback
    // reports the retryable-transient outcome to the caller.
    let r = node2.try_write_depth(crate::META_GROUP, &cmd, 0).await;
    assert!(matches!(r, Err(ClusterError::Transient)), "got {r:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn controller_initializes_hand_extended_groups_and_skips_empty() {
    set_test_tempo("10");
    let node = bootstrap_node("ctrl-ext", 1).await;
    wait_meta_ready(&node).await;
    // Hand-extend the layout: a live group anchored here and a
    // degenerate empty one. The controller must initialize the former
    // (smallest member + fresh kv) and skip the latter.
    let mut layout: Vec<(GroupId, Vec<NodeId>)> = node
        .topology()
        .groups
        .iter()
        .map(|(g, m)| (*g, m.clone()))
        .collect();
    layout.push((90, vec![1]));
    layout.push((91, vec![]));
    node.write(
        crate::META_GROUP,
        BrokerCommand::Meta(MetaCmd::SetGroups { groups: layout }),
    )
    .await
    .unwrap();
    wait_for(
        "controller initializes the hand-extended group",
        20,
        || node.groups.read().expect("groups lock").contains_key(&90),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn evicted_meta_handle_is_tolerated_by_the_controller() {
    set_test_tempo("10");
    let node = bootstrap_node("ctrl-meta", 1).await;
    wait_meta_ready(&node).await;
    // Evicting the meta handle must not wedge the controller: the tick
    // skips (no handle) and the LIVE cache restores it on demand.
    node.groups.write().expect("groups lock").remove(&crate::META_GROUP);
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let h = node.ensure_group(crate::META_GROUP).await.unwrap();
    assert!(Arc::ptr_eq(
        &h,
        node.groups.read().expect("groups lock").get(&crate::META_GROUP).unwrap()
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_ensure_group_creates_exactly_one_handle() {
    set_test_tempo("10");
    let node = bootstrap_node("ctrl-race", 1).await;
    wait_meta_ready(&node).await;
    // Two concurrent creations of the same fresh group: one takes the
    // creating permit, the other must observe the map insert.
    let (a, b) = tokio::join!(node.ensure_group(77), node.ensure_group(77));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(Arc::ptr_eq(&a, &b), "both callers must share one handle");
    assert!(node.groups.read().expect("groups lock").contains_key(&77));
}

#[tokio::test(flavor = "multi_thread")]
async fn double_initialize_of_one_group_is_benign() {
    set_test_tempo("10");
    let node = bootstrap_node("init-race", 1).await;
    wait_meta_ready(&node).await;
    // Two concurrent initializations of the same fresh group: exactly
    // one wins; the loser's initialize error is swallowed (benign).
    let (a, b) = tokio::join!(
        node.initialize_group_if_fresh(78, vec![1]),
        node.initialize_group_if_fresh(78, vec![1]),
    );
    assert!(a.is_ok() || b.is_ok(), "{a:?} {b:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_forwarded_write_is_transient() {
    set_test_tempo("10");
    // A forwarded write (depth 1) answered on a node that knows it is
    // not the leader reports transient — the original writer owns
    // retry policy.
    let node1 = bootstrap_node("fw1", 1).await;
    let internal1 = node1.cfg.internal_addr.clone();
    let cfg2 = NodeConfig {
        id: 2,
        data_dir: std::env::temp_dir().join(format!("sb-nt-fw2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![internal1],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node2 = ClusterNode::start(cfg2).await.unwrap();
    wait_for("node2 joins", 60, || node1.topology().nodes.contains_key(&2)).await;
    wait_meta_ready(&node2).await;
    wait_leader_seen(&node2, 1, 30).await;

    let resp = node2
        .handle_request(InternalMessage::Request(InternalRequest::Forward {
            group: crate::META_GROUP,
            command: BrokerCommand::Meta(MetaCmd::DeclareQueue {
                vhost: "/".into(),
                name: "fwd".into(),
                passive: false,
                options: switchboard_core::model::QueueOptions::default(),
                owner: ConnectionId { node: 2, conn: 1 },
            }),
        }))
        .await;
    assert!(
        matches!(resp, InternalResponse::ForwardFailed(crate::proto::ForwardError::Transient)),
        "{resp:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconfigure_relay_success_arm() {
    set_test_tempo("1");
    // The one-hop reconfigure relay also maps SUCCESS replies.
    let node1 = bootstrap_node("rs1", 1).await;
    let internal1 = node1.cfg.internal_addr.clone();
    let cfg2 = NodeConfig {
        id: 2,
        data_dir: std::env::temp_dir().join(format!("sb-nt-rs2-{}-{}", std::process::id(), free_port())),
        client_addr: format!("127.0.0.1:{}", free_port()),
        internal_addr: format!("127.0.0.1:{}", free_port()),
        seeds: vec![internal1],
        bootstrap: false,
        expected_nodes: 1,
        peers: vec![],
    };
    let node2 = ClusterNode::start(cfg2).await.unwrap();
    wait_for("node2 joins", 60, || node1.topology().nodes.contains_key(&2)).await;
    wait_meta_ready(&node2).await;
    wait_leader_seen(&node2, 1, 30).await;

    // Leader peer answers Reconfigured: the relay surfaces success.
    // The local reconfigure must FAIL for the hop to trigger: driving
    // META toward an unknown voter set errors on the follower, whose
    // hint points at node1 — whose address we fake.
    let ok_peer = fake_peer(InternalMessage::Response(InternalResponse::Admin(
        crate::proto::AdminResponse::Reconfigured,
    )))
    .await;
    node2.reroute_peer(1, ok_peer).await;
    let resp = node2
        .handle_request(InternalMessage::Request(InternalRequest::Admin(
            AdminRequest::ReconfigureGroup {
                group: crate::META_GROUP,
                voters: vec![9],
                forwarded: false,
            },
        )))
        .await;
    assert!(
        matches!(resp, InternalResponse::Admin(crate::proto::AdminResponse::Reconfigured)),
        "{resp:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_group_leader_reports_exhaustion() {
    set_test_tempo("10");
    let node = bootstrap_node("wgl", 1).await;
    // Group 97 is a fresh raft core that never initializes: no leader
    // ever appears and the bounded wait reports the exhaustion.
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(40),
        node.wait_group_leader(97),
    )
    .await
    .expect("wait must terminate");
    assert!(
        matches!(&r, Err(ClusterError::Raft { message, .. }) if message.contains("no leader")),
        "{r:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_pooled_conn_retries_and_silence_times_out() {
    set_test_tempo("50");
    let node = bootstrap_node("pool", 1).await;
    wait_meta_ready(&node).await;
    // The first connection is answered (its conn parks in the pool);
    // every later connection is accepted and then says nothing.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut conn = crate::transport::accept(tcp, None).await.unwrap();
        if conn.read_frame_full().await.is_ok() {
            let pong = InternalMessage::Response(InternalResponse::Admin(AdminResponse::Pong));
            let _ = conn.write_frame(&bincode::serialize(&pong).unwrap()).await;
        }
        loop {
            let Ok((tcp, _)) = listener.accept().await else { return };
            tokio::spawn(async move {
                if let Ok(conn) = crate::transport::accept(tcp, None).await {
                    // Hold it: no read, no reply.
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    drop(conn);
                }
            });
        }
    });
    node.dir.update(42, addr);

    // First call: answered, connection parked for reuse.
    assert!(matches!(
        node.call(42, InternalRequest::Admin(AdminRequest::Ping)).await,
        Ok(InternalResponse::Admin(AdminResponse::Pong))
    ));

    // Second call: the parked conn is stale (its handler dropped it), so
    // the transport failure retries over a fresh dial; the fresh conn is
    // answered with silence, which must surface as a bounded timeout —
    // never a park.
    match node.call(42, InternalRequest::Admin(AdminRequest::Ping)).await {
        Err(ClusterError::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::TimedOut, "{e}")
        }
        other => panic!("expected a bounded timeout, got {other:?}"),
    }
}
