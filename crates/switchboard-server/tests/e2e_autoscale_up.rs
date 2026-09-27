//! Auto-scale-up test: a 3-node cluster grows to 9, one joiner at a time.
//!
//! After each join the test asserts the cluster reconfigured on its own:
//! the new node is registered everywhere, the meta leader extended the
//! shard layout to cover it (a fresh raft group anchored on the joiner,
//! still ≤3 voters), and the joiner accepts multi-master writes that land
//! on its own shard group.

mod support;

use std::collections::BTreeSet;

use switchboard_wire::BasicProperties;

use support::{
    basic_get, connect_and_open, declare_queue_everywhere, free_port, publish, spawn_client_accepts,
    start_broker_node, start_cluster, teardown_cluster, wait_until, TestClient,
};

#[tokio::test(flavor = "multi_thread")]
async fn cluster_autoscales_up_from_3_to_9_nodes() {
    support::init_tracing();
    let (mut nodes, mut listeners, mut addrs) = start_cluster("scaleup", 3).await;

    // Queues declared now stay pinned to their groups; declare one early
    // to prove placement is stable while the layout grows.
    let mut admin = connect_and_open(&addrs[0], "/").await.expect("open admin");
    declare_queue_everywhere(&mut admin, &nodes, "pinned").await;
    publish(&mut admin, 1, "", "pinned", &BasicProperties::new(), b"early", false)
        .await
        .unwrap();

    for id in 4..=9u64 {
        // Boot the joiner wired to the running cluster.
        let internal = format!("127.0.0.1:{}", free_port());
        let client = format!("127.0.0.1:{}", free_port());
        let existing = addrs_nodes_internal(&nodes);
        let peers: Vec<(u64, String)> = (1..id)
            .map(|p| (p, existing[p as usize - 1].clone()))
            .collect();
        let (node, listener) = start_broker_node(
            "scaleup",
            id,
            client.clone(),
            internal,
            vec![existing[0].clone()],
            false,
            9,
            peers,
        )
        .await;
        listeners.push(spawn_client_accepts(listener, node.clone()));
        nodes.push(node.clone());
        addrs.push(client);

        // The joiner registered itself through the seeds: visible everywhere.
        let ok = wait_until(
            || nodes.iter().all(|n| n.topology().nodes.contains_key(&id)),
            60,
        )
        .await;
        assert!(ok, "join {id}: node never became visible cluster-wide");

        // The meta leader extended the layout: some group now lists the
        // joiner as a member, and every node agrees on it.
        let ok = wait_until(
            || {
                nodes.iter().all(|n| {
                    n.topology()
                        .groups
                        .values()
                        .any(|m| m.contains(&id))
                })
            },
            60,
        )
        .await;
        assert!(ok, "join {id}: layout was never extended to cover the joiner");

        // Voter-cap invariant after the reconfiguration.
        for (g, members) in &nodes[0].topology().groups {
            assert!(
                members.len() <= switchboard_cluster::MAX_VOTERS,
                "join {id}: group {g} has {} members",
                members.len()
            );
        }
        // A beat for the new group's raft to elect through the joiner.
        tokio::time::sleep(std::time::Duration::from_millis(900)).await;

        // Multi-master through the freshest node: it declares its own
        // queue, publishes, and the message is retrievable from node 1.
        // (Plain publishes are fire-and-forget — §4.2 — so the consumer
        // polls until the write resolves.)
        let mut jc = connect_and_open(&addrs[id as usize - 1], "/").await.expect("open joiner");
        let queue = format!("q{id}");
        declare_queue_everywhere(&mut jc, &nodes, &queue).await;
        let body = format!("from-{id}");
        publish(&mut jc, 1, "", &queue, &BasicProperties::new(), body.as_bytes(), false)
            .await
            .unwrap();
        get_eventually(&mut admin, &queue, body.as_bytes()).await;

        // Coverage invariant: every registered node sits in ≥1 group.
        let topo = nodes[0].topology();
        let covered: BTreeSet<u64> = topo.groups.values().flatten().copied().collect();
        for known in topo.nodes.keys() {
            assert!(covered.contains(known), "node {known} hosts no group");
        }
    }

    // Final state: 9 nodes, and every one of them still serves writes.
    assert_eq!(nodes.len(), 9);
    let mut last = connect_and_open(&addrs[8], "/").await.expect("open last");
    publish(&mut last, 1, "", "pinned", &BasicProperties::new(), b"final", false)
        .await
        .unwrap();
    get_eventually(&mut admin, "pinned", b"early").await;
    get_eventually(&mut admin, "pinned", b"final").await;

    teardown_cluster(&nodes, listeners).await;
}

/// Poll a Basic.Get until `want` shows up (bounded); panics otherwise.
/// Publishes are asynchronous across connections, so a single immediate
/// get would race the enqueue's raft write.
async fn get_eventually(c: &mut TestClient, queue: &str, want: &[u8]) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "message {want:?} never arrived on {queue:?}"
        );
        match basic_get(c, 1, queue, true).await {
            Ok(Some((_, body))) => {
                assert_eq!(body, want, "payload mismatch on {queue:?}");
                return;
            }
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("get on {queue:?} failed: {e}"),
        }
    }
}

/// The internal addresses of `nodes`, in list order (from NodeConfig).
fn addrs_nodes_internal(nodes: &[std::sync::Arc<switchboard_cluster::ClusterNode>]) -> Vec<String> {
    nodes.iter().map(|n| n.cfg.internal_addr.clone()).collect()
}
