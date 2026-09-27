//! Auto-scale-down test: a 9-node cluster sheds nodes one at a time down
//! to 3, with the cluster reconfiguring itself as each node leaves.
//!
//! Each departure runs [`ClusterNode::leave`]: the node is forgotten in
//! meta (directory + shard-group member lists) and its raft cores stop.
//! The layout never loses groups, so queue→group placement is stable, and
//! every 3-voter group keeps quorum through any single departure — writes
//! must keep landing everywhere after every round.

mod support;

use switchboard_wire::BasicProperties;

use support::{
    basic_get, connect_and_open, declare_queue_everywhere, publish, start_cluster,
    teardown_cluster, wait_until, TestClient,
};

#[tokio::test(flavor = "multi_thread")]
async fn cluster_autoscales_down_from_9_to_3_nodes() {
    support::init_tracing();
    let (nodes, listeners, addrs) = start_cluster("scaledown", 9).await;

    // Workload queue declared at full size; its group must keep quorum
    // through every departure below.
    let mut admin = connect_and_open(&addrs[0], "/").await.expect("open admin");
    declare_queue_everywhere(&mut admin, &nodes, "dd").await;

    // Nodes 9..4 leave, one at a time. Every round: the departed node
    // disappears from every survivor's topology and group member lists,
    // the voter cap still holds, and a fresh write→read round-trip works.
    for id in (4..=9u64).rev() {
        nodes[id as usize - 1].leave().await.expect("leave");

        // Registration gone everywhere (survivors only — the leaver is
        // winding down and no longer counts).
        let survivors = &nodes[..id as usize - 1];
        let ok = wait_until(
            || survivors.iter().all(|n| !n.topology().nodes.contains_key(&id)),
            60,
        )
        .await;
        assert!(ok, "leave {id}: node still registered somewhere");

        // Group member lists no longer mention the departed node.
        let ok = wait_until(
            || {
                survivors.iter().all(|n| {
                    n.topology()
                        .groups
                        .values()
                        .all(|m| !m.contains(&id))
                })
            },
            60,
        )
        .await;
        assert!(ok, "leave {id}: node still listed in some group");

        // Voter-cap invariant after the reconfiguration.
        for (g, members) in &survivors[0].topology().groups {
            assert!(
                members.len() <= switchboard_cluster::MAX_VOTERS,
                "leave {id}: group {g} has {} members",
                members.len()
            );
            assert!(!members.is_empty(), "leave {id}: group {g} left empty");
        }

        // Self-healing: every group refills to 3 live members (the meta
        // view converges first; raft membership follows on the controller's
        // ticks).
        let survivor_ids: Vec<u64> = (1..id).collect();
        let ok = wait_until(
            || {
                survivors.iter().all(|n| {
                    n.topology().groups.values().all(|m| {
                        m.len() == switchboard_cluster::MAX_VOTERS
                            && m.iter().all(|x| survivor_ids.contains(x))
                    })
                })
            },
            60,
        )
        .await;
        assert!(ok, "leave {id}: groups did not refill with live members");
        // A beat for the raft side (learner catch-up + voter promotion).
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

        // The cluster keeps serving writes through distinct survivors.
        // (Publishes are fire-and-forget across connections, so poll.)
        let mut w = connect_and_open(&addrs[id as usize - 2], "/").await.expect("open writer");
        let body = format!("round-{id}");
        publish(&mut w, 1, "", "dd", &BasicProperties::new(), body.as_bytes(), false)
            .await
            .unwrap();
        get_eventually(&mut admin, "dd", body.as_bytes()).await;
    }

    // Final state: 3 nodes remain, topology agrees, and multi-master
    // writes still work through every survivor.
    let survivors = &nodes[..3];
    assert_eq!(survivors[0].topology().nodes.len(), 3);
    let mut clients: Vec<TestClient> = Vec::new();
    for addr in addrs.iter().take(3) {
        clients.push(connect_and_open(addr, "/").await.expect("open survivor"));
    }
    for (i, c) in clients.iter_mut().enumerate() {
        publish(c, 1, "", "dd", &BasicProperties::new(), format!("survivor{i}").as_bytes(), false)
            .await
            .unwrap();
    }
    // Enqueues from three distinct connections race, so drain the queue
    // and assert on the set, not on FIFO order.
    let mut drained: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while drained.len() < 3 {
        assert!(tokio::time::Instant::now() < deadline, "survivor messages incomplete: {drained:?}");
        match basic_get(&mut admin, 1, "dd", true).await {
            Ok(Some((_, body))) => drained.push(body),
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            Err(e) => panic!("final get failed: {e}"),
        }
    }
    drained.sort();
    assert_eq!(
        drained,
        vec![b"survivor0".to_vec(), b"survivor1".to_vec(), b"survivor2".to_vec()],
        "exactly the survivor writes must land"
    );

    teardown_cluster(&nodes[..3], listeners).await;
}

/// Poll a Basic.Get until `want` shows up (bounded); panics otherwise.
/// Publishes are asynchronous across connections: a single immediate get
/// would race the enqueue's raft write.
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
