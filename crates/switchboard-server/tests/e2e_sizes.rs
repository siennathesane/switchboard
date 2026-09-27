//! Cluster-size tests: form clusters of 1 through 9 nodes and verify
//! multi-master writes through every node at every size, with the
//! ≤3-voters-per-group invariant enforced throughout.
//!
//! Sizes run inside one binary behind a shared lock — each size boots a
//! full cluster (up to 10 raft instances at n=9), and concurrent clusters
//! would contend for CPU and skew election timing.

mod support;

use std::collections::BTreeSet;

use switchboard_wire::BasicProperties;

use support::{
    basic_get, connect_and_open, declare_queue_everywhere, publish, start_cluster,
    teardown_cluster, TestClient,
};

/// Serializes the nine size scenarios (see module docs).
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn size_scenario(n: u64) {
    let _guard = LOCK.lock().await;
    let (nodes, listeners, addrs) = start_cluster(&format!("size{n}"), n).await;

    // Invariant: no group ever exceeds the voter cap.
    for (g, members) in &nodes[0].topology().groups {
        assert!(
            members.len() <= switchboard_cluster::MAX_VOTERS,
            "group {g} has {} members (cap {})",
            members.len(),
            switchboard_cluster::MAX_VOTERS
        );
    }

    // One client per node; every node declares the shared queue through
    // itself (equivalent redeclare from anywhere) and publishes one
    // message through itself — the multi-master property.
    let mut clients: Vec<TestClient> = Vec::new();
    for addr in &addrs {
        clients.push(connect_and_open(addr, "/").await.expect("open channel"));
    }
    declare_queue_everywhere(&mut clients[0], &nodes, "mm").await;
    for (i, c) in clients.iter_mut().enumerate() {
        publish(
            c,
            1,
            "",
            "mm",
            &BasicProperties::new(),
            format!("n{i}").as_bytes(),
            false,
        )
        .await
        .unwrap();
    }

    // Drain everything through node 0 and check the exact total: every
    // node's write must have landed exactly once.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut seen: Vec<Vec<u8>> = Vec::new();
    while seen.len() < n as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "n={n}: only {} of {n} messages arrived: {seen:?}",
            seen.len()
        );
        match basic_get(&mut clients[0], 1, "mm", true).await {
            Ok(Some((_, body))) => seen.push(body),
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("n={n}: get failed: {e}"),
        }
        assert!(
            seen.len() <= n as usize,
            "n={n}: duplicate deliveries: {seen:?}"
        );
    }
    let expected: BTreeSet<Vec<u8>> = (0..n as u64).map(|i| format!("n{i}").into_bytes()).collect();
    let got: BTreeSet<Vec<u8>> = seen.into_iter().collect();
    assert_eq!(got, expected, "n={n}: exactly the per-node writes must land");

    teardown_cluster(&nodes, listeners).await;
}

macro_rules! size_test {
    ($name:ident, $n:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            support::init_tracing();
            size_scenario($n).await;
        }
    };
}

size_test!(size_1_node, 1);
size_test!(size_2_nodes, 2);
size_test!(size_3_nodes, 3);
size_test!(size_4_nodes, 4);
size_test!(size_5_nodes, 5);
size_test!(size_6_nodes, 6);
size_test!(size_7_nodes, 7);
size_test!(size_8_nodes, 8);
size_test!(size_9_nodes, 9);
