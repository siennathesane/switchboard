//! Regression tests for the lost-`CreateQueueData` drive.
//!
//! Between the meta `DeclareQueue` commit and the creating node's
//! shard-side `CreateQueueData` write there is a window: if the declaring
//! node is paused or killed there (chaos nemesis SIGSTOP/restart), the
//! queue exists in meta forever but has no shard-side data. Before the
//! heal existed, nothing ever re-drove `CreateQueueData` (an existing
//! declare replies `created: false` and carries no shard command), so
//! every subsequent delete and passive declare 404'd "on this shard" for
//! the life of the run — the jepsen `topo x chaos` runs of 2026-10-09
//! collapsed exactly this way (`:delete` ok-count 0 across 61 ops, every
//! one `:fail` "delete lost the CreateQueueData race").
//!
//! The tests inject that exact state deterministically: commit the meta
//! write alone (the wire path issues the two writes back-to-back; the
//! injection reproduces the crash between them), then drive the queue
//! lifecycle over the real wire protocol.

mod support;

use std::time::Duration;

use support::{basic_get, connect_and_open, publish, start_broker};

use switchboard_cluster::{BrokerCommand, BrokerReply, META_GROUP};
use switchboard_core::model::{ConnectionId, QueueOptions};
use switchboard_core::shard::ShardCmd;
use switchboard_core::topology::{MetaCmd, MetaReply};
use switchboard_wire::method::Method;

/// Commit a meta-only `DeclareQueue` (no shard-side `CreateQueueData`):
/// the exact durable state a node crash/pause between the two writes of
/// `queue_declare` leaves behind. Returns the queue's shard group.
async fn inject_meta_only_queue(
    node: &std::sync::Arc<switchboard_cluster::ClusterNode>,
    name: &str,
) -> u32 {
    let reply = node
        .write(
            META_GROUP,
            BrokerCommand::Meta(MetaCmd::DeclareQueue {
                vhost: "/".into(),
                name: name.into(),
                passive: false,
                options: QueueOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    arguments: Default::default(),
                },
                owner: ConnectionId { node: 0, conn: 0 },
            }),
        )
        .await
        .expect("meta declare write");
    match reply {
        BrokerReply::Meta(MetaReply::QueueDeclared {
            shard,
            created: true,
            ..
        }) => {
            node.refresh_topology().await;
            shard
        }
        other => panic!("expected QueueDeclared(created), got {other:?}"),
    }
}

/// Assert the shard group really has no data for `queue` — the bug's
/// precondition.
async fn assert_shard_data_missing(
    node: &std::sync::Arc<switchboard_cluster::ClusterNode>,
    shard: u32,
    queue: &str,
) {
    let reply = node
        .write(
            shard,
            BrokerCommand::Shard(ShardCmd::Stats {
                queue: queue.into(),
            }),
        )
        .await
        .expect("stats write reachable");
    match reply {
        BrokerReply::Error(e) => assert_eq!(e.code, 404, "expected shard 404, got {e:?}"),
        other => panic!("expected shard 404 for {queue:?}, got {other:?}"),
    }
}

async fn declare_wire(
    c: &mut support::TestClient,
    queue: &str,
    passive: bool,
) -> Result<Method, String> {
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: queue.into(),
            passive,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    c.expect(1).await
}

async fn delete_wire(c: &mut support::TestClient, queue: &str) -> Result<Method, String> {
    c.send_method(
        1,
        &Method::QueueDelete {
            ticket: 0,
            queue: queue.into(),
            if_unused: false,
            if_empty: false,
            nowait: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    c.expect(1).await
}

/// The jepsen `:declare` op against a lost-drive queue: an ACTIVE declare
/// must heal the missing shard data (idempotent, raft-durable re-drive)
/// and return QueueDeclareOk — not close the channel with 404 "on this
/// shard". Afterwards the queue is fully usable (passive declare, publish
/// + get, delete) and a fresh queue's lifecycle is unaffected.
#[tokio::test(flavor = "multi_thread")]
async fn active_declare_heals_queue_missing_shard_data() {
    let (node, addr) = start_broker("lostdrive-declare").await;

    let shard = inject_meta_only_queue(&node, "stuck").await;
    assert_shard_data_missing(&node, shard, "stuck").await;

    let mut c = connect_and_open(&addr, "/").await.expect("open");

    // Active declare: heals and succeeds.
    let got = declare_wire(&mut c, "stuck", false).await.expect("declare");
    let Method::QueueDeclareOk { queue, .. } = got else {
        panic!("expected QueueDeclareOk (healed), got {got:?}");
    };
    assert_eq!(queue, "stuck");

    // The jepsen `:exists` read: passive declare now sees the queue.
    let got = declare_wire(&mut c, "stuck", true).await.expect("passive declare");
    let Method::QueueDeclareOk { .. } = got else {
        panic!("expected passive QueueDeclareOk after heal, got {got:?}");
    };

    // The healed queue is usable: publish + get roundtrip.
    publish(&mut c, 1, "", "stuck", &switchboard_wire::BasicProperties::new(), b"healed", false)
        .await
        .unwrap();
    let got = basic_get(&mut c, 1, "stuck", true).await.unwrap();
    assert_eq!(got.map(|(_, body)| body), Some(b"healed".to_vec()));

    // The jepsen `:delete` op against the healed queue succeeds.
    let got = delete_wire(&mut c, "stuck").await.expect("delete");
    let Method::QueueDeleteOk { .. } = got else {
        panic!("expected QueueDeleteOk, got {got:?}");
    };

    // Control: a fresh queue's normal lifecycle is untouched.
    let got = declare_wire(&mut c, "fresh", false).await.expect("fresh declare");
    let Method::QueueDeclareOk { queue, .. } = got else {
        panic!("expected QueueDeclareOk, got {got:?}");
    };
    assert_eq!(queue, "fresh");
    let got = delete_wire(&mut c, "fresh").await.expect("fresh delete");
    let Method::QueueDeleteOk { .. } = got else {
        panic!("expected QueueDeleteOk, got {got:?}");
    };
}

/// The jepsen `:delete` op against a lost-drive queue: the shard's 404
/// (missing queue data) proves the queue holds zero messages and has zero
/// consumers — messages and subscriptions both require queue data — so
/// the `if_empty`/`if_unused` precheck is vacuously satisfied and the
/// delete must succeed, leaving the queue absent at the meta level (the
/// client's "in vhost" 404 on a later passive declare).
#[tokio::test(flavor = "multi_thread")]
async fn delete_succeeds_when_shard_data_missing() {
    let (node, addr) = start_broker("lostdrive-delete").await;

    let shard = inject_meta_only_queue(&node, "orphan").await;
    assert_shard_data_missing(&node, shard, "orphan").await;

    let mut c = connect_and_open(&addr, "/").await.expect("open");

    // Delete of a lost-drive queue succeeds (message_count 0).
    let got = delete_wire(&mut c, "orphan").await.expect("delete");
    let Method::QueueDeleteOk { message_count } = got else {
        panic!("expected QueueDeleteOk, got {got:?}");
    };
    assert_eq!(message_count, 0);

    // The queue is gone from meta: a passive declare answers the
    // meta-level 404 ("in vhost" — the client's definitively-absent
    // classification), not the shard-level one.
    let got = declare_wire(&mut c, "orphan", true).await.expect("passive declare");
    match got {
        Method::ChannelClose {
            reply_code,
            reply_text,
            ..
        } => {
            assert_eq!(reply_code, 404);
            assert!(
                reply_text.contains("in vhost"),
                "expected meta-level 404, got: {reply_text}"
            );
            let _ = c.send_method(1, &Method::ChannelCloseOk {}).await;
        }
        other => panic!("expected Channel.Close 404, got {other:?}"),
    }
    // Let the ChannelCloseOk drain before teardown.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
