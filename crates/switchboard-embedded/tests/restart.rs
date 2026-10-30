//! Durable-restart suite — embedded design §7 tests 11-13.
//!
//! In-process restart is contractually impossible (§5.3: the node keeps
//! the RocksDB LOCK for the whole process), so each of these tests is a
//! SEPARATE PROCESS PER PHASE, using the standard self-exec pattern: the
//! parent spawns `std::env::current_exe()` with `--exact` filters selecting
//! just this test plus an environment-variable phase marker
//! (`SWITCHBOARD_EMBEDDED_RESTART_PHASE`; an argv marker would not work —
//! the child is a libtest/nextest binary that parses its arguments and
//! aborts on unrecognized flags). Each phase constructs a fresh `Broker`
//! on the same `data_dir` — exactly the supported restart path.

mod support;

use std::time::Duration;

use switchboard_embedded::Broker;
use switchboard_embedded::BrokerConfig;
use switchboard_embedded::ConsumeOptions;
use switchboard_embedded::MessageProperties;
use switchboard_embedded::QueueOptions;
use switchboard_wire::field::FieldTable;

use support::RESTART_PHASE_ENV;
use support::init;

fn persistent(body: &[u8]) -> (MessageProperties, Vec<u8>) {
    let mut p = MessageProperties::default();
    p.delivery_mode = Some(2);
    p.headers = Some(FieldTable::new());
    (p, body.to_vec())
}

async fn open(broker: &Broker) -> switchboard_embedded::EmbeddedChannel {
    let conn = broker.connect("guest", "guest").await.expect("connect");
    conn.open_channel().await.expect("channel")
}

// ===========================================================================
// 11. durable restart (graceful), two processes
// ===========================================================================

#[test]
fn restart_durable_graceful() {
    init();
    match std::env::var(RESTART_PHASE_ENV).as_deref() {
        Ok("durable-a") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                let broker =
                    Broker::start(BrokerConfig::new(dir).timeouts(support::test_timeouts()))
                        .await
                        .expect("phase A start");
                let ch = open(&broker).await;
                ch.queue_declare("persist-q", &QueueOptions::new().durable(true))
                    .await
                    .expect("declare");
                for i in 0..5u32 {
                    let (props, body) = persistent(format!("p{i}").as_bytes());
                    ch.publish("", "persist-q", props, body)
                        .await
                        .expect("publish");
                }
                // Consume + ack 2 of them.
                let first = ch.get("persist-q", false).await.expect("get").expect("p0");
                assert_eq!(first.body, b"p0");
                ch.ack(first.delivery_tag).await.expect("ack");
                let second = ch.get("persist-q", false).await.expect("get").expect("p1");
                assert_eq!(second.body, b"p1");
                ch.ack(second.delivery_tag).await.expect("ack");

                broker
                    .stop(Duration::from_secs(60))
                    .await
                    .expect("graceful stop");
            });
            // Explicit exit: lingering join handles must not hang the
            // libtest child after the phase body is done.
            std::process::exit(0);
        }
        Ok("durable-b") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                // Restart = a NEW process calling Broker::start on the SAME
                // data_dir (§5.3).
                let broker =
                    Broker::start(BrokerConfig::new(dir).timeouts(support::test_timeouts()))
                        .await
                        .expect("phase B start");
                let ch = open(&broker).await;
                // The 3 remaining persistent messages reappear, in order.
                for i in 2..5u32 {
                    let d = ch
                        .get("persist-q", true)
                        .await
                        .expect("get")
                        .unwrap_or_else(|| panic!("message p{i} lost across restart"));
                    assert_eq!(d.body, format!("p{i}").into_bytes());
                    assert_eq!(d.properties.delivery_mode, Some(2), "still persistent");
                }
                assert!(ch.get("persist-q", true).await.expect("get").is_none());
                broker.stop(Duration::from_secs(60)).await.expect("stop");
            });
            std::process::exit(0);
        }
        _ => {}
    }

    // ---- parent: run both phases as separate processes ----
    let dir = support::temp_dir("restart-durable");
    let a = support::spawn_phase("restart_durable_graceful", "durable-a", &dir);
    assert!(a.success(), "phase A failed: {a}");
    let b = support::spawn_phase("restart_durable_graceful", "durable-b", &dir);
    assert!(b.success(), "phase B failed: {b}");
}

// ===========================================================================
// 12. restart with outstanding hold (graceful) + credential skip
// ===========================================================================

#[test]
fn restart_hold_and_credential_skip() {
    init();
    match std::env::var(RESTART_PHASE_ENV).as_deref() {
        Ok("hold-a") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                // Phase A configures bootstrap_credentials("app", "pw1").
                let broker = Broker::start(
                    BrokerConfig::new(dir)
                        .bootstrap_credentials("app", "pw1")
                        .timeouts(support::test_timeouts()),
                )
                .await
                .expect("phase A start");
                // app/pw1 authenticates right after start (ensure-user
                // write, §5.1).
                let conn = broker.connect("app", "pw1").await.expect("app/pw1 connect");
                let ch = conn.open_channel().await.expect("channel");
                ch.queue_declare("hold-q", &QueueOptions::new().durable(true))
                    .await
                    .expect("declare");
                let (props, body) = persistent(b"held-message");
                ch.publish("", "hold-q", props, body)
                    .await
                    .expect("publish");
                // Consume WITHOUT ack, then stop: the connection close
                // requeues the hold (methods/tx.rs:123-167).
                let d = ch.get("hold-q", false).await.expect("get").expect("held");
                assert_eq!(d.body, b"held-message");
                assert!(!d.redelivered);
                broker
                    .stop(Duration::from_secs(60))
                    .await
                    .expect("graceful stop");
            });
            std::process::exit(0);
        }
        Ok("hold-b") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                // Phase B reconfigures to ("app", "pw2"): the user EXISTS,
                // so NOTHING is written and pw1 survives (the §5.1 skip
                // rule).
                let broker = Broker::start(
                    BrokerConfig::new(dir)
                        .bootstrap_credentials("app", "pw2")
                        .timeouts(support::test_timeouts()),
                )
                .await
                .expect("phase B start");
                let conn = broker
                    .connect("app", "pw1")
                    .await
                    .expect("pw1 survives the pw2 reconfiguration (skip rule)");
                let ch = conn.open_channel().await.expect("channel");

                // pw2 must NOT authenticate: nothing was written.
                let refused = broker.connect("app", "pw2").await;
                assert!(
                    matches!(
                        refused,
                        Err(switchboard_embedded::ConnectError::AccessRefused)
                    ),
                    "pw2 was correctly NOT written, got {refused:?}"
                );

                // The outstanding hold was requeued by the graceful stop;
                // the new consumer receives it with redelivered = true.
                let mut redelivered = None;
                for _ in 0..100 {
                    if let Ok(Some(d)) = ch.get("hold-q", true).await {
                        assert_eq!(d.body, b"held-message");
                        assert!(d.redelivered, "requeued across restart");
                        redelivered = Some(d);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                assert!(
                    redelivered.is_some(),
                    "held message redelivered after restart"
                );
                broker.stop(Duration::from_secs(60)).await.expect("stop");
            });
            std::process::exit(0);
        }
        _ => {}
    }

    // ---- parent ----
    let dir = support::temp_dir("restart-hold");
    let a = support::spawn_phase("restart_hold_and_credential_skip", "hold-a", &dir);
    assert!(a.success(), "phase A failed: {a}");
    let b = support::spawn_phase("restart_hold_and_credential_skip", "hold-b", &dir);
    assert!(b.success(), "phase B failed: {b}");
}

// ===========================================================================
// 13. hard restart (no stop): held messages release via sub-id collision
// ===========================================================================

#[test]
fn restart_hard_no_stop() {
    init();
    match std::env::var(RESTART_PHASE_ENV).as_deref() {
        Ok("hard-a") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                let broker =
                    Broker::start(BrokerConfig::new(dir).timeouts(support::test_timeouts()))
                        .await
                        .expect("phase A start");
                let conn = broker.connect("guest", "guest").await.expect("connect");
                let ch = conn.open_channel().await.expect("channel");
                ch.queue_declare("hard-q", &QueueOptions::new().durable(true))
                    .await
                    .expect("declare");
                let (props, body) = persistent(b"orphan-hold");
                ch.publish("", "hard-q", props, body)
                    .await
                    .expect("publish");
                // Consume-and-HOLD via a real consumer: its subscription id
                // is {node: 1, sub: 1} (node-global counter restarting at 1
                // per process), and the unacked hold persists inside the SM
                // blob (whole-BrokerState persist). A basic.get hold would
                // be keyed under GET_HOLDER_NODE (u64::MAX,
                // shard/mod.rs:42, basic.rs:63-69) and could never collide
                // with a consumer sub — the release mechanism (R4) is
                // consumer-keyed.
                let mut consumer = ch
                    .consume("hard-q", &ConsumeOptions::new().no_ack(false))
                    .await
                    .expect("consume");
                let d = support::with_timeout(consumer.next(), 30)
                    .await
                    .expect("held delivery");
                assert_eq!(d.body, b"orphan-hold");
                assert!(!d.redelivered);
                // Every raft apply already persisted before the write
                // resolved, so dying without stop is safe for the data.
                std::process::abort();
            });
            unreachable!("abort never returns");
        }
        Ok("hard-b") => {
            let rt = support::child_runtime();
            rt.block_on(async {
                let dir = support::restart_dir();
                let broker =
                    Broker::start(BrokerConfig::new(dir).timeouts(support::test_timeouts()))
                        .await
                        .expect("phase B start");
                let conn = broker.connect("guest", "guest").await.expect("connect");
                let ch = conn.open_channel().await.expect("channel");

                // The hold must be re-delivered via the orphan-hold release
                // on sub re-registration (shard/mod.rs:362-367): subscription
                // ids restart at 1 per process (node/mod.rs:283-284, 339), so
                // this process's FIRST consumer on the same shard
                // re-registers the dead process's id {1, 1} and frees its
                // holds (R4; janitor exempts self, node/mod.rs:2215-2218).
                let mut consumer = ch
                    .consume("hard-q", &ConsumeOptions::new().no_ack(false))
                    .await
                    .expect("consume");
                let d = support::with_timeout(consumer.next(), 60)
                    .await
                    .unwrap_or_else(|| {
                        panic!(
                            "orphan-held message re-delivered after a hard restart \
                         (release_orphan_holds on sub-id collision, shard/mod.rs:362-367)"
                        )
                    });
                assert_eq!(d.body, b"orphan-hold");
                assert!(d.redelivered, "orphan release sets redelivered");
                broker.stop(Duration::from_secs(60)).await.expect("stop");
            });
            std::process::exit(0);
        }
        _ => {}
    }

    // ---- parent: phase A aborts (SIGABRT), phase B must succeed ----
    let dir = support::temp_dir("restart-hard");
    let a = support::spawn_phase("restart_hard_no_stop", "hard-a", &dir);
    assert!(!a.success(), "phase A must die by abort(), got {a}");
    let b = support::spawn_phase("restart_hard_no_stop", "hard-b", &dir);
    assert!(b.success(), "phase B failed: {b}");
}
