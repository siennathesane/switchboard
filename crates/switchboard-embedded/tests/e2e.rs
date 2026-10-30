//! In-process e2e suite — embedded design §7 tests 1-10.
//!
//! Every test here is pure in-process: `Broker::start` + `connect`; the
//! only sockets are the node's ephemeral internal bind and the duplex
//! (not a socket).

mod support;

use std::time::Duration;

use switchboard_embedded::Broker;
use switchboard_embedded::BrokerConfig;
use switchboard_embedded::ConfirmOutcome;
use switchboard_embedded::ConnectError;
use switchboard_embedded::ConsumeOptions;
use switchboard_embedded::MessageProperties;
use switchboard_embedded::OpError;
use switchboard_embedded::QueueDeleteOptions;
use switchboard_embedded::QueueOptions;
use switchboard_embedded::StartError;
use switchboard_wire::field::FieldTable;

use support::init;

fn persistent_props() -> MessageProperties {
    let mut p = MessageProperties::default();
    p.delivery_mode = Some(2);
    p.content_type = Some("text/plain".into());
    p
}

async fn start_broker(name: &str) -> Broker {
    Broker::start(BrokerConfig::new(support::temp_dir(name)).timeouts(support::test_timeouts()))
        .await
        .expect("broker start")
}

fn is_access_refused(e: &OpError) -> bool {
    matches!(e, OpError::AccessRefused { .. })
}

// ---------------------------------------------------------------------------
// 1. declare / publish / consume / ack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn declare_publish_consume_ack() {
    init();
    let broker = start_broker("e2e-1").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");

    let state = ch
        .queue_declare("orders", &QueueOptions::new().durable(true))
        .await
        .expect("declare");
    assert_eq!(state.queue, "orders");
    assert_eq!(state.message_count, 0);
    ch.queue_bind("orders", "amq.direct", "orders-key")
        .await
        .expect("bind");

    // Prefetch BEFORE the consumer exists: with global=false it applies to
    // new consumers (methods/mod.rs:161-172).
    ch.qos(10, false).await.expect("qos");

    for i in 0..3u32 {
        let mut props = persistent_props();
        props.app_id = Some(format!("pub-{i}"));
        ch.publish(
            "amq.direct",
            "orders-key",
            props,
            format!("m{i}").into_bytes(),
        )
        .await
        .expect("publish");
    }

    let mut consumer = ch
        .consume("orders", &ConsumeOptions::new().tag("c1").no_ack(false))
        .await
        .expect("consume");
    assert_eq!(consumer.tag, "c1");

    for i in 0..3u32 {
        let d = support::with_timeout(consumer.next(), 30)
            .await
            .unwrap_or_else(|| panic!("delivery {i} never arrived"));
        assert_eq!(d.body, format!("m{i}").into_bytes(), "in-order bodies");
        assert_eq!(d.exchange, "amq.direct");
        assert_eq!(d.routing_key, "orders-key");
        assert_eq!(d.consumer_tag, "c1");
        assert_eq!(d.properties.delivery_mode, Some(2));
        assert_eq!(
            d.properties.app_id.as_deref(),
            Some(format!("pub-{i}").as_str())
        );
        assert!(!d.redelivered);
        ch.ack(d.delivery_tag).await.expect("ack");
    }

    // Queue drained; the consumer still registered.
    let st = ch
        .queue_declare("orders", &QueueOptions::new().passive(true))
        .await
        .expect("passive declare");
    assert_eq!(st.message_count, 0, "queue drains after acks");
    assert_eq!(st.consumer_count, 1);

    consumer.cancel().await.expect("cancel consumer");
    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 2. get / ack / nack-requeue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_ack_nack_requeue() {
    init();
    let broker = start_broker("e2e-2").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");

    ch.queue_declare("pull-q", &QueueOptions::new().durable(true))
        .await
        .expect("declare");

    // Empty queue: Basic.Get-Empty → Ok(None).
    assert!(ch.get("pull-q", true).await.expect("get").is_none());

    for i in 0..2u32 {
        ch.publish(
            "",
            "pull-q",
            persistent_props(),
            format!("g{i}").into_bytes(),
        )
        .await
        .expect("publish");
    }

    // First message: get + ack.
    let d1 = ch.get("pull-q", false).await.expect("get").expect("some");
    assert_eq!(d1.body, b"g0");
    assert!(!d1.redelivered);
    ch.ack(d1.delivery_tag).await.expect("ack");

    // Second message: nack with requeue → redelivered.
    let d2 = ch.get("pull-q", false).await.expect("get").expect("some");
    assert_eq!(d2.body, b"g1");
    ch.nack(d2.delivery_tag, true).await.expect("nack");
    let mut got_redelivery = None;
    for _ in 0..50 {
        if let Ok(Some(d)) = ch.get("pull-q", false).await {
            assert_eq!(d.body, b"g1");
            assert!(d.redelivered, "nack-requeue sets the redelivered flag");
            got_redelivery = Some(d);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let d = got_redelivery.expect("redelivered message");
    ch.ack(d.delivery_tag).await.expect("ack");

    // nack with requeue=false: dead-letter path (drop; no DLX configured,
    // shard/mod.rs:724-750).
    ch.publish("", "pull-q", persistent_props(), b"drop-me".to_vec())
        .await
        .expect("publish");
    let d = ch.get("pull-q", false).await.expect("get").expect("some");
    assert_eq!(d.body, b"drop-me");
    ch.nack(d.delivery_tag, false).await.expect("nack");
    let mut empty = false;
    for _ in 0..50 {
        if matches!(ch.get("pull-q", false).await, Ok(None)) {
            empty = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(empty, "nack(requeue=false) drops the message");

    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 3. tx commit AND rollback (both explicitly required, §7/3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tx_commit_and_rollback() {
    init();
    let broker = start_broker("e2e-3").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");

    ch.queue_declare("tx-q", &QueueOptions::new().durable(true))
        .await
        .expect("declare");
    ch.tx_select().await.expect("tx_select");

    // --- COMMIT: publishes buffer, then become visible ---
    ch.publish("", "tx-q", persistent_props(), b"a".to_vec())
        .await
        .expect("buffered publish");
    ch.publish("", "tx-q", persistent_props(), b"b".to_vec())
        .await
        .expect("buffered publish");
    // Nothing visible before commit (buffered at methods/publish.rs:131-147).
    assert!(ch.get("tx-q", true).await.expect("get").is_none());
    ch.tx_commit().await.expect("tx_commit");
    let a = ch.get("tx-q", true).await.expect("get").expect("a");
    assert_eq!(a.body, b"a");
    let b = ch.get("tx-q", true).await.expect("get").expect("b");
    assert_eq!(b.body, b"b");
    assert!(ch.get("tx-q", true).await.expect("get").is_none());

    // --- ROLLBACK: buffered publishes vanish ---
    ch.publish("", "tx-q", persistent_props(), b"c".to_vec())
        .await
        .expect("buffered publish");
    ch.publish("", "tx-q", persistent_props(), b"d".to_vec())
        .await
        .expect("buffered publish");
    ch.tx_rollback().await.expect("tx_rollback");
    assert!(ch.get("tx-q", true).await.expect("get").is_none());

    // --- Buffered ack inside a rolled-back tx leaves the message
    //     unacked/still held (§2.2.9 rule, methods/mod.rs:452-466). ---
    ch.publish("", "tx-q", persistent_props(), b"held".to_vec())
        .await
        .expect("publish");
    ch.tx_commit().await.expect("commit");
    let d = ch.get("tx-q", false).await.expect("get").expect("held");
    assert_eq!(d.body, b"held");
    ch.ack(d.delivery_tag).await.expect("buffered ack");
    ch.tx_rollback().await.expect("rollback drops the ack");
    assert!(
        ch.get("tx-q", false).await.expect("get").is_none(),
        "the ack was rolled back: the message is still held, not requeued"
    );

    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 4. publisher confirms
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publisher_confirms() {
    init();
    let broker = start_broker("e2e-4").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");

    ch.queue_declare("cf-q", &QueueOptions::new().durable(true))
        .await
        .expect("declare");
    ch.queue_bind("cf-q", "amq.direct", "cf-key")
        .await
        .expect("bind");

    ch.confirm_select().await.expect("confirm_select");

    // N confirmed publishes resolve Acked; sequential awaiting exercises
    // the per-channel confirm sequence (1..N in order — the waiter for
    // seq k only resolves on the ack for k, channel/mod.rs:123-129).
    for i in 0..5u32 {
        let outcome = ch
            .publish_confirmed(
                "amq.direct",
                "cf-key",
                persistent_props(),
                format!("cf{i}").into_bytes(),
            )
            .await
            .expect("confirmed publish");
        assert_eq!(outcome, ConfirmOutcome::Acked, "seq {i} acked");
    }
    let depth = ch
        .queue_declare("cf-q", &QueueOptions::new().passive(true))
        .await
        .expect("passive declare");
    assert_eq!(depth.message_count, 5);

    // A pipelined pair still correlates per sequence.
    let (r1, r2) = tokio::join!(
        ch.publish_confirmed("amq.direct", "cf-key", persistent_props(), b"p1".to_vec()),
        ch.publish_confirmed("amq.direct", "cf-key", persistent_props(), b"p2".to_vec()),
    );
    assert_eq!(r1.expect("p1"), ConfirmOutcome::Acked);
    assert_eq!(r2.expect("p2"), ConfirmOutcome::Acked);

    // Mandatory unroutable publish yields Returned AND is still confirmed
    // (methods/publish.rs:83-99): one outcome, return + confirm.
    let outcome = ch
        .publish_confirmed(
            "amq.direct",
            "no-such-binding",
            persistent_props(),
            b"lost".to_vec(),
        )
        .await
        .expect("confirmed publish");
    assert_eq!(
        outcome,
        ConfirmOutcome::Returned {
            code: 312,
            text: "NO_ROUTE".into()
        }
    );

    // A plain publish interleaved in confirm mode advances the sequence
    // (methods/mod.rs:143-153); the next confirmed publish still lands.
    ch.publish(
        "amq.direct",
        "cf-key",
        persistent_props(),
        b"plain".to_vec(),
    )
    .await
    .expect("plain publish in confirm mode");
    let outcome = ch
        .publish_confirmed(
            "amq.direct",
            "cf-key",
            persistent_props(),
            b"after".to_vec(),
        )
        .await
        .expect("confirmed publish");
    assert_eq!(outcome, ConfirmOutcome::Acked);

    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 5. authz: handshake-stage errors + permission gates + the publish gap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn authz_errors_and_gates() {
    init();
    let broker = start_broker("e2e-5").await;

    // --- stage-of-handshake mapping (§4.3) ---
    let err = broker
        .connect("guest", "wrong-password")
        .await
        .expect_err("refused");
    assert!(
        matches!(err, ConnectError::AccessRefused),
        "wrong password folds into AccessRefused, got {err:?}"
    );
    let err = broker
        .connect_vhost("guest", "guest", "/nope")
        .await
        .expect_err("no vhost");
    assert!(
        matches!(&err, ConnectError::NoSuchVhost(v) if v == "/nope"),
        "unknown vhost maps to NoSuchVhost, got {err:?}"
    );
    // A wrong password can NOT be mistaken for a vhost error: it fails
    // before Tune-Ok is ever sent (EOF after Start-Ok).

    // --- a restricted user ---
    broker
        .create_user("restricted", "rpw")
        .await
        .expect("create_user");
    broker
        .set_permissions(
            "restricted",
            "/",
            switchboard_embedded::Permissions {
                configure: "^allowed-.*".into(),
                write: "^allowed-.*".into(),
                read: "^allowed-.*".into(),
            },
        )
        .await
        .expect("set_permissions");

    // Pre-declare the denied queue as the unrestricted bootstrap user so
    // the Read-gated ops below hit the permission check, not a 404.
    let gconn = broker
        .connect("guest", "guest")
        .await
        .expect("guest connect");
    let gch = gconn.open_channel().await.expect("guest channel");
    gch.queue_declare("denied-q", &QueueOptions::new().durable(true))
        .await
        .expect("guest declare");

    let rconn = broker
        .connect("restricted", "rpw")
        .await
        .expect("restricted connect");

    // Every refused op CLOSES its channel (§4.8.1), so each gate gets a
    // fresh channel.
    let rch = rconn.open_channel().await.expect("channel");
    let err = rch
        .queue_declare("denied-other", &QueueOptions::new())
        .await
        .expect_err("declare gate");
    assert!(
        is_access_refused(&err),
        "queue_declare gate (Configure), got {err:?}"
    );

    let rch = rconn.open_channel().await.expect("channel");
    let err = rch.get("denied-q", true).await.expect_err("read gate");
    assert!(
        is_access_refused(&err),
        "basic.get gate (Read), got {err:?}"
    );

    let rch = rconn.open_channel().await.expect("channel");
    let err = rch
        .consume("denied-q", &ConsumeOptions::new())
        .await
        .expect_err("read gate");
    assert!(
        is_access_refused(&err),
        "basic.consume gate (Read), got {err:?}"
    );

    let rch = rconn.open_channel().await.expect("channel");
    let err = rch.queue_purge("denied-q").await.expect_err("read gate");
    assert!(
        is_access_refused(&err),
        "queue_purge gate (Read), got {err:?}"
    );

    let rch = rconn.open_channel().await.expect("channel");
    let err = rch
        .queue_delete("denied-q", &QueueDeleteOptions::new())
        .await
        .expect_err("configure gate");
    assert!(
        is_access_refused(&err),
        "queue_delete gate (Configure), got {err:?}"
    );

    // GAP FLIPPED (embedded design §6/§7-5): the publish gap this test
    // used to pin landed in the control-plane hardening — Basic.Publish
    // now requires `write` on the exchange resource
    // (methods/publish.rs:54-69, `authorize_ch(.., Perm::Write, &exchange)`).
    // The restricted user's write regex (`^allowed-.*`) does not match the
    // default exchange `""`, so the publish to a denied-* queue is refused
    // before any routing work and nothing enqueues.
    //
    // Basic.Publish itself is fire-and-forget, so the gate is observed
    // through `publish_confirmed`: the 403 Channel.Close fails the
    // outstanding confirm waiter with the decoded close error
    // (driver.rs `fail_channel_waiters`; error.rs `decode_channel_close`
    // maps 403 to OpError::AccessRefused). That resolution is race-free:
    // the waiter is registered before the publish frames are written.
    let rch = rconn.open_channel().await.expect("channel");
    let err = rch
        .publish_confirmed(
            "",
            "denied-q",
            MessageProperties::default(),
            b"gap".to_vec(),
        )
        .await
        .expect_err("publish gate (Write on the exchange resource)");
    assert!(
        is_access_refused(&err),
        "publish gate (Write on \"\"), got {err:?}"
    );
    // A refused publish must not enqueue anywhere (the gate runs before
    // routing, methods/publish.rs:54-58): the guest still sees an empty
    // queue.
    assert!(
        gch.get("denied-q", true)
            .await
            .expect("guest get")
            .is_none(),
        "the refused publish landed anyway"
    );

    // An unrestricted user (no permissions entry) passes everything
    // (topology/mod.rs:170-176).
    broker
        .create_user("free", "fpw")
        .await
        .expect("create_user");
    let fconn = broker.connect("free", "fpw").await.expect("free connect");
    let fch = fconn.open_channel().await.expect("channel");
    fch.queue_declare("allowed-free-q", &QueueOptions::new())
        .await
        .expect("unrestricted declare works");
    fch.publish(
        "",
        "allowed-free-q",
        MessageProperties::default(),
        b"x".to_vec(),
    )
    .await
    .expect("unrestricted publish works");
    assert_eq!(
        fch.get("allowed-free-q", true)
            .await
            .expect("get")
            .expect("msg")
            .body,
        b"x"
    );

    // set_permissions for a missing user is REFUSED by the meta SM
    // (topology/mod.rs:214-219) → AdminError::Refused (there is no
    // NoSuchUser variant: create/delete reply Ok unconditionally).
    let err = broker
        .set_permissions(
            "nobody-here",
            "/",
            switchboard_embedded::Permissions::default(),
        )
        .await
        .expect_err("no such user");
    assert!(matches!(err, switchboard_embedded::AdminError::Refused(_)));

    gconn.close().await;
    rconn.close().await;
    fconn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 6. bootstrap credentials (ensure-user-if-absent)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bootstrap_credentials() {
    init();
    let broker = Broker::start(
        BrokerConfig::new(support::temp_dir("e2e-6"))
            .bootstrap_credentials("app", "pw")
            .timeouts(support::test_timeouts()),
    )
    .await
    .expect("start");

    // The configured user authenticates with NO admin call.
    let app = broker.connect("app", "pw").await.expect("app connect");
    // guest is untouched: CreateUser adds without touching existing users
    // (topology/mod.rs:240-246), and guest was created by the node's own
    // bootstrap with guest/guest.
    let guest = broker
        .connect("guest", "guest")
        .await
        .expect("guest untouched");
    let _ = app;
    let _ = guest;
    broker.stop(Duration::from_secs(30)).await.expect("stop");

    // The literal name "guest" is refused with InvalidConfig (gate could
    // never fire for it; §5.1).
    let err = Broker::start(
        BrokerConfig::new(support::temp_dir("e2e-6-guest"))
            .bootstrap_credentials("guest", "x")
            .timeouts(support::test_timeouts()),
    )
    .await
    .expect_err("guest bootstrap refused");
    assert!(
        matches!(err, StartError::InvalidConfig(_)),
        "guest bootstrap refused with InvalidConfig, got {err:?}"
    );

    // The skip-when-user-exists half (rotated passwords preserved) is
    // asserted cross-process in the restart suite (restart.rs,
    // restart_hold_and_credential_skip).
}

// ---------------------------------------------------------------------------
// 7. channel lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn channel_lifecycle() {
    init();
    let broker = start_broker("e2e-7").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch1 = conn.open_channel().await.expect("channel 1");
    let ch2 = conn.open_channel().await.expect("channel 2");
    assert_ne!(ch1.id(), ch2.id(), "channels get distinct numbers");

    // Two channels interleaved on one connection.
    ch1.queue_declare("lca", &QueueOptions::new().durable(true))
        .await
        .expect("declare a");
    ch2.queue_declare("lcb", &QueueOptions::new().durable(true))
        .await
        .expect("declare b");
    ch1.publish("", "lca", MessageProperties::default(), b"a1".to_vec())
        .await
        .expect("pub a1");
    ch2.publish("", "lcb", MessageProperties::default(), b"b1".to_vec())
        .await
        .expect("pub b1");
    ch1.publish("", "lcb", MessageProperties::default(), b"a->b".to_vec())
        .await
        .expect("pub cross");
    let from1 = ch1.get("lca", true).await.expect("get").expect("a1");
    assert_eq!(from1.body, b"a1");
    let from2 = ch2.get("lcb", true).await.expect("get").expect("b1");
    assert_eq!(from2.body, b"b1");
    let cross = ch2.get("lcb", true).await.expect("get").expect("cross");
    assert_eq!(cross.body, b"a->b");

    // Server-side consumer cancellation surfaces as next() -> None
    // (consumer-cancellation notification, methods/consume.rs:257-269):
    // deleting the queue cancels its consumers.
    let mut consumer = ch1
        .consume("lca", &ConsumeOptions::new().tag("victim"))
        .await
        .expect("consume");
    ch1.queue_delete("lca", &QueueDeleteOptions::new())
        .await
        .expect("delete");
    let mut cancelled_flag = false;
    for _ in 0..60 {
        match tokio::time::timeout(Duration::from_millis(200), consumer.next()).await {
            Ok(None) => {
                cancelled_flag = true;
                break;
            }
            Ok(Some(_)) => {}
            Err(_elapsed) => {}
        }
    }
    assert!(cancelled_flag, "consumer cancellation surfaces as None");

    // connection.close() requeues that connection's unacked deliveries
    // (session teardown, session/mod.rs:471-478).
    ch2.publish("", "lcb", persistent_props(), b"unacked".to_vec())
        .await
        .expect("pub");
    let held = ch2.get("lcb", false).await.expect("get").expect("held");
    assert_eq!(held.body, b"unacked");
    conn.close().await;

    // A fresh connection redelivers it.
    let conn2 = broker.connect("guest", "guest").await.expect("reconnect");
    let fresh = conn2.open_channel().await.expect("channel");
    let mut redelivered = None;
    for _ in 0..100 {
        if let Ok(Some(d)) = fresh.get("lcb", true).await {
            assert_eq!(d.body, b"unacked");
            assert!(d.redelivered, "requeued delivery comes back redelivered");
            redelivered = Some(d);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(redelivered.is_some(), "close requeued the unacked delivery");

    // Server-side ChannelClosed poisons ALL clones of that channel (§4.3):
    // an inequivalent re-declare is a 406 → the channel closes.
    let poison = conn2.open_channel().await.expect("channel");
    poison
        .queue_declare(
            "dup-q",
            &QueueOptions::new()
                .durable(true)
                .arguments(FieldTable::new()),
        )
        .await
        .expect("first declare");
    let err = poison
        .queue_declare("dup-q", &QueueOptions::new().durable(false))
        .await
        .expect_err("inequivalent re-declare");
    assert!(
        matches!(&err, OpError::PreconditionFailed { .. }),
        "406 decodes to PreconditionFailed, got {err:?}"
    );
    let clone = poison.clone();
    let err = clone
        .queue_purge("dup-q")
        .await
        .expect_err("clone fails fast");
    assert!(
        matches!(&err, OpError::ChannelClosed { code: 406, .. }),
        "clones fail fast with the stored close pair, got {err:?}"
    );
    let err = poison
        .queue_purge("dup-q")
        .await
        .expect_err("original fails fast too");
    assert!(matches!(&err, OpError::ChannelClosed { code: 406, .. }));

    // The CONNECTION stays usable after a channel-level close.
    let still_open = conn2
        .open_channel()
        .await
        .expect("new channel on same conn");
    still_open
        .queue_declare("still-open-q", &QueueOptions::new())
        .await
        .expect("connection alive");

    conn2.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 8. fanout publish
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fanout_publish() {
    init();
    let broker = start_broker("e2e-8").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");

    ch.queue_declare("fan1", &QueueOptions::new().durable(true))
        .await
        .expect("declare f1");
    ch.queue_declare("fan2", &QueueOptions::new().durable(true))
        .await
        .expect("declare f2");
    ch.queue_bind("fan1", "amq.fanout", "k1")
        .await
        .expect("bind f1");
    ch.queue_bind("fan2", "amq.fanout", "k2")
        .await
        .expect("bind f2");

    // One publish, two destinations (multi-destination fanout path,
    // methods/publish.rs:163-180).
    ch.publish(
        "amq.fanout",
        "whatever",
        persistent_props(),
        b"fan".to_vec(),
    )
    .await
    .expect("fanout publish");

    for q in ["fan1", "fan2"] {
        let mut got = None;
        for _ in 0..100 {
            if let Ok(Some(d)) = ch.get(q, true).await {
                assert_eq!(d.body, b"fan", "{q} received the fanout message");
                assert_eq!(d.exchange, "amq.fanout");
                got = Some(d);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(got.is_some(), "{q} received the fanout message");
    }

    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

// ---------------------------------------------------------------------------
// 9. runtime contract (§3.1)
// ---------------------------------------------------------------------------

/// The whole flow under a CURRENT-THREAD runtime: all spawned futures are
/// Send, so no LocalSet is needed anywhere (§3.1).
#[tokio::test(flavor = "current_thread")]
async fn everything_works_on_a_current_thread_runtime() {
    init();
    let broker = start_broker("e2e-9-ct").await;
    let conn = broker.connect("guest", "guest").await.expect("connect");
    let ch = conn.open_channel().await.expect("channel");
    ch.queue_declare("ct-q", &QueueOptions::new())
        .await
        .expect("declare");
    ch.publish("", "ct-q", MessageProperties::default(), b"ct".to_vec())
        .await
        .expect("publish");
    let mut consumer = ch
        .consume("ct-q", &ConsumeOptions::new())
        .await
        .expect("consume");
    let d = support::with_timeout(consumer.next(), 30)
        .await
        .expect("delivery");
    assert_eq!(d.body, b"ct");
    ch.ack(d.delivery_tag).await.expect("ack");
    consumer.cancel().await.expect("cancel");
    conn.close().await;
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}

/// `Broker::start` called from OUTSIDE any runtime context panics at first
/// poll — the documented tokio spawn contract (§3.1), not an error.
#[test]
fn start_outside_a_runtime_panics_at_first_poll() {
    init();
    let cfg = BrokerConfig::new(support::temp_dir("e2e-9-panic"));
    let mut fut = Box::pin(Broker::start(cfg));

    // Poll once with a no-op waker: no runtime exists, so the internal
    // tokio machinery (net/timer/spawn) panics.
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(&mut cx)));
    std::panic::set_hook(hook);

    assert!(
        result.is_err(),
        "Broker::start polled outside a runtime must panic (§3.1), got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// 10. in-process double start fails with Storage (RocksDB LOCK, §5.3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_process_double_start_fails() {
    init();
    let dir = support::temp_dir("e2e-10");
    let broker = Broker::start(BrokerConfig::new(dir.clone()).timeouts(support::test_timeouts()))
        .await
        .expect("first start");
    let err = Broker::start(BrokerConfig::new(dir.clone()).timeouts(support::test_timeouts()))
        .await
        .expect_err("second start on a live data_dir");
    assert!(
        matches!(&err, StartError::Storage(_)),
        "RocksDB LOCK surfaces as Storage, got {err:?}"
    );
    broker.stop(Duration::from_secs(30)).await.expect("stop");
}
