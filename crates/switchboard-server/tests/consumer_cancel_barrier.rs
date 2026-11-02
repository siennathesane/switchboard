//! Regression: `basic.cancel-ok` is a per-consumer delivery barrier.
//!
//! Field shape (harness `node_amqp091.log`): amqplib passed all checks,
//! then died at teardown on "Unknown consumer: ct-5" — code 541, an
//! unhandled error event, process exit — because the broker wrote a
//! `basic.deliver` for a consumer tag AFTER the client had processed
//! `basic.cancel-ok` for it. The 0-9-1 ordering contract (and RabbitMQ's
//! observable behavior, which amqplib relies on): once the broker has
//! sent Cancel-ok for a consumer, no further Deliver frames for that tag
//! may be written; deliveries dispatched before the cancel completes ARE
//! delivered, before Cancel-ok.
//!
//! This drives that shape through a raw socket: a live delivery stream
//! from a second connection (the cancel lands mid-stream, against
//! dispatches in flight, as in the 3-node harness teardown), and strict
//! wire-order assertions — no Deliver for the tag after its Cancel-ok —
//! plus the accounting: the consumer runs in ack mode, so every message
//! dispatched before the cancel either arrives before Cancel-ok or is
//! requeued by the unregister (at-least-once), and the remainder is
//! fully consumable afterwards. Nothing vanishes across the roundtrip.

mod support;

use std::env::set_var;
use std::env::var;
use std::time::Duration;

use support::basic_get;
use support::connect_and_open;
use support::publish;
use support::start_broker;
use switchboard_wire::BasicProperties;
use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use tokio::spawn;
use tokio::sync::oneshot::channel;
use tokio::time::timeout;

const QUEUE: &str = "cancel-barrier";
const TAG: &str = "ct-race";
const TAG_AFTER: &str = "ct-after";
const FLOOD: u32 = 1500;

#[tokio::test(flavor = "multi_thread")]
async fn cancel_ok_is_a_per_consumer_delivery_barrier_on_the_wire() {
    // Real-time tempo for this binary: the support default (50x) shrinks
    // the server's heartbeat to ~1.2 s, and this test deliberately stays
    // read-only for seconds while it drains ~1500 deliveries — the
    // silence deadline would kill the connection mid-assertion.
    if var("SB_TIME_SCALE").is_err() {
        unsafe { set_var("SB_TIME_SCALE", "1") };
    }
    support::init_tracing();
    let (_node, addr) = start_broker("cancel-barrier").await;
    let mut a = connect_and_open(&addr, "/").await.expect("consumer conn");

    a.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: QUEUE.into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    let Method::QueueDeclareOk { .. } = a.expect(1).await.unwrap() else {
        panic!("expected QueueDeclareOk");
    };

    // Known tag, ack mode: dispatched messages are HELD by the
    // subscription shard-side, and the cancel's unregister releases
    // whatever was still unacked — at-least-once, nothing can vanish
    // across the roundtrip (no-ack would let a dispatch racing the
    // cancel's detach be dropped after the shard already deleted it).
    a.send_method(
        1,
        &Method::BasicConsume {
            ticket: 0,
            queue: QUEUE.into(),
            consumer_tag: TAG.into(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    let Method::BasicConsumeOk { consumer_tag } = a.expect(1).await.unwrap() else {
        panic!("expected BasicConsumeOk");
    };
    assert_eq!(consumer_tag, TAG);

    // A second connection floods the queue while the consumer runs, so
    // the cancel lands mid-stream against dispatches in flight — the
    // same shape as the harness teardown crash.
    let (flood_done_tx, mut flood_done_rx) = channel::<()>();
    let flood_addr = addr.clone();
    spawn(async move {
        let mut b = connect_and_open(&flood_addr, "/")
            .await
            .expect("flood conn");
        for i in 0..FLOOD {
            publish(
                &mut b,
                1,
                "",
                QUEUE,
                &BasicProperties::new(),
                format!("m{i}").as_bytes(),
                false,
            )
            .await
            .expect("flood publish");
        }
        let _ = flood_done_tx.send(());
    });

    // Let the stream reach the consumer, acking like a real client, then
    // cancel mid-stream.
    let mut delivered_before_ok = 0u32;
    loop {
        let (m, _) = timeout(Duration::from_secs(30), a.expect_method(1))
            .await
            .expect("timed out waiting for the delivery stream to start")
            .unwrap();
        match m {
            Method::BasicDeliver {
                consumer_tag,
                delivery_tag,
                ..
            } => {
                assert_eq!(consumer_tag, TAG);
                a.send_method(
                    1,
                    &Method::BasicAck {
                        delivery_tag,
                        multiple: false,
                    },
                )
                .await
                .unwrap();
                delivered_before_ok += 1;
                if delivered_before_ok >= 25 {
                    break;
                }
            }
            other => panic!("unexpected {} before cancel", other.name()),
        }
    }
    a.send_method(
        1,
        &Method::BasicCancel {
            consumer_tag: TAG.into(),
            nowait: false,
        },
    )
    .await
    .unwrap();

    // Strict wire order: the Deliver frames still in flight for the tag
    // (dispatched before the cancel completed) drain first — Cancel-ok
    // comes last for the tag, and NOTHING for the tag follows it. (This
    // is exactly where amqplib threw 541 "Unknown consumer".)
    let mut cancel_ok_seen = false;
    while !cancel_ok_seen {
        let (m, _) = timeout(Duration::from_secs(30), a.expect_method(1))
            .await
            .expect("timed out waiting for BasicCancelOk")
            .unwrap();
        match m {
            Method::BasicDeliver {
                consumer_tag,
                delivery_tag,
                ..
            } => {
                assert!(!cancel_ok_seen, "Deliver for {TAG} after Cancel-ok");
                assert_eq!(consumer_tag, TAG);
                a.send_method(
                    1,
                    &Method::BasicAck {
                        delivery_tag,
                        multiple: false,
                    },
                )
                .await
                .unwrap();
                delivered_before_ok += 1;
            }
            Method::BasicCancelOk { consumer_tag } => {
                assert_eq!(consumer_tag, TAG);
                cancel_ok_seen = true;
            }
            other => panic!("unexpected {} around the cancel", other.name()),
        }
    }
    assert!(
        delivered_before_ok >= 25,
        "deliveries observed before the cancel must have been delivered"
    );

    // What the cancel found undispatched (or requeued from the unacked
    // holds it released) must still be consumable afterwards: consume
    // the remainder under a fresh tag. Any Deliver for the cancelled tag
    // from here on is the 541 crash reproduced.
    a.send_method(
        1,
        &Method::BasicConsume {
            ticket: 0,
            queue: QUEUE.into(),
            consumer_tag: TAG_AFTER.into(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    // At-least-once: the total may overshoot FLOOD by the last pre-cancel
    // deliveries whose acks raced the unregister (their holds were
    // released and they are redelivered) — but it can never undershoot:
    // every published message is delivered at least once across the
    // roundtrip. End on FLOOD counted, or on a quiet wire once the flood
    // connection has finished publishing.
    let mut total = delivered_before_ok;
    let mut flood_finished = false;
    loop {
        if total >= FLOOD {
            break;
        }
        match timeout(Duration::from_millis(1500), a.expect_method(1)).await {
            Ok(Ok((m, _))) => match m {
                Method::BasicConsumeOk { consumer_tag } => {
                    assert_eq!(consumer_tag, TAG_AFTER);
                }
                Method::BasicDeliver { consumer_tag, .. } => {
                    assert_eq!(
                        consumer_tag, TAG_AFTER,
                        "Deliver for {consumer_tag} after {TAG}'s Cancel-ok"
                    );
                    total += 1;
                }
                other => panic!("unexpected {} after the cancel", other.name()),
            },
            Ok(Err(e)) => panic!("read failed after the cancel: {e}"),
            Err(_) => {
                if !flood_finished {
                    flood_finished = flood_done_rx.try_recv().is_ok();
                }
                if flood_finished {
                    break; // quiet wire, flood fully enqueued: that's all
                }
            }
        }
    }
    // Retire ct-after first (its own Cancel-ok barrier holds here too),
    // so the sweep below cannot race a live consumer for stragglers.
    a.send_method(
        1,
        &Method::BasicCancel {
            consumer_tag: TAG_AFTER.into(),
            nowait: false,
        },
    )
    .await
    .unwrap();
    loop {
        let (m, _) = timeout(Duration::from_secs(30), a.expect_method(1))
            .await
            .expect("timed out waiting for ct-after's CancelOk")
            .unwrap();
        match m {
            Method::BasicDeliver { consumer_tag, .. } => {
                assert_ne!(consumer_tag, TAG, "Deliver for {TAG} after Cancel-ok");
                assert_eq!(consumer_tag, TAG_AFTER);
                total += 1;
            }
            Method::BasicCancelOk { consumer_tag } => {
                assert_eq!(consumer_tag, TAG_AFTER);
                break;
            }
            other => panic!("unexpected {} retiring ct-after", other.name()),
        }
    }
    // Absorb any straggler redeliveries still sitting ready (bounded):
    // every drain must reach an empty queue.
    for _ in 0..FLOOD {
        if basic_get(&mut a, 1, QUEUE, true).await.unwrap().is_none() {
            break;
        }
        total += 1;
    }
    assert!(
        basic_get(&mut a, 1, QUEUE, true).await.unwrap().is_none(),
        "queue must drain"
    );
    assert!(
        total >= FLOOD,
        "messages lost across the cancel roundtrip: {total}/{FLOOD} delivered"
    );
}
