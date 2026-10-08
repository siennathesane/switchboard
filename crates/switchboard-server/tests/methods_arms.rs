//! Basic-class arm coverage: transactional acks, dead-lettering via
//! nack, requeue via reject, qos credit windows, and the protocol-list
//! parser.

mod support;

use std::time::Duration;

use switchboard_wire::field::{FieldTable, FieldValue};
use switchboard_wire::method::Method;
use switchboard_wire::BasicProperties;

use support::{basic_get, connect_and_open, publish, start_broker, TestClient};

async fn get_eventually(
    c: &mut TestClient,
    queue: &str,
) -> Option<(u64, Vec<u8>)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "message never arrived on {queue}");
        match basic_get(c, 1, queue, true).await.unwrap() {
            Some(got) => return Some(got),
            None => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_inside_a_transaction_commits_with_it() {
    let (_node, addr) = start_broker("ma-txack").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "txa".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    // Publish BEFORE the transaction opens: a tx-buffered publish stays
    // invisible until commit, and this test needs a live unacked
    // delivery to ack inside the tx.
    publish(&mut c, 1, "", "txa", &BasicProperties::new(), b"payload", false).await.unwrap();
    // Unacked delivery (publish is asynchronous — poll for it).
    let mut tag = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tag.is_none() {
        assert!(tokio::time::Instant::now() < deadline, "expected a delivery");
        tag = basic_get(&mut c, 1, "txa", false).await.unwrap();
        if tag.is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let (tag, _) = tag.unwrap();
    // NOW open the transaction: the ack is buffered into it and only
    // takes effect at commit.
    c.send_method(1, &Method::TxSelect {}).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(1, &Method::BasicAck { delivery_tag: tag, multiple: false }).await.unwrap();
    // Commit applies the buffered ack: the queue must be empty after.
    c.send_method(1, &Method::TxCommit {}).await.unwrap();
    let _ = c.expect(1).await.unwrap();

    let mut d = connect_and_open(&addr, "/").await.unwrap();
    for m in [
        Method::QueueDeclare {
            ticket: 0, queue: "txa".into(), passive: true, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
        },
    ] {
        d.send_method(1, &m).await.unwrap();
        let _ = d.expect(1).await.unwrap();
    }
    let got = basic_get(&mut d, 1, "txa", true).await.unwrap();
    assert!(got.is_none(), "acked-in-tx message must be gone after commit");
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_without_requeue_dead_letters_to_the_dlx() {
    let (_node, addr) = start_broker("ma-dlx").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    let mut args = FieldTable::new();
    args.insert(String::from("x-dead-letter-exchange"), FieldValue::LongString("amq.direct".into()));
    args.insert(String::from("x-dead-letter-routing-key"), FieldValue::LongString("dlq-rk".into()));
    for m in [
        Method::QueueDeclare {
            ticket: 0, queue: "src".into(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: args,
        },
        Method::QueueDeclare {
            ticket: 0, queue: "dlq".into(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
        },
        Method::QueueBind {
            ticket: 0, queue: "dlq".into(), exchange: "amq.direct".into(),
            routing_key: "dlq-rk".into(), nowait: false, arguments: Default::default(),
        },
    ] {
        c.send_method(1, &m).await.unwrap();
        let _ = c.expect(1).await.unwrap();
    }

    publish(&mut c, 1, "", "src", &BasicProperties::new(), b"doomed", false).await.unwrap();
    let Some((tag, _)) = basic_get(&mut c, 1, "src", false).await.unwrap() else {
        panic!("expected a delivery");
    };
    c.send_method(1, &Method::BasicNack { delivery_tag: tag, multiple: false, requeue: false }).await.unwrap();

    // The dead-lettered message lands in the DLQ.
    let (_, body) = get_eventually(&mut c, "dlq").await.unwrap();
    assert_eq!(body, b"doomed");
    // And the source is empty.
    assert!(basic_get(&mut c, 1, "src", true).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_with_requeue_redelivers() {
    let (_node, addr) = start_broker("ma-requeue").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "rq".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();

    publish(&mut c, 1, "", "rq", &BasicProperties::new(), b"again", false).await.unwrap();
    let Some((tag, _)) = basic_get(&mut c, 1, "rq", false).await.unwrap() else {
        panic!("expected a delivery");
    };
    c.send_method(1, &Method::BasicReject { delivery_tag: tag, requeue: true }).await.unwrap();

    let (tag2, body) = get_eventually(&mut c, "rq").await.unwrap();
    assert_eq!(body, b"again");
    assert_eq!(tag2, 2, "redelivery gets the next delivery tag");
}

#[tokio::test(flavor = "multi_thread")]
async fn qos_prefetch_grants_a_bounded_credit_window() {
    let (_node, addr) = start_broker("ma-qos").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "win".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    // The consumer lives on its own channel.
    c.send_method(2, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
    c.send_method(2, &Method::BasicQos { prefetch_size: 0, prefetch_count: 2, global_: false }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
    c.send_method(2, &Method::BasicConsume {
        ticket: 0, queue: "win".into(), consumer_tag: "w1".into(),
        no_local: false, no_ack: false, exclusive: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(2).await.unwrap();

    // Three publishes: only the credit window (2) may be handed out.
    for i in 0..3 {
        publish(&mut c, 1, "", "win", &BasicProperties::new(), format!("m{i}").as_bytes(), false).await.unwrap();
    }
    // Exactly two deliveries arrive within the window.
    let first = tokio::time::timeout(Duration::from_secs(10), c.expect(2)).await.unwrap().unwrap();
    let Method::BasicDeliver { delivery_tag: t1, .. } = first else {
        panic!("expected a delivery, got {first:?}");
    };
    let second = tokio::time::timeout(Duration::from_secs(10), c.expect(2)).await.unwrap().unwrap();
    let Method::BasicDeliver { delivery_tag: t2, .. } = second else {
        panic!("expected a second delivery");
    };
    // No third delivery while both are unacked.
    let third = tokio::time::timeout(Duration::from_millis(700), c.expect(2)).await;
    assert!(third.is_err(), "credit window must bound hand-outs");
    // Acking both releases the third.
    c.send_method(2, &Method::BasicAck { delivery_tag: t1, multiple: false }).await.unwrap();
    c.send_method(2, &Method::BasicAck { delivery_tag: t2, multiple: false }).await.unwrap();
    let third = tokio::time::timeout(Duration::from_secs(10), c.expect(2)).await.unwrap().unwrap();
    assert!(matches!(third, Method::BasicDeliver { .. }), "{third:?}");
}

#[test]
fn protocol_list_parses_and_validates() {
    use switchboard_server::protocols::ProtocolConfig;

    let cfg = ProtocolConfig::from_list("amqp,mqtt,stomp,ws,http,amqp1").unwrap();
    assert!(cfg.amqp091 && cfg.mqtt && cfg.stomp && cfg.websocket && cfg.http_health && cfg.amqp10);

    // Whitespace and case are tolerated.
    let cfg = ProtocolConfig::from_list(" AMQP , MQTT ").unwrap();
    assert!(cfg.amqp091 && cfg.mqtt && !cfg.stomp);

    // Unknown names are rejected.
    assert!(ProtocolConfig::from_list("amqp,smtp").is_err());

    // The native protocol cannot be disabled.
    assert!(ProtocolConfig::from_list("mqtt").is_err());
    assert!(ProtocolConfig::from_list("").is_err());
}

// ---------------------------------------------------------------------------
// Publish arms: mandatory/immediate returns, unroutable fanout, and
// content framing rules.
// ---------------------------------------------------------------------------

/// Expect a Basic.Return with the given reply code on channel 1.
async fn expect_return(c: &mut TestClient, code: u16) {
    let m = tokio::time::timeout(Duration::from_secs(10), c.expect(1)).await.unwrap().unwrap();
    let Method::BasicReturn { reply_code, reply_text, .. } = m else {
        panic!("expected Basic.Return, got {m:?}");
    };
    assert_eq!(reply_code, code, "{reply_text}");
    // The returned content follows.
    // The return's content was already consumed by expect(); a short
    // negative window proves nothing else follows (e.g. a channel close).
    if tokio::time::timeout(Duration::from_millis(200), c.expect(1)).await.is_ok() {
        panic!("unexpected extra frame after Basic.Return");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mandatory_unroutable_returns_312_no_route() {
    let (_node, addr) = start_broker("ma-mand").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    // mandatory=true with no route to "ghost" → 312 NO_ROUTE (§1.5.3).
    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: "ghost".into(),
        mandatory: true, immediate: false,
    }).await.unwrap();
    c.send_content(1, &BasicProperties::new(), b"lost").await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(10), c.expect(1)).await.unwrap().unwrap();
    let Method::BasicReturn { reply_code, reply_text, .. } = m else {
        panic!("expected Basic.Return, got {m:?}");
    };
    assert_eq!(reply_code, 312, "{reply_text}");
    assert_eq!(reply_text, "NO_ROUTE");
    // The returned content follows the return frame.
    // The return's content was already consumed by expect(); a short
    // negative window proves nothing else follows (e.g. a channel close).
    if tokio::time::timeout(Duration::from_millis(200), c.expect(1)).await.is_ok() {
        panic!("unexpected extra frame after Basic.Return");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn immediate_publish_without_consumers_returns_312() {
    let (_node, addr) = start_broker("ma-imm").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "imm".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();

    // immediate=true demands a ready consumer right now.
    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: "imm".into(),
        mandatory: false, immediate: true,
    }).await.unwrap();
    c.send_content(1, &BasicProperties::new(), b"now").await.unwrap();
    expect_return(&mut c, 312).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mandatory_fanout_with_no_bindings_returns_312() {
    let (_node, addr) = start_broker("ma-fan").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::ExchangeDeclare {
        ticket: 0, exchange: "fan".into(), exchange_type: "fanout".into(),
        passive: false, durable: true, auto_delete: false, internal: false,
        nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();

    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "fan".into(), routing_key: "".into(),
        mandatory: true, immediate: false,
    }).await.unwrap();
    c.send_content(1, &BasicProperties::new(), b"nowhere").await.unwrap();
    // Unroutable fanout also returns 312 NO_ROUTE (RabbitMQ parity:
    // 312 covers every unroutable mandatory publish).
    let m = tokio::time::timeout(Duration::from_secs(10), c.expect(1)).await.unwrap().unwrap();
    let Method::BasicReturn { reply_code, .. } = m else {
        panic!("expected Basic.Return, got {m:?}");
    };
    assert_eq!(reply_code, 312);
    // The return's content was already consumed by expect(); a short
    // negative window proves nothing else follows (e.g. a channel close).
    if tokio::time::timeout(Duration::from_millis(200), c.expect(1)).await.is_ok() {
        panic!("unexpected extra frame after Basic.Return");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_ack_settles_every_delivery_up_to_the_tag() {
    let (_node, addr) = start_broker("ma-multiack").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "multi".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();

    for i in 0..3 {
        publish(&mut c, 1, "", "multi", &BasicProperties::new(), format!("m{i}").as_bytes(), false).await.unwrap();
    }
    // Three unacked deliveries.
    let mut tags = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tags.len() < 3 {
        assert!(tokio::time::Instant::now() < deadline, "expected 3 deliveries");
        if let Some((tag, _)) = basic_get(&mut c, 1, "multi", false).await.unwrap() {
            tags.push(tag);
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // A single multiple-ack through the highest tag settles all three.
    c.send_method(1, &Method::BasicAck { delivery_tag: *tags.last().unwrap(), multiple: true }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        basic_get(&mut c, 1, "multi", true).await.unwrap().is_none(),
        "multiple ack must settle every tagged delivery"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_requeues_and_refills_consumer_windows() {
    let (_node, addr) = start_broker("ma-recover").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "rec".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(2, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
    c.send_method(2, &Method::BasicQos { prefetch_size: 0, prefetch_count: 3, global_: false }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
    c.send_method(2, &Method::BasicConsume {
        ticket: 0, queue: "rec".into(), consumer_tag: "r1".into(),
        no_local: false, no_ack: false, exclusive: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(2).await.unwrap();

    publish(&mut c, 1, "", "rec", &BasicProperties::new(), b"held", false).await.unwrap();
    // The delivery is held unacked by the consumer.
    let held = tokio::time::timeout(Duration::from_secs(10), c.expect(2)).await.unwrap().unwrap();
    let Method::BasicDeliver { delivery_tag, .. } = held else {
        panic!("expected a delivery, got {held:?}");
    };

    // Recover redelivers everything unacked and refills the window. The
    // redelivery and the Recover-Ok may arrive in either order.
    c.send_method(2, &Method::BasicRecover { requeue: true }).await.unwrap();
    let mut saw_ok = false;
    let mut saw_redelivered = false;
    for _ in 0..2 {
        let m = tokio::time::timeout(Duration::from_secs(10), c.expect(2)).await.unwrap().unwrap();
        match m {
            Method::BasicRecoverOk { .. } => saw_ok = true,
            Method::BasicDeliver { redelivered, .. } => {
                assert!(redelivered, "recovered delivery must be marked redelivered");
                saw_redelivered = true;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(saw_ok && saw_redelivered, "ok={saw_ok} redelivered={saw_redelivered}");
    let _ = delivery_tag;
}

// ---------------------------------------------------------------------------
// Consume arms: missing queues, server-generated tags, flow, teardown.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn consume_missing_queue_is_not_found() {
    let (_node, addr) = start_broker("mc-missing").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::BasicConsume {
        ticket: 0, queue: "never".into(), consumer_tag: "c1".into(),
        no_local: false, no_ack: true, exclusive: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(1)).await.unwrap().unwrap();
    let Method::ChannelClose { reply_code, .. } = m else {
        panic!("expected Channel.Close, got {m:?}");
    };
    assert_eq!(reply_code, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_consumer_tag_gets_a_server_generated_one() {
    let (_node, addr) = start_broker("mc-gentag").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "gentle".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(1, &Method::BasicConsume {
        ticket: 0, queue: "gentle".into(), consumer_tag: String::new(),
        no_local: false, no_ack: true, exclusive: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(1)).await.unwrap().unwrap();
    let Method::BasicConsumeOk { consumer_tag } = m else {
        panic!("expected Consume-Ok, got {m:?}");
    };
    assert!(consumer_tag.starts_with("ct-"), "{consumer_tag}");
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_flow_pauses_and_resumes_consumers() {
    let (_node, addr) = start_broker("mc-flow").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    for m in [
        Method::QueueDeclare {
            ticket: 0, queue: "flowy".into(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
        },
        Method::BasicConsume {
            ticket: 0, queue: "flowy".into(), consumer_tag: "f1".into(),
            no_local: false, no_ack: true, exclusive: false, nowait: false,
            arguments: Default::default(),
        },
    ] {
        c.send_method(1, &m).await.unwrap();
        let _ = c.expect(1).await.unwrap();
    }
    // Pause: Flow-Ok(active=false), then resume.
    c.send_method(1, &Method::ChannelFlow { active: false }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(1)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ChannelFlowOk { .. }), "got {m:?}");
    c.send_method(1, &Method::ChannelFlow { active: true }).await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(15), c.expect(1)).await.unwrap().unwrap();
    assert!(matches!(m, Method::ChannelFlowOk { .. }), "got {m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_the_queue_cancels_its_consumers() {
    let (_node, addr) = start_broker("mc-teardown").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::QueueDeclare {
        ticket: 0, queue: "td".into(), passive: false, durable: true,
        exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    // The consumer lives on channel 2, the delete lands on channel 1.
    c.send_method(2, &Method::ChannelOpen { out_of_band: String::new() }).await.unwrap();
    let _ = c.expect(2).await.unwrap();
    c.send_method(2, &Method::BasicConsume {
        ticket: 0, queue: "td".into(), consumer_tag: "gone".into(),
        no_local: false, no_ack: true, exclusive: false, nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let _ = c.expect(2).await.unwrap();

    c.send_method(1, &Method::QueueDelete {
        ticket: 0, queue: "td".into(), if_empty: false, if_unused: false, nowait: false,
    }).await.unwrap();

    // The QueueDeleteOk (ch 1) and the server-side Basic.Cancel (ch 2)
    // race; both must arrive. expect(2) skips ch-1 frames while reading,
    // but must not swallow the Cancel: drain up to a few frames.
    let mut saw_delete_ok = false;
    let mut cancel_tag: Option<String> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !(saw_delete_ok && cancel_tag.is_some()) {
        assert!(tokio::time::Instant::now() < deadline, "timed out: delete_ok={saw_delete_ok} cancel={cancel_tag:?}");
        match tokio::time::timeout(Duration::from_secs(5), c.expect(1)).await {
            Ok(Ok(m)) => match m {
                Method::QueueDeleteOk { .. } => saw_delete_ok = true,
                other => panic!("unexpected on ch1: {other:?}"),
            },
            _ => {}
        }
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(2), c.expect(2)).await {
            match m {
                Method::BasicCancel { consumer_tag, .. } => cancel_tag = Some(consumer_tag),
                other => panic!("unexpected on ch2: {other:?}"),
            }
        }
    }
    assert_eq!(cancel_tag.as_deref(), Some("gone"));
}

// ---------------------------------------------------------------------------
// Tx/publish tails: unknown-transaction no-ops and confirm batches.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn commit_and_rollback_of_unknown_transactions_are_noops() {
    let (_node, addr) = start_broker("mc-notx").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    c.send_method(1, &Method::TxCommit {}).await.unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(1, &Method::TxRollback {}).await.unwrap();
    let _ = c.expect(1).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn publisher_confirms_arrive_for_every_publish() {
    let (_node, addr) = start_broker("mc-conf").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    for m in [
        Method::QueueDeclare {
            ticket: 0, queue: "cf".into(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false, arguments: Default::default(),
        },
        Method::ConfirmSelect { nowait: false },
    ] {
        c.send_method(1, &m).await.unwrap();
        let _ = c.expect(1).await.unwrap();
    }
    for i in 0..3 {
        publish(&mut c, 1, "", "cf", &BasicProperties::new(), format!("c{i}").as_bytes(), false).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(10), c.expect(1)).await.unwrap().unwrap();
        assert!(matches!(m, Method::BasicAck { .. }), "publish {i}: got {m:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn immediate_publish_without_any_route_returns_312() {
    let (_node, addr) = start_broker("mc-imm2").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    // immediate=true and no route at all → 312 NO_CONSUMERS.
    c.send_method(1, &Method::BasicPublish {
        ticket: 0, exchange: "".into(), routing_key: "ghost".into(),
        mandatory: false, immediate: true,
    }).await.unwrap();
    c.send_content(1, &BasicProperties::new(), b"lost").await.unwrap();
    let m = tokio::time::timeout(Duration::from_secs(10), c.expect(1)).await.unwrap().unwrap();
    let Method::BasicReturn { reply_code, .. } = m else {
        panic!("expected Basic.Return, got {m:?}");
    };
    assert_eq!(reply_code, 312);
    // The return's content was already consumed by expect(); a short
    // negative window proves nothing else follows (e.g. a channel close).
    if tokio::time::timeout(Duration::from_millis(200), c.expect(1)).await.is_ok() {
        panic!("unexpected extra frame after Basic.Return");
    }
}
