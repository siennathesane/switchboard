//! Server-level coverage for the queue-admin and basic-method paths that
//! the protocol suites touch only incidentally: purge, delete (with its
//! precondition variants), passive declares, exchange deletion, bindings,
//! recover, nack, and the QoS guards.

mod support;

use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;
use switchboard_wire::BasicProperties;

use support::{connect_and_open, publish, start_broker, TestClient};

async fn declare(c: &mut TestClient, queue: &str, durable: bool, passive: bool) -> Method {
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: queue.into(),
            passive,
            durable,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_purge_reports_and_clears_depth() {
    let (_node, addr) = start_broker("purge").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    assert!(matches!(
        declare(&mut c, "pq", true, false).await,
        Method::QueueDeclareOk { .. }
    ));
    for i in 0..3u32 {
        publish(&mut c, 1, "", "pq", &BasicProperties::new(), format!("m{i}").as_bytes(), false)
            .await
            .unwrap();
    }

    c.send_method(1, &Method::QueuePurge { ticket: 0, queue: "pq".into(), nowait: false })
        .await
        .unwrap();
    let Method::QueuePurgeOk { message_count } = c.expect(1).await.unwrap() else {
        panic!("expected QueuePurgeOk");
    };
    assert_eq!(message_count, 3, "purge must report the cleared depth");

    // The queue is now empty: a get returns empty.
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "pq".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetEmpty { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_delete_preconditions_and_success() {
    let (_node, addr) = start_broker("qdel").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "dq", true, false).await;

    // if_empty on a non-empty queue → 406 precondition failed.
    publish(&mut c, 1, "", "dq", &BasicProperties::new(), b"x", false).await.unwrap();
    c.send_method(
        1,
        &Method::QueueDelete {
            ticket: 0,
            queue: "dq".into(),
            if_unused: false,
            if_empty: true,
            nowait: false,
        },
    )
    .await
    .unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected ChannelClose");
    };
    assert_eq!(reply_code, 406);
    // The client MUST answer Channel.Close-Ok before reopening (§4.8.1).
    c.send_method(1, &Method::ChannelCloseOk {}).await.unwrap();

    // Fresh channel: delete succeeds and reports the dropped depth.
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .await
        .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::ChannelOpenOk { .. }));
    c.send_method(
        1,
        &Method::QueueDelete {
            ticket: 0,
            queue: "dq".into(),
            if_unused: false,
            if_empty: false,
            nowait: false,
        },
    )
    .await
    .unwrap();
    let Method::QueueDeleteOk { message_count } = c.expect(1).await.unwrap() else {
        panic!("expected QueueDeleteOk");
    };
    assert_eq!(message_count, 1);

    // Deleting again → 404 not found (channel-level). Channel 1 is still
    // open: a successful delete does not close it.
    c.send_method(
        1,
        &Method::QueueDelete {
            ticket: 0,
            queue: "dq".into(),
            if_unused: false,
            if_empty: false,
            nowait: false,
        },
    )
    .await
    .unwrap();
    let Method::ChannelClose { reply_code, reply_text, .. } = c.expect(1).await.unwrap() else {
        panic!("expected ChannelClose");
    };
    assert_eq!(reply_code, 404, "double delete must 404: {reply_text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn passive_declare_ok_and_missing() {
    let (_node, addr) = start_broker("passive").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "here", true, false).await;
    // Passive on an existing queue is an assertion: Ok.
    assert!(matches!(
        declare(&mut c, "here", true, true).await,
        Method::QueueDeclareOk { .. }
    ));
    // Passive on a missing queue → 404.
    let Method::ChannelClose { reply_code, .. } = declare(&mut c, "gone", false, true).await else {
        panic!("expected ChannelClose");
    };
    assert_eq!(reply_code, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_delete_lifecycle() {
    let (_node, addr) = start_broker("edel").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();

    // Declare + bind + delete + verify unroutable after deletion.
    c.send_method(
        1,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: "ex".into(),
            exchange_type: "direct".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ExchangeDeclareOk { .. }
    ));
    declare(&mut c, "target", true, false).await;
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "target".into(),
            exchange: "ex".into(),
            routing_key: "k".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::QueueBindOk { .. }));

    // The default exchange cannot be deleted (403 access refused).
    c.send_method(
        1,
        &Method::ExchangeDelete {
            ticket: 0,
            exchange: String::new(),
            if_unused: false,
            nowait: false,
        },
    )
    .await
    .unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected ChannelClose");
    };
    assert_eq!(reply_code, 403);

    // New channel: delete "ex", then a publish to it is unroutable.
    c.send_method(1, &Method::ChannelOpen { out_of_band: String::new() })
        .await
        .unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(
        1,
        &Method::ExchangeDelete {
            ticket: 0,
            exchange: "ex".into(),
            if_unused: false,
            nowait: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ExchangeDeleteOk { .. }
    ));

    // Mandatory publish now returns the message: 404 NO_ROUTE.
    publish(&mut c, 1, "ex", "k", &BasicProperties::new(), b"lost", true)
        .await
        .unwrap();
    let Method::BasicReturn { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected BasicReturn");
    };
    assert_eq!(reply_code, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn qos_prefetch_size_is_accepted() {
    let (_node, addr) = start_broker("qos").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    // §3.1.7 prefetch_size (byte window) is supported, not rejected.
    c.send_method(1, &Method::BasicQos { prefetch_size: 1, prefetch_count: 0, global_: false })
        .await
        .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::BasicQosOk { .. }));
    // Count-based windows keep working too.
    c.send_method(1, &Method::BasicQos { prefetch_size: 0, prefetch_count: 3, global_: false })
        .await
        .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::BasicQosOk { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_with_requeue_redelivers() {
    let (_node, addr) = start_broker("nack").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "nq", true, false).await;
    publish(&mut c, 1, "", "nq", &BasicProperties::new(), b"job", false).await.unwrap();

    // Get without ack, then nack with requeue.
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "nq".into(), no_ack: false })
        .await
        .unwrap();
    let Method::BasicGetOk { delivery_tag, .. } = c.expect(1).await.unwrap() else {
        panic!("expected BasicGetOk");
    };
    c.send_method(1, &Method::BasicNack { delivery_tag, multiple: false, requeue: true })
        .await
        .unwrap();

    // The message comes back as redelivered.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "nacked message never returned");
        c.send_method(1, &Method::BasicGet { ticket: 0, queue: "nq".into(), no_ack: true })
            .await
            .unwrap();
        match c.expect(1).await.unwrap() {
            Method::BasicGetOk { redelivered, .. } => {
                assert!(redelivered, "the returned message must be redelivered");
                return;
            }
            Method::BasicGetEmpty { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_requeues_unacked_messages() {
    let (_node, addr) = start_broker("recover").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "rq", true, false).await;
    publish(&mut c, 1, "", "rq", &BasicProperties::new(), b"r", false).await.unwrap();

    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "rq".into(), no_ack: false })
        .await
        .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetOk { .. }
    ));

    // Basic.Recover (requeue=true) puts the unacked message back.
    c.send_method(1, &Method::BasicRecover { requeue: true }).await.unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicRecoverOk { .. }
    ));

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "recovered message never returned");
        c.send_method(1, &Method::BasicGet { ticket: 0, queue: "rq".into(), no_ack: true })
            .await
            .unwrap();
        match c.expect(1).await.unwrap() {
            Method::BasicGetOk { redelivered, .. } => {
                assert!(redelivered);
                return;
            }
            Method::BasicGetEmpty { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------
// Priority, TTL, dead-lettering (queue arguments)
// ---------------------------------------------------------------------

async fn declare_with_args(c: &mut TestClient, queue: &str, args: FieldTable) {
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: queue.into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: args,
        },
    )
    .await
    .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::QueueDeclareOk { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn priority_delivers_high_first() {
    let (_node, addr) = start_broker("prio").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "pq", true, false).await;

    let mut low = BasicProperties::new();
    low.priority = Some(1);
    let mut high = BasicProperties::new();
    high.priority = Some(9);

    publish(&mut c, 1, "", "pq", &low, b"low-1", false).await.unwrap();
    publish(&mut c, 1, "", "pq", &high, b"urgent", false).await.unwrap();
    publish(&mut c, 1, "", "pq", &low, b"low-2", false).await.unwrap();

    let mut seen = Vec::new();
    for _ in 0..3 {
        c.send_method(1, &Method::BasicGet { ticket: 0, queue: "pq".into(), no_ack: true })
            .await
            .unwrap();
        let (Method::BasicGetOk { .. }, content) = c.expect_method(1).await.unwrap() else {
            panic!("expected GetOk with content");
        };
        seen.push(String::from_utf8(content.unwrap().1).unwrap());
    }
    assert_eq!(seen, vec!["urgent".to_string(), "low-1".to_string(), "low-2".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn message_ttl_expires_and_dlq_routes() {
    let (_node, addr) = start_broker("ttl").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();

    // Dead-letter topology: dlx exchange → dlq.
    c.send_method(
        1,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: "dlx".into(),
            exchange_type: "fanout".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap();
    declare(&mut c, "dlq", true, false).await;
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "dlq".into(),
            exchange: "dlx".into(),
            routing_key: "".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap();

    // Main queue with a 200ms message TTL routed to the DLX.
    let mut args = FieldTable::new();
    args.insert("x-message-ttl", switchboard_wire::field::FieldValue::SignedLongLong(200));
    args.insert(
        "x-dead-letter-exchange",
        switchboard_wire::field::FieldValue::LongString(b"dlx".to_vec()),
    );
    declare_with_args(&mut c, "ttlq", args).await;

    publish(&mut c, 1, "", "ttlq", &BasicProperties::new(), b"tick", false).await.unwrap();

    // Alive immediately.
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "dlq".into(), no_ack: true })
        .await
        .unwrap();
    assert!(matches!(c.expect(1).await.unwrap(), Method::BasicGetEmpty { .. }));

    // Expired: the janitor sweep (1 s period) dead-letters it to the DLQ.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "expired message never reached the DLQ"
        );
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        c.send_method(1, &Method::BasicGet { ticket: 0, queue: "dlq".into(), no_ack: true })
            .await
            .unwrap();
        match c.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => return, // dead-lettered ✓
            Method::BasicGetEmpty { .. } => continue,
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_without_requeue_dead_letters() {
    let (_node, addr) = start_broker("dlx-nack").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();

    c.send_method(
        1,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: "dlx".into(),
            exchange_type: "fanout".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap();
    declare(&mut c, "dlq", true, false).await;
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "dlq".into(),
            exchange: "dlx".into(),
            routing_key: "".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap();

    let mut args = FieldTable::new();
    args.insert(
        "x-dead-letter-exchange",
        switchboard_wire::field::FieldValue::LongString(b"dlx".to_vec()),
    );
    declare_with_args(&mut c, "work", args).await;

    publish(&mut c, 1, "", "work", &BasicProperties::new(), b"poison", false).await.unwrap();

    // Get without ack, then nack with requeue=false → dead-letter.
    c.send_method(1, &Method::BasicGet { ticket: 0, queue: "work".into(), no_ack: false })
        .await
        .unwrap();
    let Method::BasicGetOk { delivery_tag, .. } = c.expect(1).await.unwrap() else {
        panic!("expected GetOk");
    };
    c.send_method(1, &Method::BasicNack { delivery_tag, multiple: false, requeue: false })
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "message never reached the DLQ");
        c.send_method(1, &Method::BasicGet { ticket: 0, queue: "dlq".into(), no_ack: true })
            .await
            .unwrap();
        match c.expect(1).await.unwrap() {
            Method::BasicGetOk { .. } => return,
            Method::BasicGetEmpty { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
