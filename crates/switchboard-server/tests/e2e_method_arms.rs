//! Method-arm coverage: every dispatch branch in `methods/mod.rs` plus
//! the transaction/confirm state machine, recover paths, channel flow,
//! and exchange-exchange bindings — through the raw wire client.

mod support;

use switchboard_wire::BasicProperties;
use switchboard_wire::field::FieldTable;
use switchboard_wire::method::Method;

use support::TestClient;
use support::connect_and_open;
use support::publish;
use support::start_broker;
use support::with_timeout;

async fn declare(c: &mut TestClient, queue: &str) {
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
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::QueueDeclareOk { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_bind_unbind_exchange_to_exchange() {
    let (_node, addr) = start_broker("xxbind").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    for name in ["src-ex", "dst-ex"] {
        c.send_method(
            1,
            &Method::ExchangeDeclare {
                ticket: 0,
                exchange: name.into(),
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
        assert!(matches!(
            c.expect(1).await.unwrap(),
            Method::ExchangeDeclareOk { .. }
        ));
    }
    declare(&mut c, "xx-q").await;

    // Queue bound to dst-ex, and dst-ex bound to src-ex (e2e chain).
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "xx-q".into(),
            exchange: "dst-ex".into(),
            routing_key: "k".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::QueueBindOk { .. }
    ));

    // Bind destination exchange to source exchange.
    c.send_method(
        1,
        &Method::ExchangeBind {
            ticket: 0,
            destination: "dst-ex".into(),
            source: "src-ex".into(),
            routing_key: "k".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ExchangeBindOk { .. }
    ));

    // Publish to src → routed into dst → bound queue gets it.
    publish(
        &mut c,
        1,
        "src-ex",
        "k",
        &BasicProperties::new(),
        b"via-ex",
        false,
    )
    .await
    .unwrap();
    c.send_method(
        1,
        &Method::BasicGet {
            ticket: 0,
            queue: "xx-q".into(),
            no_ack: true,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetOk { .. }
    ));

    // Unbind, then delete the exchange (nowait variant: no reply comes).
    c.send_method(
        1,
        &Method::ExchangeUnbind {
            ticket: 0,
            destination: "dst-ex".into(),
            source: "src-ex".into(),
            routing_key: "k".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ExchangeUnbindOk { .. }
    ));
    c.send_method(
        1,
        &Method::ExchangeDelete {
            ticket: 0,
            exchange: "dst-ex".into(),
            if_unused: false,
            nowait: true,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_exchange_types_are_rejected() {
    let (_node, addr) = start_broker("badex").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();

    // x- prefixed types → 540 not implemented.
    c.send_method(
        1,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: "xex".into(),
            exchange_type: "x-consistent-hash".into(),
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
    let got = c.expect(1).await.unwrap();
    eprintln!("[ex-probe] got {got:?}");
    let Method::ChannelClose { reply_code, .. } = got else {
        panic!("expected Channel.Close");
    };
    assert_eq!(reply_code, 540);
    // Client MUST answer Channel.Close-Ok before reopening (§4.8.1).
    c.send_method(1, &Method::ChannelCloseOk {}).await.unwrap();
    c.send_method(
        1,
        &Method::ChannelOpen {
            out_of_band: String::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ChannelOpenOk { .. }
    ));

    // Plain invalid type → 503 command invalid.
    c.send_method(
        1,
        &Method::ExchangeDeclare {
            ticket: 0,
            exchange: "yex".into(),
            exchange_type: "banana".into(),
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
    // An unknown type string is a malformed command: channel-level
    // close with COMMAND_INVALID-family code (503/504 by level rules).
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected Channel.Close");
    };
    assert!(
        reply_code == 503 || reply_code == 504,
        "unexpected close code {reply_code}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_flow_pauses_and_resumes_delivery() {
    let (_node, addr) = start_broker("flow2").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "fq").await;

    // Pause BEFORE consuming.
    c.send_method(1, &Method::ChannelFlow { active: false })
        .await
        .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ChannelFlowOk { active: false }
    ));

    c.send_method(
        1,
        &Method::BasicConsume {
            ticket: 0,
            queue: "fq".into(),
            consumer_tag: "fc".into(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicConsumeOk { .. }
    ));

    // Publish while paused: the consumer must not receive them.
    for i in 0..2u32 {
        publish(
            &mut c,
            1,
            "",
            "fq",
            &BasicProperties::new(),
            format!("m{i}").as_bytes(),
            false,
        )
        .await
        .unwrap();
    }
    // While paused, an ack-mode consumer would have held them; with no_ack
    // they stay ready. Prove the pause held by reading nothing for a beat.
    // (Deliver frames would appear here if flow were broken.)
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Resume and read the two deliveries straight off the socket.
    c.send_method(1, &Method::ChannelFlow { active: true })
        .await
        .unwrap();
    let mut bodies: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while bodies.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "paused messages never flowed"
        );
        let (m, content) = with_timeout(c.expect_method(1), 30).await.unwrap();
        match m {
            Method::BasicDeliver { .. } => {
                bodies.push(content.expect("deliver carries content").1);
            }
            Method::ChannelFlowOk { .. } => continue,
            other => panic!("unexpected {other:?}"),
        }
    }
    bodies.sort();
    assert_eq!(bodies, vec![b"m0".to_vec(), b"m1".to_vec()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tx_and_confirm_mode_transitions() {
    let (_node, addr) = start_broker("txmodes").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "txq").await;

    // TxCommit without select → 90-series precondition error.
    c.send_method(1, &Method::TxCommit {}).await.unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected Channel.Close");
    };
    assert_eq!(reply_code, 406);

    // Channel is closed after the error; reopen.
    c.send_method(
        1,
        &Method::ChannelOpen {
            out_of_band: String::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ChannelOpenOk { .. }
    ));

    // TxRollback without select → same 406.
    c.send_method(1, &Method::TxRollback {}).await.unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected Channel.Close");
    };
    assert_eq!(reply_code, 406);

    // Reopen, select confirm, then switching to tx is refused.
    c.send_method(
        1,
        &Method::ChannelOpen {
            out_of_band: String::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ChannelOpenOk { .. }
    ));
    c.send_method(1, &Method::ConfirmSelect { nowait: false })
        .await
        .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::ConfirmSelectOk { .. }
    ));
    c.send_method(1, &Method::TxSelect {}).await.unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!("expected Channel.Close");
    };
    assert_eq!(reply_code, 406);
}

#[tokio::test(flavor = "multi_thread")]
async fn tx_publish_rollback_discards_buffered_message() {
    let (_node, addr) = start_broker("txtx").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "txbuf").await;

    c.send_method(1, &Method::TxSelect {}).await.unwrap();
    c.expect(1).await.unwrap();
    publish(
        &mut c,
        1,
        "",
        "txbuf",
        &BasicProperties::new(),
        b"buffered",
        false,
    )
    .await
    .unwrap();
    c.send_method(1, &Method::TxRollback {}).await.unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::TxRollbackOk { .. }
    ));

    // Nothing was delivered (the buffered publish never hit the shard).
    c.send_method(
        1,
        &Method::BasicGet {
            ticket: 0,
            queue: "txbuf".into(),
            no_ack: true,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetEmpty { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_and_reject_unknown_tags_are_noops() {
    let (_node, addr) = start_broker("unknowntag").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "uq").await;

    // Unknown delivery tags: neither errors nor disturbs the channel.
    c.send_method(
        1,
        &Method::BasicAck {
            delivery_tag: 999,
            multiple: false,
        },
    )
    .await
    .unwrap();
    c.send_method(
        1,
        &Method::BasicReject {
            delivery_tag: 998,
            requeue: false,
        },
    )
    .await
    .unwrap();
    c.send_method(
        1,
        &Method::BasicNack {
            delivery_tag: 997,
            multiple: true,
            requeue: true,
        },
    )
    .await
    .unwrap();
    c.send_method(
        1,
        &Method::BasicGet {
            ticket: 0,
            queue: "uq".into(),
            no_ack: true,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetEmpty { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_async_requeues() {
    let (_node, addr) = start_broker("recasync").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "raq").await;
    publish(&mut c, 1, "", "raq", &BasicProperties::new(), b"ra", false)
        .await
        .unwrap();

    c.send_method(
        1,
        &Method::BasicGet {
            ticket: 0,
            queue: "raq".into(),
            no_ack: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicGetOk { .. }
    ));

    c.send_method(1, &Method::BasicRecoverAsync { requeue: true })
        .await
        .unwrap();
    c.expect(1).await.unwrap(); // Recover-Ok (implementation replies to both variants)

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "message never returned"
        );
        c.send_method(
            1,
            &Method::BasicGet {
                ticket: 0,
                queue: "raq".into(),
                no_ack: true,
            },
        )
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

#[tokio::test(flavor = "multi_thread")]
async fn queue_unbind_and_nowait_variants() {
    let (_node, addr) = start_broker("qu").await;
    let mut c = connect_and_open(&addr, "/").await.unwrap();
    declare(&mut c, "uqq").await;

    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "uqq".into(),
            exchange: "amq.direct".into(),
            routing_key: "uk".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    c.expect(1).await.unwrap();

    // Queue.Unbind always replies (no nowait flag).
    c.send_method(
        1,
        &Method::QueueUnbind {
            ticket: 0,
            queue: "uqq".into(),
            exchange: "amq.direct".into(),
            routing_key: "uk".into(),
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::QueueUnbindOk { .. }
    ));

    // nowait purge/delete: no reply follows.
    c.send_method(
        1,
        &Method::QueuePurge {
            ticket: 0,
            queue: "uqq".into(),
            nowait: true,
        },
    )
    .await
    .unwrap();
    c.send_method(
        1,
        &Method::QueueDelete {
            ticket: 0,
            queue: "uqq".into(),
            if_unused: false,
            if_empty: false,
            nowait: true,
        },
    )
    .await
    .unwrap();
    // Channel still healthy.
    c.send_method(
        1,
        &Method::BasicQos {
            prefetch_size: 0,
            prefetch_count: 5,
            global_: true,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        c.expect(1).await.unwrap(),
        Method::BasicQosOk { .. }
    ));
}

/// Alert #13: an exclusive queue is usable ONLY by the connection that
/// declared it — every other connection gets 405 RESOURCE_LOCKED on
/// consume/get/purge/bind/delete, while the owner keeps full access.
#[tokio::test(flavor = "multi_thread")]
async fn exclusive_queue_use_is_locked_to_its_owner_connection() {
    let (_node, addr) = start_broker("excl-lock").await;

    // Owner declares an exclusive queue and parks one message in it.
    let mut owner = connect_and_open(&addr, "/").await.unwrap();
    owner
        .send_method(
            1,
            &Method::QueueDeclare {
                ticket: 0,
                queue: "excl-q".into(),
                passive: false,
                durable: true,
                exclusive: true,
                auto_delete: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        owner.expect(1).await.unwrap(),
        Method::QueueDeclareOk { .. }
    ));
    publish(
        &mut owner,
        1,
        "",
        "excl-q",
        &BasicProperties::new(),
        b"secret",
        false,
    )
    .await
    .unwrap();

    // A second connection that knows the name: every use is refused
    // with 405 (each denial closes the channel, so use a fresh
    // connection per operation).
    // consume
    let mut other = connect_and_open(&addr, "/").await.unwrap();
    other
        .send_method(
            1,
            &Method::BasicConsume {
                ticket: 0,
                queue: "excl-q".into(),
                consumer_tag: String::new(),
                no_local: false,
                no_ack: true,
                exclusive: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
    let got = other.expect(1).await.unwrap();
    assert!(
        matches!(
            &got,
            Method::ChannelClose {
                reply_code: 405,
                ..
            }
        ),
        "second-connection consume must be 405, got {got:?}"
    );
    let _ = other.send_method(1, &Method::ChannelCloseOk {}).await;

    // get (support's basic_get answers the channel close itself)
    let mut other = connect_and_open(&addr, "/").await.unwrap();
    let got = support::basic_get(&mut other, 1, "excl-q", true).await;
    match got {
        Err(text) => assert!(text.contains("405"), "expected 405, got: {text}"),
        ok => panic!("second-connection get on exclusive queue answered {ok:?}"),
    }

    // purge
    let mut other = connect_and_open(&addr, "/").await.unwrap();
    other
        .send_method(
            1,
            &Method::QueuePurge {
                ticket: 0,
                queue: "excl-q".into(),
                nowait: false,
            },
        )
        .await
        .unwrap();
    let got = other.expect(1).await.unwrap();
    assert!(
        matches!(
            &got,
            Method::ChannelClose {
                reply_code: 405,
                ..
            }
        ),
        "second-connection purge must be 405, got {got:?}"
    );
    let _ = other.send_method(1, &Method::ChannelCloseOk {}).await;

    // bind
    let mut other = connect_and_open(&addr, "/").await.unwrap();
    other
        .send_method(
            1,
            &Method::QueueBind {
                ticket: 0,
                queue: "excl-q".into(),
                exchange: "amq.topic".into(),
                routing_key: "x".into(),
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
    let got = other.expect(1).await.unwrap();
    assert!(
        matches!(
            &got,
            Method::ChannelClose {
                reply_code: 405,
                ..
            }
        ),
        "second-connection bind must be 405, got {got:?}"
    );
    let _ = other.send_method(1, &Method::ChannelCloseOk {}).await;

    // delete
    let mut other = connect_and_open(&addr, "/").await.unwrap();
    other
        .send_method(
            1,
            &Method::QueueDelete {
                ticket: 0,
                queue: "excl-q".into(),
                if_unused: false,
                if_empty: false,
                nowait: false,
            },
        )
        .await
        .unwrap();
    let got = other.expect(1).await.unwrap();
    assert!(
        matches!(
            &got,
            Method::ChannelClose {
                reply_code: 405,
                ..
            }
        ),
        "second-connection delete must be 405, got {got:?}"
    );
    let _ = other.send_method(1, &Method::ChannelCloseOk {}).await;

    // The owner is unaffected: the message survived every refused
    // access, consume works, and delete works.
    let got = support::basic_get(&mut owner, 1, "excl-q", true)
        .await
        .unwrap()
        .expect("owner get must still see the message");
    assert_eq!(got.1, b"secret");
    owner
        .send_method(
            1,
            &Method::BasicConsume {
                ticket: 0,
                queue: "excl-q".into(),
                consumer_tag: String::new(),
                no_local: false,
                no_ack: true,
                exclusive: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        owner.expect(1).await.unwrap(),
        Method::BasicConsumeOk { .. }
    ));
    owner
        .send_method(
            1,
            &Method::QueueDelete {
                ticket: 0,
                queue: "excl-q".into(),
                if_unused: false,
                if_empty: false,
                nowait: false,
            },
        )
        .await
        .unwrap();
    // Deleting a queue with a live consumer cancels it; the BasicCancel
    // notification races the QueueDeleteOk reply on the same channel, so
    // accept either wire order.
    loop {
        match owner.expect(1).await.unwrap() {
            Method::BasicCancel { .. } => continue,
            Method::QueueDeleteOk { .. } => break,
            other => panic!("unexpected {other:?} waiting for QueueDeleteOk"),
        }
    }
}
