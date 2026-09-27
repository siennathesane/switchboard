//! End-to-end single-node tests: real wire traffic over TCP against an
//! in-process broker, exercising the full stack (wire → session → cluster
//! → raft → rocksdb).

mod support;

use support::{basic_get, connect_and_open, publish, start_broker};
use switchboard_wire::method::Method;
use switchboard_wire::BasicProperties;

#[tokio::test(flavor = "multi_thread")]
async fn handshake_declare_publish_get_roundtrip() {
    let (_node, addr) = start_broker("roundtrip").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    // Declare a durable queue through the meta group.
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "jobs".into(),
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
    let Method::QueueDeclareOk {
        queue,
        message_count,
        consumer_count,
    } = c.expect(1).await.unwrap()
    else {
        panic!("expected QueueDeclareOk");
    };
    assert_eq!(queue, "jobs");
    assert_eq!((message_count, consumer_count), (0, 0));

    // Publish to the default exchange, routed by queue name.
    publish(&mut c, 1, "", "jobs", &BasicProperties::new(), b"hello switchboard", false)
        .await
        .unwrap();

    // Get it back.
    let got = basic_get(&mut c, 1, "jobs", true).await.unwrap();
    assert!(got.is_some(), "message must be deliverable");
    let (tag, body) = got.unwrap();
    assert_eq!(tag, 1);
    assert_eq!(body, b"hello switchboard");

    // Empty now.
    assert!(basic_get(&mut c, 1, "jobs", true).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_exchange_routing_and_fanout() {
    let (_node, addr) = start_broker("routing").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    for q in ["q1", "q2", "q3"] {
        c.send_method(
            1,
            &Method::QueueDeclare {
                ticket: 0,
                queue: q.into(),
                passive: false,
                durable: false,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
        let Method::QueueDeclareOk { .. } = c.expect(1).await.unwrap() else {
            panic!()
        };
    }
    // Bind q1 to amq.direct with key "a", q2 with "b", q3 fanout.
    for (q, key) in [("q1", "a"), ("q2", "b")] {
        c.send_method(
            1,
            &Method::QueueBind {
                ticket: 0,
                queue: q.into(),
                exchange: "amq.direct".into(),
                routing_key: key.into(),
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
        let Method::QueueBindOk {} = c.expect(1).await.unwrap() else {
            panic!()
        };
    }
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "q3".into(),
            exchange: "amq.fanout".into(),
            routing_key: String::new(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::QueueBindOk {} = c.expect(1).await.unwrap() else {
        panic!()
    };

    // Direct: only q1 gets key "a".
    publish(&mut c, 1, "amq.direct", "a", &BasicProperties::new(), b"to-a", false)
        .await
        .unwrap();
    assert_eq!(
        basic_get(&mut c, 1, "q1", true).await.unwrap().unwrap().1,
        b"to-a"
    );
    assert!(basic_get(&mut c, 1, "q2", true).await.unwrap().is_none());

    // Fanout: q3 receives.
    publish(&mut c, 1, "amq.fanout", "ignored", &BasicProperties::new(), b"to-q3", false)
        .await
        .unwrap();
    assert_eq!(
        basic_get(&mut c, 1, "q3", true).await.unwrap().unwrap().1,
        b"to-q3"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mandatory_unroutable_gets_returned() {
    let (_node, addr) = start_broker("mandatory").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    publish(&mut c, 1, "amq.direct", "no-such-key", &BasicProperties::new(), b"lost", true)
        .await
        .unwrap();
    let (m, content) = c.expect_method(1).await.unwrap();
    let Method::BasicReturn {
        reply_code,
        reply_text,
        ..
    } = m
    else {
        panic!("expected BasicReturn, got {}", m.name());
    };
    assert_eq!(reply_code, 404);
    assert_eq!(reply_text, "NO_ROUTE");
    assert_eq!(content.unwrap().1, b"lost");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_queue_declare_passive_is_404() {
    let (_node, addr) = start_broker("passive404").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "ghost".into(),
            passive: true,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::ChannelClose {
        reply_code,
        reply_text,
        ..
    } = c.expect(1).await.unwrap()
    else {
        panic!("expected Channel.Close");
    };
    assert_eq!(reply_code, 404);
    assert!(reply_text.contains("ghost"));
    // The close handshake: server closed channel 1 after our next frame;
    // here we just confirm the error semantics.
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_vhost_is_rejected_before_open() {
    let (_node, addr) = start_broker("badvhost").await;
    let mut c = support::TestClient::connect(&addr).await.unwrap();
    let _ = c.expect(0).await.unwrap(); // Start
    c.send_method(
        0,
        &Method::ConnectionStartOk {
            client_properties: Default::default(),
            mechanism: "PLAIN".into(),
            response: {
                let mut r = vec![0u8];
                r.extend_from_slice(b"guest");
                r.push(0);
                r.extend_from_slice(b"guest");
                r
            },
            locale: "en_US".into(),
        },
    )
    .await
    .unwrap();
    let Method::ConnectionTune { .. } = c.expect(0).await.unwrap() else {
        panic!()
    };
    c.send_method(
        0,
        &Method::ConnectionTuneOk {
            channel_max: 10,
            frame_max: 131072,
            heartbeat: 0,
        },
    )
    .await
    .unwrap();
    c.send_method(
        0,
        &Method::ConnectionOpen {
            virtual_host: "/nope".into(),
            capabilities: String::new(),
            insist: false,
        },
    )
    .await
    .unwrap();
    // §2.2.4: pre-Open errors close the socket without further data.
    let res = c.expect(0).await;
    assert!(res.is_err(), "socket must be closed without OpenOk");
}

#[tokio::test(flavor = "multi_thread")]
async fn protocol_header_garbage_gets_protocol_header_reply() {
    let (_node, addr) = start_broker("garbage").await;
    let mut c = support::TestClient::connect_raw(&addr, Some(b"HTTP/1.1 GET /"))
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt as _;
    c.writer.flush().await.unwrap();
    // Server writes a valid protocol header then closes (§4.2.2).
    let mut buf = [0u8; 8];
    let n = tokio::io::AsyncReadExt::read(&mut c.read_half, &mut buf).await.unwrap();
    assert_eq!(n, 8);
    assert_eq!(&buf, &switchboard_wire::PROTOCOL_HEADER);
    // Then EOF.
    let n2 = tokio::io::AsyncReadExt::read(&mut c.read_half, &mut buf).await.unwrap();
    assert_eq!(n2, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_confirms_arrive_in_order() {
    let (_node, addr) = start_broker("confirms").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "acked".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::QueueDeclareOk { .. } = c.expect(1).await.unwrap() else {
        panic!()
    };
    c.send_method(1, &Method::ConfirmSelect { nowait: false }).await.unwrap();
    let Method::ConfirmSelectOk {} = c.expect(1).await.unwrap() else {
        panic!()
    };

    for i in 0..5u64 {
        publish(&mut c, 1, "", "acked", &BasicProperties::new(), format!("m{i}").as_bytes(), false)
            .await
            .unwrap();
        let Method::BasicAck { delivery_tag, multiple } = c.expect(1).await.unwrap() else {
            panic!("expected confirm ack");
        };
        assert_eq!((delivery_tag, multiple), (i + 1, false));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_exchange_pattern_matching_e2e() {
    let (_node, addr) = start_broker("topic").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "prices".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let _ = c.expect(1).await.unwrap();
    c.send_method(
        1,
        &Method::QueueBind {
            ticket: 0,
            queue: "prices".into(),
            exchange: "amq.topic".into(),
            routing_key: "*.stock.#".into(),
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let _ = c.expect(1).await.unwrap();

    // Matches per §3.1.3.3.
    publish(&mut c, 1, "amq.topic", "usd.stock.nyse", &BasicProperties::new(), b"x", false)
        .await
        .unwrap();
    assert_eq!(basic_get(&mut c, 1, "prices", true).await.unwrap().unwrap().1, b"x");
    // Does not match.
    publish(&mut c, 1, "amq.topic", "stock.nasdaq", &BasicProperties::new(), b"y", false)
        .await
        .unwrap();
    assert!(basic_get(&mut c, 1, "prices", true).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_receives_deliveries_and_acks() {
    let (_node, addr) = start_broker("consume").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "stream".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let _ = c.expect(1).await.unwrap();

    // Publish three first.
    for i in 0..3 {
        publish(&mut c, 1, "", "stream", &BasicProperties::new(), format!("m{i}").as_bytes(), false)
            .await
            .unwrap();
    }

    // Consume.
    c.send_method(
        1,
        &Method::BasicConsume {
            ticket: 0,
            queue: "stream".into(),
            consumer_tag: "worker".into(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::BasicConsumeOk { consumer_tag } = c.expect(1).await.unwrap() else {
        panic!()
    };
    assert_eq!(consumer_tag, "worker");

    // Deliveries arrive with tags 1..=3.
    for i in 0..3 {
        let (m, content) = c.expect_method(1).await.unwrap();
        let Method::BasicDeliver {
            delivery_tag,
            consumer_tag: ct,
            ..
        } = m
        else {
            panic!("expected deliver, got {}", m.name());
        };
        assert_eq!(ct, "worker");
        assert_eq!(delivery_tag, i + 1);
        assert_eq!(content.unwrap().1, format!("m{i}").into_bytes());
    }

    // Ack all, then the queue is empty.
    c.send_method(1, &Method::BasicAck { delivery_tag: 3, multiple: true })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(basic_get(&mut c, 1, "stream", true).await.unwrap().is_none());

    // Cancel cleanly.
    c.send_method(1, &Method::BasicCancel { consumer_tag: "worker".into(), nowait: false })
        .await
        .unwrap();
    let Method::BasicCancelOk { consumer_tag } = c.expect(1).await.unwrap() else {
        panic!()
    };
    assert_eq!(consumer_tag, "worker");
}

#[tokio::test(flavor = "multi_thread")]
async fn unacked_messages_are_requeued_on_channel_close() {
    let (_node, addr) = start_broker("requeue").await;
    {
        let mut c = connect_and_open(&addr, "/").await.expect("open");
        c.send_method(
            1,
            &Method::QueueDeclare {
                ticket: 0,
                queue: "rq".into(),
                passive: false,
                durable: false,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: Default::default(),
            },
        )
        .await
        .unwrap();
        let _ = c.expect(1).await.unwrap();
        publish(&mut c, 1, "", "rq", &BasicProperties::new(), b"survivor", false)
            .await
            .unwrap();
        // Get it unacked, then drop the connection without acking.
        let got = basic_get(&mut c, 1, "rq", false).await.unwrap();
        assert!(got.is_some());
        // (connection dropped at scope end)
        c.send_method(1, &Method::ChannelClose {
            reply_code: 200,
            reply_text: "done".into(),
            class_id: 0,
            method_id: 0,
        })
        .await
        .unwrap();
        let Method::ChannelCloseOk {} = c.expect(1).await.unwrap() else {
            panic!()
        };
    }
    // New connection: the message is back.
    let mut c2 = connect_and_open(&addr, "/").await.expect("reopen");
    let got = basic_get(&mut c2, 1, "rq", true).await.unwrap();
    assert!(got.is_some(), "unacked message must be requeued on channel close");
    assert_eq!(got.unwrap().1, b"survivor");
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_commit_and_rollback() {
    let (_node, addr) = start_broker("tx").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "txq".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let _ = c.expect(1).await.unwrap();

    c.send_method(1, &Method::TxSelect {}).await.unwrap();
    let Method::TxSelectOk {} = c.expect(1).await.unwrap() else {
        panic!()
    };

    // Rollback: nothing lands.
    publish(&mut c, 1, "", "txq", &BasicProperties::new(), b"rolled", false)
        .await
        .unwrap();
    c.send_method(1, &Method::TxRollback {}).await.unwrap();
    let Method::TxRollbackOk {} = c.expect(1).await.unwrap() else {
        panic!()
    };
    assert!(basic_get(&mut c, 1, "txq", true).await.unwrap().is_none());

    // Commit: message lands.
    publish(&mut c, 1, "", "txq", &BasicProperties::new(), b"committed", false)
        .await
        .unwrap();
    c.send_method(1, &Method::TxCommit {}).await.unwrap();
    let Method::TxCommitOk {} = c.expect(1).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        basic_get(&mut c, 1, "txq", true).await.unwrap().unwrap().1,
        b"committed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_redeclare_with_different_arguments_is_406() {
    let (_node, addr) = start_broker("equivalence").await;
    let mut c = connect_and_open(&addr, "/").await.expect("open");

    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "eq".into(),
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
    let _ = c.expect(1).await.unwrap();

    // Same flags: ok.
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "eq".into(),
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
    let _ = c.expect(1).await.unwrap();

    // Different durability: 406.
    c.send_method(
        1,
        &Method::QueueDeclare {
            ticket: 0,
            queue: "eq".into(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: Default::default(),
        },
    )
    .await
    .unwrap();
    let Method::ChannelClose { reply_code, .. } = c.expect(1).await.unwrap() else {
        panic!()
    };
    assert_eq!(reply_code, 406);
}
