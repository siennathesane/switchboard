//! Fanout duplicate-delivery regression: one confirmed fanout publish
//! must enqueue exactly once per bound queue — a second fresh (not
//! redelivered) delivery of the same message is a violation.

mod support;

use std::time::Duration;

use support::{connect_and_open, publish, start_cluster};
use switchboard_wire::method::Method;
use switchboard_wire::BasicProperties;

#[tokio::test(flavor = "multi_thread")]
async fn fanout_enqueues_exactly_once_per_queue() {
    let (_nodes, _listeners, addrs) = start_cluster("fanout-dup", 3).await;

    // Setup on node 0: fanout exchange + 3 durable queues, bound.
    let mut c = connect_and_open(&addrs[0], "/").await.expect("open");
    c.send_method(1, &Method::ExchangeDeclare {
        ticket: 0,
        exchange: "dup.fx".into(),
        exchange_type: "fanout".into(),
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        nowait: false,
        arguments: Default::default(),
    }).await.unwrap();
    let Method::ExchangeDeclareOk { .. } = c.expect(1).await.unwrap() else {
        panic!("expected ExchangeDeclareOk");
    };
    for i in 0..3u8 {
        let q = format!("dup.f.{i}");
        c.send_method(1, &Method::QueueDeclare {
            ticket: 0, queue: q.clone(), passive: false, durable: true,
            exclusive: false, auto_delete: false, nowait: false,
            arguments: Default::default(),
        }).await.unwrap();
        let Method::QueueDeclareOk { .. } = c.expect(1).await.unwrap() else {
            panic!("expected QueueDeclareOk");
        };
        c.send_method(1, &Method::QueueBind {
            ticket: 0, queue: q, exchange: "dup.fx".into(),
            routing_key: String::new(), nowait: false,
            arguments: Default::default(),
        }).await.unwrap();
        let Method::QueueBindOk { .. } = c.expect(1).await.unwrap() else {
            panic!("expected QueueBindOk");
        };
    }

    // One confirmed fanout publish.
    c.send_method(1, &Method::ConfirmSelect { nowait: false }).await.unwrap();
    let Method::ConfirmSelectOk { .. } = c.expect(1).await.unwrap() else {
        panic!("expected ConfirmSelectOk");
    };
    publish(&mut c, 1, "dup.fx", "", &BasicProperties::default(), b"payload-1", false).await.unwrap();
    let got = c.expect(1).await.unwrap();
    let Method::BasicAck { .. } = got else {
        panic!("expected BasicAck for fanout publish, got {got:?}");
    };

    // Give the executor a beat, then drain each queue with basic.get
    // until empty, counting occurrences of the payload.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut total = 0u64;
    for i in 0..3u8 {
        let q = format!("dup.f.{i}");
        let mut hc = connect_and_open(&addrs[i as usize], "/").await.expect("open get");
        let mut count = 0u64;
        for _ in 0..10 {
            match support::basic_get(&mut hc, 1, &q, true).await.unwrap() {
                Some((_tag, body)) => {
                    assert_eq!(body, b"payload-1");
                    count += 1;
                }
                None => break,
            }
        }
        assert_eq!(count, 1, "queue {q} must hold the fanout message exactly once");
        total += count;
    }
    assert_eq!(total, 3);
}
