//! Tests for `shard.

use super::*;
use switchboard_wire::BasicProperties;

fn msg(body: &[u8]) -> StoredMessage {
    StoredMessage {
        properties: BasicProperties::new(),
        body: body.to_vec(),
        exchange: "amq.direct".into(),
        routing_key: "rk".into(),
        persistent: false,
    }
}

fn sub(id: u64) -> SubscriptionId {
    SubscriptionId { node: 1, sub: id }
}

fn conn() -> ConnectionId {
    ConnectionId { node: 1, conn: 100 }
}

fn state_with_queue() -> ShardState {
    let mut s = ShardState::default();
    s.apply(&ShardCmd::CreateQueueData { queue: "q".into(), policy: Default::default() }).unwrap();
    s
}

/// Queue depth only.
fn stats_of(s: &mut ShardState, q: &str) -> u32 {
    stats_full(s, q).0
}

fn stats_full(s: &mut ShardState, q: &str) -> (u32, u32) {
    let (ShardReply::Stats { depth, consumer_count }, _) =
        s.apply(&ShardCmd::Stats { queue: q.into() }).unwrap()
    else {
        panic!("expected Stats")
    };
    (depth, consumer_count)
}

fn enqueue(s: &mut ShardState, body: &[u8]) -> u64 {
    let (ShardReply::Enqueued { seq }, _) =
        s.apply(&ShardCmd::Enqueue { queue: "q".into(), message: msg(body), at_ms: 100 }).unwrap()
    else {
        panic!("expected Enqueued")
    };
    seq
}

fn effects_of(r: &Result<(ShardReply, Vec<ShardEffect>), BrokerError>) -> Vec<ShardEffect> {
    r.as_ref().unwrap().1.clone()
}

#[test]
fn enqueue_allocates_sequence_numbers_in_order() {
    let mut s = state_with_queue();
    let a = enqueue(&mut s, b"one");
    let b = enqueue(&mut s, b"two");
    assert_eq!((a, b), (0, 1));
    assert_eq!(stats_of(&mut s, "q"), 2);
}

#[test]
fn credit_delivers_and_marks_unacked() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"hello");

    // Credit 1: one message handed out, marked unacked.
    let effects = effects_of(&s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }));
    assert_eq!(effects.len(), 1);
    let ShardEffect::MessageReady { sub: got_sub, seq, message, redelivered, deleted, .. } =
        &effects[0]
    else {
        panic!("expected MessageReady");
    };
    assert_eq!(*got_sub, sub(1));
    assert_eq!(*seq, 0);
    assert_eq!(message.body, b"hello");
    assert!(!redelivered);
    assert!(!deleted);

    // The message is still queued but held.
    let (depth, consumer_count) = stats_full(&mut s, "q");
    assert_eq!(depth, 1);
    assert_eq!(consumer_count, 1);
}

#[test]
fn unused_credit_delivers_future_enqueues() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    // Credit with an empty queue: stored, no effects.
    let effects = effects_of(&s.apply(&ShardCmd::Credit { sub: sub(1), count: 5 }));
    assert!(effects.is_empty());

    // Next enqueue is delivered immediately.
    enqueue(&mut s, b"x");
    let effects = effects_of(&s.apply(&ShardCmd::Ack { queue: "q".into(), seqs: [0].into() }));
    // Ack of a held message does not pump (message already gone).
    assert!(effects.is_empty());
}

#[test]
fn no_ack_consumers_delete_on_delivery() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(2),
        queue: "q".into(),
        node: 9,
        consumer_tag: "fire".into(),
        no_ack: true,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"ephemeral");
    let effects = effects_of(&s.apply(&ShardCmd::Credit { sub: sub(2), count: 1 }));
    assert_eq!(effects.len(), 1);
    let ShardEffect::MessageReady { deleted, redelivered, .. } = &effects[0] else {
        panic!()
    };
    assert!(*deleted);
    assert!(!*redelivered);
    // Gone from the queue.
    assert_eq!(stats_of(&mut s, "q"), 0);
}

#[test]
fn ack_removes_and_release_redelivers() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"m1");
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 });
    // Client rejects with requeue: release the hold.
    let (ShardReply::Released { released }, effects) =
        s.apply(&ShardCmd::Release { queue: "q".into(), sub: Some(sub(1)), seqs: vec![], dead: false })
            .unwrap()
    else {
        panic!()
    };
    assert_eq!(released, 1);
    // No credit left: nothing pumped out yet.
    assert!(effects.is_empty());

    // Next hand-out is a redelivery.
    let effects = effects_of(&s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }));
    let ShardEffect::MessageReady { redelivered, seq, .. } = &effects[0] else { panic!() };
    assert_eq!(*seq, 0);
    assert!(*redelivered);

    // Ack removes it for good.
    s.apply(&ShardCmd::Ack { queue: "q".into(), seqs: [0].into() });
    assert_eq!(stats_of(&mut s, "q"), 0);
}

#[test]
fn release_by_explicit_seqs() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"m");
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 });
    // Channel close path: release specific seqs (e.g. a Basic.Get hold).
    let (ShardReply::Released { released }, _) = s
        .apply(&ShardCmd::Release {
            queue: "q".into(),
            sub: None,
            seqs: vec![0, 99], // 99 never existed; ignored
            dead: false,
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(released, 1);
}

#[test]
fn cancel_releases_that_consumers_holds_only() {
    let mut s = state_with_queue();
    for id in [1u64, 2u64] {
        s.apply(&ShardCmd::RegisterSubscription {
            sub: sub(id),
            queue: "q".into(),
            node: 9,
            consumer_tag: format!("c{id}"),
            no_ack: false,
            exclusive: false,
            conn: conn(),
            byte_limit: 0,
        })
        .unwrap();
    }
    enqueue(&mut s, b"a"); // -> sub 1 (lowest id wins first)
    enqueue(&mut s, b"b"); // -> sub 2
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 });
    s.apply(&ShardCmd::Credit { sub: sub(2), count: 1 });
    // Both holds are out; cancel sub 1: only its message returns.
    let (ShardReply::Unsubscribed { released, consumer_count }, _) =
        s.apply(&ShardCmd::UnregisterSubscription { sub: sub(1) }).unwrap()
    else {
        panic!("expected Unsubscribed")
    };
    assert_eq!((released, consumer_count), (1, 1));
}

#[test]
fn round_robin_falls_to_next_consumer_when_one_is_full() {
    let mut s = state_with_queue();
    for id in [1u64, 2u64] {
        s.apply(&ShardCmd::RegisterSubscription {
            sub: sub(id),
            queue: "q".into(),
            node: 9,
            consumer_tag: format!("c{id}"),
            no_ack: false,
            exclusive: false,
            conn: conn(),
            byte_limit: 0,
        })
        .unwrap();
    }
    // Only sub 2 has credit.
    s.apply(&ShardCmd::Credit { sub: sub(2), count: 3 });
    enqueue(&mut s, b"a");
    enqueue(&mut s, b"b");
    let _ = s.apply(&ShardCmd::Enqueue { queue: "q".into(), message: msg(b"c"), at_ms: 100 });
    // All three went to sub 2 (sub 1 has no credit).
    let to_sub2 = s
        .subs
        .iter()
        .filter(|(id, _)| **id == sub(2))
        .count();
    assert_eq!(to_sub2, 1);
    assert_eq!(s.subs[&sub(2)].credit, 0);
}

#[test]
fn exclusive_consumer_blocks_other_connections() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "solo".into(),
        no_ack: false,
        exclusive: true,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    let other = ConnectionId { node: 2, conn: 5 };
    let e = s
        .apply(&ShardCmd::RegisterSubscription {
            sub: sub(2),
            queue: "q".into(),
            node: 9,
            consumer_tag: "intruder".into(),
            no_ack: false,
            exclusive: false,
            conn: other,
                byte_limit: 0,
        })
        .unwrap_err();
    assert_eq!(e.code, 403);

    // Same connection: fine.
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(3),
        queue: "q".into(),
        node: 9,
        consumer_tag: "sidekick".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
}

#[test]
fn flow_pauses_and_resumes() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 3 });
    s.apply(&ShardCmd::Flow { sub: sub(1), active: false });
    enqueue(&mut s, b"held");
    // Paused: no delivery.
    assert!(effects_of(&s.apply(&ShardCmd::Enqueue { queue: "q".into(), message: msg(b"x"), at_ms: 100 }))
        .is_empty());
    // Resume: deliveries flow.
    let effects = effects_of(&s.apply(&ShardCmd::Flow { sub: sub(1), active: true }));
    assert_eq!(effects.len(), 2);
}

#[test]
fn get_semantics() {
    let mut s = state_with_queue();
    // Empty: GetEmpty with depth 0.
    let (r, _) = s
        .apply(&ShardCmd::Get { queue: "q".into(), no_ack: false, get_id: 7 })
        .unwrap();
    assert_eq!(r, ShardReply::GetEmpty { depth: 0 });

    enqueue(&mut s, b"first");
    enqueue(&mut s, b"second");
    let (ShardReply::Got { seq, redelivered, depth, message }, _) = s
        .apply(&ShardCmd::Get { queue: "q".into(), no_ack: false, get_id: 7 })
        .unwrap()
    else {
        panic!("expected Got")
    };
    assert_eq!((seq, redelivered, depth, message.body), (0, false, 2, b"first".to_vec()));

    // Depth counts both: the held message is still queued.
    // Ack removes it.
    s.apply(&ShardCmd::Ack { queue: "q".into(), seqs: [0].into() });
    // no_ack get deletes immediately.
    let (ShardReply::Got { seq, .. }, _) = s
        .apply(&ShardCmd::Get { queue: "q".into(), no_ack: true, get_id: 8 })
        .unwrap()
    else {
        panic!("expected Got")
    };
    assert_eq!(seq, 1);
    assert_eq!(stats_of(&mut s, "q"), 0);

    // Get on a missing queue: 404.
    assert_eq!(
        s.apply(&ShardCmd::Get { queue: "nope".into(), no_ack: false, get_id: 9 })
            .unwrap_err()
            .code,
        404
    );
}

#[test]
fn purge_keeps_unacked() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"held");
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 });
    enqueue(&mut s, b"ready");
    let (ShardReply::Purged { message_count }, _) =
        s.apply(&ShardCmd::Purge { queue: "q".into() }).unwrap()
    else {
        panic!("expected Purged")
    };
    assert_eq!(message_count, 1, "only the ready message purges");
    assert_eq!(stats_of(&mut s, "q"), 1, "the unacked message survives");
}

#[test]
fn queue_deletion_cancels_consumers() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(1),
        queue: "q".into(),
        node: 42,
        consumer_tag: "ct".into(),
        no_ack: false,
        exclusive: false,
        conn: conn(),
            byte_limit: 0,
    })
    .unwrap();
    enqueue(&mut s, b"doomed");
    let (_, effects) = s.apply(&ShardCmd::DeleteQueueData { queue: "q".into() }).unwrap();
    assert_eq!(
        effects,
        vec![ShardEffect::ConsumerCancelled {
            sub: sub(1),
            node: 42,
            consumer_tag: "ct".into(),
        }]
    );
    assert!(s.subs.is_empty());
    assert!(s.queues.is_empty());
}

#[test]
fn transactions_prepare_commit_abort_expire() {
    let mut s = state_with_queue();
    let tx: TxId = (1, 1);

    // Prepare enqueues + acks.
    s.apply(&ShardCmd::PrepareTx {
        tx,
        ops: vec![
            TxOp::Enqueue { queue: "q".into(), message: msg(b"tx-msg") },
            TxOp::Ack { queue: "q".into(), seq: u64::MAX }, // harmless ack
        ],
    })
    .unwrap();
    assert!(s.prepared.contains_key(&tx));

    // Abort discards.
    s.apply(&ShardCmd::AbortTx { tx }).unwrap();
    assert!(!s.prepared.contains_key(&tx));

    // Prepare + commit applies.
    s.apply(&ShardCmd::PrepareTx {
        tx,
        ops: vec![TxOp::Enqueue { queue: "q".into(), message: msg(b"tx-msg") }],
    })
    .unwrap();
    s.apply(&ShardCmd::CommitTx { tx }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 1);

    // Re-commit is idempotent.
    s.apply(&ShardCmd::CommitTx { tx }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 1);

    // Expiry: prepare, advance past the timeout, sweep.
    s.tx_timeout_ticks = 5;
    let tx2: TxId = (1, 2);
    s.apply(&ShardCmd::PrepareTx { tx: tx2, ops: vec![] }).unwrap();
    for _ in 0..10 {
        s.apply(&ShardCmd::ExpirePrepared {}).unwrap();
    }
    assert!(!s.prepared.contains_key(&tx2));
}

#[test]
fn commit_drops_ops_for_deleted_queues() {
    let mut s = state_with_queue();
    let tx: TxId = (1, 1);
    s.apply(&ShardCmd::PrepareTx {
        tx,
        ops: vec![TxOp::Enqueue { queue: "q".into(), message: msg(b"ok") }],
    })
    .unwrap();
    s.apply(&ShardCmd::DeleteQueueData { queue: "q".into() }).unwrap();
    // Commit after deletion: the enqueue vanishes with its queue.
    let (_, _) = s.apply(&ShardCmd::CommitTx { tx }).unwrap();
    assert!(!s.queues.contains_key("q"));
}

#[test]
fn errors_and_no_ops() {
    let mut s = state_with_queue();
    // Enqueue to a missing queue: 404.
    assert_eq!(
        s.apply(&ShardCmd::Enqueue { queue: "nope".into(), message: msg(b"x"), at_ms: 100 })
            .unwrap_err()
            .code,
        404
    );
    // Credit for an unknown sub: silent no-op.
    let (r, e) = s.apply(&ShardCmd::Credit { sub: sub(99), count: 5 }).unwrap();
    assert_eq!(r, ShardReply::Ok);
    assert!(e.is_empty());
    // Unregister for unknown sub: harmless.
    let (r, _) = s.apply(&ShardCmd::UnregisterSubscription { sub: sub(99) }).unwrap();
    assert_eq!(r, ShardReply::Unsubscribed { released: 0, consumer_count: 0 });
    // Ack on missing queue: 404.
    assert_eq!(
        s.apply(&ShardCmd::Ack { queue: "ghost".into(), seqs: [1].into() })
            .unwrap_err()
            .code,
        404
    );
    // Purge missing queue: 404.
    assert_eq!(s.apply(&ShardCmd::Purge { queue: "g".into() }).unwrap_err().code, 404);
    // Register on missing queue: 404.
    assert_eq!(
        s.apply(&ShardCmd::RegisterSubscription {
            sub: sub(1),
            queue: "g".into(),
            node: 1,
            consumer_tag: "x".into(),
            no_ack: false,
            exclusive: false,
            conn: conn(),
            byte_limit: 0,
        })
        .unwrap_err()
        .code,
        404
    );
    // CreateQueueData is idempotent (or_default).
    s.apply(&ShardCmd::CreateQueueData { queue: "q".into(), policy: Default::default() }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
}

// ---------------------------------------------------------------------
// Priority, TTL, dead-lettering, byte windows
// ---------------------------------------------------------------------

fn msg_with_priority(body: &[u8], priority: u8) -> StoredMessage {
    let mut m = msg(body);
    m.properties.priority = Some(priority);
    m
}

fn register(sub_id: u64, s: &mut ShardState, no_ack: bool, byte_limit: u64) {
    s.apply(&ShardCmd::RegisterSubscription {
        sub: sub(sub_id),
        queue: "q".into(),
        node: 9,
        consumer_tag: "ct".into(),
        no_ack,
        exclusive: false,
        conn: conn(),
        byte_limit,
    })
    .unwrap();
}

fn delivered_bodies(effects: &[ShardEffect]) -> Vec<Vec<u8>> {
    effects
        .iter()
        .filter_map(|e| match e {
            ShardEffect::MessageReady { message, .. } => Some(message.body.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn priority_orders_delivery() {
    let mut s = state_with_queue();
    register(1, &mut s, true, 0);
    // Delivered in enqueue order absent priorities.
    enqueue(&mut s, b"low-a");
    let hi = {
        let (ShardReply::Enqueued { seq }, _) = s
            .apply(&ShardCmd::Enqueue {
                queue: "q".into(),
                message: msg_with_priority(b"urgent", 9),
                at_ms: 100,
            })
            .unwrap()
        else { panic!("enqueue") };
        seq
    };
    enqueue(&mut s, b"low-b");

    let effects = s.apply(&ShardCmd::Credit { sub: sub(1), count: 10 }).unwrap().1;
    let bodies = delivered_bodies(&effects);
    // The priority-9 message jumps the queue; the others keep FIFO.
    assert_eq!(bodies[0], b"urgent".to_vec(), "priority 9 first");
    assert_eq!(bodies[1], b"low-a".to_vec());
    assert_eq!(bodies[2], b"low-b".to_vec());
    let _ = hi;
}

#[test]
fn ttl_expires_and_dead_letters() {
    let mut s = ShardState::default();
    let policy = QueuePolicy {
        message_ttl_ms: Some(500),
        dead_letter_exchange: Some("dlx".into()),
        dead_letter_routing_key: None,
    };
    s.apply(&ShardCmd::CreateQueueData { queue: "q".into(), policy }).unwrap();
    enqueue(&mut s, b"short-lived");

    // Not expired yet: still delivered.
    s.apply(&ShardCmd::Sweep { at_ms: 400 }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 1);

    // Expired: sweep removes it and emits a dead-letter effect.
    let (_, effects) = s.apply(&ShardCmd::Sweep { at_ms: 1000 }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
    match &effects[0] {
        ShardEffect::DeadLettered { message, dlx, .. } => {
            assert_eq!(message.body, b"short-lived".to_vec());
            assert_eq!(dlx.as_ref().unwrap().0, "dlx");
        }
        other => panic!("expected DeadLettered, got {other:?}"),
    }

    // An expired message is never handed out even without a sweep.
    let mut s2 = ShardState::default();
    s2.apply(&ShardCmd::CreateQueueData { queue: "q".into(), policy: Default::default() })
        .unwrap();
    register(1, &mut s2, true, 0);
    {
        // Per-message expiration of 50ms set at t=100 → dead at t=200.
        let mut m = msg(b"expiring");
        m.properties.expiration = Some("50".into());
        s2.apply(&ShardCmd::Enqueue { queue: "q".into(), message: m, at_ms: 100 }).unwrap();
    }
    s2.apply(&ShardCmd::Credit { sub: sub(1), count: 5 }).unwrap().1;
    assert!(
        delivered_bodies(&s2.apply(&ShardCmd::Credit { sub: sub(1), count: 5 }).unwrap().1).is_empty(),
        "expired message must not deliver"
    );
}

#[test]
fn nack_without_requeue_dead_letters() {
    let mut s = state_with_queue();
    enqueue(&mut s, b"doomed");
    register(1, &mut s, false, 0);
    let effects = s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap().1;
    assert_eq!(delivered_bodies(&effects), vec![b"doomed".to_vec()]);

    let (_, effects) = s
        .apply(&ShardCmd::Release {
            queue: "q".into(),
            sub: Some(sub(1)),
            seqs: vec![0],
            dead: true,
        })
        .unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
    match &effects[0] {
        ShardEffect::DeadLettered { message, dlx, .. } => {
            assert_eq!(message.body, b"doomed".to_vec());
            assert!(dlx.is_none(), "no DLX configured: the message is dropped");
        }
        other => panic!("expected DeadLettered, got {other:?}"),
    }
}

#[test]
fn byte_window_gates_delivery_until_ack() {
    let mut s = state_with_queue();
    register(1, &mut s, false, 8); // window of 8 unacked bytes
    enqueue(&mut s, b"12345678"); // exactly 8 bytes
    enqueue(&mut s, b"more");

    let effects = s.apply(&ShardCmd::Credit { sub: sub(1), count: 10 }).unwrap().1;
    // Only the first message fits the byte window.
    assert_eq!(delivered_bodies(&effects), vec![b"12345678".to_vec()]);

    // Ack reopens the window: the next message flows.
    let (ShardReply::Ok, effects) = s
        .apply(&ShardCmd::Ack {
            queue: "q".into(),
            seqs: [0u64].into_iter().collect(),
        })
        .unwrap()
    else { panic!("ack") };
    let effects = s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap().1;
    assert_eq!(delivered_bodies(&effects), vec![b"more".to_vec()]);
}

#[test]
fn release_restores_byte_window() {
    let mut s = state_with_queue();
    register(1, &mut s, false, 4);
    enqueue(&mut s, b"abcd");
    s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap();
    // Held 4/4 bytes: a second message cannot fit even with credit.
    enqueue(&mut s, b"wxyz");
    assert!(
        delivered_bodies(&s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap().1).is_empty()
    );
    // Release returns the bytes to the window AND the message to the
    // ready set — the standing credit re-delivers it inside the Release
    // apply itself (FIFO at priority 0).
    let (_, effects) = s
        .apply(&ShardCmd::Release {
            queue: "q".into(),
            sub: Some(sub(1)),
            seqs: vec![0],
            dead: false,
        })
        .unwrap();
    assert_eq!(delivered_bodies(&effects), vec![b"abcd".to_vec()]);
    // The window now holds 4/4 again, so the second message stays queued.
    assert!(
        delivered_bodies(&s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap().1).is_empty()
    );
}

// ---------------------------------------------------------------------
// QueuePolicy parsing + shard janitor arms
// ---------------------------------------------------------------------

fn policy_from(table: switchboard_wire::field::FieldTable) -> QueuePolicy {
    QueuePolicy::from_arguments(&table)
}

#[test]
fn queue_policy_parses_ttl_and_dlx_variants() {
    use switchboard_wire::field::FieldValue;

    let mut t = switchboard_wire::field::FieldTable::new();
    t.insert("x-message-ttl", FieldValue::SignedLongLong(5000));
    t.insert("x-dead-letter-exchange", FieldValue::LongString(b"dlx".to_vec()));
    t.insert("x-dead-letter-routing-key", FieldValue::LongString(b"dlrk".to_vec()));
    let p = policy_from(t);
    assert_eq!(p.message_ttl_ms, Some(5000));
    assert_eq!(p.dead_letter_exchange.as_deref(), Some("dlx"));
    assert_eq!(p.dead_letter_routing_key.as_deref(), Some("dlrk"));

    let mut t = switchboard_wire::field::FieldTable::new();
    t.insert("x-message-ttl", FieldValue::SignedInt(300));
    assert_eq!(policy_from(t).message_ttl_ms, Some(300));

    let mut t = switchboard_wire::field::FieldTable::new();
    t.insert("x-message-ttl", FieldValue::UnsignedInt(250));
    assert_eq!(policy_from(t).message_ttl_ms, Some(250));

    // Unknown args and negative TTLs are ignored.
    let mut t = switchboard_wire::field::FieldTable::new();
    t.insert("x-unknown", FieldValue::Boolean(true));
    t.insert("x-message-ttl", FieldValue::SignedInt(-1));
    let ignored = policy_from(t);
    assert_eq!(ignored.message_ttl_ms, None);
    assert_eq!(ignored.dead_letter_exchange, None);
}

#[test]
fn tx_commit_applies_ack_ops_too() {
    let mut s = state_with_queue();
    enqueue(&mut s, b"tx-acked");
    register(1, &mut s, false, 0);

    // Deliver then prepare an Ack op for the held message.
    let effects = s.apply(&ShardCmd::Credit { sub: sub(1), count: 1 }).unwrap().1;
    assert_eq!(delivered_bodies(&effects), vec![b"tx-acked".to_vec()]);

    s.apply(&ShardCmd::PrepareTx {
        tx: (7, 1),
        ops: vec![TxOp::Ack { queue: "q".into(), seq: 0 }],
    })
    .unwrap();
    assert_eq!(stats_of(&mut s, "q"), 1);

    s.apply(&ShardCmd::CommitTx { tx: (7, 1) }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
}

#[test]
fn tx_abort_keeps_state() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::PrepareTx {
        tx: (7, 2),
        ops: vec![TxOp::Enqueue { queue: "q".into(), message: msg(b"never") }],
    })
    .unwrap();
    s.apply(&ShardCmd::AbortTx { tx: (7, 2) }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
    // Committing after abort is an idempotent no-op.
    s.apply(&ShardCmd::CommitTx { tx: (7, 2) }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
}

#[test]
fn missing_queue_arms_surface_404() {
    let mut s = state_with_queue();
    assert_eq!(
        s.apply(&ShardCmd::Release {
            queue: "nope".into(),
            sub: None,
            seqs: vec![1],
            dead: false,
        })
        .unwrap()
        .0,
        ShardReply::Released { released: 0 }
    );
    assert_eq!(
        s.apply(&ShardCmd::Stats { queue: "nope".into() }).unwrap_err().code,
        404
    );
    assert_eq!(
        s.apply(&ShardCmd::Purge { queue: "nope".into() }).unwrap_err().code,
        404
    );
    // Credit for an unknown consumer is a harmless no-op.
    assert_eq!(
        s.apply(&ShardCmd::Credit { sub: sub(99), count: 1 }).unwrap().0,
        ShardReply::Ok
    );
    // Unregistering an unknown consumer too.
    assert_eq!(
        s.apply(&ShardCmd::UnregisterSubscription { sub: sub(99) }).unwrap().0,
        ShardReply::Unsubscribed { released: 0, consumer_count: 0 }
    );
}

#[test]
fn sweep_on_empty_state_is_fine() {
    let mut s = ShardState::default();
    let (ShardReply::Swept { count }, effects) =
        s.apply(&ShardCmd::Sweep { at_ms: 1000 }).unwrap()
    else {
        panic!("expected Swept");
    };
    assert_eq!((count, effects.len()), (0, 0));
}

#[test]
fn expire_prepared_janitor_drops_stale_transactions() {
    let mut s = state_with_queue();
    s.apply(&ShardCmd::PrepareTx {
        tx: (9, 1),
        ops: vec![TxOp::Enqueue { queue: "q".into(), message: msg(b"stale") }],
    })
    .unwrap();
    // Advance the logical clock past the timeout via many no-op commands.
    for _ in 0..(s.tx_timeout_ticks + 2) {
        s.apply(&ShardCmd::Stats { queue: "q".into() }).unwrap();
    }
    let (ShardReply::Expired { count }, _) = s.apply(&ShardCmd::ExpirePrepared {}).unwrap() else {
        panic!("expected Expired");
    };
    assert_eq!(count, 1);
    // Committing the expired tx is an idempotent no-op.
    s.apply(&ShardCmd::CommitTx { tx: (9, 1) }).unwrap();
    assert_eq!(stats_of(&mut s, "q"), 0);
}
