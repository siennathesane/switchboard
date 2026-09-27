//! The data-plane (shard) state machine: replicated queue contents and
//! consumer bookkeeping.
//!
//! Each shard raft group owns the messages of the queues assigned to it.
//! The group leader drives deliveries; followers replicate the same state
//! so leader failover preserves message and unacked state exactly.
//!
//! # Delivery protocol (credit-based pull)
//!
//! Delivery is *pulled* by the node hosting the consumer so that channel
//! prefetch windows (§3.1.7) are respected without the shard knowing about
//! channels:
//!
//! 1. `RegisterSubscription` records the consumer.
//! 2. The node sends `Credit { sub, count }` ("hand me up to `count`
//!    messages"). The state machine hands out ready messages — marking
//!    them unacked, or deleting them for `no_ack` consumers — and emits
//!    [`ShardEffect::MessageReady`] effects carrying the full message.
//! 3. Unused credit is remembered, so later enqueues are delivered
//!    immediately while the window stays open.
//! 4. The client acks; the node sends `Ack` plus fresh `Credit` when its
//!    window opens. Reject/Nack/recover/cancel/close send `Release`,
//!    returning messages to *ready* state; the next hand-out sets
//!    `redelivered`.
//!
//! Crash safety: the message and its unacked marker live in the raft log,
//! so after leader failover the new leader continues from identical state
//! and no acknowledged message is lost — at-least-once delivery.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::BrokerError;
use crate::model::{ConnectionId, StoredMessage, SubscriptionId};

/// Coordinator identity for transactions: (originating node, counter).
pub type TxId = (u64, u64);

/// `SubscriptionId.node` value marking a hold taken by `Basic.Get` on a
/// channel rather than by a registered consumer.
pub const GET_HOLDER_NODE: u64 = u64::MAX;

/// Upper bound on stored pull credit per consumer; the serving node keeps
/// its own window arithmetic and only needs "small" credits anyway.
const MAX_CREDIT: u32 = 1_000_000;

/// One queue's replicated message storage. Messages are ordered by a
/// per-queue sequence number allocated at apply time, so enqueue order is
/// the total order of the raft log — the "content processing path" order
/// guarantee of §4.7.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueData {
    /// Next sequence number to allocate.
    pub next_seq: u64,
    pub msgs: BTreeMap<u64, QueueMessage>,
    /// Queue-level policy parsed from declare arguments (`x-message-ttl`,
    /// `x-dead-letter-exchange`, `x-dead-letter-routing-key`).
    pub policy: QueuePolicy,
}

/// Queue policies from declare arguments.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueuePolicy {
    /// Per-queue message TTL in milliseconds (`x-message-ttl`).
    pub message_ttl_ms: Option<u64>,
    /// Exchange dead-lettered messages are republished to
    /// (`x-dead-letter-exchange`).
    pub dead_letter_exchange: Option<String>,
    /// Routing key for dead-lettered messages
    /// (`x-dead-letter-routing-key`; default: the original key).
    pub dead_letter_routing_key: Option<String>,
}

impl QueuePolicy {
    /// Parse the policy out of declare-time arguments. Unknown arguments
    /// are ignored (they may belong to other server extensions).
    pub fn from_arguments(arguments: &switchboard_wire::field::FieldTable) -> Self {
        use switchboard_wire::field::FieldValue;
        let mut policy = QueuePolicy::default();
        for (name, value) in arguments.iter() {
            match (name.as_str(), value) {
                ("x-message-ttl", FieldValue::SignedLongLong(ttl)) => {
                    policy.message_ttl_ms = u64::try_from(*ttl).ok();
                }
                ("x-message-ttl", FieldValue::SignedInt(ttl)) => {
                    policy.message_ttl_ms = u64::try_from(*ttl).ok();
                }
                ("x-message-ttl", FieldValue::UnsignedInt(ttl)) => {
                    policy.message_ttl_ms = Some(u64::from(*ttl));
                }
                ("x-dead-letter-exchange", FieldValue::LongString(ex)) => {
                    policy.dead_letter_exchange = Some(String::from_utf8_lossy(ex).into_owned());
                }
                ("x-dead-letter-routing-key", FieldValue::LongString(rk)) => {
                    policy.dead_letter_routing_key = Some(String::from_utf8_lossy(rk).into_owned());
                }
                _ => {}
            }
        }
        policy
    }
}

/// A stored message plus its delivery state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueMessage {
    pub message: StoredMessage,
    /// Set while a consumer (or a `Basic.Get` hold) is responsible for the
    /// message; `None` while it waits in the queue.
    pub held_by: Option<SubscriptionId>,
    /// True once handed out at least once; drives the `redelivered` flag of
    /// Deliver/Get-Ok (§4.7 rules on when redelivered may be set).
    pub delivered_once: bool,
    /// Wall-clock ms (command-carried, deterministic on replicas) after
    /// which an undelivered message expires (message TTL or queue TTL).
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    pub queue: String,
    /// Node hosting the client channel.
    pub node: u64,
    pub consumer_tag: String,
    pub no_ack: bool,
    /// Exclusive consumer (Basic.Consume `exclusive` flag).
    pub exclusive: bool,
    /// Owning connection, for the exclusive-consumer check.
    pub conn: ConnectionId,
    /// `channel.flow` — a paused consumer neither receives nor spends credit.
    pub active: bool,
    /// `basic.qos` byte window (§3.1.7 `prefetch_size`): while the bytes of
    /// unacknowledged held messages reach this limit, hand-out pauses.
    /// `0` = no byte window (count-based `prefetch_count` only).
    pub byte_limit: u64,
    /// Bytes of messages currently held (unacknowledged) by this consumer.
    pub held_bytes: u64,
    /// Unused pull credit.
    pub credit: u32,
}

/// One operation inside a transaction, prepared into a shard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TxOp {
    Enqueue { queue: String, message: StoredMessage },
    Ack { queue: String, seq: u64 },
}

/// The shard state machine.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ShardState {
    pub queues: BTreeMap<String, QueueData>,
    pub subs: BTreeMap<SubscriptionId, Subscription>,
    /// Transactions prepared on this shard, keyed by tx id.
    pub prepared: BTreeMap<TxId, Vec<TxOp>>,
    /// Tick each prepared tx was created at, for expiry.
    pub prepared_at: BTreeMap<TxId, u64>,
    /// How many ticks a prepared tx may live before the janitor expires it.
    pub tx_timeout_ticks: u64,
    /// Logical clock advanced once per applied command.
    pub now: u64,
    /// Wall-clock ms carried by the most recent command that stamps time
    /// (`Enqueue`, `Sweep`). Command-carried, so replicas stay identical.
    pub last_ms: u64,
}

impl ShardState {
    /// Active consumer count for a queue.
    pub fn consumer_count(&self, queue: &str) -> u32 {
        self.subs.values().filter(|s| s.queue == queue).count() as u32
    }

    /// Next ready (not held, not expired) sequence for a queue: highest
    /// message priority first (§2.1.3? priority is a RabbitMQ/`basic`
    /// property server extension), FIFO within a priority.
    pub fn next_ready(&self, queue: &str) -> Option<u64> {
        let q = self.queues.get(queue)?;
        let mut best: Option<(u8, u64)> = None;
        for (s, m) in q.msgs.iter() {
            if m.held_by.is_some() {
                continue;
            }
            if m.expires_at.map_or(false, |t| t <= self.last_ms) {
                continue;
            }
            let prio = m.message.properties.priority.unwrap_or(0);
            let better = match best {
                None => true,
                Some((bp, bs)) => prio > bp || (prio == bp && *s < bs),
            };
            if better {
                best = Some((prio, *s));
            }
        }
        best.map(|(_, s)| s)
    }

    fn queue(&self, name: &str) -> Result<&QueueData, BrokerError> {
        self.queues
            .get(name)
            .ok_or_else(|| BrokerError::not_found(format!("no queue {name:?} on this shard")))
    }

    /// Apply a shard command: mutate state, reply, emit effects. Pure —
    /// identical on every replica of the group.
    pub fn apply(&mut self, cmd: &ShardCmd) -> Result<(ShardReply, Vec<ShardEffect>), BrokerError> {
        self.now += 1;
        match cmd {
            ShardCmd::CreateQueueData { queue, policy } => {
                self.queues.entry(queue.clone()).or_insert_with(|| QueueData {
                    next_seq: 0,
                    msgs: BTreeMap::new(),
                    policy: policy.clone(),
                });
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::DeleteQueueData { queue } => {
                self.queues.remove(queue);
                // Cancel every consumer of this queue; the nodes inform the
                // client channels.
                let mut effects = Vec::new();
                for (id, s) in self
                    .subs
                    .iter()
                    .filter(|(_, s)| &s.queue == queue)
                    .map(|(id, s)| (*id, s.clone()))
                    .collect::<Vec<_>>()
                {
                    effects.push(ShardEffect::ConsumerCancelled {
                        sub: id,
                        node: s.node,
                        consumer_tag: s.consumer_tag,
                    });
                    self.subs.remove(&id);
                }
                Ok((ShardReply::Ok, effects))
            }

            ShardCmd::Enqueue { queue, message, at_ms } => {
                self.last_ms = self.last_ms.max(*at_ms);
                let seq = self.enqueue(queue, message.clone(), *at_ms)?;
                let effects = self.pump(queue);
                Ok((ShardReply::Enqueued { seq }, effects))
            }

            ShardCmd::RegisterSubscription { sub, queue, node, consumer_tag, no_ack, exclusive, conn, byte_limit } => {
                if !self.queues.contains_key(queue) {
                    return Err(BrokerError::not_found(format!("no queue {queue:?}")));
                }
                // Re-registering a sub id means the previous holder of it
                // is gone (reconnect after a crash, or cancel+consume).
                // Its unacked (held) messages can never be acked by a dead
                // session — release them so the pump redelivers (at-least-
                // once; redelivered flag set).
                self.release_orphan_holds(sub);
                // An exclusive consumer excludes every other connection;
                // joining a queue with an existing exclusive consumer is
                // refused just the same (Basic.Consume `exclusive`).
                let clash = self.subs.values().any(|s| {
                    &s.queue == queue && s.conn != *conn && (s.exclusive || *exclusive)
                });
                if clash {
                    return Err(BrokerError::access_refused(format!(
                        "queue {queue:?} has an exclusive consumer"
                    )));
                }
                self.subs.insert(
                    *sub,
                    Subscription {
                        queue: queue.clone(),
                        node: *node,
                        consumer_tag: consumer_tag.clone(),
                        no_ack: *no_ack,
                        exclusive: *exclusive,
                        conn: *conn,
                        active: true,
                        byte_limit: *byte_limit,
                        held_bytes: 0,
                        credit: 0,
                    },
                );
                Ok((
                    ShardReply::Subscribed { consumer_count: self.consumer_count(queue) },
                    vec![],
                ))
            }

            ShardCmd::UnregisterSubscription { sub } => {
                let (released, consumer_count, effects) = match self.subs.remove(sub) {
                    Some(s) => {
                        let queue = s.queue.clone();
                        let released = self.release(&queue, Some(*sub), None, vec![]);
                        let effects = self.pump(&queue);
                        let cc = self.consumer_count(&queue);
                        (released, cc, effects)
                    }
                    None => (0, 0, vec![]),
                };
                Ok((ShardReply::Unsubscribed { released, consumer_count }, effects))
            }

            ShardCmd::Flow { sub, active } => {
                if let Some(s) = self.subs.get_mut(sub) {
                    s.active = *active;
                    if *active {
                        let queue = s.queue.clone();
                        let effects = self.pump(&queue);
                        return Ok((ShardReply::Ok, effects));
                    }
                }
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::Credit { sub, count } => {
                if let Some(s) = self.subs.get_mut(sub) {
                    s.credit = s.credit.saturating_add(*count).min(MAX_CREDIT);
                    let queue = s.queue.clone();
                    let effects = self.pump(&queue);
                    return Ok((ShardReply::Ok, effects));
                }
                // Unknown consumer (vanished with its queue): harmless no-op.
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::Ack { queue, seqs } => {
                let q = self
                    .queues
                    .get_mut(queue)
                    .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")))?;
                let mut freed: Vec<(SubscriptionId, u64)> = Vec::new();
                for seq in seqs {
                    if let Some(m) = q.msgs.remove(seq) {
                        if let Some(holder) = m.held_by {
                            freed.push((holder, m.message.size()));
                        }
                    }
                }
                for (holder, bytes) in freed {
                    if let Some(s) = self.subs.get_mut(&holder) {
                        s.held_bytes = s.held_bytes.saturating_sub(bytes);
                    }
                }
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::Release { queue, sub, seqs, dead } => {
                if *dead {
                    let effects = self.dead_letter(queue, sub.clone(), seqs.clone());
                    return Ok((ShardReply::Released { released: effects.len() as u32 }, effects));
                }
                let released = self.release(queue, *sub, None, seqs.clone());
                let effects = self.pump(queue);
                Ok((ShardReply::Released { released }, effects))
            }

            ShardCmd::Sweep { at_ms } => {
                self.last_ms = self.last_ms.max(*at_ms);
                let queues: Vec<String> = self.queues.keys().cloned().collect();
                let mut effects = Vec::new();
                let mut count = 0u32;
                for q in queues {
                    count += self.expire_ready(&q, &mut effects);
                }
                Ok((ShardReply::Swept { count }, effects))
            }

            ShardCmd::RequeueOrphaned { live_nodes } => {
                // A consumer whose host node vanished cannot ack; release
                // its held deliveries so the pump redelivers them (the
                // consumer itself is dropped below, its channel is gone).
                let dead_subs: Vec<SubscriptionId> = self
                    .subs
                    .iter()
                    .filter(|(_, s)| !live_nodes.contains(&s.node))
                    .map(|(id, _)| *id)
                    .collect();
                if !dead_subs.is_empty() {
                    eprintln!("[orphan-probe] live={live_nodes:?} dead_subs={dead_subs:?}");
                }
                let mut effects = Vec::new();
                let mut requeued = 0u32;
                for sub in &dead_subs {
                    let Some(s) = self.subs.remove(sub) else { continue };
                    let Some(q) = self.queues.get_mut(&s.queue) else { continue };
                    for (seq, m) in q.msgs.iter_mut() {
                        if m.held_by == Some(*sub) {
                            m.held_by = None;
                            m.delivered_once = true;
                            requeued += 1;
                        }
                    }
                    let effects_here = self.pump(&s.queue);
                    effects.extend(effects_here);
                }
                let _ = requeued;
                Ok((ShardReply::Ok, effects))
            }

            ShardCmd::Get { queue, no_ack, get_id } => {
                // Missing queue: 404 (checked before the empty case).
                if !self.queues.contains_key(queue.as_str()) {
                    return Err(BrokerError::not_found(format!("no queue {queue:?} on this shard")));
                }
                let Some(seq) = self.next_ready(queue) else {
                    let depth = self
                        .queues
                        .get(queue)
                        .map(|q| q.msgs.len() as u32)
                        .unwrap_or(0);
                    return Ok((ShardReply::GetEmpty { depth }, vec![]));
                };
                let q = self
                    .queues
                    .get_mut(queue)
                    .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")))?;
                // Scope the mutable borrow so depth is readable after.
                let (redelivered, message) = {
                    let m = q.msgs.get_mut(&seq).expect("seq from live map");
                    let redelivered = m.delivered_once;
                    m.delivered_once = true;
                    if *no_ack {
                        let holder = SubscriptionId { node: GET_HOLDER_NODE, sub: *get_id };
                        m.held_by = Some(holder);
                    } else {
                        let holder = SubscriptionId { node: GET_HOLDER_NODE, sub: *get_id };
                        m.held_by = Some(holder);
                    }
                    (redelivered, m.message.clone())
                };
                if *no_ack {
                    q.msgs.remove(&seq);
                }
                let depth = q.msgs.len() as u32;
                Ok((ShardReply::Got { seq, redelivered, depth, message }, vec![]))
            }

            ShardCmd::Purge { queue } => {
                let q = self
                    .queues
                    .get_mut(queue)
                    .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")))?;
                // Purge clears ready messages; messages held unacked are not
                // the queue's to purge (they belong to their consumer).
                let before = q.msgs.len() as u32;
                q.msgs.retain(|_, m| m.held_by.is_some());
                Ok((ShardReply::Purged { message_count: before - q.msgs.len() as u32 }, vec![]))
            }

            ShardCmd::Stats { queue } => {
                let q = self.queue(queue)?;
                Ok((
                    ShardReply::Stats {
                        depth: q.msgs.len() as u32,
                        consumer_count: self.consumer_count(queue),
                    },
                    vec![],
                ))
            }

            ShardCmd::PrepareTx { tx, ops } => {
                self.prepared.insert(*tx, ops.clone());
                self.prepared_at.insert(*tx, self.now);
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::CommitTx { tx } => {
                let Some(ops) = self.prepared.remove(tx) else {
                    // Re-commit after recovery: idempotent.
                    return Ok((ShardReply::Ok, vec![]));
                };
                self.prepared_at.remove(tx);
                for op in &ops {
                    match op {
                        TxOp::Enqueue { queue, message } => {
                            // A queue deleted mid-transaction takes its
                            // prepared messages with it.
                            let _ = self.enqueue(queue, message.clone(), self.last_ms);
                        }
                        TxOp::Ack { queue, seq } => {
                            if let Some(q) = self.queues.get_mut(queue) {
                                q.msgs.remove(seq);
                            }
                        }
                    }
                }
                let effects = self.pump_all();
                Ok((ShardReply::Ok, effects))
            }

            ShardCmd::AbortTx { tx } => {
                self.prepared.remove(tx);
                self.prepared_at.remove(tx);
                Ok((ShardReply::Ok, vec![]))
            }

            ShardCmd::ExpirePrepared {} => {
                let cutoff = self.now.saturating_sub(self.tx_timeout_ticks);
                let expired: Vec<TxId> = self
                    .prepared_at
                    .iter()
                    .filter(|(_, t)| **t <= cutoff)
                    .map(|(id, _)| *id)
                    .collect();
                for id in &expired {
                    self.prepared.remove(id);
                    self.prepared_at.remove(id);
                }
                Ok((ShardReply::Expired { count: expired.len() as u32 }, vec![]))
            }
        }
    }

    /// Append a message to a queue, allocating its sequence number.
    fn enqueue(
        &mut self,
        queue: &str,
        message: StoredMessage,
        at_ms: u64,
    ) -> Result<u64, BrokerError> {
        // §2.1.3: the per-message expiration applies from arrival; the
        // queue-level TTL covers messages without their own.
        let per_message = message
            .properties
            .expiration
            .as_ref()
            .and_then(|e| e.parse::<u64>().ok());
        let ttl = match per_message {
            Some(t) => Some(t),
            None => {
                let q = self
                    .queues
                    .get(queue)
                    .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")))?;
                q.policy.message_ttl_ms
            }
        };
        let q = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| BrokerError::not_found(format!("no queue {queue:?}")))?;
        let seq = q.next_seq;
        q.next_seq += 1;
        q.msgs.insert(
            seq,
            QueueMessage {
                message,
                held_by: None,
                delivered_once: false,
                expires_at: ttl.map(|ttl| at_ms.saturating_add(ttl)),
            },
        );
        Ok(seq)
    }

    /// Drop expired ready messages; emit dead-letter effects for them.
    fn expire_ready(&mut self, queue: &str, effects: &mut Vec<ShardEffect>) -> u32 {
        let Some(q) = self.queues.get_mut(queue) else { return 0 };
        let expired: Vec<u64> = q
            .msgs
            .iter()
            .filter(|(_, m)| m.held_by.is_none() && m.expires_at.map_or(false, |t| t <= self.last_ms))
            .map(|(s, _)| *s)
            .collect();
        let n = expired.len() as u32;
        for seq in expired {
            if let Some(m) = q.msgs.remove(&seq) {
                let dlx = Self::dlx_of(&q.policy);
                effects.push(ShardEffect::DeadLettered {
                    queue: queue.to_string(),
                    message: m.message,
                    dlx,
                });
            }
        }
        n
    }

    fn dlx_of(policy: &QueuePolicy) -> Option<(String, Option<String>)> {
        policy
            .dead_letter_exchange
            .clone()
            .map(|ex| (ex, policy.dead_letter_routing_key.clone()))
    }

    /// Remove held messages and emit dead-letter effects (nack with
    /// `requeue = false`).
    fn dead_letter(
        &mut self,
        queue: &str,
        sub: Option<SubscriptionId>,
        seqs: Vec<u64>,
    ) -> Vec<ShardEffect> {
        let Some(q) = self.queues.get_mut(queue) else { return vec![] };
        let dlx = Self::dlx_of(&q.policy);
        let mut effects = Vec::new();
        let mut dead: Vec<u64> = Vec::new();
        for (s, m) in q.msgs.iter() {
            let hit = match &sub {
                Some(holder) => m.held_by == Some(*holder),
                None => seqs.contains(s) && m.held_by.is_some(),
            };
            if hit {
                dead.push(*s);
            }
        }
        for seq in dead {
            if let Some(m) = q.msgs.remove(&seq) {
                if let Some(holder) = sub {
                    if let Some(s) = self.subs.get_mut(&holder) {
                        s.held_bytes = s.held_bytes.saturating_sub(m.message.size());
                    }
                }
                effects.push(ShardEffect::DeadLettered {
                    queue: queue.to_string(),
                    message: m.message,
                    dlx: dlx.clone(),
                });
            }
        }
        effects
    }

    /// Return messages to ready state: `sub` selects a consumer's holds
    /// (or all holds if `None`), else exactly `seqs`.
    /// Release every message held by `sub`, across all queues of this
    /// shard. Used when a sub id is re-registered: the previous session
    /// holding messages under that id is provably gone (a live client
    /// re-registering wants redelivery), so holding them forever would
    /// strand confirmed messages after a crash.
    fn release_orphan_holds(&mut self, sub: &SubscriptionId) {
        for q in self.queues.values_mut() {
            let mut freed = 0u64;
            for m in q.msgs.values_mut() {
                if m.held_by == Some(*sub) {
                    m.held_by = None;
                    m.delivered_once = true;
                    freed += m.message.size();
                }
            }
            if freed > 0 {
                if let Some(s) = self.subs.get_mut(sub) {
                    s.held_bytes = s.held_bytes.saturating_sub(freed);
                }
            }
        }
    }

    fn release(
        &mut self,
        queue: &str,
        sub: Option<SubscriptionId>,
        _unused: Option<()>,
        seqs: Vec<u64>,
    ) -> u32 {
        let Some(q) = self.queues.get_mut(queue) else { return 0 };
        let mut freed: Vec<(SubscriptionId, u64)> = Vec::new();
        let mut n = 0;
        for (s, m) in q.msgs.iter_mut() {
            let hit = match sub {
                Some(holder) => m.held_by == Some(holder),
                None => seqs.contains(s) && m.held_by.is_some(),
            };
            if hit {
                if let Some(holder) = m.held_by {
                    freed.push((holder, m.message.size()));
                }
                m.held_by = None;
                n += 1;
            }
        }
        for (holder, bytes) in freed {
            if let Some(s) = self.subs.get_mut(&holder) {
                s.held_bytes = s.held_bytes.saturating_sub(bytes);
            }
        }
        n as u32
    }

    /// The delivery pump: hand ready messages to consumers with credit.
    /// Runs after any state change that could satisfy a pending pull.
    fn pump(&mut self, queue: &str) -> Vec<ShardEffect> {
        let mut effects = Vec::new();
        self.expire_ready(queue, &mut effects);
        loop {
            // Round-robin by subscription id: the lowest-id consumer with
            // credit and a ready message gets the next message. (True
            // round-robin across consumers lives with the channel; this is
            // deterministic on the shard.)
            let candidate = self
                .subs
                .iter()
                .filter(|(_, s)| {
                    &s.queue == queue
                        && s.active
                        && s.credit > 0
                        // Byte window (§3.1.7 prefetch_size): pause while
                        // unacknowledged bytes reach the limit.
                        && (s.byte_limit == 0 || s.held_bytes < s.byte_limit)
                })
                .map(|(id, _)| *id)
                .min();
            let Some(sub) = candidate else { break };
            let Some(seq) = self.next_ready(queue) else { break };

            let no_ack = self.subs[&sub].no_ack;
            let byte_limit = self.subs[&sub].byte_limit;
            let q = self.queues.get_mut(queue).expect("queue exists");
            let m = q.msgs.get_mut(&seq).expect("seq from live map");
            let redelivered = m.delivered_once;
            m.delivered_once = true;
            let message = m.message.clone();

            if no_ack {
                q.msgs.remove(&seq);
            } else {
                m.held_by = Some(sub);
            }
            {
                let s = self.subs.get_mut(&sub).expect("candidate sub");
                s.credit -= 1;
                if !no_ack && byte_limit > 0 {
                    s.held_bytes = s.held_bytes.saturating_add(message.size());
                }
            }
            effects.push(ShardEffect::MessageReady {
                sub,
                queue: queue.to_string(),
                seq,
                message,
                redelivered,
                deleted: no_ack,
            });
        }
        effects
    }

    fn pump_all(&mut self) -> Vec<ShardEffect> {
        let queues: Vec<String> = self.queues.keys().cloned().collect();
        let mut out = Vec::new();
        for q in queues {
            out.extend(self.pump(&q));
        }
        out
    }
}

/// Shard commands, applied through the owning shard raft group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardCmd {
    CreateQueueData { queue: String, policy: QueuePolicy },
    DeleteQueueData { queue: String },
    /// `at_ms` stamps wall-clock time for TTL bookkeeping (command-carried
    /// so every replica computes identical expiry).
    Enqueue { queue: String, message: StoredMessage, at_ms: u64 },
    RegisterSubscription {
        sub: SubscriptionId,
        queue: String,
        node: u64,
        consumer_tag: String,
        no_ack: bool,
        exclusive: bool,
        conn: ConnectionId,
        byte_limit: u64,
    },
    UnregisterSubscription { sub: SubscriptionId },
    /// `channel.flow` pause/resume for one consumer.
    Flow { sub: SubscriptionId, active: bool },
    /// Grant delivery credit: up to `count` messages may be handed out.
    Credit { sub: SubscriptionId, count: u32 },
    /// Acknowledge (remove) messages.
    Ack { queue: String, seqs: BTreeSet<u64> },
    /// Return messages to ready state: a consumer's holds (`sub`), or
    /// exactly `seqs` (`sub = None`). `dead = true` removes them and
    /// emits [`ShardEffect::DeadLettered`] (Basic.Nack/Reject with
    /// `requeue = false`).
    Release {
        queue: String,
        sub: Option<SubscriptionId>,
        seqs: Vec<u64>,
        dead: bool,
    },
    /// Janitor sweep: drop (dead-letter) expired ready messages.
    Sweep { at_ms: u64 },
    /// Release held messages whose consumer's node is no longer in
    /// `live_nodes` (crashed channel host): at-least-once redelivery.
    RequeueOrphaned { live_nodes: std::collections::BTreeSet<u64> },
    /// Synchronous single-message take (Basic.Get). `get_id` is the
    /// channel's local identifier for the resulting hold.
    Get { queue: String, no_ack: bool, get_id: u64 },
    Purge { queue: String },
    Stats { queue: String },
    /// Two-phase transactions: prepare stores, commit applies, abort drops.
    PrepareTx { tx: TxId, ops: Vec<TxOp> },
    CommitTx { tx: TxId },
    AbortTx { tx: TxId },
    /// Janitor sweep for abandoned prepared transactions.
    ExpirePrepared {},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardReply {
    Ok,
    Enqueued { seq: u64 },
    Subscribed { consumer_count: u32 },
    Unsubscribed { released: u32, consumer_count: u32 },
    Released { released: u32 },
    Got { seq: u64, redelivered: bool, depth: u32, message: StoredMessage },
    GetEmpty { depth: u32 },
    Purged { message_count: u32 },
    Stats { depth: u32, consumer_count: u32 },
    Expired { count: u32 },
    Swept { count: u32 },
}

/// Outbound work the leader performs after applying entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardEffect {
    /// Ship a message to the node hosting this consumer. `deleted` marks
    /// auto-ack deliveries (the message is already gone from the queue).
    MessageReady {
        sub: SubscriptionId,
        queue: String,
        seq: u64,
        message: StoredMessage,
        redelivered: bool,
        deleted: bool,
    },
    /// A message expired or was rejected with `requeue = false`. The
    /// leader republishes it to the dead-letter exchange when the queue
    /// has one, else drops it.
    DeadLettered {
        queue: String,
        message: StoredMessage,
        /// `(dead-letter-exchange, routing-key override)`; `None` in the
        /// second slot means "keep the original routing key".
        dlx: Option<(String, Option<String>)>,
    },
    /// The queue went away: cancel the consumer on its channel.
    ConsumerCancelled {
        sub: SubscriptionId,
        node: u64,
        consumer_tag: String,
    },
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
