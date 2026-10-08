//! `RaftStateMachine` over RocksDB: the replicated broker state plus
//! snapshot install/emit.
//!
//! The state machine holds a [`BrokerState`] (meta + shard halves). Every
//! applied command produces a [`BrokerReply`] for the waiting caller and
//! zero or more [`Effect`]s pushed onto an unbounded channel; the group
//! leader drains that channel to perform cross-node work (shipping
//! delivered messages, cascading deletions). Replicas drop their effects —
//! only the leader acts on them.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use crate::typ::BrokerCommand;

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::raft::ClientWriteResponse;
use openraft::AnyError;
use openraft::Entry;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::StorageError;
use openraft::StorageIOError;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::UnboundedSender;

use crate::kv::RocksKv;
use crate::typ::BrokerReply;
use crate::typ::BrokerState;
use crate::typ::Effect;
use crate::typ::NodeId;
use crate::typ::SwitchboardTypeConfig;

fn sm_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"sm");
    k
}

fn last_applied_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"last-applied");
    k
}

fn membership_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"membership");
    k
}

fn snap_meta_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"snap/meta");
    k
}

fn snap_data_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"snap/data");
    k
}

/// Prefix covering every per-message record of the group.
fn msg_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"m/");
    k
}

/// Prefix covering one queue's per-message records. The queue name is
/// length-prefixed (queue names may contain `/` and any other byte), so
/// the key is `gN:m/<u16 len><queue>/<seq:020>`.
fn msg_queue_prefix(prefix: &[u8], queue: &str) -> Vec<u8> {
    let mut k = msg_prefix(prefix);
    k.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    k.extend_from_slice(queue.as_bytes());
    k.push(b'/');
    k
}

fn msg_key(prefix: &[u8], queue: &str, seq: u64) -> Vec<u8> {
    let mut k = msg_queue_prefix(prefix, queue);
    k.extend_from_slice(format!("{seq:020}").as_bytes());
    k
}

/// Split a message key back into `(queue, seq)`.
fn parse_msg_key(key: &[u8], prefix: &[u8]) -> Option<(String, u64)> {
    let rest = key.strip_prefix(msg_prefix(prefix).as_slice())?;
    if rest.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([rest[0], rest[1]]) as usize;
    let queue = rest.get(2..2 + len)?;
    let queue = std::str::from_utf8(queue).ok()?.to_string();
    let tail = rest.get(2 + len..)?;
    let tail = tail.strip_prefix(b"/".as_slice())?;
    if tail.len() != 20 {
        return None;
    }
    let seq: u64 = std::str::from_utf8(tail).ok()?.parse().ok()?;
    Some((queue, seq))
}

/// Snapshot image of a group's full state: [`BrokerState`] with every
/// message record included. The live-state blob omits message bodies
/// (they persist under their own keys), but a snapshot must carry the
/// whole state, so this mirrors the shape with `msgs` present.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct BrokerStateImage {
    meta: switchboard_core::topology::MetaState,
    shard: ShardStateImage,
    bootstrapped: bool,
    dedup: crate::typ::DedupLog,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct ShardStateImage {
    queues: BTreeMap<String, QueueImage>,
    subs: BTreeMap<switchboard_core::model::SubscriptionId, switchboard_core::shard::Subscription>,
    prepared: BTreeMap<switchboard_core::shard::TxId, Vec<switchboard_core::shard::TxOp>>,
    prepared_at: BTreeMap<switchboard_core::shard::TxId, u64>,
    tx_timeout_ticks: u64,
    now: u64,
    last_ms: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct QueueImage {
    next_seq: u64,
    msgs: BTreeMap<u64, switchboard_core::shard::QueueMessage>,
    policy: switchboard_core::shard::QueuePolicy,
}

impl BrokerStateImage {
    fn of(state: &BrokerState) -> Self {
        BrokerStateImage {
            meta: state.meta.clone(),
            bootstrapped: state.bootstrapped,
            dedup: state.dedup.clone(),
            shard: ShardStateImage {
                queues: state
                    .shard
                    .queues
                    .iter()
                    .map(|(name, q)| {
                        (
                            name.clone(),
                            QueueImage { next_seq: q.next_seq, msgs: q.msgs.clone(), policy: q.policy.clone() },
                        )
                    })
                    .collect(),
                subs: state.shard.subs.clone(),
                prepared: state.shard.prepared.clone(),
                prepared_at: state.shard.prepared_at.clone(),
                tx_timeout_ticks: state.shard.tx_timeout_ticks,
                now: state.shard.now,
                last_ms: state.shard.last_ms,
            },
        }
    }

    fn into_state(self) -> BrokerState {
        let mut state = BrokerState {
            meta: self.meta,
            shard: switchboard_core::shard::ShardState {
                queues: self
                    .shard
                    .queues
                    .into_iter()
                    .map(|(name, q)| {
                        let mut qd = switchboard_core::shard::QueueData::new(q.policy);
                        qd.next_seq = q.next_seq;
                        qd.msgs = q.msgs;
                        qd.reindex();
                        (name, qd)
                    })
                    .collect(),
                subs: self.shard.subs,
                prepared: self.shard.prepared,
                prepared_at: self.shard.prepared_at,
                tx_timeout_ticks: self.shard.tx_timeout_ticks,
                now: self.shard.now,
                last_ms: self.shard.last_ms,
                store: Vec::new(),
            },
            bootstrapped: self.bootstrapped,
            dedup: self.dedup,
        };
        state.shard.reindex_all();
        state
    }
}

fn any_err(e: impl std::error::Error + Send + Sync + 'static) -> AnyError {
    AnyError::new(&e)
}

fn io_fail(verb: openraft::ErrorVerb, e: impl std::error::Error + Send + Sync + 'static) -> StorageError<NodeId> {
    StorageIOError::new(openraft::ErrorSubject::StateMachine, verb, AnyError::new(&e)).into()
}

fn ser<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(v).map_err(|e| StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Write, any_err(e)).into())
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<NodeId>> {
    bincode::deserialize(bytes).map_err(|e| StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, any_err(e)).into())
}

/// A snapshot builder that serializes the group's full state (message
/// records included) from the live in-memory state.
#[derive(Clone)]
pub struct SnapshotBuilder {
    kv: RocksKv,
    prefix: Vec<u8>,
    state: Arc<std::sync::Mutex<BrokerState>>,
}

impl RaftSnapshotBuilder<SwitchboardTypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<SwitchboardTypeConfig>, StorageError<NodeId>> {
        let last_applied = self
            .kv
            .get(&last_applied_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?
            .map(|b| de::<LogId<NodeId>>(&b))
            .transpose()?
            .unwrap_or_else(|| LogId::new(Default::default(), 0));
        let membership = self
            .kv
            .get(&membership_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?
            .map(|b| de::<StoredMembership<NodeId, openraft::impls::BasicNode>>(&b))
            .transpose()?
            .unwrap_or_default();
        let meta = SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: membership,
            snapshot_id: format!("snap-{}", last_applied.index),
        };
        // Clone the image under the lock (consistency), then serialize
        // with the lock released: both passes walk every queued message,
        // and holding the lock through both would stall raft applies for
        // the whole snapshot.
        let image = BrokerStateImage::of(&self.state.lock().expect("sm lock"));
        let data = ser(&image)?;
        self.kv
            .write_mixed(
                [
                    (snap_meta_key(&self.prefix), ser(&meta)?),
                    (snap_data_key(&self.prefix), data.clone()),
                ],
                [],
            )
            .map_err(|e| io_fail(openraft::ErrorVerb::Write, e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// In-memory message-lifecycle counters (not raft state; every replica
/// applies the same entries and counts identically). Surfaced via
/// `GET /stats` so a live cluster can prove where messages go.
#[derive(Debug, Default)]
pub struct SmCounters {
    pub enqueued: std::sync::atomic::AtomicU64,
    pub handed_out: std::sync::atomic::AtomicU64,
    pub acked: std::sync::atomic::AtomicU64,
    pub noack_removed: std::sync::atomic::AtomicU64,
    pub released: std::sync::atomic::AtomicU64,
    pub purged: std::sync::atomic::AtomicU64,
    pub expired: std::sync::atomic::AtomicU64,
    pub deleted: std::sync::atomic::AtomicU64,
}

impl SmCounters {
    pub fn snapshot_u64(&self) -> Vec<(&'static str, u64)> {
        use std::sync::atomic::Ordering;
        [
            ("enqueued", self.enqueued.load(Ordering::Relaxed)),
            ("handed_out", self.handed_out.load(Ordering::Relaxed)),
            ("acked", self.acked.load(Ordering::Relaxed)),
            ("noack_removed", self.noack_removed.load(Ordering::Relaxed)),
            ("released", self.released.load(Ordering::Relaxed)),
            ("purged", self.purged.load(Ordering::Relaxed)),
            ("expired", self.expired.load(Ordering::Relaxed)),
            ("deleted", self.deleted.load(Ordering::Relaxed)),
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Clone)]
pub struct StateMachine {
    kv: RocksKv,
    prefix: Vec<u8>,
    state: Arc<std::sync::Mutex<BrokerState>>,
    /// Message-lifecycle counters for this group's state machine. Arc:
    /// every clone of this struct shares one tally.
    pub counters: Arc<SmCounters>,
    last_applied: Arc<std::sync::Mutex<Option<LogId<NodeId>>>>,
    /// Last applied membership, persisted so a restart restores the
    /// group with its voter set intact (openraft reads it through
    /// `applied_state`; an empty answer would strand the group).
    membership: Arc<std::sync::Mutex<StoredMembership<NodeId, openraft::impls::BasicNode>>>,
    /// Effects from applies; drained by the group's leader-side pump.
    pub effects_tx: UnboundedSender<Effect>,
}

impl StateMachine {
    /// Open (or resume) the state machine for a group. A fresh group starts
    /// with an empty [`BrokerState`]; the cluster bootstrap applies the
    /// initial entities through normal log entries.
    /// Tally one applied command's message-lifecycle effect. Counted on
    /// every replica (identical applies → identical counts), so any node
    /// — not just the leader — can report where messages went.
    fn count_lifecycle(&self, cmd: &BrokerCommand, reply: &BrokerReply, effects: &[Effect]) {
        let c = &self.counters;
        // Idempotent-wrapped commands (fanout legs, retries) count as
        // their inner command; batched commands count element-wise with
        // their own replies.
        let mut pairs: Vec<(&BrokerCommand, &BrokerReply)> = Vec::new();
        match (cmd, reply) {
            (BrokerCommand::Batch { commands }, BrokerReply::Batch(replies)) => {
                for (inner, r) in commands.iter().zip(replies) {
                    pairs.push((inner, r));
                }
            }
            (c, r) => pairs.push((c, r)),
        }
        for (cmd, reply) in pairs {
            count_one(c, cmd, reply, effects);
        }
    }

    pub fn new(kv: RocksKv, group_id: u32, effects_tx: UnboundedSender<Effect>) -> Result<Self, StorageError<NodeId>> {
        let prefix = format!("g{group_id}:").into_bytes();
        let mut state = match kv.get(&sm_key(&prefix)).map_err(|e| io_fail(openraft::ErrorVerb::Read, e))? {
            Some(bytes) => de::<BrokerState>(&bytes)?,
            None => BrokerState::default(),
        };
        // Reload per-message records. The blob carries none of them; each
        // lives under its own key (see `ShardStoreOp`). Keys for queues
        // the state no longer knows are stale debris (e.g. a crash
        // between a queue's message purge and its blob persist) — drop.
        let mut orphan_queues: BTreeMap<String, ()> = BTreeMap::new();
        for (k, v) in kv.prefix_pairs(&msg_prefix(&prefix)).map_err(|e| io_fail(openraft::ErrorVerb::Read, e))? {
            let Some((queue, seq)) = parse_msg_key(&k, &prefix) else {
                return Err(io_fail(
                    openraft::ErrorVerb::Read,
                    std::io::Error::other("bad message key"),
                ));
            };
            let m: switchboard_core::shard::QueueMessage = de(&v)?;
            match state.shard.queues.get_mut(&queue) {
                Some(q) => {
                    q.msgs.insert(seq, m);
                }
                None => {
                    orphan_queues.insert(queue, ());
                }
            }
        }
        for queue in orphan_queues.keys() {
            kv.delete_prefix(&msg_queue_prefix(&prefix, queue))
                .map_err(|e| io_fail(openraft::ErrorVerb::Delete, e))?;
        }
        state.shard.reindex_all();
        let membership = kv
            .get(&membership_key(&prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?
            .map(|b| de::<StoredMembership<NodeId, openraft::impls::BasicNode>>(&b))
            .transpose()?
            .unwrap_or_default();
        let last_applied = kv
            .get(&last_applied_key(&prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?
            .map(|b| de::<LogId<NodeId>>(&b))
            .transpose()?;
        Ok(StateMachine {
            kv,
            prefix,
            counters: Arc::new(SmCounters::default()),
            state: Arc::new(std::sync::Mutex::new(state)),
            last_applied: Arc::new(std::sync::Mutex::new(last_applied)),
            membership: Arc::new(std::sync::Mutex::new(membership)),
            effects_tx,
        })
    }

    /// Snapshot of the in-memory state (for tests and admin tooling).
    pub fn read_state(&self) -> BrokerState {
        self.state.lock().expect("sm lock").clone()
    }

    /// Cheap copy of the meta half only (topology refreshes run on every
    /// reconcile tick and fanout poll; cloning the shard's message store
    /// for each would be the very cost this design removed).
    pub fn read_meta(&self) -> switchboard_core::topology::MetaState {
        self.state.lock().expect("sm lock").meta.clone()
    }

    /// Which node hosts the channel behind `sub`, looked up under the
    /// state lock without cloning the shard state. Runs once per
    /// cross-node delivered message; a full-state clone here is a
    /// per-message stall proportional to every queued message.
    pub fn sub_owner(&self, sub: switchboard_core::model::SubscriptionId) -> Option<u64> {
        self.state
            .lock()
            .expect("sm lock")
            .shard
            .subs
            .get(&sub)
            .map(|s| s.node)
    }

    /// Per-queue `(ready, held)` message counts, computed under the state
    /// lock without cloning any message body. `/stats` is polled by the
    /// readiness probe on every pod; a full-state clone here stalls raft
    /// applies on every probe once queues hold real content.
    pub fn account_queues(&self) -> Vec<(String, u64, u64)> {
        let st = self.state.lock().expect("sm lock");
        let mut out = Vec::new();
        for (name, q) in &st.shard.queues {
            let held = q.msgs.values().filter(|m| m.held_by.is_some()).count() as u64;
            if held > 0 || !q.msgs.is_empty() {
                out.push((name.clone(), q.msgs.len() as u64 - held, held));
            }
        }
        out
    }

    /// Identity of the shared state cell (diagnostics).
    pub fn state_ptr(&self) -> usize {
        std::sync::Arc::as_ptr(&self.state) as usize
    }

    pub fn last_applied(&self) -> Option<LogId<NodeId>> {
        *self.last_applied.lock().expect("sm lock")
    }

    fn persist(&self) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(&*self.state.lock().expect("sm lock"))?;
        let mut puts = vec![(sm_key(&self.prefix), bytes)];
        if let Some(la) = self.last_applied() {
            puts.push((last_applied_key(&self.prefix), ser(&la)?));
        }
        let mem = self.membership.lock().expect("sm lock").clone();
        if mem.log_id().is_some() {
            puts.push((membership_key(&self.prefix), ser(&mem)?));
        }
        self.kv
            .write_mixed(puts, [])
            .map_err(|e| io_fail(openraft::ErrorVerb::Write, e))
    }
}

/// Tally one unwrapped (post-Idempotent, post-Batch) command against the
/// group's lifecycle counters.
fn count_one(
    c: &SmCounters,
    cmd: &BrokerCommand,
    reply: &BrokerReply,
    effects: &[Effect],
) {
    use std::sync::atomic::Ordering;
    let cmd = match cmd {
        BrokerCommand::Idempotent { command, .. } => command.as_ref(),
        other => other,
    };
    if let BrokerCommand::Shard(shard) = cmd {
        match shard {
            switchboard_core::shard::ShardCmd::Enqueue { .. } => {
                c.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            switchboard_core::shard::ShardCmd::Ack { queue: _, seqs } => {
                c.acked.fetch_add(seqs.len() as u64, Ordering::Relaxed);
            }
            switchboard_core::shard::ShardCmd::Purge { queue: _ } => {
                if let BrokerReply::Shard(switchboard_core::shard::ShardReply::Purged {
                    message_count,
                }) = reply
                {
                    c.purged.fetch_add(u64::from(*message_count), Ordering::Relaxed);
                }
            }
            switchboard_core::shard::ShardCmd::DeleteQueueData { queue: _ } => {
                // The messages went with the queue; the state is already
                // applied, so there is nothing countable here.
                c.deleted.fetch_add(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }
    for eff in effects {
        if let Effect::Shard(switchboard_core::shard::ShardEffect::MessageReady { deleted, .. }) = eff {
            if *deleted {
                c.noack_removed.fetch_add(1, Ordering::Relaxed);
            } else {
                c.handed_out.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    if let BrokerReply::Shard(switchboard_core::shard::ShardReply::Released { released }) = reply {
        c.released.fetch_add(u64::from(*released), Ordering::Relaxed);
    }
}

impl RaftStateMachine<SwitchboardTypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::impls::BasicNode>), StorageError<NodeId>>
    {
        Ok((self.last_applied(), self.membership.lock().expect("sm lock").clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<BrokerReply>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<SwitchboardTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        let mut msg_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut msg_dels: Vec<Vec<u8>> = Vec::new();
        let mut del_queues: Vec<String> = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let reply = match &entry.payload {
                openraft::EntryPayload::Blank => BrokerReply::Bootstrapped,
                openraft::EntryPayload::Normal(cmd) => {
                    let (reply, effects) = {
                        let mut st = self.state.lock().expect("sm lock");
                        let applied = st.apply(cmd);
                        // Drain the storage journal this apply recorded —
                        // mutations before a mid-batch error still count
                        // (the entry is consumed either way). `Put`
                        // resolves the message's current in-memory state,
                        // so no body is copied here.
                        for op in std::mem::take(&mut st.shard.store) {
                            match op {
                                switchboard_core::shard::ShardStoreOp::Put { queue, seq } => {
                                    if let Some(m) =
                                        st.shard.queues.get(&queue).and_then(|q| q.msgs.get(&seq))
                                    {
                                        msg_puts.push((msg_key(&self.prefix, &queue, seq), ser(m)?));
                                    }
                                }
                                switchboard_core::shard::ShardStoreOp::Del { queue, seq } => {
                                    msg_dels.push(msg_key(&self.prefix, &queue, seq));
                                }
                                switchboard_core::shard::ShardStoreOp::DelQueue { queue } => {
                                    del_queues.push(queue);
                                }
                            }
                        }
                        match applied {
                            Ok((r, effs)) => (r, effs),
                            // Errors are part of the protocol (404s etc.):
                            // they must still consume the log entry, so they
                            // are folded into a normal reply carrying the
                            // error.
                            Err(e) => (BrokerReply::Error(e), vec![]),
                        }
                    };
                    self.count_lifecycle(cmd, &reply, &effects);
                    for eff in effects {
                        // Unbounded: the leader drains promptly; a dead
                        // receiver (no leader task) just drops effects.
                        let _ = self.effects_tx.send(eff);
                    }
                    reply
                }
                openraft::EntryPayload::Membership(mem) => {
                    tracing::debug!(?mem, "membership change applied");
                    let stored: StoredMembership<NodeId, openraft::impls::BasicNode> =
                        StoredMembership::new(Some(log_id), (*mem).clone());
                    *self.membership.lock().expect("sm lock") = stored;
                    BrokerReply::Bootstrapped
                }
            };
            let _ = ClientWriteResponse::<SwitchboardTypeConfig> {
                log_id,
                data: reply.clone(),
                membership: None,
            }; // response type documented; the raft layer wraps `data`.
            responses.push(reply);
            *self.last_applied.lock().expect("sm lock") = Some(log_id);
        }
        // Persist message records first, then the blob that advances
        // last_applied: a crash in between replays the log from
        // last_applied and re-applies these very commands, and every
        // journal op is idempotent (Put overwrites, Del deletes), so both
        // orders converge — but only this order survives with no window
        // where a message is lost.
        self.kv
            .write_mixed(msg_puts, msg_dels)
            .map_err(|e| io_fail(openraft::ErrorVerb::Write, e))?;
        for queue in &del_queues {
            self.kv
                .delete_prefix(&msg_queue_prefix(&self.prefix, queue))
                .map_err(|e| io_fail(openraft::ErrorVerb::Delete, e))?;
        }
        self.persist()?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder { kv: self.kv.clone(), prefix: self.prefix.clone(), state: self.state.clone() }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::impls::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let image: BrokerStateImage = de(snapshot.get_ref())?;
        let new_state = image.into_state();
        // Write every message record, then the (message-free) blob. Same
        // crash logic as `apply`: replay from last_applied re-applies what
        // a crash dropped, and the loader drops keys of unknown queues.
        let mut msg_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (queue, q) in &new_state.shard.queues {
            for (seq, m) in &q.msgs {
                msg_puts.push((msg_key(&self.prefix, queue, *seq), ser(m)?));
            }
        }
        self.kv
            .write_mixed(msg_puts, [])
            .map_err(|e| io_fail(openraft::ErrorVerb::Write, e))?;
        *self.state.lock().expect("sm lock") = new_state;
        *self.last_applied.lock().expect("sm lock") = meta.last_log_id;
        self.persist()?;
        let snap_bytes = snapshot.get_ref().clone();
        let meta_bytes = ser(meta)?;
        self.kv
            .write_mixed(
                [(snap_meta_key(&self.prefix), meta_bytes), (snap_data_key(&self.prefix), snap_bytes)],
                [],
            )
            .map_err(|e| io_fail(openraft::ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SwitchboardTypeConfig>>, StorageError<NodeId>> {
        let meta = self
            .kv
            .get(&snap_meta_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?;
        let data = self
            .kv
            .get(&snap_data_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?;
        match (meta, data) {
            (Some(m), Some(d)) => {
                let meta = de::<SnapshotMeta<NodeId, openraft::impls::BasicNode>>(&m)?;
                Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(d)) }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typ::BrokerCommand;
    use switchboard_core::shard::ShardCmd;
    use switchboard_core::topology::MetaCmd;
    use switchboard_wire::BasicProperties;

    fn sm(name: &str) -> StateMachine {
        let kv = RocksKv::open_temp(name);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        StateMachine::new(kv, 3, tx).unwrap()
    }

    fn entry(cmd: BrokerCommand, index: u64) -> Entry<SwitchboardTypeConfig> {
        Entry {
            log_id: LogId::new(Default::default(), index),
            payload: openraft::EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn applies_commands_and_persists() {
        let machine = sm("sm-apply");
        let mut m2 = machine.clone();

        let resp = m2
            .apply([
                entry(
                    BrokerCommand::Bootstrap {
                        vhost: "/".into(),
                        user: "guest".into(),
                        password: "guest".into(),
                    },
                    0,
                ),
                entry(
                    BrokerCommand::Meta(MetaCmd::DeclareVhost { name: "staging".into() }),
                    1,
                ),
            ])
            .await
            .unwrap();
        assert_eq!(resp.len(), 2);
        assert_eq!(resp[0], BrokerReply::Bootstrapped);
        assert_eq!(resp[1], BrokerReply::Meta(switchboard_core::topology::MetaReply::Ok));

        // State survives reopening from disk.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let reopened = StateMachine::new(machine.kv.clone(), 3, tx).unwrap();
        assert!(reopened.read_state().bootstrapped);
        assert!(reopened.read_state().meta.vhosts.contains_key("staging"));
        assert_eq!(reopened.last_applied().unwrap().index, 1);
    }

    #[tokio::test]
    async fn shard_applies_emit_effects_to_channel() {
        let kv = RocksKv::open_temp("sm-effects");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut machine = StateMachine::new(kv, 5, tx).unwrap();

        machine
            .apply([
                entry(BrokerCommand::Shard(ShardCmd::CreateQueueData { queue: "q".into(), policy: Default::default() }), 0),
                entry(
                    BrokerCommand::Shard(ShardCmd::Enqueue {
                        at_ms: 100,
                        queue: "q".into(),
                        message: switchboard_core::model::StoredMessage {
                            properties: BasicProperties::new(),
                            body: b"m".to_vec(),
                            exchange: "amq.direct".into(),
                            routing_key: "q".into(),
                            persistent: false,
                        },
                    }),
                    1,
                ),
            ])
            .await
            .unwrap();

        // Registering a consumer + crediting it emits MessageReady effects.
        machine
            .apply([
                entry(
                    BrokerCommand::Shard(ShardCmd::RegisterSubscription {
                        byte_limit: 0,
                        sub: switchboard_core::model::SubscriptionId { node: 1, sub: 1 },
                        queue: "q".into(),
                        node: 1,
                        consumer_tag: "c".into(),
                        no_ack: false,
                        exclusive: false,
                        conn: switchboard_core::model::ConnectionId { node: 1, conn: 1 },
                    }),
                    2,
                ),
                entry(
                    BrokerCommand::Shard(ShardCmd::Credit {
                        sub: switchboard_core::model::SubscriptionId { node: 1, sub: 1 },
                        count: 1,
                    }),
                    3,
                ),
            ])
            .await
            .unwrap();

        let mut saw_ready = false;
        while let Ok(e) = rx.try_recv() {
            if let Effect::Shard(switchboard_core::shard::ShardEffect::MessageReady { .. }) = e {
                saw_ready = true;
            }
        }
        assert!(saw_ready, "leader pump must see MessageReady effects");
    }

    #[tokio::test]
    async fn protocol_errors_do_not_break_the_log() {
        let machine = sm("sm-errors");
        let mut m2 = machine.clone();
        let resp = m2
            .apply([entry(
                BrokerCommand::Meta(MetaCmd::DeclareVhost { name: "/".into() }),
                0,
            )])
            .await
            .unwrap();
        // Errors are folded into replies, never broken futures.
        let _ = resp[0].clone();
        assert_eq!(m2.last_applied().unwrap().index, 0);
    }

    #[tokio::test]
    async fn snapshot_build_install_roundtrip() {
        let machine = sm("sm-snapshot");
        let mut m2 = machine.clone();
        m2.apply([
            entry(
                BrokerCommand::Bootstrap {
                    vhost: "/".into(),
                    user: "guest".into(),
                    password: "guest".into(),
                },
                0,
            ),
            entry(BrokerCommand::Meta(MetaCmd::DeclareVhost { name: "x".into() }), 1),
        ])
        .await
        .unwrap();

        let mut builder = m2.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 1);

        // Install into a fresh machine: same state.
        let kv = RocksKv::open_temp("sm-snapshot-target");
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut target = StateMachine::new(kv, 9, tx).unwrap();
        let data = *snap.snapshot;
        target.install_snapshot(&snap.meta, Box::new(data)).await.unwrap();
        assert_eq!(target.read_state(), m2.read_state());
        assert_eq!(target.last_applied().unwrap().index, 1);

        // And it can be read back.
        let got = target.get_current_snapshot().await.unwrap().expect("snapshot");
        assert_eq!(got.meta.snapshot_id, snap.meta.snapshot_id);
    }
}
