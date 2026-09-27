//! `RaftStateMachine` over RocksDB: the replicated broker state plus
//! snapshot install/emit.
//!
//! The state machine holds a [`BrokerState`] (meta + shard halves). Every
//! applied command produces a [`BrokerReply`] for the waiting caller and
//! zero or more [`Effect`]s pushed onto an unbounded channel; the group
//! leader drains that channel to perform cross-node work (shipping
//! delivered messages, cascading deletions). Replicas drop their effects —
//! only the leader acts on them.

use std::io::Cursor;
use std::sync::Arc;

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

/// A snapshot builder that serializes the whole state-machine blob.
#[derive(Clone)]
pub struct SnapshotBuilder {
    kv: RocksKv,
    prefix: Vec<u8>,
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
        let data = self
            .kv
            .get(&sm_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorVerb::Read, e))?
            .unwrap_or_default();
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

/// The replicated state machine for one group.
#[derive(Clone)]
pub struct StateMachine {
    kv: RocksKv,
    prefix: Vec<u8>,
    state: Arc<std::sync::Mutex<BrokerState>>,
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
    pub fn new(kv: RocksKv, group_id: u32, effects_tx: UnboundedSender<Effect>) -> Result<Self, StorageError<NodeId>> {
        let prefix = format!("g{group_id}:").into_bytes();
        let state = match kv.get(&sm_key(&prefix)).map_err(|e| io_fail(openraft::ErrorVerb::Read, e))? {
            Some(bytes) => de::<BrokerState>(&bytes)?,
            None => BrokerState::default(),
        };
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
        for entry in entries {
            let log_id = entry.log_id;
            let reply = match &entry.payload {
                openraft::EntryPayload::Blank => BrokerReply::Bootstrapped,
                openraft::EntryPayload::Normal(cmd) => {
                    let (reply, effects) = {
                        let mut st = self.state.lock().expect("sm lock");
                        match st.apply(cmd) {
                            Ok((r, effs)) => (r, effs),
                            // Errors are part of the protocol (404s etc.):
                            // they must still consume the log entry, so they
                            // are folded into a normal reply carrying the
                            // error.
                            Err(e) => (BrokerReply::Error(e), vec![]),
                        }
                    };
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
        self.persist()?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder { kv: self.kv.clone(), prefix: self.prefix.clone() }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::impls::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_state: BrokerState = de(snapshot.get_ref())?;
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
