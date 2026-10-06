//! Raft type configuration and the unified command vocabulary.
//!
//! Every raft group in a Switchboard cluster — the meta group and every
//! shard group — runs the same raft machinery over the same log payload
//! type. A [`BrokerCommand`] dispatches to the meta state machine (§2.1
//! topology) or the shard state machine (queues), so group 0 and shard
//! groups differ only in their configuration, not their code.

use std::io::Cursor;

use openraft::impls::BasicNode;
use openraft::{Entry, RaftTypeConfig, TokioRuntime};
use serde::{Deserialize, Serialize};

use switchboard_core::shard::{ShardCmd, ShardEffect, ShardReply};
use switchboard_core::topology::{MetaCmd, MetaEffect, MetaReply};

/// Node identity across the cluster: small dense integers assigned at join
/// time. `Default` must be deterministic, so it is `u64::MAX` sentinel use
/// is avoided — ids are always set explicitly.
pub type NodeId = u64;

/// The raft type configuration used by every group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SwitchboardTypeConfig {}

impl RaftTypeConfig for SwitchboardTypeConfig {
    /// Log payload: one of the broker's two state machines' commands.
    type D = BrokerCommand;
    /// Per-entry response returned to the caller of `client_write`.
    type R = BrokerReply;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = Entry<Self>;
    /// Snapshots are in-memory blobs.
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    /// How `client_write` responses reach callers: the default oneshot map.
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// A log entry payload, replicated to every member of a group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BrokerCommand {
    /// Lay down the bootstrap entities (default vhost, exchanges, user).
    /// Emitted once when the meta group is created; safe to re-apply
    /// because the underlying commands are idempotent.
    Bootstrap { vhost: String, user: String, password: String },
    /// A control-plane (topology) mutation.
    Meta(MetaCmd),
    /// A data-plane (queue) mutation.
    Shard(ShardCmd),
    /// A write tagged with a cluster-unique request id. The write path can
    /// legitimately re-issue the same logical command — a forward whose
    /// reply was lost is re-sent, and the retry loop may land it on
    /// another member of the group — and raft is content-addressed by
    /// nothing: each re-issue becomes a second log entry and the state
    /// machine would apply it twice (e.g. two queue entries for one
    /// publish). The dedup log makes the *second* application of a
    /// request a no-op that replays the first reply. The variant is last
    /// so existing log indices are unchanged.
    Idempotent { id: RequestId, command: Box<BrokerCommand> },
}

/// Cluster-unique identity for one logical write, stamped by `write()` and
/// carried unchanged through forwards and retries.
pub type RequestId = uuid::Uuid;

/// How many recent request replies the dedup log remembers. Bounds cover
/// several minutes of write retries; a duplicate older than the window is
/// one whose retry already timed out everywhere.
const DEDUP_CAPACITY: usize = 8192;

/// Bounded insertion-ordered map of recently applied request ids.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DedupLog {
    map: std::collections::HashMap<RequestId, BrokerReply>,
    order: std::collections::VecDeque<RequestId>,
}

impl DedupLog {
    fn lookup(&self, id: &RequestId) -> Option<&BrokerReply> {
        self.map.get(id)
    }

    fn remember(&mut self, id: RequestId, reply: BrokerReply) {
        if self.map.contains_key(&id) {
            return;
        }
        if self.order.len() >= DEDUP_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
        self.map.insert(id, reply);
        self.order.push_back(id);
    }
}

/// The applied result of a [`BrokerCommand`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BrokerReply {
    Bootstrapped,
    /// The command was applied but the model rejected it (404/406/...).
    /// These still consume the log entry: replicated state advances.
    Error(switchboard_core::error::BrokerError),
    Meta(MetaReply),
    Shard(ShardReply),
}

/// Side-channel effects emitted by state-machine applies. The group leader
/// consumes these (leader only — replicas drop theirs) to ship messages to
/// consumer nodes and cascaded deletions to shard owners.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Effect {
    Meta(MetaEffect),
    Shard(ShardEffect),
}

/// The complete replicated state of a group: both halves of the AMQ model.
/// Meta commands only ever run in the meta group and shard commands only in
/// shard groups, but sharing one struct keeps the raft plumbing uniform.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BrokerState {
    pub meta: switchboard_core::topology::MetaState,
    pub shard: switchboard_core::shard::ShardState,
    /// True once a `Bootstrap` command has been applied.
    pub bootstrapped: bool,
    /// Replies for recently applied idempotent requests, so a re-issued
    /// write (lost reply, retried forward) applies exactly once.
    pub dedup: DedupLog,
}

impl BrokerState {
    /// Apply a command, returning the client reply and any effects.
    pub fn apply(
        &mut self,
        cmd: &BrokerCommand,
    ) -> Result<(BrokerReply, Vec<Effect>), switchboard_core::error::BrokerError> {
        match cmd {
            BrokerCommand::Idempotent { id, command } => {
                if let Some(reply) = self.dedup.lookup(id) {
                    return Ok((reply.clone(), vec![]));
                }
                let (reply, effects) = self.apply(command)?;
                self.dedup.remember(*id, reply.clone());
                Ok((reply, effects))
            }
            BrokerCommand::Bootstrap { vhost, user, password } => {
                let (r1, _r1_effects) = self.meta.apply(&MetaCmd::DeclareVhost { name: vhost.clone() })?;
                let (r2, _r2_effects) = self.meta.apply(&MetaCmd::CreateUser {
                    name: user.clone(),
                    password: password.clone(),
                })?;
                self.bootstrapped = true;
                let _ = (&r1, &r2);
                Ok((BrokerReply::Bootstrapped, vec![]))
            }
            BrokerCommand::Meta(m) => {
                let (r, effs) = self.meta.apply(m)?;
                Ok((BrokerReply::Meta(r), effs.into_iter().map(Effect::Meta).collect()))
            }
            BrokerCommand::Shard(s) => {
                let (r, effs) = self.shard.apply(s)?;
                Ok((BrokerReply::Shard(r), effs.into_iter().map(Effect::Shard).collect()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use switchboard_core::shard::ShardCmd;
    use switchboard_core::topology::MetaCmd;

    #[test]
    fn bootstrap_is_idempotent_and_wires_defaults() {
        let mut st = BrokerState::default();
        let (r, _) = st
            .apply(&BrokerCommand::Bootstrap {
                vhost: "/".into(),
                user: "guest".into(),
                password: "guest".into(),
            })
            .unwrap();
        assert_eq!(r, BrokerReply::Bootstrapped);
        assert!(st.bootstrapped);
        assert!(st.meta.vhosts.contains_key("/"));

        // Re-applying bootstrap on a working cluster keeps state intact.
        let (r2, _) = st
            .apply(&BrokerCommand::Bootstrap {
                vhost: "/".into(),
                user: "guest".into(),
                password: "guest".into(),
            })
            .unwrap();
        assert_eq!(r2, BrokerReply::Bootstrapped);
    }

    #[test]
    fn commands_dispatch_to_their_state_machines() {
        let mut st = BrokerState::default();
        st.apply(&BrokerCommand::Bootstrap {
            vhost: "/".into(),
            user: "guest".into(),
            password: "guest".into(),
        })
        .unwrap();
        let (BrokerReply::Meta(MetaReply::ExchangeDeclared { existed }), _) = st
            .apply(&BrokerCommand::Meta(MetaCmd::DeclareExchange {
                vhost: "/".into(),
                name: "ex".into(),
                kind: switchboard_core::model::ExchangeKind::Direct,
                passive: false,
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: Default::default(),
            }))
            .unwrap()
        else {
            panic!()
        };
        assert!(!existed);

        st.shard
            .apply(&ShardCmd::CreateQueueData { queue: "q".into(), policy: Default::default() })
            .unwrap();
        let (BrokerReply::Shard(ShardReply::Enqueued { seq }), effs) = st
            .apply(&BrokerCommand::Shard(ShardCmd::Enqueue {
                at_ms: 100,
                queue: "q".into(),
                message: switchboard_core::model::StoredMessage {
                    properties: switchboard_wire::BasicProperties::new(),
                    body: b"m".to_vec(),
                    exchange: "ex".into(),
                    routing_key: "k".into(),
                    persistent: false,
                },
            }))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(seq, 0);
        assert!(effs.is_empty(), "no subscribers, no effects");
    }

    #[test]
    fn serde_roundtrip_of_commands() {
        let cmds = vec![
            BrokerCommand::Bootstrap {
                vhost: "/".into(),
                user: "u".into(),
                password: "p".into(),
            },
            BrokerCommand::Meta(MetaCmd::DeclareVhost { name: "vh".into() }),
            BrokerCommand::Shard(ShardCmd::ExpirePrepared {}),
        ];
        for c in cmds {
            let bytes = bincode::serialize(&c).unwrap();
            let back: BrokerCommand = bincode::deserialize(&bytes).unwrap();
            assert_eq!(back, c);
        }
    }

    #[test]
    fn idempotent_writes_apply_exactly_once() {
        let mut st = BrokerState::default();
        let id = uuid::Uuid::new_v4();
        let enqueue = |id| BrokerCommand::Idempotent {
            id,
            command: Box::new(BrokerCommand::Shard(ShardCmd::Enqueue {
                at_ms: 0,
                queue: "q".into(),
                message: switchboard_core::model::StoredMessage {
                    properties: switchboard_wire::BasicProperties::new(),
                    body: b"m".to_vec(),
                    exchange: "ex".into(),
                    routing_key: "k".into(),
                    persistent: false,
                },
            })),
        };
        st.apply(&BrokerCommand::Shard(ShardCmd::CreateQueueData {
            queue: "q".into(),
            policy: Default::default(),
        }))
        .unwrap();

        let (r1, _) = st.apply(&enqueue(id)).unwrap();
        let (r2, effects2) = st.apply(&enqueue(id)).unwrap();
        assert_eq!(r1, r2, "duplicate request replays the first reply");
        assert!(effects2.is_empty(), "duplicate request re-emits no effects");

        // Exactly one message in the queue despite two applies.
        let depth = st.shard.queues["q"].msgs.len();
        assert_eq!(depth, 1);

        // A *different* id is a genuinely new write.
        let (reply3, _) = st.apply(&enqueue(uuid::Uuid::new_v4())).unwrap();
        let BrokerReply::Shard(ShardReply::Enqueued { seq }) = reply3 else {
            panic!()
        };
        assert_eq!(seq, 1, "second distinct enqueue got its own sequence");
    }
}
