//! The control-plane (meta) state machine: vhosts, users, exchanges, queue
//! metadata, bindings, node registry, and the shard map.
//!
//! This is what the 3-voter *meta* raft group replicates. Every topology
//! mutation is a [`MetaCmd`] applied here; applying a command yields a
//! [`MetaReply`] (returned to the waiting client) plus [`MetaEffect`]s
//! (cross-group work, e.g. deleting a queue's message data in its shard).
//!
//! Determinism: `apply` is a pure function of (state, cmd). Shard
//! assignment for new queues is computed inside `apply` from the current
//! group map, so all replicas agree.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use switchboard_wire::constants::naming;
use switchboard_wire::field::FieldTable;

use crate::error::BrokerError;
use crate::model::{
    binding_key, exchange, Binding, Exchange, ExchangeKind, QueueInfo, QueueOptions,
};
use crate::routing::TopologyView;

/// Identifies a raft group in this broker. Group 0 is the meta group; the
/// data groups are 1..=N. No group ever has more than three voters.
pub type GroupId = u32;

/// A node's advertised addresses, replicated in meta so any node can reach
/// any peer (the "internal management protocol" directory).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Client-facing AMQP listener.
    pub client_addr: String,
    /// Internal raft/forwarding listener.
    pub internal_addr: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Vhost {
    pub exchanges: BTreeMap<String, Exchange>,
    pub queues: BTreeMap<String, QueueInfo>,
    /// Keyed by [`crate::model::binding_key`] for idempotent (un)binds.
    pub bindings: BTreeMap<String, Binding>,
}

/// The complete meta state.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetaState {
    pub vhosts: BTreeMap<String, Vhost>,
    /// User name → plaintext password. Production deployments would store a
    /// hash; the protocol layer (§2.2.4 PLAIN) sees credentials in the clear
    /// either way, and TLS (aws-lc-rs) protects the wire.
    pub users: BTreeMap<String, String>,
    pub nodes: BTreeMap<u64, NodeInfo>,
    /// Shard group map: group id → member node ids. Computed by the
    /// membership controller and applied through [`MetaCmd::SetGroups`].
    pub groups: BTreeMap<GroupId, Vec<u64>>,
    /// Monotonic counter for server-side identifiers.
    pub epoch: u64,
    /// Retained MQTT messages, replicated through meta:
    /// (vhost, topic) → last retained message.
    pub retained: BTreeMap<(String, String), crate::model::StoredMessage>,
    /// Fanout publishes accepted but not yet enqueued on all destination
    /// shard groups, in meta-log order. The meta leader executes them
    /// strictly FIFO (see the cluster crate), which makes every
    /// destination group receive concurrent fanouts in one global order;
    /// replication through meta makes that order survive leader failover.
    pub pending_fanouts: Vec<PendingFanout>,
}

/// One accepted-but-unfinished fanout publish.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingFanout {
    pub id: uuid::Uuid,
    pub vhost: String,
    pub message: crate::model::StoredMessage,
    pub queues: Vec<String>,
}

/// Bootstrap state for a fresh cluster: default vhost, mandatory exchanges,
/// default user (§3.1.3 "The server will create a set of exchanges...").
pub fn bootstrap() -> MetaState {
    use switchboard_wire::constants::{DEFAULT_PASSWORD, DEFAULT_USER, DEFAULT_VHOST};
    let mut s = MetaState::default();
    s.declare_default_entities(DEFAULT_VHOST);
    s.users.insert(DEFAULT_USER.into(), DEFAULT_PASSWORD.into());
    s
}

impl MetaState {
    /// Create a vhost with its mandatory pre-declared exchanges: the
    /// nameless default, `amq.direct`, `amq.fanout`, and (since we implement
    /// the SHOULD types) `amq.topic` and `amq.match`.
    pub fn declare_default_entities(&mut self, vhost: &str) {
        let v = self.vhosts.entry(vhost.to_string()).or_default();
        for (name, kind) in [
            ("", ExchangeKind::Direct),
            ("amq.direct", ExchangeKind::Direct),
            ("amq.fanout", ExchangeKind::Fanout),
            ("amq.topic", ExchangeKind::Topic),
            ("amq.match", ExchangeKind::Headers),
        ] {
            v.exchanges.insert(name.to_string(), exchange(name, kind, true));
        }
    }

    pub fn vhost(&self, name: &str) -> Result<&Vhost, BrokerError> {
        self.vhosts.get(name).ok_or_else(|| BrokerError::invalid_path(format!(
            "no vhost {name:?}"
        )))
    }

    /// Assign a queue to a shard group: stable hash of the queue name over
    /// the sorted group ids (rendezvous-free but deterministic; groups
    /// change rarely and queue placement is fixed at declare time).
    pub fn assign_shard(&self, queue: &str) -> Option<GroupId> {
        if self.groups.is_empty() {
            return None;
        }
        let idx = fnv1a(queue.as_bytes()) % self.groups.len() as u64;
        self.groups.keys().nth(idx as usize).copied()
    }

    /// Apply a meta command. Pure; identical on every replica.
    pub fn apply(&mut self, cmd: &MetaCmd) -> Result<(MetaReply, Vec<MetaEffect>), BrokerError> {
        match cmd {
            MetaCmd::DeclareVhost { name } => {
                self.declare_default_entities(name);
                Ok((MetaReply::Ok, vec![]))
            }
            MetaCmd::CreateUser { name, password } => {
                self.users.insert(name.clone(), password.clone());
                Ok((MetaReply::Ok, vec![]))
            }
            MetaCmd::SetGroups { groups } => {
                self.groups = groups.iter().cloned().collect();
                Ok((MetaReply::Ok, vec![]))
            }
            MetaCmd::RegisterNode { node, info } => {
                self.nodes.insert(*node, info.clone());
                Ok((MetaReply::Ok, vec![]))
            }
            MetaCmd::ForgetNode { node } => {
                self.nodes.remove(node);
                // NOTE: the departed node deliberately stays in the shard
                // groups' member lists here — the membership controller
                // detects the loss and heals each affected group
                // (drop + refill with live nodes) through SetGroups, which
                // is replicated and keeps groups at full strength.
                // Cascade: queues owned by connections on the node die with
                // it (their holding connection is gone).
                let dead_queues: Vec<QueueInfo> = self
                    .vhosts
                    .values()
                    .flat_map(|v| v.queues.values())
                    .filter(|q| q.owner.map(|o| o.node) == Some(*node))
                    .cloned()
                    .collect();
                let mut effects = Vec::new();
                for q in dead_queues {
                    self.remove_queue(&q.name);
                    effects.push(MetaEffect::QueueDeleted { queue: q.name.clone(), shard: q.shard });
                }
                Ok((MetaReply::Ok, effects))
            }

            MetaCmd::FanoutBegin { id, vhost, message, queues } => {
                // Idempotent: a re-applied Begin (dedup missed across a
                // snapshot boundary) must not enqueue the fanout twice.
                if self.pending_fanouts.iter().any(|f| &f.id == id) {
                    return Ok((MetaReply::FanoutBegun, vec![]));
                }
                self.pending_fanouts.push(PendingFanout {
                    id: *id,
                    vhost: vhost.clone(),
                    message: message.clone(),
                    queues: queues.clone(),
                });
                Ok((
                    MetaReply::FanoutBegun,
                    vec![MetaEffect::FanoutPending { id: *id }],
                ))
            }

            MetaCmd::FanoutDone { id } => {
                self.pending_fanouts.retain(|f| &f.id != id);
                Ok((MetaReply::FanoutDone, vec![]))
            }

            MetaCmd::DeclareExchange { vhost, name, kind, passive, durable, auto_delete, internal, arguments } => {
                if name.starts_with(naming::RESERVED_PREFIX) && !is_bootstrap_exchange(name) {
                    return Err(BrokerError::access_refused(format!(
                        "exchange name {name:?} contains reserved prefix {:?}",
                        naming::RESERVED_PREFIX
                    )));
                }
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                match v.exchanges.get(name) {
                    Some(existing) => {
                        if *passive {
                            return Ok((MetaReply::ExchangeDeclared { existed: true }, vec![]));
                        }
                        let equivalent = existing.kind == *kind
                            && existing.durable == *durable
                            && existing.auto_delete == *auto_delete
                            && existing.internal == *internal
                            && existing.arguments == *arguments;
                        if !equivalent {
                            return Err(BrokerError::precondition_failed(format!(
                                "exchange {name:?} in vhost {vhost:?} exists with different type, durability, auto-delete or arguments"
                            )));
                        }
                        Ok((MetaReply::ExchangeDeclared { existed: true }, vec![]))
                    }
                    None => {
                        if *passive {
                            return Err(BrokerError::not_found(format!(
                                "no exchange {name:?} in vhost {vhost:?}"
                            )));
                        }
                        v.exchanges.insert(
                            name.clone(),
                            Exchange {
                                name: name.clone(),
                                kind: *kind,
                                durable: *durable,
                                auto_delete: *auto_delete,
                                internal: *internal,
                                arguments: arguments.clone(),
                            },
                        );
                        Ok((MetaReply::ExchangeDeclared { existed: false }, vec![]))
                    }
                }
            }

            MetaCmd::DeleteExchange { vhost, name, if_unused } => {
                if name.is_empty() {
                    return Err(BrokerError::access_refused(
                        "the default exchange cannot be deleted",
                    ));
                }
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                if !v.exchanges.contains_key(name) {
                    return Err(BrokerError::not_found(format!(
                        "no exchange {name:?} in vhost {vhost:?}"
                    )));
                }
                let in_use = v.bindings.values().any(|b| &b.exchange == name);
                if *if_unused && in_use {
                    return Err(BrokerError::precondition_failed(format!(
                        "exchange {name:?} in vhost {vhost:?} in use"
                    )));
                }
                v.bindings.retain(|_, b| &b.exchange != name);
                v.exchanges.remove(name);
                Ok((MetaReply::Ok, vec![]))
            }

            MetaCmd::DeclareQueue { vhost, name, passive, options, owner } => {
                // The caller resolves server-named queues BEFORE issuing the
                // command: every replica must apply byte-identical commands,
                // so randomness never lives inside apply().
                if name.is_empty() {
                    return Err(BrokerError::resource_error(
                        "queue name must be resolved before the command is replicated",
                    ));
                }
                // Read phase: existence, equivalence, ownership.
                let existing = self
                    .vhosts
                    .get(vhost)
                    .and_then(|vh| vh.queues.get(name))
                    .cloned();
                if let Some(existing) = existing {
                    if existing.options.exclusive && existing.owner != Some(*owner) {
                        return Err(BrokerError::resource_locked(format!(
                            "queue {name:?} in vhost {vhost:?} is exclusive to another connection"
                        )));
                    }
                    if *passive {
                        return Ok((MetaReply::QueueDeclared { name: name.clone(), shard: existing.shard, created: false }, vec![]));
                    }
                    let equivalent = existing.options.durable == options.durable
                        && existing.options.exclusive == options.exclusive
                        && existing.options.auto_delete == options.auto_delete
                        && existing.options.arguments == options.arguments;
                    if !equivalent {
                        return Err(BrokerError::precondition_failed(format!(
                            "queue {name:?} in vhost {vhost:?} exists with different durability, exclusivity, auto-delete or arguments"
                        )));
                    }
                    return Ok((MetaReply::QueueDeclared { name: name.clone(), shard: existing.shard, created: false }, vec![]));
                }
                if *passive {
                    return Err(BrokerError::not_found(format!(
                        "no queue {name:?} in vhost {vhost:?}"
                    )));
                }
                // `amq.` is reserved for the server, but its own generated
                // names (amq.gen-...) are exactly the sanctioned users of it.
                if name.starts_with(naming::RESERVED_PREFIX)
                    && !name.starts_with("amq.gen-")
                {
                    return Err(BrokerError::access_refused(format!(
                        "queue name {name:?} contains reserved prefix {:?}",
                        naming::RESERVED_PREFIX
                    )));
                }
                let Some(shard) = self.assign_shard(name) else {
                    return Err(BrokerError::resource_error(
                        "no shard groups are configured yet",
                    ));
                };
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                v.queues.insert(
                    name.clone(),
                    QueueInfo {
                        name: name.clone(),
                        options: options.clone(),
                        owner: if options.exclusive { Some(*owner) } else { None },
                        shard,
                    },
                );
                Ok((MetaReply::QueueDeclared { name: name.clone(), shard, created: true }, vec![]))
            }

            MetaCmd::DeleteQueue { vhost, name, if_unused, if_empty, depth, consumers } => {
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                let Some(info) = v.queues.get(name) else {
                    return Err(BrokerError::not_found(format!(
                        "no queue {name:?} in vhost {vhost:?}"
                    )));
                };
                if *if_unused && *consumers > 0 {
                    return Err(BrokerError::precondition_failed(format!(
                        "queue {name:?} in vhost {vhost:?} in use ({} consumers)",
                        consumers
                    )));
                }
                if *if_empty && *depth > 0 {
                    return Err(BrokerError::precondition_failed(format!(
                        "queue {name:?} in vhost {vhost:?} not empty ({} messages)",
                        depth
                    )));
                }
                let shard = info.shard;
                let purged = *depth;
                self.remove_queue(name);
                Ok((
                    MetaReply::QueueDeleted { message_count: purged as u32 },
                    vec![MetaEffect::QueueDeleted { queue: name.clone(), shard }],
                ))
            }

            MetaCmd::Bind { vhost, exchange: ex_name, queue, routing_key, arguments } => {
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                if ex_name.is_empty() {
                    return Err(BrokerError::access_refused(
                        "cannot bind to the default exchange",
                    ));
                }
                if !v.exchanges.contains_key(ex_name) {
                    return Err(BrokerError::not_found(format!(
                        "no exchange {ex_name:?} in vhost {vhost:?}"
                    )));
                }
                // The destination may be a queue OR another exchange
                // (exchange-exchange binding, advertised in
                // Connection.Start capabilities).
                if !v.queues.contains_key(queue) && !v.exchanges.contains_key(queue) {
                    return Err(BrokerError::not_found(format!(
                        "no queue or exchange {queue:?} in vhost {vhost:?}"
                    )));
                }
                let b = crate::model::Binding {
                    exchange: ex_name.clone(),
                    queue: queue.clone(),
                    routing_key: routing_key.clone(),
                    arguments: arguments.clone(),
                };
                let key = binding_key(&b);
                let existed = v.bindings.contains_key(&key);
                v.bindings.insert(key, b);
                // Auto-delete exchanges stay alive while they have bindings;
                // they are reaped on Unbind when the last binding leaves.
                Ok((MetaReply::Bound { existed }, vec![]))
            }

            MetaCmd::Unbind { vhost, exchange: ex_name, queue, routing_key, arguments } => {
                let v = self.vhosts.get_mut(vhost).ok_or_else(|| {
                    BrokerError::invalid_path(format!("no vhost {vhost:?}"))
                })?;
                if ex_name.is_empty() {
                    return Err(BrokerError::access_refused(
                        "cannot unbind from the default exchange",
                    ));
                }
                let b = crate::model::Binding {
                    exchange: ex_name.clone(),
                    queue: queue.clone(),
                    routing_key: routing_key.clone(),
                    arguments: arguments.clone(),
                };
                let key = binding_key(&b);
                if v.bindings.remove(&key).is_none() {
                    return Err(BrokerError::not_found(format!(
                        "no binding of queue {queue:?} to exchange {ex_name:?}"
                    )));
                }
                let mut effects = Vec::new();
                // auto-delete exchange with no bindings left: delete it.
                let mut deleted_ex = false;
                if let Some(e) = v.exchanges.get(ex_name) {
                    if e.auto_delete && !e.name.is_empty() {
                        let still_bound = v.bindings.values().any(|x| &x.exchange == ex_name);
                        if !still_bound {
                            deleted_ex = true;
                        }
                    }
                }
                if deleted_ex {
                    v.bindings.retain(|_, b| &b.exchange != ex_name);
                    v.exchanges.remove(ex_name);
                }
                let _ = &mut effects;
                Ok((MetaReply::Ok, effects))
            }

            MetaCmd::SetRetained { vhost, topic, message } => {
                match message {
                    Some(m) => {
                        self.retained.insert((vhost.clone(), topic.clone()), m.clone());
                    }
                    None => {
                        self.retained.remove(&(vhost.clone(), topic.clone()));
                    }
                }
                Ok((MetaReply::Ok, vec![]))
            }
            MetaCmd::Authorize { user, password } => {
                match self.users.get(user) {
                    Some(pw) if pw == password => Ok((MetaReply::Authorized, vec![])),
                    _ => Err(BrokerError::connection_access_refused(format!(
                        "authentication refused for user {user:?}"
                    ))),
                }
            }
        }
    }

    fn remove_queue(&mut self, name: &str) {
        for v in self.vhosts.values_mut() {
            v.queues.remove(name);
            let dead: Vec<String> = v
                .bindings
                .iter()
                .filter(|(_, b)| b.queue == name)
                .map(|(k, _)| k.clone())
                .collect();
            for k in dead {
                v.bindings.remove(&k);
            }
        }
    }
}

fn is_bootstrap_exchange(name: &str) -> bool {
    matches!(name, "amq.direct" | "amq.fanout" | "amq.topic" | "amq.match")
}

/// Deterministic 64-bit FNV-1a: stable across builds and processes, unlike
/// `DefaultHasher`.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Topology mutations, applied through the meta raft group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaCmd {
    DeclareVhost { name: String },
    CreateUser { name: String, password: String },
    /// Install the shard group map (membership controller output).
    SetGroups { groups: Vec<(GroupId, Vec<u64>)> },
    RegisterNode { node: u64, info: NodeInfo },
    /// Node left the cluster (crash or shutdown). Cascades cleanup of the
    /// exclusive queues its connections owned.
    ForgetNode { node: u64 },
    /// Accept a multi-destination publish into the pending-fanout queue
    /// (total-order broadcast: the meta leader executes pending fanouts
    /// strictly in this log's order).
    FanoutBegin {
        id: uuid::Uuid,
        vhost: String,
        message: crate::model::StoredMessage,
        queues: Vec<String>,
    },
    /// Every destination of the fanout has been enqueued; drop it from the
    /// pending queue.
    FanoutDone { id: uuid::Uuid },
    DeclareExchange {
        vhost: String,
        name: String,
        kind: ExchangeKind,
        passive: bool,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: FieldTable,
    },
    DeleteExchange { vhost: String, name: String, if_unused: bool },
    /// Declare (create or assert) a queue. `depth`/`consumers` accompany
    /// DeleteQueue because queue live-data lives in shards; DeclareQueue
    /// needs none of it.
    DeclareQueue {
        vhost: String,
        name: String,
        passive: bool,
        options: QueueOptions,
        owner: crate::model::ConnectionId,
    },
    /// Delete a queue. `depth` and `consumers` are the owner shard's
    /// linearizable counts, fetched just before this command; the state
    /// machine re-checks `if_empty`/`if_unused` against them.
    DeleteQueue {
        vhost: String,
        name: String,
        if_unused: bool,
        if_empty: bool,
        depth: u64,
        consumers: u32,
    },
    Bind {
        vhost: String,
        exchange: String,
        queue: String,
        routing_key: String,
        arguments: FieldTable,
    },
    Unbind {
        vhost: String,
        exchange: String,
        queue: String,
        routing_key: String,
        arguments: FieldTable,
    },
    /// Check credentials (Connection.Start-Ok handling).
    Authorize { user: String, password: String },
    /// Store (or clear, `message = None`) a retained MQTT message,
    /// replicated through meta so every node serves the same retained
    /// state. Keyed by vhost + MQTT topic.
    SetRetained {
        vhost: String,
        topic: String,
        message: Option<crate::model::StoredMessage>,
    },
}

/// Result of applying a meta command, sent back to the requesting client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaReply {
    Ok,
    FanoutBegun,
    FanoutDone,
    Authorized,
    ExchangeDeclared { existed: bool },
    QueueDeclared { name: String, shard: GroupId, created: bool },
    QueueDeleted { message_count: u32 },
    Bound { existed: bool },
}

/// Cross-group follow-ups the meta leader (or applying node) must perform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaEffect {
    /// Drop the queue's message data in its shard group.
    QueueDeleted { queue: String, shard: GroupId },
    /// A fanout is pending; the meta leader's executor should wake.
    FanoutPending { id: uuid::Uuid },
}

/// Implement [`TopologyView`] over a vhost so publish routing works against
/// any applied meta snapshot.
pub struct VhostView<'a> {
    pub vhost: &'a Vhost,
}

impl<'a> TopologyView for VhostView<'a> {
    fn exchange(&self, name: &str) -> Option<&Exchange> {
        self.vhost.exchanges.get(name)
    }

    fn bindings_of(&self, exchange: &str) -> impl Iterator<Item = &Binding> {
        self.vhost
            .bindings
            .values()
            .filter(move |b| b.exchange == exchange)
    }

    fn queue_exists(&self, name: &str) -> bool {
        self.vhost.queues.contains_key(name)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
