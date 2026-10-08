//! A Switchboard cluster node: its raft groups, the internal server, and
//! the forwarding/membership machinery.
//!
//! # Group layout
//!
//! Group 0 is the *meta* group: the topology (vhosts, exchanges, queue
//! metadata, bindings, users, node directory, shard map), replicated on up
//! to 3 voters. Groups `1..` are *shard* groups: each owns the message
//! data of the queues assigned to it (assignment is a stable hash of the
//! queue name over the group ids, computed in the meta state machine).
//! No group ever exceeds [`MAX_VOTERS`] (3) members.
//!
//! # Writes on every node (multi-master)
//!
//! Any node accepts any command:
//! * If the command targets a group this node hosts, it goes through the
//!   local raft instance; openraft returns `ForwardToLeader` when we are a
//!   follower, and we re-send to the leader.
//! * Otherwise the command is forwarded over the internal network to a
//!   member of the target group ([`ClusterNode::write`]), which applies the
//!   previous rule. One hop is enough: the receiving member either is the
//!   leader or knows who is.
//!
//! # Internal management protocol
//!
//! * **Join**: a new node sends [`AdminRequest::Join`] to each seed until
//!   one succeeds; the seed registers the node in meta (raft-replicated),
//!   so every node learns the new member and its addresses.
//! * **Formation**: the bootstrap node initializes the meta group alone,
//!   applies the bootstrap entities, and — as nodes register — grows meta
//!   membership toward 3 voters, then installs the initial shard groups as
//!   sliding windows over the sorted node ids (see
//!   [`sliding_windows`]). Each shard group is initialized by its smallest
//!   member, which avoids initialization races.
//! * **Reconciliation**: every node re-reads the topology periodically and
//!   ensures a raft handle exists for each group listing it as a member,
//!   and drops handles for groups it left.
//! * **Effects**: only the leader of a shard group acts on [`Effect`]s
//!   from its state machine, shipping delivered messages to the nodes that
//!   host the consumers (followers produce identical effects while
//!   applying the same entries and drop them).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;
use openraft::error::RaftError;
use openraft::Config;
use openraft::Raft;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use switchboard_core::error::BrokerError;
use switchboard_core::model::ConnectionId;
use switchboard_core::model::StoredMessage;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardEffect;
use switchboard_core::topology::GroupId;
use switchboard_core::topology::MetaCmd;
use switchboard_core::topology::MetaState;
use switchboard_core::topology::NodeInfo;

use crate::net::Directory;
use crate::net::NetworkFactory;
use crate::proto::AdminRequest;
use crate::proto::AdminResponse;
use crate::proto::Envelope;
use crate::proto::ForwardError;
use crate::proto::InternalMessage;
use crate::proto::InternalRequest;
use crate::proto::InternalResponse;
use crate::proto::RaftPayload;
use crate::proto::RaftResponse;
use crate::proto::TopologySnapshot;
use switchboard_store::LogStore;
use switchboard_store::StateMachine;

use crate::typ::BrokerCommand;
use crate::typ::BrokerReply;
use crate::typ::Effect;
use crate::typ::NodeId;
use crate::typ::SwitchboardTypeConfig;

/// Wall-clock milliseconds (command-carried time for TTL bookkeeping).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Hard ceiling on voters per raft group (the design constraint).
pub const MAX_VOTERS: usize = 3;
/// Group id of the meta group.
pub const META_GROUP: GroupId = 0;

/// Static configuration of a node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub data_dir: PathBuf,
    /// Client-facing AMQP address (advertised in the directory).
    pub client_addr: String,
    /// Internal cluster address (raft + forwarding).
    pub internal_addr: String,
    /// Internal addresses of existing nodes to join through. Empty means
    /// "form a new cluster".
    pub seeds: Vec<String>,
    /// True for the node that forms the cluster.
    pub bootstrap: bool,
    /// Static peer directory for formation: (node id, internal address)
    /// for every initial voter. Seeding these lets the initial election
    /// conclude without waiting for directory propagation.
    pub peers: Vec<(u64, String)>,
    /// Total nodes expected in the cluster at formation time. Every node
    /// whose id is among the first [`MAX_VOTERS`] registers-and-initializes
    /// the meta group; formation of the shard layout starts once this many
    /// nodes have registered.
    pub expected_nodes: u64,
    /// Every wait the node can experience, in one place. This is a
    /// realtime distributed system: defaults keep any single operation's
    /// worst case at five seconds, and everything is configurable.
    pub timeouts: Timeouts,
}

/// All broker timeouts and retry budgets. Defaults bound every operation
/// to ≤ 5 s; every field is configurable via CLI/env (see the binary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Timeouts {
    /// Retry budget for one logical write (elections, forwards, retries).
    pub write_budget: Duration,
    /// Retry budget for one fanout publish (leader resolution + legs).
    pub fanout_budget: Duration,
    /// One internal RPC's reply wait (raft, forwards, admin calls).
    pub rpc_reply_budget: Duration,
    /// Retry budget for a membership reconfiguration.
    pub reconfigure_budget: Duration,
    /// Retry budget for joining an existing cluster at startup.
    pub join_budget: Duration,
    /// Raft heartbeat interval.
    pub raft_heartbeat: Duration,
    /// Raft election timeout range (min..max); min must be ≥ 2× heartbeat
    /// (clamped here if configured lower).
    pub raft_election_min: Duration,
    pub raft_election_max: Duration,
    /// Reconciliation (topology refresh) tick.
    pub reconcile_interval: Duration,
    /// Liveness-probe (consumer janitor) tick…
    pub janitor_interval: Duration,
    /// …and how many consecutive refused probes declare a peer dead.
    pub janitor_dead_after: u32,
    /// Retries for a failed off-node delivery's Release write, and the
    /// pause between attempts.
    pub deliver_release_retries: u32,
    pub deliver_release_interval: Duration,
    /// How many consecutive refused liveness probes (controller tick)
    /// before a registered node is reaped from the directory. Only
    /// ECONNREFUSED counts — the definitive "nothing listens" — so
    /// partitions and load fail open. Long enough to ride out a
    /// container restart, short enough to heal a SIGKILLed member
    /// before the next churn wave.
    pub reap_dead_after: u32,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            write_budget: Duration::from_secs(5),
            fanout_budget: Duration::from_secs(5),
            rpc_reply_budget: Duration::from_secs(5),
            reconfigure_budget: Duration::from_secs(5),
            join_budget: Duration::from_secs(5),
            raft_heartbeat: Duration::from_millis(100),
            raft_election_min: Duration::from_millis(300),
            raft_election_max: Duration::from_millis(600),
            reconcile_interval: Duration::from_millis(500),
            janitor_interval: Duration::from_millis(1000),
            janitor_dead_after: 3,
            deliver_release_retries: 10,
            deliver_release_interval: Duration::from_millis(200),
            reap_dead_after: 25,
        }
    }
}

/// Errors surfaced by the cluster layer.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("broker rejected the command: {0}")]
    Broker(#[from] BrokerError),
    #[error("raft error on group {group}: {message}")]
    Raft { group: GroupId, message: String },
    #[error("group {group} is not hosted locally and no peer accepted the forward")]
    Unreachable { group: GroupId },
    /// Transient condition (election in flight, peer starting). Retry.
    #[error("transient condition; retry")]
    Transient,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(#[from] rocksdb::Error),
    #[error("codec: {0}")]
    Codec(String),
    #[error("not joined to any cluster yet")]
    NotJoined,
}

struct GroupHandle {
    raft: Raft<SwitchboardTypeConfig>,
    sm: StateMachine,
    _effects: tokio::task::JoinHandle<()>,
}

impl ClusterError {
    /// A per-waiter copy of this error for fanned-out batch replies
    /// (`ClusterError` is not `Clone`: `Io`/`Storage` hold native errors).
    fn for_waiter(&self, group: GroupId) -> ClusterError {
        match self {
            ClusterError::Broker(e) => ClusterError::Broker(e.clone()),
            ClusterError::Raft { group, message } => {
                ClusterError::Raft { group: *group, message: message.clone() }
            }
            ClusterError::Transient => ClusterError::Unreachable { group },
            ClusterError::Unreachable { group } => ClusterError::Unreachable { group: *group },
            ClusterError::NotJoined => ClusterError::NotJoined,
            other => ClusterError::Codec(format!("{other}")),
        }
    }
}

/// A message handed to a locally-hosted consumer channel. An empty `queue`
/// with `deleted = true` signals consumer cancellation.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub sub: SubscriptionId,
    pub queue: String,
    pub seq: u64,
    pub message: StoredMessage,
    pub redelivered: bool,
    pub deleted: bool,
}

/// Sink set registered by the server layer for one consumer.
#[derive(Clone)]
pub struct ConsumerSink {
    pub deliveries: tokio::sync::mpsc::UnboundedSender<Delivery>,
    pub cancelled: tokio::sync::mpsc::UnboundedSender<String>,
}

pub struct ClusterNode {
    pub id: NodeId,
    pub cfg: NodeConfig,
    pub dir: Arc<Directory>,
    channel: Arc<crate::transport::PeerChannel>,
    kv: switchboard_store::RocksKv,
    groups: RwLock<HashMap<GroupId, Arc<GroupHandle>>>,
    /// Serializes raft-group creation (which awaits).
    creating: tokio::sync::Mutex<()>,
    /// Set when the node is shutting down: listeners and loops wind down.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Consecutive failed liveness probes per peer (dead-consumer
    /// detection). A single failure must not declare a peer dead:
    /// transient connect errors (ephemeral-port exhaustion, backlog)
    /// would otherwise eat live consumers.
    peer_probe_fails: std::sync::Mutex<HashMap<NodeId, u32>>,
    /// Consecutive refused probes per registered peer (dead-node reaping
    /// in the controller). Same fail-open discipline as the consumer
    /// janitor; ECONNREFUSED is the only counting signal.
    reap_fails: std::sync::Mutex<HashMap<NodeId, u32>>,
    /// When this node started (uptime reporting via `GET /stats`).
    started: std::time::Instant,
    /// Node-global subscription-id counter. SubscriptionId is keyed
    /// node-global in the consumer-sink map, so this must be unique
    /// across every connection and channel on this node — a per-channel
    /// counter collides on the first consumer of every channel and
    /// delivers one session's messages into another.
    next_sub: std::sync::atomic::AtomicU64,
    /// Latest applied meta state (topology + shard map).
    topology_tx: tokio::sync::watch::Sender<Arc<MetaState>>,
    /// Local consumers awaiting deliveries, keyed by subscription.
    consumer_sinks: Arc<tokio::sync::RwLock<HashMap<SubscriptionId, ConsumerSink>>>,
    /// Serializes fanout publishes cluster-wide: multi-destination
    /// publishes execute one at a time on the meta leader so every
    /// destination shard group receives them in one global order.
    fanout_lock: tokio::sync::Mutex<()>,
    /// Wakes the fanout executor when a fanout becomes pending, so
    /// execution starts at once instead of on the next tick.
    fanout_wake: Arc<tokio::sync::Notify>,
    /// Per-group write coalescers (group commit on the shard groups' hot
    /// path: concurrent writes leave as one raft entry).
    coalescers: std::sync::Mutex<HashMap<GroupId, tokio::sync::mpsc::UnboundedSender<CoalesceReq>>>,
}

/// One write waiting for a batched commit.
struct CoalesceReq {
    /// Already `Idempotent`-wrapped: every retry of a batch re-applies
    /// each element exactly once through the dedup log.
    command: BrokerCommand,
    reply: tokio::sync::oneshot::Sender<Result<BrokerReply, ClusterError>>,
}

impl ClusterNode {
    /// Start the node: open storage, form or join the cluster, run the
    /// internal server and the background controllers.
    pub async fn start(cfg: NodeConfig) -> Result<Arc<Self>, ClusterError> {
        let kv = switchboard_store::RocksKv::open(&cfg.data_dir)?;
        let dir = Arc::new(Directory::new());
        dir.update(cfg.id, cfg.internal_addr.clone());
        let channel = Arc::new(crate::transport::PeerChannel::with_reply_budget(
            None,
            "switchboard-internal".into(),
            cfg.timeouts.rpc_reply_budget,
        ));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (topology_tx, _) = tokio::sync::watch::channel(Arc::new(MetaState::default()));
        for (pid, paddr) in &cfg.peers {
            dir.update(*pid, paddr.clone());
        }
        let node = Arc::new(ClusterNode {
            id: cfg.id,
            dir,
            channel,
            kv,
            groups: RwLock::new(HashMap::new()),
            creating: tokio::sync::Mutex::new(()),
            shutdown_tx,
            topology_tx,
            consumer_sinks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            peer_probe_fails: std::sync::Mutex::new(HashMap::new()),
            reap_fails: std::sync::Mutex::new(HashMap::new()),
            started: std::time::Instant::now(),
            next_sub: std::sync::atomic::AtomicU64::new(1),
            fanout_lock: tokio::sync::Mutex::new(()),
            fanout_wake: Arc::new(tokio::sync::Notify::new()),
            coalescers: std::sync::Mutex::new(HashMap::new()),
            cfg: cfg.clone(),
        });

        // The internal server must be up before formation: the initial
        // voters grant each other's candidacies over it. Bind here —
        // synchronously — so peers that dial us immediately are never
        // refused; the spawned task only runs the accept loop.
        let internal_listener = tokio::net::TcpListener::bind(&cfg.internal_addr)
            .await
            .map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;
        {
            let n = node.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move { n.internal_server(internal_listener, rx).await });
        }

        // Formation vs join vs discovery-pending:
        // * `bootstrap` forms a new cluster (first min(3, expected) ids
        //   are the meta voters);
        // * a node with `seeds` joins through them;
        // * a node with neither stays pending — discovery (DNS/mDNS)
        //   introduces it via `introduce` once it finds the cluster.
        if cfg.bootstrap {
            node.form_cluster().await?;
        } else if !cfg.seeds.is_empty() {
            if let Err(e) = node.join_seeds().await {
                // All seeds were unreachable for the full retry budget.
                // The node stays up in pending mode (discovery may still
                // introduce it), and a background task keeps retrying the
                // configured seeds: a node that lists seeds must never
                // strand as pending just because formation was busy at
                // boot — it is useless until it joins.
                tracing::warn!(node = node.id, err = ?e, "seed join failed; pending, retrying in background");
                let n = node.clone();
                tokio::spawn(async move {
                    let pause = switchboard_core::tempo::scale(
                        n.cfg.timeouts.reconcile_interval,
                    );
                    loop {
                        tokio::time::sleep(pause).await;
                        if n.join_seeds().await.is_ok() {
                            tracing::info!(node = n.id, "background seed join succeeded");
                            break;
                        }
                    }
                });
            }
        } else {
            tracing::info!(
                node = node.id,
                "no seeds configured; node is pending until discovery introduces it"
            );
        }
        // A node whose configured seeds were all unreachable also enters
        // pending mode here: discovery (DNS/mDNS) may still find and
        // introduce it later, so a dead seed at boot must not be fatal.
        {
            let n = node.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move { n.reconcile_loop(rx).await });
        }
        {
            // Total-order fanout executor: applies pending fanouts in
            // meta-log order while this node leads the meta group.
            let n = node.clone();
            tokio::spawn(async move { n.fanout_executor_loop().await });
        }
        {
            let n = node.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move { n.controller_loop(rx).await });
            let n = node.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move { n.meta_recovery_loop(rx).await });
        }
        {
            let n = node.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move { n.janitor_loop(rx).await });
        }
        Ok(node)
    }

    /// Allocate a connection id for a freshly accepted client connection.
    pub async fn new_connection_id(&self) -> ConnectionId {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        ConnectionId {
            node: self.id,
            conn: COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Initiate graceful shutdown: internal listeners and background loops
    /// wind down, and every raft core is stopped so the process holds no
    /// campaigning raft state after the node is dropped.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<Arc<GroupHandle>> = self
            .groups
            .read()
            .expect("groups lock")
            .values()
            .cloned()
            .collect();
        for h in handles {
            let _ = h.raft.shutdown().await;
        }
        // Give loops a beat to observe the flag.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    /// The meta group's current raft voter set (for admin/inspection).
    /// Current meta-group voter ids (sync, from raft metrics). Exposed
    /// via `GET /stats` so orchestrators can protect voters from churn.
    pub fn meta_voter_ids(&self) -> Vec<NodeId> {
        let groups = self.groups.read().expect("groups lock");
        match groups.get(&META_GROUP) {
            Some(h) => {
                let mut v: Vec<NodeId> =
                    h.raft.metrics().borrow().membership_config.voter_ids().collect();
                v.sort();
                v
            }
            None => Vec::new(),
        }
    }

    pub async fn meta_voters(&self) -> Option<std::collections::BTreeSet<NodeId>> {
        let handle = self.groups.read().expect("groups lock").get(&META_GROUP).cloned()?;
        Some(handle.raft.metrics().borrow().membership_config.voter_ids().collect())
    }

    pub fn shutting_down(&self) -> bool {
        *self.shutdown_tx.subscribe().borrow()
    }

    /// Seconds since this node started (exposed via `GET /stats`).
    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Aggregate message-lifecycle counters across every group this node
    /// hosts, plus per-queue (ready, held) for queues holding messages.
    /// Surfaced via `GET /stats` so a live cluster can prove where its
    /// messages went — the soak's message-conservation triage tool.
    pub fn message_accounting(&self) -> serde_json::Value {
        let groups = self.groups.read().expect("groups lock");
        let mut total: std::collections::BTreeMap<String, u64> = Default::default();
        let mut queues: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
        for h in groups.values() {
            let c = &h.sm.counters;
            for (k, n) in c.snapshot_u64() {
                *total.entry(k.to_string()).or_default() += n;
            }
            for (name, ready, held) in h.sm.account_queues() {
                let e = queues.entry(name).or_default();
                e.0 += ready;
                e.1 += held;
            }
        }
        let mut list: Vec<(String, u64, u64)> = queues
            .into_iter()
            .map(|(q, (ready, held))| (q, ready, held))
            .collect();
        list.sort_by_key(|(_, r, h)| std::cmp::Reverse(r + h));
        list.truncate(24);
        serde_json::json!({
            "totals": total,
            "queues": list.into_iter().map(|(q, ready, held)| {
                serde_json::json!({"queue": q, "ready": ready, "held": held})
            }).collect::<Vec<_>>(),
        })
    }

    /// A fresh, node-unique subscription id.
    pub fn next_subscription_id(&self) -> switchboard_core::model::SubscriptionId {
        use std::sync::atomic::Ordering;
        switchboard_core::model::SubscriptionId {
            node: self.id,
            sub: self.next_sub.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Leave the cluster: announce the departure through meta (Forgetter
    /// removes the node from the directory and from every shard group's
    /// member list), then stop every local raft core.
    ///
    /// The shard layout never loses groups, so queue→group placement is
    /// stable across departures; each group retains its remaining voters
    /// (a 3-voter group keeps quorum through any single departure).
    pub async fn leave(self: &Arc<Self>) -> Result<(), ClusterError> {
        // Two attempts: the forget must outlive this process, but a
        // transient election mid-announcement should not strand the node
        // in the directory forever either.
        for _ in 0..2 {
            match self
                .write(
                    META_GROUP,
                    BrokerCommand::Meta(MetaCmd::ForgetNode { node: self.id }),
                )
                .await
            {
                Ok(_) => break,
                Err(e) => warn!(node = self.id, err = ?e, "forget announcement failed; retrying"),
            }
        }
        self.shutdown().await;
        info!(node = self.id, "left the cluster");
        Ok(())
    }

    /// The applied meta state (topology + shard map).
    pub fn topology(&self) -> Arc<MetaState> {
        self.topology_tx.subscribe().borrow().clone()
    }

    pub fn topology_watcher(&self) -> tokio::sync::watch::Receiver<Arc<MetaState>> {
        self.topology_tx.subscribe()
    }

    /// Register a local consumer sink: deliveries and cancellations for
    /// `sub` arrive on these channels.
    pub async fn attach_consumer(&self, sub: SubscriptionId, sink: ConsumerSink) {
        self.consumer_sinks.write().await.insert(sub, sink);
    }

    pub async fn detach_consumer(&self, sub: SubscriptionId) {
        self.consumer_sinks.write().await.remove(&sub);
    }

    // ------------------------------------------------------------------
    // Cluster formation & join
    // ------------------------------------------------------------------

    async fn form_cluster(self: &Arc<Self>) -> Result<(), ClusterError> {
        self.ensure_group(META_GROUP).await?;
        // Deterministic formation: the meta group starts with the first
        // min(3, expected_nodes) node ids as voters. openraft supports
        // concurrent identical initialize on every voter; the election
        // resolves which replica's vote wins.
        let initial_voters: Vec<NodeId> =
            (1..=self.cfg.expected_nodes.min(MAX_VOTERS as u64)).collect();
        self.initialize_group_if_fresh(META_GROUP, initial_voters).await?;

        // Leadership requires the other initial voters to finish starting;
        // the remaining formation steps run as a background task so
        // `start()` returns without waiting for them.
        {
            let n = self.clone();
            tokio::spawn(async move {
                if let Err(e) = n.form_cluster_tail().await {
                    warn!(node = n.id, err = ?e, "formation tail failed");
                }
            });
        }
        info!(node = self.id, "cluster formation started");
        Ok(())
    }

    /// Post-leadership formation steps: wait for the meta leader, register
    /// this node, apply the bootstrap entities, and refresh the topology.
    async fn form_cluster_tail(self: &Arc<Self>) -> Result<(), ClusterError> {
        self.wait_group_leader(META_GROUP).await?;
        self.write(
            META_GROUP,
            BrokerCommand::Meta(MetaCmd::RegisterNode {
                node: self.id,
                info: NodeInfo {
                    client_addr: self.cfg.client_addr.clone(),
                    internal_addr: self.cfg.internal_addr.clone(),
                },
            }),
        )
        .await?;
        self.write(
            META_GROUP,
            BrokerCommand::Bootstrap {
                vhost: switchboard_wire::constants::DEFAULT_VHOST.into(),
                user: switchboard_wire::constants::DEFAULT_USER.into(),
                password: switchboard_wire::constants::DEFAULT_PASSWORD.into(),
            },
        )
        .await?;
        self.refresh_topology().await;
        info!(node = self.id, "cluster formed");
        Ok(())
    }

    async fn join_seeds(self: &Arc<Self>) -> Result<(), ClusterError> {
        let info = NodeInfo {
            client_addr: self.cfg.client_addr.clone(),
            internal_addr: self.cfg.internal_addr.clone(),
        };
        // Retry until the deadline: seeds may still be forming (leader not
        // elected yet, registration write deferred) or briefly restarting.
        let deadline = tokio::time::Instant::now() + switchboard_core::tempo::scale(self.cfg.timeouts.join_budget);
        loop {
            for seed in &self.cfg.seeds {
                match self.join_one_seed(seed, &info).await {
                    Ok(()) => return Ok(()),
                    Err(e) => debug!(seed, err = ?e, "join attempt failed; will retry"),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClusterError::NotJoined);
            }
            tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(200))).await;
        }
    }

    /// Introduce this node to a peer discovered at runtime discovered at runtime (DNS/mDNS):
    /// same handshake as the join protocol, so a node that finds a
    /// cluster registers itself without any static configuration.
    pub async fn introduce(self: &Arc<Self>, internal_addr: &str) -> Result<(), ClusterError> {
        let info = NodeInfo {
            client_addr: self.cfg.client_addr.clone(),
            internal_addr: self.cfg.internal_addr.clone(),
        };
        self.join_one_seed(internal_addr, &info).await
    }

    /// One join attempt against a single seed.
    async fn join_one_seed(
        self: &Arc<Self>,
        seed: &str,
        info: &NodeInfo,
    ) -> Result<(), ClusterError> {
        let mut conn = self.channel.connect(seed).await?;
        let req = Envelope {
            from: self.id,
            message: InternalMessage::Request(InternalRequest::Admin(AdminRequest::Join {
                node: self.id,
                info: info.clone(),
                bootstrap: false,
            })),
        };
        let bytes = bincode::serialize(&req).map_err(|e| ClusterError::Codec(e.to_string()))?;
        conn.send(&bytes).await?;
        let back = conn.recv().await?;
        let msg: InternalMessage = bincode::deserialize(&back)
            .map_err(|e| ClusterError::Codec(e.to_string()))?;
        if let InternalMessage::Response(InternalResponse::Admin(AdminResponse::Joined {
            directory,
            ..
        })) = msg
        {
            // Learn the whole peer directory from the seed immediately,
            // then pull the real topology through it. Do NOT adopt the
            // seed's group map directly: an empty map would make
            // reconciliation tear down live raft handles.
            for (peer, addr) in &directory {
                self.dir.update(*peer, addr.clone());
            }
            self.refresh_topology().await;
            info!(seed, "joined cluster");
            return Ok(());
        }
        Err(ClusterError::Codec(format!("{seed}: refused")))
    }

    // ------------------------------------------------------------------
    // Raft group lifecycle
    // ------------------------------------------------------------------

    async fn ensure_group(self: &Arc<Self>, group: GroupId) -> Result<Arc<GroupHandle>, ClusterError> {
        if let Some(h) = self.groups.read().expect("groups lock").get(&group) {
            return Ok(h.clone());
        }
        // Process-global single-instance guard: two raft cores driving the
        // same (storage, group) would corrupt each other's votes and logs.
        // Keyed by the RocksDB instance pointer (node identity) + group.
        //
        // NOTE: entries hold Weak handles so shutdown simply drops rafts.
        static LIVE: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<(usize, GroupId), std::sync::Weak<GroupHandle>>>> =
            std::sync::OnceLock::new();

        {
            let mut live = LIVE.get_or_init(Default::default).lock().expect("live lock");
            let key = (self.kv.identity(), group);
            if let Some(h) = live.get(&key).and_then(std::sync::Weak::upgrade) {
                self.groups.write().expect("groups lock").insert(group, h.clone());
                return Ok(h);
            }
            live.remove(&key);
        }

        // Serialize creation across concurrent requests; the tokio mutex is
        // held across the (awaiting) raft startup.
        let _permit = self.creating.lock().await;
        if let Some(h) = self.groups.read().expect("groups lock").get(&group) {
            return Ok(h.clone());
        }

        // Test tempo shrinks these, but keep a floor: sub-30 ms election
        // timeouts flap under scheduler load and destabilize formation.
        let scaled = |d: &Duration| -> u64 {
            switchboard_core::tempo::scale(*d)
                .max(Duration::from_millis(40))
                .as_millis() as u64
        };
        // Openraft requires heartbeat < election_min; the floor must keep
        // that invariant at any tempo.
        let config = Config {
            heartbeat_interval: scaled(&self.cfg.timeouts.raft_heartbeat),
            election_timeout_min: scaled(&self.cfg.timeouts.raft_election_min)
                .max(scaled(&self.cfg.timeouts.raft_heartbeat) * 2),
            election_timeout_max: (scaled(&self.cfg.timeouts.raft_election_min)
                .max(scaled(&self.cfg.timeouts.raft_heartbeat) * 2)
                * 2)
            .max(scaled(&self.cfg.timeouts.raft_election_max)),
            // Building a snapshot serializes the group's whole message
            // store; at the 10k-entry default a busy group pays that tax
            // every few seconds of load. Keep logs longer so snapshots
            // are rare and large instead of frequent and stall-y.
            max_in_snapshot_log_to_keep: 100_000,
            purge_batch_size: 50_000,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| ClusterError::Raft { group, message: e.to_string() })?,
        );

        let log_store = LogStore::new(self.kv.clone(), group);
        let (effects_tx, effects_rx) = tokio::sync::mpsc::unbounded_channel::<Effect>();
        let sm = StateMachine::new(self.kv.clone(), group, effects_tx)
            .map_err(|e| ClusterError::Raft { group, message: e.to_string() })?;

        let network = NetworkFactory {
            group,
            from: self.id,
            dir: self.dir.clone(),
            channel: self.channel.clone(),
        };

        let raft = Raft::new(self.id, config, network, log_store, sm.clone())
            .await
            .map_err(|e| ClusterError::Raft { group, message: e.to_string() })?;

        let handle = Arc::new(GroupHandle {
            raft,
            sm: sm.clone(),
            _effects: self.spawn_effect_pump(group, effects_rx),
        });
        {
            let mut live = LIVE.get_or_init(Default::default).lock().expect("live lock");
            live.insert((self.kv.identity(), group), Arc::downgrade(&handle));
        }
        self.groups.write().expect("groups lock").insert(group, handle.clone());
        info!(node = self.id, group, "raft group handle created");
        Ok(handle)
    }

    /// One-shot raft initialization for a fresh group. No-ops (without
    /// error) when this node's log store already has state or when another
    /// member won the initialization race — both mean the group exists.
    async fn initialize_group_if_fresh(
        self: &Arc<Self>,
        group: GroupId,
        members: Vec<NodeId>,
    ) -> Result<(), ClusterError> {
        let handle = self.ensure_group(group).await?;
        // `initialize` accepts any IntoNodes: pass the voter ids directly.
        let voters: std::collections::BTreeSet<NodeId> = members.iter().copied().collect();
        match handle.raft.initialize(voters).await {
            Ok(()) => {
                info!(node = self.id, group, members = ?members, "initialized raft group");
                Ok(())
            }
            // Any error here means the group already exists (a concurrent
            // member initialized first, or our store is not fresh) — benign.
            Err(_) => Ok(()),
        }
    }

    async fn wait_group_leader(self: &Arc<Self>, group: GroupId) -> Result<NodeId, ClusterError> {
        let handle = self.ensure_group(group).await?;
        for _ in 0..600 {
            if let Some(l) = handle.raft.current_leader().await {
                return Ok(l);
            }
            tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(50))).await;
        }
        Err(ClusterError::Raft { group, message: "no leader elected".into() })
    }

    /// Is this node's store for `group` still untouched (formation time)?
    fn group_is_fresh(&self, group: GroupId) -> bool {
        self.kv
            .get(&format!("g{group}:sm").into_bytes())
            .ok()
            .flatten()
            .is_none()
    }

    fn spawn_effect_pump(
        self: &std::sync::Arc<Self>,
        group: GroupId,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<Effect>,
    ) -> tokio::task::JoinHandle<()> {
        let node = Arc::downgrade(self);
        tokio::spawn(async move {
            while let Some(effect) = rx.recv().await {
                let Some(node) = node.upgrade() else { break };
                let group_for_effect = group;
                // Only the leader acts on effects; followers produce the
                // same effects applying the same entries and drop them.
                // The check must be THIS node's own raft role — a
                // follower's role is never Leader, even while the remote
                // leader *hint* (current_leader) transiently reads None
                // under load. Treating an unknown hint as "I lead" made
                // busy followers ship deliveries too, and remote
                // consumers received every message twice. (A leader
                // deposed in the instant between applying an entry and
                // shipping its effect drops it; the client lifecycle —
                // channel close, consumer cancel, node restart —
                // requeues such hand-outs.)
                let leader_is_someone_else = {
                    let handle = node.groups.read().expect("groups lock").get(&group).cloned();
                    match handle {
                        Some(h) => h.raft.metrics().borrow().state != openraft::ServerState::Leader,
                        None => true,
                    }
                };
                if leader_is_someone_else {
                    continue;
                }
                node.dispatch_effect(group_for_effect, effect).await;
            }
        })
    }

    async fn dispatch_effect(self: &Arc<Self>, group: GroupId, effect: Effect) {
        match effect {
            Effect::Meta(_) => {
                // Topology distribution happens via the reconcile loop.
            }
            Effect::Shard(ShardEffect::MessageReady {
                sub,
                queue,
                seq,
                message,
                redelivered,
                deleted,
            }) => {
                let delivery = Delivery {
                    sub,
                    queue: queue.clone(),
                    seq,
                    message: message.clone(),
                    redelivered,
                    deleted,
                };
                if let Some(sink) = self.consumer_sinks.read().await.get(&sub) {
                    let _ = sink.deliveries.send(delivery);
                    return;
                }
                // The consumer lives on another node: hand it over.
                // At-least-once: a failed hand-off (peer unreachable, sink
                // gone, owner unknown) releases the hold so the pump
                // redelivers — never strand a confirmed message.
                let target = self.owner_node_of(sub);
                let t_del = std::time::Instant::now();
                let response = match target {
                    Some(peer) => self
                        .call(
                            peer,
                            InternalRequest::Admin(AdminRequest::Deliver {
                                sub,
                                queue: queue.clone(),
                                seq,
                                message: message.clone(),
                                redelivered,
                                deleted,
                            }),
                        )
                        .await,
                    _ => Err(ClusterError::Unreachable { group }),
                };
                if std::env::var("SB_TIMING").is_ok() {
                    eprintln!("[timing] deliver rpc took {:?} peer={:?}", t_del.elapsed(), target);
                }
                match response {
                    Ok(InternalResponse::Admin(AdminResponse::Delivered)) => {}
                    // The hold must not outlive the delivery path. Two
                    // failure shapes:
                    // * owner answered NotFound — its sink is gone (the
                    //   consumer session died without unregistering).
                    //   Releasing alone leaves a credit-carrying zombie
                    //   sub that the pump re-fills every tick: a
                    //   hand-out → failed-Deliver → release wheel that
                    //   burned hundreds of thousands of raft applies and
                    //   stranded messages once release retries ran out.
                    //   Unregister the zombie instead — that releases its
                    //   holds and lets live consumers take over.
                    // * owner unreachable / unknown — release and retry
                    //   until it lands (at-least-once): a stranded hold
                    //   is never acked by anyone.
                    Ok(InternalResponse::Admin(AdminResponse::NotFound)) => {
                        let node = self.clone();
                        tokio::spawn(async move {
                            let cmd = BrokerCommand::Shard(
                                switchboard_core::shard::ShardCmd::UnregisterSubscription {
                                    sub,
                                },
                            );
                            for _ in 0..node.cfg.timeouts.deliver_release_retries {
                                match node.write(group, cmd.clone()).await {
                                    Ok(_) => return,
                                    Err(ClusterError::Transient) => {
                                        tokio::time::sleep(switchboard_core::tempo::scale(
                                            node.cfg.timeouts.deliver_release_interval,
                                        ))
                                        .await
                                    }
                                    Err(_) => return,
                                }
                            }
                        });
                    }
                    _ => {
                        let node = self.clone();
                        tokio::spawn(async move {
                            let cmd = BrokerCommand::Shard(
                                switchboard_core::shard::ShardCmd::Release {
                                    queue: queue.clone(),
                                    sub: Some(sub),
                                    seqs: vec![seq],
                                    dead: false,
                                },
                            );
                            for _ in 0..node.cfg.timeouts.deliver_release_retries {
                                match node.write(group, cmd.clone()).await {
                                    Ok(_) => return,
                                    Err(ClusterError::Transient) => {
                                        tokio::time::sleep(switchboard_core::tempo::scale(
                                            node.cfg.timeouts.deliver_release_interval,
                                        ))
                                        .await
                                    }
                                    Err(_) => return,
                                }
                            }
                        });
                    }
                }
            }
            Effect::Shard(ShardEffect::DeadLettered { message, dlx, .. }) => {
                // Route the dead-lettered message through the configured
                // DLX (or drop it when the queue has none).
                let Some((exchange, rk_override)) = dlx else { return };
                let rk = rk_override.unwrap_or_else(|| message.routing_key.clone());
                self.route_and_enqueue("/", &exchange, &rk, &message).await;
            }
            Effect::Shard(ShardEffect::ConsumerCancelled { sub, node, consumer_tag }) => {
                if node == self.id {
                    if let Some(sink) = self.consumer_sinks.read().await.get(&sub) {
                        let _ = sink.cancelled.send(consumer_tag);
                        return;
                    }
                }
                let _ = self
                    .call(
                        node,
                        InternalRequest::Admin(AdminRequest::CancelConsumer { sub, consumer_tag }),
                    )
                    .await;
            }
        }
    }

    /// Route one message through an exchange of the (local) topology view
    /// and enqueue it on every destination queue's shard. Used by the
    /// dead-letter path and available to admin flows.
    pub async fn route_and_enqueue(
        self: &Arc<Self>,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
        message: &StoredMessage,
    ) {
        let topo = self.topology();
        let Some(vh) = topo.vhosts.get(vhost) else { return };
        let view = switchboard_core::topology::VhostView { vhost: vh };
        let destinations = switchboard_core::routing::route(&view, exchange, routing_key, &message.properties);
        // This node's cached topology may lag a just-replicated declare
        // (bindings arrive via the meta log; the cache refreshes on this
        // node's own writes). A route that comes up empty against a
        // possibly-stale view gets one refreshed retry before the
        // message is treated as genuinely unroutable.
        let (vh, destinations) = if destinations.is_empty() {
            self.refresh_topology().await;
            let topo = self.topology();
            match topo.vhosts.get(vhost).cloned() {
                Some(vh) => {
                    let view = switchboard_core::topology::VhostView { vhost: &vh };
                    let d = switchboard_core::routing::route(&view, exchange, routing_key, &message.properties);
                    (std::borrow::Cow::Owned(vh), d)
                }
                None => return,
            }
        } else {
            (std::borrow::Cow::Borrowed(vh), destinations)
        };
        for q in destinations {
            let Some(shard) = vh.queues.get(&q).map(|qi| qi.shard) else { continue };
            let _ = self
                .write(
                    shard,
                    BrokerCommand::Shard(switchboard_core::shard::ShardCmd::Enqueue {
                        queue: q,
                        message: message.clone(),
                        at_ms: now_ms(),
                    }),
                )
                .await;
        }
    }

    /// Which node hosts the channel behind subscription `sub`? Answered
    /// from the owning shard's state machine when hosted here.
    fn owner_node_of(&self, sub: SubscriptionId) -> Option<NodeId> {
        for handle in self.groups.read().expect("groups lock").values() {
            if let Some(node) = handle.sm.sub_owner(sub) {
                return Some(node);
            }
        }
        None
    }

    // ------------------------------------------------------------------
    // Fanout publishes (cross-queue total order)
    // ------------------------------------------------------------------

    /// Enqueue `message` on every queue in `destinations`, preserving one
    /// global order across all destination shard groups.
    ///
    /// Multi-destination publishes are the one place where independent
    /// raft groups can disagree: group A and group B apply two concurrent
    /// publishes in opposite orders, and consumers of queues on A and B
    /// observe different sequences for the same messages. This path
    /// replicates the fanout through the META group (`FanoutBegin`): the
    /// meta leader executes pending fanouts strictly in meta-log order,
    /// one at a time, so every destination group receives them in one
    /// total order — and because that order lives in replicated meta
    /// state, it survives meta-leader failover (a deposed leader's
    /// unfinished fanout stays pending and is re-executed by its
    /// successor; per-leg request ids make re-execution exactly-once).
    ///
    /// Returns (and confirms the publish) only after every destination's
    /// enqueue has quorum-applied.
    pub async fn fanout_publish(
        self: &Arc<Self>,
        vhost: String,
        message: StoredMessage,
        destinations: Vec<String>,
    ) -> Result<(), ClusterError> {
        let id = uuid::Uuid::new_v4();
        self.write(
            META_GROUP,
            BrokerCommand::Meta(MetaCmd::FanoutBegin {
                id,
                vhost,
                message,
                queues: destinations,
            }),
        )
        .await
        .map_err(|e| {
            if matches!(e, ClusterError::Unreachable { .. }) {
                ClusterError::Unreachable { group: META_GROUP }
            } else {
                e
            }
        })?;
        // The pending entry is live: wake the (possibly local) executor
        // instead of waiting for its tick.
        self.fanout_wake.notify_one();
        // The executor clears the pending entry once every leg has
        // quorum-applied; that moment is the publisher confirm. On the
        // meta leader, drive the execution directly — the background loop
        // shares the lock and FIFO order, so this is the same work without
        // a wakeup round trip.
        //
        // Completion is judged on monotone state only. The old shortcut —
        // "pending entry absent, therefore done" — raced the local meta
        // apply: `write()` resolves at quorum commit, but this replica's
        // apply of the FanoutBegin can lag a beat, and the first poll then
        // saw the entry absent, confirmed the publish, and let the next
        // publish's single-destination direct write race the executor's
        // leg into the same shard group — inverting per-queue FIFO.
        let deadline = tokio::time::Instant::now()
            + switchboard_core::tempo::scale(self.cfg.timeouts.fanout_budget);
        let mut seen_pending = false;
        loop {
            let topo = self.topology();
            if topo.recent_fanouts.contains(&id) {
                return Ok(());
            }
            let pending_now = topo.pending_fanouts.iter().any(|f| f.id == id);
            if pending_now {
                seen_pending = true;
            } else if seen_pending {
                // Appeared locally, then cleared: FanoutDone applied.
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClusterError::Unreachable { group: META_GROUP });
            }
            if self.leader_hint_of(META_GROUP).await == Some(self.id) {
                self.execute_pending_fanouts().await?;
            } else {
                tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(3))).await;
            }
            self.refresh_topology().await;
        }
    }

    /// The per-leg request id for `fanout_id`: stable across retries and
    /// across nodes, distinct per destination.
    fn fanout_leg_id(fanout_id: crate::typ::RequestId, leg: usize) -> crate::typ::RequestId {
        debug_assert!(leg < 256, "fanout leg index must fit the id tag byte");
        let mut bytes = *fanout_id.as_bytes();
        bytes[15] ^= leg as u8;
        uuid::Uuid::from_bytes(bytes)
    }

    /// Background executor: while this node leads the meta group, apply
    /// pending fanouts strictly in meta-log order, one at a time, then
    /// mark them done. Woken the moment a fanout becomes pending (the
    /// `FanoutBegin` apply lands on the leader — locally or via forward);
    /// the interval is only a fallback for failovers.
    async fn fanout_executor_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.fanout_wake.notified() => {}
                _ = tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(100))) => {}
            }
            match self.leader_hint_of(META_GROUP).await {
                Some(l) if l == self.id => {
                    if let Err(e) = self.execute_pending_fanouts().await {
                        tracing::debug!(node = self.id, err = ?e, "fanout executor pass failed; will retry");
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute every pending fanout, FIFO, under the fanout lock. Legs are
    /// idempotent (`fanout_leg_id`), so a fanout half-executed by a
    /// deposed predecessor finishes exactly-once here. Strictly one fanout
    /// per pass: cross-queue order is pinned to the meta log, and keeping
    /// a pass short shrinks the window where a deposed leader's stale
    /// pass could submit a competing order against the real leader's.
    async fn execute_pending_fanouts(self: &Arc<Self>) -> Result<(), ClusterError> {
        let _guard = self.fanout_lock.lock().await;
        loop {
            self.refresh_topology().await;
            let Some(pending) = self.topology().pending_fanouts.first().cloned() else {
                return Ok(());
            };
            let topo = self.topology();
            let vhost = topo.vhosts.get(&pending.vhost).cloned();
            let legs = futures::future::join_all(pending.queues.iter().enumerate().map(|(leg, q)| {
                let group = vhost.as_ref().and_then(|vh| vh.queues.get(q).map(|qi| qi.shard));
                let q = q.clone();
                let message = pending.message.clone();
                let leg_id = Self::fanout_leg_id(pending.id, leg);
                async move {
                    let Some(group) = group else {
                        // Vanished between acceptance and execution: skip,
                        // mirroring the direct path's treatment of unrouted
                        // destinations.
                        return Ok(());
                    };
                    self.write_idempotent(
                        group,
                        BrokerCommand::Shard(switchboard_core::shard::ShardCmd::Enqueue {
                            queue: q,
                            message,
                            at_ms: now_ms(),
                        }),
                        leg_id,
                    )
                    .await
                    .map(|_| ())
                }
            }))
            .await;
            for r in legs {
                r?;
            }
            self.write(
                META_GROUP,
                BrokerCommand::Meta(MetaCmd::FanoutDone { id: pending.id }),
            )
            .await?;
        }
    }


    pub async fn refresh_topology(self: &Arc<Self>) {
        // Bind the local (sync) read result, then drop the lock before any
        // await — std guards are not Send. Meta-only clone: this runs on
        // every reconcile tick and fanout poll, and the shard half holds
        // the message store.
        let local_meta: Option<switchboard_core::topology::MetaState> = {
            let groups = self.groups.read().expect("groups lock");
            groups.get(&META_GROUP).map(|h| h.sm.read_meta())
        };
        // A local replica is authoritative once the bootstrap entities have
        // been applied to it. Before that (fresh joiner with an empty meta
        // raft) the local state is `Some(empty)` — treat it as "no data" and
        // fetch from a peer instead, otherwise the joiner stays blind.
        // Bootstrap populates both vhosts and users; an un-bootstrapped
        // replica has neither, so treat that as "no local data".
        let local_ready = local_meta
            .as_ref()
            .map(|m| !m.vhosts.is_empty() && !m.users.is_empty())
            .unwrap_or(false);
        let state = if local_ready {
            local_meta.unwrap()
        } else {
            match self.fetch_topology_via_forward().await {
                Some(s) if !s.vhosts.is_empty() => s,
                Some(_) | None => match local_meta {
                    Some(m) if !m.vhosts.is_empty() => m,
                    _ => return,
                },
            }
        };
        for (id, info) in &state.nodes {
            let old = self.dir.addr_of(*id);
            if self.dir.update(*id, info.internal_addr.clone()) {
                // The peer moved: drop idle pooled connections to the
                // previous address so they are never handed out again.
                if let Some(old) = old {
                    self.channel.invalidate(&old).await;
                }
            }
        }
        // send_replace stores the value even with no live receivers
        // (plain `send` silently drops updates when nobody is watching).
        self.topology_tx.send_replace(Arc::new(state));
    }

    /// Point the directory at `addr` for `peer` and drop pooled idle
    /// connections to the previous address. Used when a peer is known to
    /// have moved (and by tests that intercept a peer's traffic).
    pub async fn reroute_peer(&self, peer: NodeId, addr: String) {
        let old = self.dir.addr_of(peer);
        if self.dir.update(peer, addr) && let Some(old) = old {
            self.channel.invalidate(&old).await;
        }
    }

    async fn fetch_topology_via_forward(&self) -> Option<MetaState> {
        // Try every peer; prefer the first non-empty (bootstrapped) view.
        // Some peers may host only empty placeholder meta rafts.
        for (peer, _addr) in self.dir.all() {
            if peer == self.id {
                continue;
            }
            if let Ok(InternalResponse::Admin(AdminResponse::Topology(t))) =
                self.call(peer, InternalRequest::Admin(AdminRequest::Topology)).await
            {
                if !t.meta.vhosts.is_empty() {
                    return Some(t.meta);
                }
            }
        }
        None
    }

    // ------------------------------------------------------------------
    // Writes (the multi-master path)
    // ------------------------------------------------------------------

    /// Apply `command` on `group`, from anywhere in the cluster.
    ///
    /// Retries transient conditions (election in flight, unreachable
    /// leader) for up to 30 seconds; hard broker errors surface
    /// immediately. The command is stamped with a fresh request id and
    /// applied under `Idempotent`: retries and re-forwards of the same
    /// logical write apply exactly once (see `BrokerState::dedup`). For a
    /// caller that retries a logical operation across `write()` calls,
    /// use [`Self::write_idempotent`] with a stable id instead.
    pub async fn write(
        self: &Arc<Self>,
        group: GroupId,
        command: BrokerCommand,
    ) -> Result<BrokerReply, ClusterError> {
        let id = uuid::Uuid::new_v4();
        self.write_idempotent(group, command, id).await
    }

    /// [`Self::write`], with the request id chosen by the caller. The same
    /// `(id, command)` re-issued any number of times, on any member of the
    /// group, applies exactly once.
    pub async fn write_idempotent(
        self: &Arc<Self>,
        group: GroupId,
        command: BrokerCommand,
        id: crate::typ::RequestId,
    ) -> Result<BrokerReply, ClusterError> {
        let command = BrokerCommand::Idempotent {
            id,
            command: Box::new(command),
        };
        // Shard groups coalesce (group commit): concurrent writes leave as
        // one raft entry, amortizing the commit across the batch. Meta
        // writes stay direct — fanout ordering and the membership
        // controller depend on their individual commit points.
        if group != META_GROUP && Self::commit_batch_max() > 1 {
            return self.coalesced_write(group, command).await;
        }
        let deadline = tokio::time::Instant::now() + switchboard_core::tempo::scale(self.cfg.timeouts.write_budget);
        loop {
            match self.try_write_depth(group, &command, 0).await {
                Ok(reply) => return Ok(reply),
                Err(ClusterError::Transient) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ClusterError::Unreachable { group });
                    }
                    tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(100))).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Env-tunable batch ceiling for group commit; `0` or `1` disables
    /// coalescing (every write commits alone).
    fn commit_batch_max() -> usize {
        static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *MAX.get_or_init(|| {
            std::env::var("SWITCHBOARD_COMMIT_BATCH_MAX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256)
        })
    }

    /// Hand a write to the group's coalescer and wait for the batch's
    /// commit.
    async fn coalesced_write(
        self: &Arc<Self>,
        group: GroupId,
        command: BrokerCommand,
    ) -> Result<BrokerReply, ClusterError> {
        let tx = self.coalescer_for(group).await;
        let (rtx, rrx) = tokio::sync::oneshot::channel();
        tx.send(CoalesceReq { command, reply: rtx })
            .map_err(|_| ClusterError::Unreachable { group })?;
        rrx.await.map_err(|_| ClusterError::Unreachable { group })?
    }

    /// The group's coalescer channel, spawning its loop on first use.
    async fn coalescer_for(
        self: &Arc<Self>,
        group: GroupId,
    ) -> tokio::sync::mpsc::UnboundedSender<CoalesceReq> {
        if let Some(tx) = self.coalescers.lock().expect("coalescers lock").get(&group) {
            return tx.clone();
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut m = self.coalescers.lock().expect("coalescers lock");
            if let Some(existing) = m.get(&group) {
                return existing.clone();
            }
            m.insert(group, tx.clone());
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(Self::coalescer_loop(weak, group, rx));
        tx
    }

    /// Group commit: take everything queued, submit it as ONE raft entry,
    /// hand every caller its reply. Transient failures retry the whole
    /// batch — each element is `Idempotent`-wrapped, so retries and
    /// re-forwards apply exactly once — until the write budget expires.
    /// Holds only a `Weak` back-reference so a dropped node (tests) is
    /// never kept alive by its own coalescers.
    async fn coalescer_loop(
        weak: std::sync::Weak<ClusterNode>,
        group: GroupId,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<CoalesceReq>,
    ) {
        let max = Self::commit_batch_max().max(1);
        while let Some(first) = rx.recv().await {
            let Some(node) = weak.upgrade() else { break };
            let mut batch = vec![first];
            while batch.len() < max {
                match rx.try_recv() {
                    Ok(r) => batch.push(r),
                    Err(_) => break,
                }
            }
            let deadline =
                tokio::time::Instant::now() + switchboard_core::tempo::scale(node.cfg.timeouts.write_budget);
            loop {
                let commands =
                    BrokerCommand::Batch { commands: batch.iter().map(|r| r.command.clone()).collect() };
                match node.try_write_depth(group, &commands, 0).await {
                    Ok(BrokerReply::Batch(replies)) if replies.len() == batch.len() => {
                        for (req, reply) in batch.drain(..).zip(replies) {
                            let _ = req.reply.send(Ok(reply));
                        }
                        break;
                    }
                    Ok(_) => {
                        for req in batch.drain(..) {
                            let _ = req.reply.send(Err(ClusterError::Codec(
                                "batch reply arity mismatch".into(),
                            )));
                        }
                        break;
                    }
                    Err(ClusterError::Transient) => {
                        if tokio::time::Instant::now() >= deadline {
                            for req in batch.drain(..) {
                                let _ = req.reply.send(Err(ClusterError::Unreachable { group }));
                            }
                            break;
                        }
                        // Fold in anything that queued up while retrying.
                        while batch.len() < max {
                            match rx.try_recv() {
                                Ok(r) => batch.push(r),
                                Err(_) => break,
                            }
                        }
                        tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(100))).await;
                    }
                    Err(e) => {
                        for req in batch.drain(..) {
                            let _ = req.reply.send(Err(e.for_waiter(group)));
                        }
                        break;
                    }
                }
            }
        }
    }

    /// One write attempt. `Err(ClusterError::Transient)` means "retry".
    ///
    /// `depth` bounds chaining: 0 = entry from a local client (may follow
    /// its leader hint one hop); 1 = answering a forwarded write (applies
    /// from local knowledge only, never chains another forward — stale
    /// mutual hints between two members would otherwise ping-pong the
    /// request forever).
    async fn try_write_depth(
        self: &Arc<Self>,
        group: GroupId,
        command: &BrokerCommand,
        depth: u32,
    ) -> Result<BrokerReply, ClusterError> {
        // If the topology says this group is hosted here, make sure the
        // local raft handle exists before writing (handles are normally
        // created by the reconcile loop, which may not have ticked yet).
        let member_here = self
            .topology()
            .groups
            .get(&group)
            .map(|m| m.contains(&self.id))
            .unwrap_or(false);
        if member_here {
            let _ = self.ensure_group(group).await;
        }

        let local = self.groups.read().expect("groups lock").get(&group).cloned();
        if let Some(handle) = local {
            let mut hint: Option<NodeId> = handle.raft.current_leader().await;
            // Bounded: each round either lands the write, follows the
            // leader hint one hop, or gives up.
            for _ in 0..16 {
                if let Some(l) = hint {
                    if l != self.id {
                        if depth == 0 {
                            let r = self.forward_write(l, group, command.clone()).await;
                            match r {
                                Err(ClusterError::Io(_))
                                | Err(ClusterError::Unreachable { .. })
                                | Err(ClusterError::NotJoined) => {
                                    // Stale hint (deposed or departed
                                    // leader): fall through to the scan.
                                    break;
                                }
                                other => return other,
                            }
                        }
                        // Forwarded writes answer from local knowledge;
                        // the original writer owns re-routing.
                        return Err(ClusterError::Transient);
                    }
                }
                // Either we believe we lead the group, or nobody does yet;
                // in both cases attempt the write locally — openraft
                // answers ForwardToLeader immediately when the belief is
                // wrong, and a ForwardToLeader reply never applied data.
                match handle.raft.client_write(command.clone()).await {
                    Ok(resp) => return Ok(resp.data),
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(
                        ForwardToLeader { leader_id, .. },
                    ))) => {
                        if leader_id.is_none() && hint.is_none() {
                            break;
                        }
                        hint = leader_id;
                    }
                    Err(RaftError::APIError(other)) => {
                        return Err(ClusterError::Raft { group, message: other.to_string() });
                    }
                    Err(e) => return Err(ClusterError::Raft { group, message: e.to_string() }),
                }
            }
        }

        // Leaderless (or unhosted) from local knowledge. A member knows
        // its group best, and forwarded writes answer one-shot — both
        // surface retryable-transient; the original writer owns retries.
        if depth > 0 || member_here {
            return Err(ClusterError::Transient);
        }

        // Entry point, not a member: forward to one of the group's
        // members, skipping peers that cannot serve it (including stale
        // views that do not list the group yet). Members answer one-shot,
        // so this scan never loops.
        let members: Vec<NodeId> = match self.topology().groups.get(&group).cloned() {
            Some(m) => m,
            None if group == META_GROUP => {
                let mut all: Vec<NodeId> = self.dir.all().into_iter().map(|(id, _)| id).collect();
                all.sort();
                all
            }
            None => return Err(ClusterError::Unreachable { group }),
        };
        for m in &members {
            if *m == self.id {
                continue;
            }
            match self.forward_write(*m, group, command.clone()).await {
                Ok(reply) => return Ok(reply),
                Err(ClusterError::Transient) => continue,
                Err(ClusterError::Unreachable { .. }) | Err(ClusterError::NotJoined) => continue,
                // A dead peer (dial refused) is skippable like any other
                // unreachable member — never a hard failure for the write.
                Err(ClusterError::Io(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        // Every member refused — possibly mid-formation. Retryable within
        // the outer budget; a group that is genuinely gone surfaces as
        // Unreachable at the deadline.
        Err(ClusterError::Transient)
    }

    async fn forward_write(
        self: &Arc<Self>,
        peer: NodeId,
        group: GroupId,
        command: BrokerCommand,
    ) -> Result<BrokerReply, ClusterError> {
        let req = InternalRequest::Forward { group, command };
        match self.call(peer, req).await {
            Ok(InternalResponse::Forward(reply)) => match reply {
                BrokerReply::Error(e) => Err(ClusterError::Broker(e)),
                r => Ok(r),
            },
            Ok(InternalResponse::ForwardFailed(f)) => Err(match f {
                ForwardError::Transient => ClusterError::Transient,
                ForwardError::Unreachable => ClusterError::Unreachable { group },
                ForwardError::NotJoined => ClusterError::NotJoined,
                ForwardError::Broker(e) => ClusterError::Broker(e),
                ForwardError::Other(m) => ClusterError::Codec(m),
            }),
            Err(e) => Err(e),
            Ok(InternalResponse::Error(e)) => Err(ClusterError::Codec(e)),
            Ok(other) => Err(ClusterError::Codec(format!("unexpected forward reply: {other:?}"))),
        }
    }

    // ------------------------------------------------------------------
    // Admin / RPC plumbing
    // ------------------------------------------------------------------

    async fn call(
        &self,
        peer: NodeId,
        req: InternalRequest,
    ) -> Result<InternalResponse, ClusterError> {
        let Some(addr) = self.dir.addr_of(peer) else {
            return Err(ClusterError::Unreachable { group: META_GROUP });
        };
        let bytes = {
            let env = Envelope {
                from: self.id,
                message: InternalMessage::Request(req),
            };
            bincode::serialize(&env).map_err(|e| ClusterError::Codec(e.to_string()))?
        };
        // Pooled request→reply (stale-conn retry + reply budget live in
        // the channel); the reply comes back as raw bytes.
        let back = self
            .channel
            .rpc(&addr, &bytes)
            .await
            .map_err(ClusterError::Io)?;
        let msg: InternalMessage = bincode::deserialize(&back)
            .map_err(|e| ClusterError::Codec(e.to_string()))?;
        match msg {
            InternalMessage::Response(r) => Ok(r),
            other => Err(ClusterError::Codec(format!("unexpected {other:?}"))),
        }
    }

    // ------------------------------------------------------------------
    // Background loops
    // ------------------------------------------------------------------

    async fn reconcile_loop(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick =
            tokio::time::interval(switchboard_core::tempo::scale(self.cfg.timeouts.reconcile_interval));
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => break,
            }
            self.refresh_topology().await;
            let topo = self.topology();
            let mine: Vec<(GroupId, Vec<NodeId>)> = topo
                .groups
                .iter()
                .filter(|(_, members)| members.contains(&self.id))
                .map(|(g, members)| (*g, members.clone()))
                .collect();

            // Every node hosts the meta group: it may be added to the meta
            // voter set at any time and must be able to receive entries.
            if !self.groups.read().expect("groups lock").contains_key(&META_GROUP) {
                let fresh = self.group_is_fresh(META_GROUP);
                match self.ensure_group(META_GROUP).await {
                    Ok(_) => {
                        if fresh && self.cfg.bootstrap {
                            let _ = self.initialize_group_if_fresh(META_GROUP, vec![self.id]).await;
                        }
                    }
                    Err(e) => debug!(err = ?e, "meta ensure_group failed (will retry)"),
                }
            }

            // Shard groups: ensure handles for my groups; the smallest
            // member initializes a fresh group. A group whose handle already
            // exists (e.g. created by an early write) but that has never
            // been initialized is initialized here too — openraft rejects a
            // duplicate initialize benignly.
            for (g, members) in &mine {
                let fresh = self.group_is_fresh(*g);
                match self.ensure_group(*g).await {
                    Ok(_) => {
                        if fresh && members.first() == Some(&self.id) {
                            let _ =
                                self.initialize_group_if_fresh(*g, members.clone()).await;
                        }
                    }
                    Err(e) => debug!(group = g, err = ?e, "ensure_group failed (will retry)"),
                }
            }

            // NOTE: handles are never pruned based on a possibly-stale local
            // view — tearing down a live raft instance while others still
            // replicate to it would corrupt the group. Membership removal is
            // admin-driven, not view-driven.
        }
    }

    /// Meta-leader duties: register joiners, grow meta membership toward
    /// 3, and install the initial shard-group layout.
    async fn controller_loop(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(switchboard_core::tempo::scale(Duration::from_millis(400))) => {}
                _ = shutdown.changed() => break,
            }
            let Some(handle) = self.groups.read().expect("groups lock").get(&META_GROUP).cloned()
            else {
                continue;
            };
            let leader = handle.raft.current_leader().await;
            if leader != Some(self.id) {
                continue;
            }
            let topo = self.topology();
            let mut nodes: Vec<NodeId> = topo.nodes.keys().copied().collect();
            nodes.sort();
            if nodes.is_empty() || !nodes.contains(&self.id) {
                continue;
            }

            // 2. Install the initial shard layout once nodes have
            //    registered and the layout is still empty.
            if topo.groups.is_empty() {
                let layout = sliding_windows(&nodes);
                if !layout.is_empty() {
                    match self
                        .write(META_GROUP, BrokerCommand::Meta(MetaCmd::SetGroups { groups: layout.clone() }))
                        .await
                    {
                        Ok(_) => {
                            info!(groups = ?layout, "shard layout installed");
                            self.refresh_topology().await;
                        }
                        Err(e) => debug!(err = ?e, "SetGroups deferred"),
                    }
                }
                continue;
            }

            // 2b. Cover nodes registered after formation: anchor one new
            //    group at each uncovered node, backed by two established
            //    members (round-robin over the already-covered set, keyed
            //    by the new group id for determinism). Existing groups are
            //    frozen — queue placement never moves, capacity extends.
            let covered: std::collections::BTreeSet<NodeId> =
                topo.groups.values().flatten().copied().collect();
            let missing: Vec<NodeId> =
                nodes.iter().filter(|n| !covered.contains(n)).copied().collect();
            if !missing.is_empty() && !covered.is_empty() {
                let mut layout: Vec<(GroupId, Vec<NodeId>)> =
                    topo.groups.iter().map(|(g, m)| (*g, m.clone())).collect();
                let mut gid = topo.groups.keys().last().copied().unwrap_or(0);
                let elders: Vec<NodeId> = covered.iter().copied().collect();
                for m in &missing {
                    gid += 1;
                    // A single elder cluster (1-node origin) would produce
                    // duplicate members; the set dedupes them.
                    let mut members = std::collections::BTreeSet::new();
                    members.insert(*m);
                    members.insert(elders[gid as usize % elders.len()]);
                    members.insert(elders[(gid as usize + 1) % elders.len()]);
                    layout.push((gid, members.into_iter().collect()));
                }
                match self
                    .write(META_GROUP, BrokerCommand::Meta(MetaCmd::SetGroups { groups: layout.clone() }))
                    .await
                {
                    Ok(_) => {
                        info!(groups = ?layout, "shard layout extended");
                        self.refresh_topology().await;
                    }
                    Err(e) => debug!(err = ?e, "layout extension deferred"),
                }
                continue;
            }

            // 1. Grow (or shrink) meta membership, driven through the
            //    same ReconfigureGroup machinery as shard groups.
            //    Voter-set sizes of 2 are forbidden: one unclean loss in
            //    a 2-voter meta group permanently destroys its quorum
            //    (1 of 2 alive), and raft cannot re-vote without
            //    quorum — every meta write (joins, fanout, topology,
            //    transactions) would freeze forever. Target 3 voters
            //    whenever ≥3 nodes are registered, 1 when exactly 1 is,
            //    and leave the current set alone at 2 registered (it is
            //    either {1} — fine — or a 3-voter set with one member
            //    gone, which still has quorum).
            {
                // Voter set 3 or 1 — never 2 (one unclean loss in a
                // 2-voter meta group permanently destroys quorum). At
                // exactly 2 registered, the second node is a LEARNER: it
                // sees the leader and keeps its meta warm for the
                // 3-voter upgrade, without ever holding a deciding vote
                // in a configuration that one loss freezes.
                if nodes.len() >= MAX_VOTERS {
                    let voters: Vec<NodeId> = nodes.iter().take(MAX_VOTERS).copied().collect();
                    let learners: Vec<NodeId> = nodes.iter().skip(MAX_VOTERS).copied().collect();
                    let _ = self
                        .reconfigure_group_with_learners(META_GROUP, voters, learners)
                        .await;
                } else if nodes.len() == 2 {
                    let _ = self
                        .reconfigure_group_with_learners(
                            META_GROUP,
                            vec![nodes[0]],
                            vec![nodes[1]],
                        )
                        .await;
                } else if nodes.len() == 1 {
                    let _ = self.reconfigure_group(META_GROUP, vec![nodes[0]]).await;
                }
            }

            // 1b. Reap registered nodes whose internal port refuses
            //     connections. ECONNREFUSED is definitive — the peer's
            //     listener is gone — while timeouts and unreachable
            //     errors fail open (a partitioned or loaded node still
            //     answers). Thresholded so a restarting member rides it
            //     out. Without this, a SIGKILLed member (OOM kill,
            //     eviction, force deletion) stays registered forever,
            //     and the meta voter set / group member lists keep
            //     counting a node that is never coming back.
            self.reap_dead_nodes(&nodes).await;

            // 2c. Heal groups hit by a departure: drop lost members and
            //     refill with live nodes (deterministic round-robin keyed
            //     by the group id), keeping every group at full strength.
            //     Groups that are under-strength to begin with (a
            //     degenerate formation window, e.g. [1,2]) are topped up
            //     the same way once spare live nodes exist.
            let mut layout: Vec<(GroupId, Vec<NodeId>)> =
                topo.groups.iter().map(|(g, m)| (*g, m.clone())).collect();
            let mut healed = false;
            for (g, members) in layout.iter_mut() {
                let all_live = members.iter().all(|m| nodes.contains(m));
                if !all_live || members.len() < MAX_VOTERS {
                    let mut next: Vec<NodeId> =
                        members.iter().filter(|m| nodes.contains(m)).copied().collect();
                    let mut candidates: Vec<NodeId> =
                        nodes.iter().filter(|n| !next.contains(n)).copied().collect();
                    let mut k = *g as usize;
                    while next.len() < MAX_VOTERS && !candidates.is_empty() {
                        let idx = k % candidates.len();
                        next.push(candidates.remove(idx));
                        k += 1;
                    }
                    // Only rewrite when something actually moved: a
                    // cluster too small to top up (single node) would
                    // otherwise re-write the same layout every tick and
                    // starve the controller's later steps.
                    if &next != members {
                        *members = next;
                        healed = true;
                    }
                }
            }
            if healed {
                match self
                    .write(META_GROUP, BrokerCommand::Meta(MetaCmd::SetGroups { groups: layout.clone() }))
                    .await
                {
                    Ok(_) => {
                        info!(groups = ?layout, "shard layout healed after departure");
                        self.refresh_topology().await;
                    }
                    Err(e) => debug!(err = ?e, "layout heal deferred"),
                }
                continue;
            }

            // 2d. Converge each group's raft membership to its meta member
            //     list (learners added, voter set replaced). Idempotent:
            //     members whose raft already agrees answer immediately.
            for (g, members) in topo.groups.iter() {
                if *g == META_GROUP || members.is_empty() {
                    continue;
                }
                let req = InternalRequest::Admin(AdminRequest::ReconfigureGroup {
                    group: *g,
                    voters: members.clone(),
                    forwarded: false,
                });
                if let Err(e) = self.call(members[0], req).await {
                    debug!(group = g, err = ?e, "reconfigure deferred");
                }
            }

            // 3. Initialize shard groups I am the smallest member of and
            // that are still fresh.
            for (g, members) in topo.groups.iter() {
                if *g == META_GROUP || members.first() != Some(&self.id) {
                    continue;
                }
                if !self.groups.read().expect("groups lock").contains_key(g) && self.group_is_fresh(*g) {
                    let _ = self.initialize_group_if_fresh(*g, members.clone()).await;
                }
            }
        }
    }

    /// Meta-quorum self-healing. If the meta group's voter set lost
    /// quorum (unclean loss of 2 of 3 voters in one churn window), raft
    /// cannot re-vote and every meta write — joins, fanout, topology,
    /// transactions — freezes forever. Each node holds the full applied
    /// meta state, so the smallest live registered node can rebuild the
    /// group: claim a fresh term with itself as sole voter at the applied
    /// index (a snapshot-only single-voter group — the standard openraft
    /// shape), and let the normal learner catch-up + snapshot path bring
    /// everyone else back in.
    async fn meta_recovery_loop(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let window = switchboard_core::tempo::scale(Duration::from_secs(30));
        let mut leaderless_since: Option<tokio::time::Instant> = None;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => break,
            }
            let Some(handle) = self.groups.read().expect("groups lock").get(&META_GROUP).cloned() else {
                leaderless_since = None;
                continue;
            };
            let metrics = handle.raft.metrics();
            let is_registered = self.topology().nodes.contains_key(&self.id);
            let leaderless = metrics.borrow().current_leader.is_none();
            if !is_registered || !leaderless {
                leaderless_since = None;
                continue;
            }
            let since = *leaderless_since.get_or_insert(tokio::time::Instant::now());
            if since.elapsed() < window {
                continue;
            }
            // Determinism: only the smallest registered id heals. The
            // directory is shared replicated state, so every surviving
            // node computes the same smallest — a single initiator.
            let registered: Vec<NodeId> = {
                let mut v: Vec<NodeId> = self.topology().nodes.keys().copied().collect();
                v.sort();
                v
            };
            if registered.first() != Some(&self.id) {
                continue;
            }
            warn!(node = self.id, "meta group leaderless for {window:?}; rebuilding from applied state");
            match self.rebuild_meta_from_applied().await {
                Ok(()) => {
                    leaderless_since = None;
                    warn!(node = self.id, "meta group rebuilt; sole voter until peers rejoin");
                }
                Err(e) => {
                    error!(node = self.id, err = ?e, "meta rebuild failed; retrying");
                    // Keep the window open; retry next tick.
                }
            }
        }
    }

    /// Storage surgery for meta rebuild: read the old term (must exceed
    /// it), claim the applied index as purged-in-a-fresh-term with
    /// sole-voter membership, and drop vote/log artifacts. The applied
    /// state blob (`g0:sm`) and its applied index are kept verbatim.
    async fn rebuild_meta_from_applied(self: &Arc<Self>) -> Result<(), ClusterError> {
        let prefix = format!("g{META_GROUP}:").into_bytes();
        let mut vote_key = prefix.clone();
        vote_key.extend_from_slice(b"vote");
        let mut purged_key = prefix.clone();
        purged_key.extend_from_slice(b"last-purged");
        let mut logs_prefix = prefix.clone();
        logs_prefix.extend_from_slice(b"log/");

        // Old term, to strictly exceed.
        let old_term: u64 = self
            .kv
            .get(&vote_key)
            .map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?
            .and_then(|b| bincode::deserialize::<openraft::Vote<NodeId>>(&b).ok())
            .map(|v| v.leader_id.term)
            .unwrap_or(0);
        let new_term = old_term + 100;
        let last_applied: Option<openraft::LogId<NodeId>> = self
            .kv
            .get(&{
                let mut k = prefix.clone();
                k.extend_from_slice(b"last-applied");
                k
            })
            .map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?
            .and_then(|b| bincode::deserialize(&b).ok());
        let applied_index = last_applied.as_ref().map(|l| l.index).unwrap_or(0);
        let applied_log_id =
            openraft::LogId::new(openraft::LeaderId::new(new_term, self.id), applied_index);

        // Drop the live handle (its openraft core + IO tasks) first.
        let handle = self.groups.write().expect("groups lock").remove(&META_GROUP);
        if let Some(h) = handle {
            let _ = h.raft.shutdown().await;
        }

        // Surgery: purge logs + purged marker + old vote; keep sm,
        // last-applied. Then write the claimed state.
        self.kv.delete(&vote_key).map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;
        self.kv.delete(&purged_key).map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;
        self.kv.delete_prefix(&logs_prefix).map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;
        let new_vote = openraft::Vote::new_committed(new_term, self.id);
        self.kv
            .write_mixed(
                [
                    (vote_key.clone(), bincode::serialize(&new_vote).expect("vote serializes")),
                    (
                        purged_key.clone(),
                        bincode::serialize(&applied_log_id).expect("purged serializes"),
                    ),
                ],
                [],
            )
            .map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;
        // Membership {self} at the applied index.
        let voters = std::iter::once(self.id).collect();
        let mut members = std::collections::BTreeMap::new();
        members.insert(self.id, openraft::impls::BasicNode::default());
        let stored_membership = openraft::Membership::new(vec![voters], members);
        let stored =
            openraft::StoredMembership::new(Some(applied_log_id), stored_membership);
        let mut membership_key = prefix.clone();
        membership_key.extend_from_slice(b"membership");
        self.kv
            .write_mixed(
                [(membership_key, bincode::serialize(&stored).expect("membership serializes"))],
                [],
            )
            .map_err(|e| ClusterError::Io(std::io::Error::other(e.to_string())))?;

        // Fresh handle over the rebuilt state; it elects itself (sole
        // voter) and serves meta writes immediately.
        self.groups.write().expect("groups lock").remove(&META_GROUP);
        self.ensure_group(META_GROUP).await?;
        info!(node = self.id, applied_index, new_term, "meta group rebuilt from applied state");
        Ok(())
    }

    /// Probe every registered peer's internal port; reap the ones whose
    /// listener has been refusing for `reap_dead_after` consecutive
    /// controller ticks. Runs on the meta leader (the same node that
    /// owns directory writes). A reaped node rejoining later is safe:
    /// it re-registers through the join protocol and its raft groups
    /// catch up through the normal learner path.
    async fn reap_dead_nodes(self: &Arc<Self>, nodes: &[NodeId]) {
        let mut reaped = Vec::new();
        for id in nodes.iter().copied() {
            if id == self.id {
                continue;
            }
            let Some(addr) = self.topology().nodes.get(&id).map(|i| i.internal_addr.clone())
            else {
                continue;
            };
            let refused = match tokio::time::timeout(
                switchboard_core::tempo::scale(Duration::from_millis(800)),
                tokio::net::TcpStream::connect(&addr),
            )
            .await
            {
                // Anything that connects (or times out / unreachable)
                // fails open; only a live refusal counts.
                Ok(Ok(_)) => false,
                Ok(Err(e)) => e.kind() == std::io::ErrorKind::ConnectionRefused,
                Err(_) => false,
            };
            let mut fails = self.reap_fails.lock().expect("reap lock");
            if !refused {
                fails.remove(&id);
                continue;
            }
            let n = fails.entry(id).or_insert(0);
            *n += 1;
            if *n >= self.cfg.timeouts.reap_dead_after {
                reaped.push(id);
                fails.remove(&id);
            }
        }
        for id in reaped {
            warn!(node = id, "reaping registered node: internal port refuses connections");
            let _ = self
                .write(META_GROUP, BrokerCommand::Meta(MetaCmd::ForgetNode { node: id }))
                .await;
        }
    }

    /// Janitor: periodically sweeps every shard group this node leads —
    /// expiring per-message/queue TTLs (dead-lettering as configured) and
    /// abandoning stale prepared transactions.
    async fn janitor_loop(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick =
            tokio::time::interval(switchboard_core::tempo::scale(self.cfg.timeouts.janitor_interval));
        let mut janitor_tick = 0u64;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => break,
            }
            let groups: Vec<GroupId> = {
                self.groups.read().expect("groups lock").keys().copied().collect()
            };
            for group in groups {
                let is_leader = {
                    let handle = self.groups.read().expect("groups lock").get(&group).cloned();
                    match handle {
                        Some(h) => h.raft.current_leader().await == Some(self.id),
                        None => false,
                    }
                };
                if !is_leader || group == META_GROUP {
                    continue;
                }
                // Dead-consumer detection: a consumer whose host node is
                // gone can never ack; release its held deliveries. Meta
                // membership alone is not liveness — a SIGKILLed node
                // stays registered — so peers are probed on their
                // internal ports and failed connects count as dead. The
                // cached topology can be stale (it refreshes on this
                // node's own writes), so pull a fresh view first or the
                // sweep would drop live consumers' subs every tick.
                self.refresh_topology().await;
                // Threshold the probe: a peer is dead only after several
                // consecutive failed probes — a single transient error
                // (ephemeral-port exhaustion, full backlog) must not eat
                // its consumers. The probe is a pooled Ping round trip:
                // a blocking connect-per-peer-per-tick here churned
                // thousands of short-lived connections into TIME_WAIT
                // exhaustion under load.
                let dead_after = self.cfg.timeouts.janitor_dead_after;
                let peers: Vec<(NodeId, String)> = self
                    .topology()
                    .nodes
                    .iter()
                    .map(|(id, info)| (*id, info.internal_addr.clone()))
                    .chain(std::iter::once((
                        self.id,
                        self.cfg.internal_addr.clone(),
                    )))
                    .collect();
                let mut fails = self.peer_probe_fails.lock().expect("probe lock").clone();
                let mut live: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
                for (id, addr) in peers {
                    if id == self.id {
                        fails.remove(&id);
                        live.insert(id);
                        continue;
                    }
                    let probe = self
                        .channel
                        .rpc(
                            &addr,
                            &bincode::serialize(&Envelope {
                                from: self.id,
                                message: InternalMessage::Request(InternalRequest::Admin(
                                    AdminRequest::Ping,
                                )),
                            })
                            .expect("ping envelope serializes"),
                        )
                        .await;
                    match probe {
                        Ok(_) => {
                            fails.remove(&id);
                            live.insert(id);
                        }
                        // ECONNREFUSED is definitive: the peer's listener
                        // is gone. Any other error (timed-out ping,
                        // EADDRNOTAVAIL = local ephemeral-port exhaustion,
                        // load) says nothing about the peer — fail OPEN,
                        // or a loaded node would eat its live consumers.
                        Err(e)
                            if e.kind() == std::io::ErrorKind::ConnectionRefused =>
                        {
                            let n = fails.entry(id).or_insert(0);
                            *n += 1;
                            let dead = *n >= dead_after;
                            if dead {
                                info!(node = self.id, peer = id, addr = %addr, refusals = n, "declaring peer dead");
                            }
                            if !dead {
                                live.insert(id);
                            }
                        }
                        Err(_) => {
                            fails.remove(&id);
                            live.insert(id);
                        }
                    }
                }
                live.insert(self.id);
                *self.peer_probe_fails.lock().expect("probe lock") = fails;
                let _ = self
                    .write(
                        group,
                        BrokerCommand::Shard(switchboard_core::shard::ShardCmd::RequeueOrphaned {
                            live_nodes: live,
                        }),
                    )
                    .await;
                let _ = self
                    .write(
                        group,
                        BrokerCommand::Shard(switchboard_core::shard::ShardCmd::ExpirePrepared {}),
                    )
                    .await;
                let _ = self
                    .write(
                        group,
                        BrokerCommand::Shard(switchboard_core::shard::ShardCmd::Sweep {
                            at_ms: now_ms(),
                        }),
                    )
                    .await;
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal TCP server
    // ------------------------------------------------------------------

    async fn internal_server(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(addr = %self.cfg.internal_addr, node = self.id, "internal listener up");
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = shutdown.changed() => break,
            };
            let Ok((tcp, _peer)) = accepted else {
                continue;
            };
            let node = self.clone();
            tokio::spawn(async move {
                if let Err(e) = node.serve_conn(tcp).await {
                    debug!(err = ?e, "internal connection ended");
                }
            });
        }
    }

    async fn serve_conn(self: &Arc<Self>, tcp: tokio::net::TcpStream) -> std::io::Result<()> {
        let mut conn = crate::transport::accept(tcp, None).await?;
        loop {
            let frame = conn.read_frame_full().await?;
            let env: Envelope = match bincode::deserialize(&frame) {
                Ok(e) => e,
                Err(e) => {
                    let resp = InternalMessage::Response(InternalResponse::Error(e.to_string()));
                    conn.write_frame(&bincode::serialize(&resp).unwrap()).await?;
                    continue;
                }
            };
            let resp = self.handle_request(env.message).await;
            let out = InternalMessage::Response(resp);
            conn.write_frame(&bincode::serialize(&out).unwrap()).await?;
        }
    }

    async fn handle_request(self: &Arc<Self>, msg: InternalMessage) -> InternalResponse {
        let InternalMessage::Request(req) = msg else {
            return InternalResponse::Error("expected request".into());
        };
        match req {
            InternalRequest::Raft { group, payload } => self.handle_raft(group, payload).await,
            InternalRequest::Forward { group, command } => {
                // One local attempt only (depth 1): the forwarder owns
                // retry policy; chaining forwards could loop on stale
                // mutual leader hints.
                match self.try_write_depth(group, &command, 1).await {
                    Ok(reply) => {
                        // A fanout accepted through us just became pending
                        // on this (leading) node: start executing now.
                        if matches!(
                            &command,
                            BrokerCommand::Meta(MetaCmd::FanoutBegin { .. })
                        ) {
                            self.fanout_wake.notify_one();
                        }
                        InternalResponse::Forward(reply)
                    }
                    Err(e) => InternalResponse::ForwardFailed(ForwardError::from(&e)),
                }
            }
            InternalRequest::Admin(AdminRequest::Ping) => InternalResponse::Admin(AdminResponse::Pong),
            InternalRequest::Admin(AdminRequest::Topology) => {
                let groups = self.groups.read().expect("groups lock");
                if let Some(h) = groups.get(&META_GROUP) {
                    let meta = h.sm.read_meta();
                    InternalResponse::Admin(AdminResponse::Topology(TopologySnapshot { meta }))
                } else {
                    let meta = (*self.topology()).clone();
                    InternalResponse::Admin(AdminResponse::Topology(TopologySnapshot { meta }))
                }
            }
            InternalRequest::Admin(AdminRequest::Join { node, info, .. }) => {
                // Register the joiner through meta (replicated to everyone).
                self.dir.update(node, info.internal_addr.clone());
                let reg = BrokerCommand::Meta(MetaCmd::RegisterNode { node, info });
                match self.write(META_GROUP, reg).await {
                    Ok(_) => {
                        self.refresh_topology().await;
                        let topo = self.topology();
                        InternalResponse::Admin(AdminResponse::Joined {
                            groups: topo.groups.iter().map(|(k, v)| (*k, v.clone())).collect(),
                            directory: self.dir.all(),
                        })
                    }
                    Err(e) => InternalResponse::Error(format!("join registration failed: {e}")),
                }
            }
            InternalRequest::Admin(AdminRequest::Deliver {
                sub,
                queue,
                seq,
                message,
                redelivered,
                deleted,
            }) => {
                let sink = self.consumer_sinks.read().await.get(&sub).cloned();
                match sink {
                    Some(tx) => {
                        let _ = tx.deliveries.send(Delivery {
                            sub,
                            queue,
                            seq,
                            message,
                            redelivered,
                            deleted,
                        });
                        InternalResponse::Admin(AdminResponse::Delivered)
                    }
                    None => InternalResponse::Admin(AdminResponse::NotFound),
                }
            }
            InternalRequest::Admin(AdminRequest::CancelConsumer { sub, consumer_tag }) => {
                if let Some(sink) = self.consumer_sinks.read().await.get(&sub) {
                    let _ = sink.cancelled.send(consumer_tag);
                }
                InternalResponse::Admin(AdminResponse::Cancelled)
            }
            InternalRequest::Admin(AdminRequest::AttachConsumer { .. }) => {
                InternalResponse::Admin(AdminResponse::Attached)
            }
            InternalRequest::Admin(AdminRequest::ReconfigureGroup { group, voters, forwarded }) => {
                match self.reconfigure_group(group, voters.clone()).await {
                    Ok(()) => InternalResponse::Admin(AdminResponse::Reconfigured),
                    Err(e) => {
                        // One leader hop is allowed; past that the caller
                        // (the controller) retries on its next tick.
                        if let Some(leader) = self.leader_hint_of(group).await {
                            if !forwarded && leader != self.id {
                                let req = InternalRequest::Admin(AdminRequest::ReconfigureGroup {
                                    group,
                                    voters,
                                    forwarded: true,
                                });
                                return match self.call(leader, req).await {
                                    Ok(InternalResponse::Admin(AdminResponse::Reconfigured)) => {
                                        InternalResponse::Admin(AdminResponse::Reconfigured)
                                    }
                                    Ok(InternalResponse::Error(err)) => {
                                        InternalResponse::Error(err)
                                    }
                                    Ok(_) => InternalResponse::Error("bad reconfigure reply".into()),
                                    Err(err) => InternalResponse::Error(err.to_string()),
                                };
                            }
                        }
                        InternalResponse::Error(format!("reconfigure {group}: {e}"))
                    }
                }
            }
        }
    }

    /// The cached leader hint for `group`, if this node knows one.
    async fn leader_hint_of(&self, group: GroupId) -> Option<NodeId> {
        let handle = self.groups.read().expect("groups lock").get(&group).cloned()?;
        handle.raft.current_leader().await
    }

    /// Drive this group's raft membership to exactly `voters`: learners
    /// are added for unknown nodes, then the voter set is replaced. Must
    /// run on the group's leader to be meaningful; errors surface to the
    /// caller (the controller retries).
    async fn reconfigure_group(
        self: &Arc<Self>,
        group: GroupId,
        voters: Vec<NodeId>,
    ) -> Result<(), ClusterError> {
        self.reconfigure_group_with_learners(group, voters, Vec::new())
            .await
    }

    /// Drive a group's membership to exactly `voters`, with `learners`
    /// replicating (but never voting). Learners let a 2-node cluster's
    /// non-voter see the leader and stay warm for the 3-voter upgrade.
    #[allow(clippy::too_many_arguments)]
    async fn reconfigure_group_with_learners(
        self: &Arc<Self>,
        group: GroupId,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> Result<(), ClusterError> {
        let handle = self.groups.read().expect("groups lock").get(&group).cloned();
        let Some(handle) = handle else {
            return Err(ClusterError::Unreachable { group });
        };
        // A membership change blocks until the (joint) config commits; when
        // that can never happen — e.g. a voter whose address is unknown —
        // the openraft call would park forever. Bound it: the controller
        // retries on its next tick, so surfacing a timeout keeps liveness.
        let change_budget = switchboard_core::tempo::scale(self.cfg.timeouts.reconfigure_budget);
        let target: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        let current: std::collections::BTreeSet<NodeId> = handle
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        // Learners are added even when the voter set already matches:
        // the 2-node meta policy keeps the second node a learner, and
        // this call is what introduces it to replication.
        for l in &learners {
            if !target.contains(l) {
                let _ = tokio::time::timeout(
                    change_budget,
                    handle
                        .raft
                        .add_learner(*l, openraft::BasicNode::default(), false),
                )
                .await;
            }
        }
        if current == target {
            return Ok(());
        }
        // A follower must not attempt the membership write locally: openraft
        // would forward it through the replication path and park while the
        // leader is unreachable. Surface the hop immediately — the
        // handle_request arm relays to the hinted leader.
        if let Some(leader) = self.leader_hint_of(group).await {
            if leader != self.id {
                return Err(ClusterError::Raft {
                    group,
                    message: format!("not the leader; hint is {leader}"),
                });
            }
        }
        // A membership change blocks until the (joint) config commits; when
        // that can never happen — e.g. a voter whose address is unknown —
        // the openraft call would park forever. Bound it: the controller
        // retries on its next tick, so surfacing a timeout keeps liveness.
        let change_budget = switchboard_core::tempo::scale(self.cfg.timeouts.reconfigure_budget);
        for n in &target {
            if !current.contains(n) {
                let _ = tokio::time::timeout(
                    change_budget,
                    handle
                        .raft
                        .add_learner(*n, openraft::BasicNode::default(), false),
                )
                .await;
            }
        }
        for l in &learners {
            if !target.contains(l) {
                let _ = tokio::time::timeout(
                    change_budget,
                    handle
                        .raft
                        .add_learner(*l, openraft::BasicNode::default(), false),
                )
                .await;
            }
        }
        let change = tokio::time::timeout(
            change_budget,
            handle
                .raft
                .change_membership(openraft::ChangeMembers::ReplaceAllVoters(target), false),
        )
        .await;
        match change {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(ClusterError::Raft { group, message: e.to_string() }),
            Err(_) => Err(ClusterError::Raft {
                group,
                message: "membership change did not settle in time".into(),
            }),
        }
    }

    async fn handle_raft(self: &Arc<Self>, group: GroupId, payload: RaftPayload) -> InternalResponse {
        // ensure_group either returns the hosted handle or fails; its
        // success guarantees the map entry, so no separate hosted check
        // is needed.
        let handle = match self.ensure_group(group).await {
            Ok(h) => h,
            Err(_) => return InternalResponse::Error(format!("group {group} not available")),
        };
        match payload {
            RaftPayload::AppendEntries(req) => {
                let out = handle.raft.append_entries(*req).await;
                match out {
                    Ok(r) => InternalResponse::Raft(RaftResponse::AppendEntries(Box::new(r))),
                    Err(e) => InternalResponse::Error(e.to_string()),
                }
            },
            RaftPayload::Vote(req) => {
                let out = handle.raft.vote(*req).await;
                match out {
                    Ok(r) => InternalResponse::Raft(RaftResponse::Vote(Box::new(r))),
                    Err(e) => InternalResponse::Error(e.to_string()),
                }
            },
            RaftPayload::InstallSnapshot(req) => match handle.raft.install_snapshot(*req).await {
                Ok(r) => InternalResponse::Raft(RaftResponse::InstallSnapshot(Box::new(r))),
                Err(e) => InternalResponse::Error(e.to_string()),
            },
        }
    }
}

/// Shard-group formation: sliding windows over sorted node ids. For N ≥ 3
/// nodes there are N groups of 3; each node sits in 3 groups, each group
/// tolerates 1 voter failure. With 1-2 nodes a single group holds everyone.
pub fn sliding_windows(nodes: &[NodeId]) -> Vec<(GroupId, Vec<NodeId>)> {
    let n = nodes.len();
    if n == 0 {
        return vec![];
    }
    if n < 3 {
        return vec![(1, nodes.to_vec())];
    }
    (0..n)
        .map(|i| {
            let members: Vec<NodeId> = (0..MAX_VOTERS as u64)
                .map(|k| nodes[(i as usize + k as usize) % n])
                .collect();
            ((i + 1) as GroupId, members)
        })
        .collect()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
