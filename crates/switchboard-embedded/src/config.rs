//! Typed-builder configuration. No env vars, no files, no config
//! providers — every knob is a builder method taking owned typed values
//! (embedded design §1 goal 4).

use std::path::PathBuf;

/// Per-connection protocol limits for embedded connections. Defaults mirror
/// the server's negotiated defaults (`ConnectionLimits`, channel/mod.rs:55-65)
/// with heartbeat 0: embedded connections run heartbeat-free, which
/// disables the server's silence deadline entirely (session/mod.rs:112-116,
/// 369-375). Dead embedded connections are reaped only by
/// [`crate::Broker::stop`] (the connection registry) or an explicit
/// [`crate::EmbeddedConnection::close`] (embedded design §4.2, §9/R6).
///
/// Setting `heartbeat` > 0 enables the §4.2.7 machinery on both ends: the
/// client writer sends idle heartbeats like the server's writer task does.
#[derive(Debug, Clone)]
pub struct EmbeddedLimits {
    /// Largest channel number the connection may open (0 = no limit).
    pub channel_max: u16,
    /// Largest frame in octets (0 = no limit). Content bodies are split to
    /// fit; the same value is negotiated with the server (§2.3.3: the
    /// client may only lower).
    pub frame_max: u32,
    /// Heartbeat interval in seconds; 0 disables heartbeats (the default).
    pub heartbeat: u16,
}

impl Default for EmbeddedLimits {
    fn default() -> Self {
        EmbeddedLimits {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 0,
        }
    }
}

impl EmbeddedLimits {
    /// The defaults: channel-max 2047, frame-max 128 KiB, heartbeat off.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the largest channel number this connection may open.
    pub fn channel_max(mut self, v: u16) -> Self {
        self.channel_max = v;
        self
    }

    /// Set the largest frame size in octets (content bodies are split).
    pub fn frame_max(mut self, v: u32) -> Self {
        self.frame_max = v;
        self
    }

    /// Set a heartbeat interval in seconds. The default (0) runs the
    /// connection heartbeat-free.
    pub fn heartbeat(mut self, v: u16) -> Self {
        self.heartbeat = v;
        self
    }
}

/// Join an existing cluster instead of forming one (embedded design §5.1).
///
/// Join mode is **not socket-less**: raft peers dial the joining node back
/// on `internal_listen` (the internal server, node/mod.rs:350-357), so the
/// address MUST be genuinely reachable from the peers. In-process *client*
/// traffic still never touches a socket; only the node's cluster membership
/// is networked.
#[derive(Debug, Clone)]
pub struct JoinSpec {
    /// Internal addresses of existing nodes to join through. If all seeds
    /// stay unreachable for the join budget, `Broker::start` still returns
    /// `Ok` — the node stays pending with a background retry task, and
    /// `wait_ready` is how the embedder discovers the failure (§5.1).
    pub seeds: Vec<String>,
    /// Internal listen address for this node; peers dial back on it for
    /// raft/forwarding traffic.
    pub internal_listen: std::net::SocketAddr,
}

impl JoinSpec {
    /// Join through `seeds`, listening for peer traffic on `internal_listen`.
    pub fn new(seeds: Vec<String>, internal_listen: std::net::SocketAddr) -> Self {
        JoinSpec {
            seeds,
            internal_listen,
        }
    }
}

/// Configuration for one in-process [`crate::Broker`].
///
/// Typed builders only; fields stay private forever, and every builder
/// method takes `self` and returns `Self`, so adding fields later is
/// non-breaking (embedded design §3 stability rules).
///
/// ```
/// use switchboard_embedded::BrokerConfig;
///
/// let cfg = BrokerConfig::new(std::env::temp_dir().join("sb-embedded-config-doc"))
///     .node_id(1)
///     .expected_nodes(1);
/// ```
pub struct BrokerConfig {
    pub(crate) data_dir: PathBuf,
    pub(crate) node_id: u64,
    pub(crate) expected_nodes: u64,
    /// `None` (the default) means "do nothing": the node runtime's own
    /// bootstrap already creates `guest`/`guest`. `Some((user, password))`
    /// applies the ensure-user-if-absent write after readiness — with the
    /// literal name `"guest"` refused by [`crate::Broker::start`].
    pub(crate) bootstrap_credentials: Option<(String, String)>,
    pub(crate) timeouts: switchboard_cluster::node::Timeouts,
    pub(crate) limits: EmbeddedLimits,
    pub(crate) join: Option<JoinSpec>,
    pub(crate) amqp_listener: Option<std::net::SocketAddr>,
}

impl BrokerConfig {
    /// A config forming a fresh single-node cluster at `data_dir`
    /// (RocksDB location; node/mod.rs:312). Defaults: node id 1,
    /// expected-nodes 1, guest/guest bootstrap (a no-op), default
    /// timeouts/limits, no join, no TCP listener.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        BrokerConfig {
            data_dir: data_dir.into(),
            node_id: 1,
            expected_nodes: 1,
            bootstrap_credentials: None,
            timeouts: switchboard_cluster::node::Timeouts::default(),
            limits: EmbeddedLimits::default(),
            join: None,
            amqp_listener: None,
        }
    }

    /// Node id within the cluster (default 1). Keep ids dense and unique
    /// per cluster; id 1 with `expected_nodes(1)` is the single-node
    /// default.
    pub fn node_id(mut self, id: u64) -> Self {
        self.node_id = id;
        self
    }

    /// Total nodes expected in the cluster at formation time (default 1).
    pub fn expected_nodes(mut self, n: u64) -> Self {
        self.expected_nodes = n;
        self
    }

    /// Ensure this user exists with this password, applied AFTER readiness
    /// (embedded design §5.1). If `node.topology().users` (a pub field,
    /// topology/mod.rs:54) already contains the name, NOTHING is written —
    /// rotated passwords are preserved. Otherwise the broker issues
    /// `BrokerCommand::Bootstrap { vhost: "/", user, password }` itself on
    /// the replicated meta group: `DeclareVhost "/"` is an idempotent
    /// upsert on the always-present default vhost (topology/mod.rs:210-213)
    /// and `CreateUser` adds the user without touching any existing one
    /// (store/src/typ/mod.rs:157-166; topology/mod.rs:240-246).
    ///
    /// "Ensure-exists" semantics: a user deleted at runtime is re-added at
    /// the next `start` with the configured password — an embedder relying
    /// on deletion must also drop the config entry.
    ///
    /// The literal name `"guest"` is rejected by [`crate::Broker::start`]
    /// with [`StartError::InvalidConfig`](crate::StartError::InvalidConfig)
    /// (the builder itself stays infallible): bootstrap always creates
    /// guest (node/mod.rs:617-625), so the absent-user gate could never
    /// fire for it. To set or rotate guest's password explicitly, call
    /// [`crate::Broker::create_user`] after `start` (an upsert).
    ///
    /// The default (unset) is a no-op: the node's own bootstrap already
    /// created guest/guest.
    ///
    /// ```
    /// use switchboard_embedded::{Broker, BrokerConfig};
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = std::env::temp_dir().join(format!("sb-bootstrap-doc-{}", std::process::id()));
    /// let broker = Broker::start(BrokerConfig::new(dir).bootstrap_credentials("app", "pw")).await?;
    /// // "app"/"pw" now authenticates with no admin call; guest is untouched.
    /// let _conn = broker.connect("app", "pw").await?;
    /// let _guest = broker.connect("guest", "guest").await?;
    /// broker.stop(std::time::Duration::from_secs(5)).await?;
    /// # Ok(())
    /// # }
    /// # #[tokio::main]
    /// # async fn main() { demo().await.unwrap(); }
    /// ```
    pub fn bootstrap_credentials(mut self, user: &str, password: &str) -> Self {
        self.bootstrap_credentials = Some((user.to_string(), password.to_string()));
        self
    }

    /// All broker timeouts, typed and `Copy`. Default:
    /// `switchboard_cluster::node::Timeouts::default()` — reachable only
    /// via the pub `node` module; the cluster crate root does not
    /// re-export `Timeouts` (cluster/src/lib.rs:25).
    pub fn timeouts(mut self, t: switchboard_cluster::node::Timeouts) -> Self {
        self.timeouts = t;
        self
    }

    /// Per-connection limits (channel_max / frame_max / heartbeat) handed
    /// to every embedded connection. Default: the negotiated server
    /// defaults with heartbeat 0 (see [`EmbeddedLimits`]).
    pub fn limits(mut self, l: EmbeddedLimits) -> Self {
        self.limits = l;
        self
    }

    /// Join an existing cluster instead of forming one. Unset (the
    /// default) forms a new single-node cluster whose internal listener
    /// binds an ephemeral loopback port that no peer ever dials.
    pub fn join(mut self, join: JoinSpec) -> Self {
        self.join = Some(join);
        self
    }

    /// Optionally ALSO serve ordinary TCP clients on the AMQP gateway
    /// (`listener::run`, listener/mod.rs:19). Default: none — purely
    /// in-process. The listener runs until [`crate::Broker::stop`] aborts
    /// it.
    pub fn amqp_listener(mut self, addr: std::net::SocketAddr) -> Self {
        self.amqp_listener = Some(addr);
        self
    }

    /// The configured data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
}
