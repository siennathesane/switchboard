//! Broker lifecycle: start, connect, readiness, graceful stop, typed
//! admin (embedded design §5, §6).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use switchboard_cluster::BrokerCommand;
use switchboard_cluster::BrokerReply;
use switchboard_cluster::ClusterNode;
use switchboard_cluster::META_GROUP;
use switchboard_cluster::NodeConfig;
use switchboard_core::topology::MetaCmd;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::BrokerConfig;
use crate::config::EmbeddedLimits;
use crate::config::JoinSpec;
use crate::conn::EmbeddedConnection;
use crate::driver::ConnShared;
use crate::driver::WriteMsg;
use crate::driver::driver_task;
use crate::driver::handshake;
use crate::driver::writer_task;
use crate::error::AdminError;
use crate::error::ConnectError;
use crate::error::StartError;
use crate::error::StopError;
use crate::types::ReturnedMessage;

/// One tracked connection in the broker registry (embedded design §4.2
/// step 2, §5.2 step 1): the close trigger and the server task whose
/// completion proves the session (including its teardown) is gone.
pub(crate) struct TrackedConn {
    pub(crate) close: tokio::sync::watch::Sender<bool>,
    pub(crate) join: JoinHandle<std::io::Result<()>>,
}

/// One in-process broker.
///
/// Created with [`Broker::start`]; stopped with [`Broker::stop`]. The
/// restart contract is "new process, same `data_dir`": in-process
/// stop-then-start is NOT supported in 1.0 because the node runtime keeps
/// the RocksDB handle (and its exclusive LOCK) alive for the whole process
/// — a second `Broker::start` on the same `data_dir` fails with
/// [`StartError::Storage`] (embedded design §5.3).
pub struct Broker {
    node: Arc<ClusterNode>,
    data_dir: std::path::PathBuf,
    registry: Arc<Mutex<HashMap<u64, TrackedConn>>>,
    next_conn_key: AtomicU64,
    listener: Mutex<Option<JoinHandle<std::io::Result<()>>>>,
    limits: EmbeddedLimits,
    stopped: AtomicBool,
}

impl Broker {
    /// Create and start a broker in-process.
    ///
    /// * Single-node default (no [`join`](BrokerConfig::join)): node id 1,
    ///   expected-nodes 1, bootstrap; the internal listener binds an
    ///   ephemeral loopback port that no peer ever dials — the embedded
    ///   path is socket-less from the client's perspective (§5.1).
    /// * Join mode: `JoinSpec::seeds` drive the join, and
    ///   `JoinSpec::internal_listen` becomes the internal address because
    ///   peers dial the joining node back (§5.1, §9/R3). Join failures are
    ///   NOT start failures — a pending node surfaces through
    ///   [`wait_ready`](Self::wait_ready) timing out.
    /// * Ends with a bounded readiness wait and — when
    ///   [`bootstrap_credentials`](BrokerConfig::bootstrap_credentials) is
    ///   configured — the ensure-user-if-absent write on the replicated
    ///   user table (§5.1).
    ///
    /// # Errors
    /// * [`StartError::InvalidConfig`] — `bootstrap_credentials("guest", …)`;
    ///   rotate guest's password explicitly with [`create_user`](Self::create_user)
    ///   after `start` instead.
    /// * [`StartError::Storage`] — the RocksDB LOCK on `data_dir` is held
    ///   (a live broker already owns it, same or different process). The
    ///   failed start leaves the store locked for the rest of THIS process;
    ///   restart in a new process instead (§5.3).
    /// * [`StartError::Formation`] — cluster formation, the readiness
    ///   wait, or the bootstrap write failed. Same lock caveat applies.
    /// * [`StartError::Io`] — binding the internal (or optional AMQP)
    ///   listener failed.
    ///
    /// # Panics
    /// Polling this future outside a tokio runtime context panics at
    /// first poll (the internal `tokio::spawn` executes there — §3.1).
    pub async fn start(config: BrokerConfig) -> Result<Broker, StartError> {
        let join: Option<JoinSpec> = config.join.clone();
        if let Some((user, _)) = &config.bootstrap_credentials {
            if user == "guest" {
                // form_cluster_tail always applies guest/guest before
                // readiness can pass (node/mod.rs:604-629), so the
                // absent-user gate could never fire for "guest" — and
                // issuing the write would upsert-overwrite its rotated
                // password. Refuse; Broker::create_user is the explicit
                // set/rotate path (embedded design §5.1).
                return Err(StartError::InvalidConfig(
                    "bootstrap_credentials(\"guest\", …) is refused: guest always exists after \
                     bootstrap, so the ensure-user gate could never fire; use \
                     Broker::create_user(\"guest\", …) after start to set or rotate its password",
                ));
            }
        }
        let (internal_addr, seeds, bootstrap) = match &join {
            Some(spec) => (spec.internal_listen.to_string(), spec.seeds.clone(), false),
            None => ("127.0.0.1:0".to_string(), Vec::new(), true),
        };
        // client_addr is a required field on NodeConfig but is only ever
        // ADVERTISED (directory + mDNS TXT), never dialed (§5.1).
        let client_addr = config
            .amqp_listener
            .map(|a| a.to_string())
            .unwrap_or_else(|| "127.0.0.1:0".to_string());
        let cfg = NodeConfig {
            id: config.node_id,
            data_dir: config.data_dir.clone(),
            client_addr,
            internal_addr,
            seeds,
            bootstrap,
            expected_nodes: config.expected_nodes,
            peers: vec![],
            // Formation-level bootstrap stays on the public dev default:
            // the embedded path configures credentials through
            // `BrokerConfig::bootstrap_credentials` (ensure-user-if-absent,
            // post-readiness), which layers on top without clobbering.
            bootstrap_user: switchboard_wire::constants::DEFAULT_USER.into(),
            bootstrap_password: switchboard_wire::constants::DEFAULT_PASSWORD.into(),
            // In-process embedded brokers have no external internal-port
            // exposure; the shared-secret gate stays off.
            internal_auth_secret: None,
            timeouts: config.timeouts,
        };
        let node = ClusterNode::start(cfg).await.map_err(|e| match e {
            switchboard_cluster::ClusterError::Io(io) => StartError::Io(io),
            switchboard_cluster::ClusterError::Storage(store) => {
                StartError::Storage(store.to_string())
            }
            other => StartError::Formation(other.to_string()),
        })?;

        // Readiness: the exact predicate the e2e helper uses
        // (tests/support/mod.rs:413-419) — bootstrap entities (vhosts) and
        // the shard layout visible in the local topology.
        if let Err(e) = wait_until_ready(&node, START_READINESS_BUDGET).await {
            node.shutdown().await;
            return Err(e);
        }

        // Bootstrap credentials (ensure-user-if-absent, strictly
        // post-readiness; embedded design §5.1): refresh the local view
        // once first — the same anti-staleness pattern the server's meta
        // path uses (methods/mod.rs:115-123) — then write only if the
        // user is absent. If the user already exists, NOTHING is written
        // and rotated passwords are preserved.
        if let Some((user, password)) = &config.bootstrap_credentials {
            node.refresh_topology().await;
            if !node.topology().users.contains_key(user.as_str()) {
                let reply = node
                    .write(
                        META_GROUP,
                        BrokerCommand::Bootstrap {
                            vhost: switchboard_wire::constants::DEFAULT_VHOST.into(),
                            user: user.clone(),
                            password: password.clone(),
                        },
                    )
                    .await;
                match reply {
                    Ok(BrokerReply::Bootstrapped) | Ok(BrokerReply::Meta(_)) => {}
                    Ok(other) => {
                        node.shutdown().await;
                        return Err(StartError::Formation(format!(
                            "bootstrap write replied unexpectedly: {other:?}"
                        )));
                    }
                    Err(e) => {
                        node.shutdown().await;
                        return Err(StartError::Formation(format!(
                            "bootstrap write failed: {e}"
                        )));
                    }
                }
            }
        }

        // Optional TCP listener for ordinary clients (default: none).
        let listener = match config.amqp_listener {
            Some(addr) => {
                let node2 = node.clone();
                let limits = default_tcp_limits();
                let run = async move {
                    switchboard_server::listener::run(
                        node2,
                        &addr.to_string(),
                        limits,
                        None,
                        Default::default(),
                    )
                    .await
                };
                let handle = tokio::spawn(run);
                // Surface an immediate bind failure as StartError::Io.
                if handle.is_finished() {
                    match handle.await {
                        Ok(Err(e)) => {
                            node.shutdown().await;
                            return Err(StartError::Io(e));
                        }
                        _ => unreachable!("listener task cannot finish successfully"),
                    }
                }
                Some(handle)
            }
            None => None,
        };

        Ok(Broker {
            node,
            data_dir: config.data_dir,
            registry: Arc::new(Mutex::new(HashMap::new())),
            next_conn_key: AtomicU64::new(1),
            listener: Mutex::new(listener),
            limits: config.limits,
            stopped: AtomicBool::new(false),
        })
    }

    /// Open an embedded connection for vhost `"/"`. Authentication runs
    /// through the real handshake (PLAIN against the replicated user
    /// table), with the loopback guest rule intact (§6).
    ///
    /// Every connection is registered with the broker so
    /// [`stop`](Self::stop) can close it — there is no untracked way to
    /// obtain one (§5.2).
    pub async fn connect(
        &self,
        user: &str,
        password: &str,
    ) -> Result<EmbeddedConnection, ConnectError> {
        self.connect_vhost(user, password, "/").await
    }

    /// [`connect`](Self::connect) for a specific vhost.
    pub async fn connect_vhost(
        &self,
        user: &str,
        password: &str,
        vhost: &str,
    ) -> Result<EmbeddedConnection, ConnectError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(ConnectError::Closed);
        }
        // The in-memory duplex IS the transport: the server side runs the
        // unmodified generic `serve_rw` (session/mod.rs:62-71) that a TCP
        // socket would feed (§4.2).
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let node = self.node.clone();
        let limits = switchboard_server::ConnectionLimits {
            channel_max: self.limits.channel_max,
            frame_max: self.limits.frame_max,
            heartbeat: self.limits.heartbeat,
            // Server default cap (ConnectionLimits::default); embedded
            // configs carry no separate message-size knob.
            max_message_size: 128 * 1024 * 1024,
            peer_ip: Some(std::net::IpAddr::from([127, 0, 0, 1])),
            allow_remote_guest: false,
        };
        let serve_task = tokio::spawn(switchboard_server::session::serve_rw(
            server_read,
            server_write,
            node,
            limits,
        ));

        let (client_read, client_write) = tokio::io::split(client_io);
        let (agreed, client_read, client_write) = handshake(
            client_read,
            client_write,
            user,
            password,
            vhost,
            &self.limits,
        )
        .await?;
        // A failed handshake drops the client halves: `serve_rw` returns
        // on its own (pre-Open failures close the socket, §4.3), so no
        // registry entry is needed.

        // Writer task owns the client write half; the driver owns the
        // read half and demultiplexes inbound frames (§4.3).
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<WriteMsg>();
        tokio::spawn(writer_task(client_write, writer_rx, agreed.heartbeat));
        let (returns_tx, returns_rx) = mpsc::unbounded_channel::<ReturnedMessage>();
        let (close_tx, close_rx) = tokio::sync::watch::channel(false);
        let conn = Arc::new(ConnShared {
            writer: writer_tx,
            valid: AtomicBool::new(true),
            closing: AtomicBool::new(false),
            close_pair: Mutex::new((0, String::new())),
            channels: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
            pending_deliveries: Mutex::new(HashMap::new()),
            returns_tx: Mutex::new(Some(returns_tx)),
            mandatory_returns: Mutex::new(HashMap::new()),
            close_waiters: Mutex::new(Vec::new()),
            next_channel: AtomicU16::new(1),
            channel_max: agreed.channel_max,
            frame_max: agreed.frame_max,
            pending: Mutex::new(HashMap::new()),
        });
        tokio::spawn(driver_task(client_read, conn.clone(), close_rx));

        // Register with the broker: `stop` closes the connection through
        // the watch trigger and joins the SERVER task, whose completion
        // proves the session teardown finished (§4.2 step 2, §5.2 step 1).
        let key = self.next_conn_key.fetch_add(1, Ordering::SeqCst);
        self.registry.lock().unwrap().insert(
            key,
            TrackedConn {
                close: close_tx,
                join: serve_task,
            },
        );
        Ok(EmbeddedConnection {
            conn,
            registry: self.registry.clone(),
            key,
            returns_rx: Mutex::new(Some(returns_rx)),
        })
    }

    /// True once the bootstrap entities are visible in the local topology
    /// (same readiness test as tests/support/mod.rs:413-419).
    pub fn is_ready(&self) -> bool {
        let topo = self.node.topology();
        !topo.vhosts.is_empty() && !topo.groups.is_empty()
    }

    /// Wait until [`is_ready`](Self::is_ready), for at most `budget`.
    /// Readiness does NOT include the ~700 ms settle beat the e2e helper
    /// adds for shard-group raft handles (tests/support/mod.rs:420-422):
    /// first publishes right after this are absorbed by the write retry
    /// budget (node/mod.rs:1404-1417) at the cost of latency, never
    /// correctness. Embedders that care about first-publish latency may
    /// wait one `timeouts.reconcile_interval` (§5.1).
    pub async fn wait_ready(&self, budget: Duration) -> Result<(), StartError> {
        let deadline = Instant::now() + budget;
        while !self.is_ready() {
            if Instant::now() >= deadline {
                return Err(StartError::Formation(format!(
                    "broker did not become ready within {budget:?} \
                     (formation may still be pending — e.g. unreachable join seeds)"
                )));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }

    /// Graceful stop (embedded design §5.2):
    ///
    /// 1. close every tracked connection (the registry is complete —
    ///    connections exist only through `connect`), joining the server
    ///    tasks within `grace` so all teardown writes land BEFORE the
    ///    rafts stop;
    /// 2. run the existing bounded `ClusterNode::leave` (ForgetNode
    ///    announcement + raft shutdown);
    /// 3. on leave timeout/error call `node.shutdown()` EXPLICITLY — the
    ///    last statement of `leave` would otherwise never run when the
    ///    future is dropped at the deadline (§5.2 step 3, load-bearing) —
    ///    and return [`StopError::TimedOut`] / [`StopError::LeaveFailed`];
    /// 4. abort the optional TCP listener.
    pub async fn stop(self, grace: Duration) -> Result<(), StopError> {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // 1. Close every tracked connection.
        let entries: Vec<TrackedConn> = {
            let mut reg = self.registry.lock().unwrap();
            reg.drain().map(|(_, v)| v).collect()
        };
        for e in &entries {
            let _ = e.close.send(true);
        }
        let deadline = tokio::time::Instant::now() + grace;
        for e in entries {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let _ = tokio::time::timeout(remaining, e.join).await;
        }
        // 2. Bounded leave.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let leave =
            tokio::time::timeout(remaining.max(Duration::from_millis(1)), self.node.leave()).await;
        // 3. Explicit shutdown fallback (load-bearing: `shutdown()` is the
        // LAST statement of `leave`; a dropped leave future never runs it).
        match leave {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.node.shutdown().await;
                if let Some(h) = self.listener.lock().unwrap().take() {
                    h.abort();
                }
                return Err(StopError::LeaveFailed(e.to_string()));
            }
            Err(_elapsed) => {
                self.node.shutdown().await;
                if let Some(h) = self.listener.lock().unwrap().take() {
                    h.abort();
                }
                return Err(StopError::TimedOut);
            }
        }
        // 4. Abort the optional TCP listener.
        if let Some(h) = self.listener.lock().unwrap().take() {
            h.abort();
        }
        Ok(())
    }

    /// The configured data directory (RocksDB location). Restarting means
    /// starting a NEW process whose `Broker::start` uses this same
    /// directory (§5.3).
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    // ------------------------------------------------------------------
    // typed admin (§6): replicated MetaCmd writes, the same path the
    // protocol gateways use — no new SM commands.
    // ------------------------------------------------------------------

    async fn admin_write(&self, cmd: BrokerCommand) -> Result<(), AdminError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(AdminError::Closed);
        }
        match self.node.write(META_GROUP, cmd).await {
            // The meta SM folds rejections into BrokerReply::Error.
            Ok(BrokerReply::Error(e)) => Err(AdminError::Refused(e.text.clone())),
            Ok(_) => {
                // Refresh so the acting caller immediately observes its own
                // change (the visibility guarantee the server's meta path
                // gives, methods/mod.rs:115-123).
                self.node.refresh_topology().await;
                Ok(())
            }
            Err(e) => Err(AdminError::Raft(e.to_string())),
        }
    }

    /// Declare a vhost (idempotent upsert of the mandatory bootstrap
    /// exchanges).
    pub async fn declare_vhost(&self, name: &str) -> Result<(), AdminError> {
        self.admin_write(BrokerCommand::Meta(MetaCmd::DeclareVhost {
            name: name.to_string(),
        }))
        .await
    }

    /// Create a user or REPLACE the existing user's password (upsert —
    /// `CreateUser` writes unconditionally, topology/mod.rs:240-246).
    /// This is the explicit set/rotate path for any user, `guest`
    /// included.
    pub async fn create_user(&self, user: &str, password: &str) -> Result<(), AdminError> {
        self.admin_write(BrokerCommand::Meta(MetaCmd::CreateUser {
            name: user.to_string(),
            password: password.to_string(),
        }))
        .await
    }

    /// Delete a user and their permissions. Idempotent: deleting a
    /// missing user replies Ok (`DeleteUser` removes unconditionally,
    /// topology/mod.rs:235-239). Note the ensure-exists semantics of
    /// [`bootstrap_credentials`](BrokerConfig::bootstrap_credentials): a
    /// user deleted at runtime is re-added at the next start if still
    /// configured there (§5.1).
    pub async fn delete_user(&self, user: &str) -> Result<(), AdminError> {
        self.admin_write(BrokerCommand::Meta(MetaCmd::DeleteUser {
            name: user.to_string(),
        }))
        .await
    }

    /// Set one user's permissions on one vhost (RabbitMQ-shaped regexes;
    /// all-empty patterns clear the entry). Fails with
    /// [`AdminError::Refused`] for a user that does not exist.
    pub async fn set_permissions(
        &self,
        user: &str,
        vhost: &str,
        p: switchboard_core::topology::Permissions,
    ) -> Result<(), AdminError> {
        self.admin_write(BrokerCommand::Meta(MetaCmd::SetPermissions {
            user: user.to_string(),
            vhost: vhost.to_string(),
            permissions: p,
        }))
        .await
    }
}

fn default_tcp_limits() -> switchboard_server::ConnectionLimits {
    switchboard_server::ConnectionLimits::default()
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("data_dir", &self.data_dir)
            .field("stopped", &self.stopped.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Async readiness wait (the e2e helper's loop, minus the settle beat).
async fn wait_until_ready(node: &Arc<ClusterNode>, budget: Duration) -> Result<(), StartError> {
    let deadline = Instant::now() + budget;
    loop {
        let topo = node.topology();
        if !topo.vhosts.is_empty() && !topo.groups.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(StartError::Formation(format!(
                "broker did not become ready within {budget:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Budget used internally by [`Broker::start`] before it applies bootstrap
/// credentials. Generous: formation under the default timeouts concludes
/// in milliseconds; 30 s absorbs loaded CI runners.
const START_READINESS_BUDGET: Duration = Duration::from_secs(30);
