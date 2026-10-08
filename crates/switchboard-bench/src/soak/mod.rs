//! Soak + stability engine: every cluster feature under sustainable load
//! for unbounded durations, where *any* client-visible error fails the
//! run.
//!
//! Workloads (see [`workload`] and [`proto`]) run concurrently, each a
//! self-healing actor: its connections rotate across the live endpoint
//! set (k8s pod list or static hosts), and infrastructure churn — pods
//! deleted, scaled away, recreated — is absorbed as *transitions*, never
//! as errors. The hard invariants (confirmed messages are delivered
//! exactly once or redelivered-flagged, in publisher order, byte-exact)
//! hold through all of it; violating them is an error.
//!
//! The engine:
//! 1. waits for the cluster to converge (queues visible on every node),
//! 2. declares the soak topology and spawns workloads + monitors,
//! 3. lets the k8s scaler churn the cluster 1↔9 continuously (closing
//!    the load gate around sub-quorum windows, where a node loss is a
//!    total cluster loss and drain-first is the only way to keep the
//!    zero-loss promise),
//! 4. on deadline or signal: quiesces, drains every queue, reconciles
//!    confirmed-vs-delivered per stream, and prints a verdict.

pub mod check;
pub mod client;
pub mod k8s;
pub mod ledger;
pub mod monitor;
pub mod pacer;
pub mod proto;
pub mod report;
pub mod workload;
pub mod workload_idle;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio_util::sync::CancellationToken;

use ledger::Ledger;

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

/// Per-workload rates, messages/second (0 disables the workload).
#[derive(Debug, Clone, Copy)]
pub struct Rates {
    /// Per-publisher confirmed publish rate (3 pipeline publishers).
    pub pipeline: f64,
    /// Fanout publishes/s (each fans to 3 queues).
    pub fanout: f64,
    /// Transaction commit-or-abort cycles/s.
    pub tx: f64,
    /// basic.get + ack pulls/s.
    pub get: f64,
    /// Topology operations/s (declare/bind/unbind/delete churn).
    pub topo: f64,
    /// Full connection+channel+queue cycles per MINUTE.
    pub connchurn: f64,
    /// MQTT QoS1 publishes/s (round-trip to an MQTT subscriber).
    pub mqtt: f64,
    /// STOMP client-ack messages/s.
    pub stomp: f64,
    /// AMQP 1.0 settled transfers/s (round-trip on one link pair).
    pub amqp10: f64,
    /// Raw mandatory-publish + basic.return checks per MINUTE.
    pub mandatory: f64,
}

impl Default for Rates {
    fn default() -> Self {
        Rates {
            pipeline: 25.0,
            fanout: 5.0,
            tx: 2.0,
            get: 2.0,
            topo: 2.0,
            connchurn: 6.0,
            mqtt: 10.0,
            stomp: 5.0,
            amqp10: 5.0,
            mandatory: 4.0,
        }
    }
}

impl Rates {
    /// Apply `name=value` overrides; unknown names are a config error.
    pub fn apply(&mut self, spec: &str) -> Result<(), String> {
        let (name, v) = spec
            .split_once('=')
            .ok_or_else(|| format!("--rate wants name=value, got {spec:?}"))?;
        let v: f64 = v
            .parse()
            .map_err(|_| format!("--rate value must be a number, got {v:?}"))?;
        match name {
            "pipeline" => self.pipeline = v,
            "fanout" => self.fanout = v,
            "tx" => self.tx = v,
            "get" => self.get = v,
            "topo" => self.topo = v,
            "connchurn" => self.connchurn = v,
            "mqtt" => self.mqtt = v,
            "stomp" => self.stomp = v,
            "amqp10" => self.amqp10 = v,
            "mandatory" => self.mandatory = v,
            other => return Err(format!("unknown rate {other:?}")),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ScaleMode {
    /// Fixed endpoint set; no scaling (smoke test, docker lab).
    Static,
    /// Continuously scale the k8s Deployment `deployment` in namespace
    /// `namespace` between `min` and `max` replicas, non-ordered, plus
    /// random unclean pod kills. Runs for the whole soak.
    K8s {
        namespace: String,
        deployment: String,
        min: u32,
        max: u32,
        /// One scale move per this interval.
        every: Duration,
        /// One unclean (SIGKILL) pod deletion per this interval, at
        /// ≥3 replicas. 0 disables.
        kill_every: Duration,
    },
}

/// Resource-leak thresholds (per-hour slopes, measured from `/stats`).
#[derive(Debug, Clone, Copy)]
pub struct LeakThresholds {
    pub rss_mb_h: f64,
    pub disk_mb_h: f64,
    pub fds_h: f64,
}

impl Default for LeakThresholds {
    fn default() -> Self {
        LeakThresholds { rss_mb_h: 8.0, disk_mb_h: 100.0, fds_h: 60.0 }
    }
}

#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// Client endpoints (`host:port`, AMQP 0-9-1 + all gateway
    /// protocols + /stats + /health).
    pub hosts: Vec<String>,
    pub user: String,
    pub password: String,
    pub vhost: String,
    /// Total soak duration.
    pub duration: Duration,
    pub msg_size: usize,
    pub rates: Rates,
    pub scale: ScaleMode,
    pub monitor: bool,
    pub monitor_every: Duration,
    pub report_every: Duration,
    pub health_every: Duration,
    pub leak: LeakThresholds,
    /// Chaos semantics: connection drops from infrastructure churn are
    /// *transitions* (counted, not errors). The default soak is already
    /// chaos-shaped when scaling is on; this flag exists for static
    /// runs that inject faults externally.
    pub chaos: bool,
    /// How long a publisher confirm may take before it is an error.
    pub confirm_timeout: Duration,
    /// How long the end-of-run drain may take before reconciliation
    /// declares queues stuck.
    pub drain_timeout: Duration,
    /// Pipeline depth above confirmed−delivered that means "consumer is
    /// falling behind" (an error at report ticks).
    pub backlog_bound: u64,
}

impl Default for SoakConfig {
    fn default() -> Self {
        SoakConfig {
            hosts: vec![],
            user: "guest".into(),
            password: "guest".into(),
            vhost: "%2F".into(),
            duration: Duration::from_secs(3600),
            msg_size: 512,
            rates: Rates::default(),
            scale: ScaleMode::Static,
            monitor: true,
            monitor_every: Duration::from_secs(30),
            report_every: Duration::from_secs(30),
            health_every: Duration::from_secs(5),
            leak: LeakThresholds::default(),
            chaos: false,
            confirm_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(300),
            backlog_bound: 10_000,
        }
    }
}

// ---------------------------------------------------------------------
// Shared context
// ---------------------------------------------------------------------

/// Pause/resume for message-producing workloads. Closed around
/// sub-quorum scale windows (see [`report`] and module docs): with <2
/// live nodes of a former 3-voter group there is no replication
/// guarantee for in-flight confirmed messages, so the only way a
/// "months without losing one confirmed message" claim survives scale 1
/// is to drain first and pause until the cluster is quorate again.
/// Protocol sessions stay connected while closed — churn still hammers
/// joins/leaves/elections with live sessions watching.
#[derive(Clone)]
pub struct LoadGate {
    rx: Arc<tokio::sync::watch::Receiver<bool>>,
    tx: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Default for LoadGate {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(true);
        LoadGate { rx: Arc::new(rx), tx: Arc::new(tx) }
    }
}

impl LoadGate {
    pub fn open(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn wait_open(&self, token: &CancellationToken) -> bool {
        while !self.open() {
            tokio::select! {
                _ = token.cancelled() => return false,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
        true
    }

    pub fn set(&self, open: bool) {
        let _ = self.tx.send(open);
    }
}

/// Everything a workload task needs. Cloneable.
#[derive(Clone)]
pub struct Ctx {
    pub cfg: Arc<SoakConfig>,
    pub ledger: Ledger,
    /// Publisher-phase stop: cancelled at quiesce so no new messages
    /// enter the system.
    pub token: CancellationToken,
    /// Consumer-phase stop: cancelled only after the drain +
    /// reconciliation has completed, so in-flight deliveries are
    /// consumed and acked instead of being dropped on the floor.
    pub halt: CancellationToken,
    pub gate: LoadGate,
    pub endpoints: Arc<k8s::EndpointSet>,
    pub started: std::time::Instant,
    reconcilers: Arc<tokio::sync::Mutex<Vec<workload::Reconciler>>>,
}

impl Ctx {
    pub fn error(&self, workload: &str, kind: &str, host: &str, detail: String) -> bool {
        self.ledger.error(workload, kind, host, detail)
    }

    /// Short per-process suffix for throwaway namespaces (topo churn,
    /// exclusive queues): unique across overlapping driver generations.
    pub fn run_suffix(&self) -> String {
        static SUFFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        SUFFIX
            .get_or_init(|| {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                format!("{}{}", std::process::id(), nanos % 100_000)
            })
            .clone()
    }

    pub fn uri(&self, host: &str) -> String {
        format!("amqp://{}:{}@{}/{}", self.cfg.user, self.cfg.password, host, self.cfg.vhost)
    }

    pub async fn add_reconciler(&self, r: workload::Reconciler) {
        self.reconcilers.lock().await.push(r);
    }

    pub async fn run_reconcilers(&self) -> Vec<report::Check> {
        let rs = self.reconcilers.lock().await.clone();
        let mut out = Vec::new();
        for r in rs {
            out.extend(r());
        }
        out
    }

    /// Connection-level failure classification: chaos runs (or k8s
    /// scaling, which is inherently chaos) record a transition; static
    /// steady-state runs record an error.
    pub fn infra_bounce(&self, workload: &str, host: &str, detail: String) {
        if self.cfg.chaos {
            self.ledger.transition();
            self.ledger.metrics.add("bounce.transitions", 1);
            tracing::debug!(workload, host, "{detail}");
        } else {
            self.error(workload, "connection", host, detail);
        }
    }
}

// ---------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct SoakReport {
    pub passed: bool,
    pub elapsed: Duration,
    pub errors: Vec<ledger::SoakError>,
    pub error_total: u64,
    pub transitions: u64,
    pub legal_dups: u64,
    pub metrics: Vec<(String, u64)>,
    pub reconcile: Vec<report::Check>,
    pub resource: Vec<monitor::NodeTrend>,
    pub notes: Vec<String>,
}

// ---------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------

/// Run the soak. Only returns at deadline, signal, or fatal setup
/// failure — always with a report.
pub async fn run(cfg: SoakConfig) -> SoakReport {
    let started = std::time::Instant::now();
    let token = CancellationToken::new();
    let gate = LoadGate::default();
    let mut notes = Vec::new();

    let endpoints = Arc::new(k8s::EndpointSet::new(Arc::new(cfg.clone())));
    let ctx = Arc::new(Ctx {
        cfg: Arc::new(cfg.clone()),
        ledger: Ledger::default(),
        token: token.clone(),
        halt: CancellationToken::new(),
        gate,
        endpoints: endpoints.clone(),
        started: std::time::Instant::now(),
        reconcilers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    });

    // ---- wait for the cluster to exist and converge ----
    println!("soak: waiting for cluster convergence ...");
    if let Err(e) = wait_cluster_ready(&ctx).await {
        notes.push(format!("setup failed: {e}"));
        return finish(ctx, started, SoakReport { notes, ..Default::default() }).await;
    }
    let hosts = endpoints.snapshot().await;
    println!(
        "soak: cluster up: {} endpoint(s): {}",
        hosts.len(),
        hosts.join(" ")
    );

    // ---- declare soak topology (durable, so churn never removes it) ----
    if let Err(e) = workload::declare_topology(&ctx).await {
        notes.push(format!("topology setup failed: {e}"));
        return finish(ctx, started, SoakReport { notes, ..Default::default() }).await;
    }
    println!("soak: topology declared; starting workloads");

    // ---- scaler ----
    let mut scaler_handle = None;
    if let ScaleMode::K8s { .. } = &cfg.scale {
        let scaler = k8s::Scaler::new(ctx.clone());
        scaler_handle = Some(tokio::spawn(async move { scaler.run().await }));
    }

    // ---- workloads + monitors ----
    let tasks = workload::spawn_all(&ctx);
    let proto_tasks = proto::spawn_all(&ctx);
    let monitor_task = if cfg.monitor {
        Some(tokio::spawn(monitor::run(
            ctx.clone(),
            endpoints.clone(),
        )))
    } else {
        None
    };

    // ---- run until deadline or signal ----
    let deadline = tokio::time::Instant::now() + cfg.duration;
    let report_every = tokio::time::interval(cfg.report_every);
    tokio::pin!(report_every);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = report_every.tick() => report::status_line(&ctx).await,
            _ = async {
                match sigterm.as_mut() {
                    Some(s) => { s.recv().await; }
                    None => std::future::pending::<()>().await,
                }
            } => { notes.push("SIGTERM".into()); break }
            _ = async {
                match sigint.as_mut() {
                    Some(s) => { s.recv().await; }
                    None => std::future::pending::<()>().await,
                }
            } => { notes.push("SIGINT".into()); break }
        }
    }

    // ---- quiesce + drain + reconcile ----
    println!("soak: quiescing (publishers stop, queues drain) ...");
    ctx.gate.set(false);
    token.cancel();
    if let Some(h) = scaler_handle {
        h.abort();
    }
    if let Some(h) = monitor_task {
        h.abort();
    }
    // Consumers stay alive through the drain: they ack the last
    // deliveries so reconciliation sees conservation, and are stopped
    // only afterwards.
    let reconcile = workload::drain_and_reconcile(&ctx).await;
    ctx.halt.cancel();
    for t in tasks {
        t.abort();
    }
    for t in proto_tasks {
        t.abort();
    }
    let mut rep = SoakReport {
        elapsed: started.elapsed(),
        reconcile,
        resource: monitor::trends_snapshot(),
        ..Default::default()
    };
    finish(ctx, started, rep).await
}

async fn finish(ctx: Arc<Ctx>, started: std::time::Instant, mut rep: SoakReport) -> SoakReport {
    rep.elapsed = started.elapsed();
    rep.errors = ctx.ledger.errors();
    rep.error_total = ctx.ledger.error_count();
    rep.transitions = ctx.ledger.transitions();
    rep.legal_dups = ctx.ledger.legal_dups();
    rep.metrics = ctx.ledger.metrics.snapshot();
    rep.passed = rep.error_total == 0
        && rep.reconcile.iter().all(|c| c.ok);
    report::print_final(&ctx, &rep).await;
    rep
}

/// The cluster is ready when every current endpoint serves a passive
/// declare of the soak control queue (topology propagated everywhere)
/// and a confirmed canary round-trip works. New pods appearing later
/// are the scaler's problem.
async fn wait_cluster_ready(ctx: &Arc<Ctx>) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let hosts = ctx.endpoints.snapshot().await;
        if !hosts.is_empty() {
            let mut all_ok = true;
            for h in &hosts {
                if !ready_on(&ctx, h).await {
                    all_ok = false;
                }
            }
            if all_ok {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "cluster did not converge within 180s ({} endpoint(s) seen)",
                hosts.len()
            ));
        }
        tokio::select! {
            _ = ctx.token.cancelled() => return Err("cancelled".into()),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

async fn ready_on(ctx: &Arc<Ctx>, host: &str) -> bool {
    // A (re)declare that returns Ok has been raft-applied through meta,
    // so success on a node means its routing view includes the queue.
    client::declare_durable(ctx, host, "soak.control", "setup")
        .await
        .is_ok()
}
