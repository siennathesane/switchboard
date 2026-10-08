//! Kubernetes integration for the soak driver.
//!
//! A minimal in-cluster REST client (rustls + HTTP/1.1 + the service
//! account token): just enough API to list the broker pods, patch the
//! Deployment's replica count, delete pods (cleanly or with a zero
//! grace period for unclean-churn coverage), and tail pod logs for
//! error scanning. No watch streams — the soak polls, which is both
//! simpler and kinder to the API server over month-long runs.
//!
//! The [`Scaler`] is the churn engine the user asked for: it moves the
//! Deployment between 1 and `max` replicas **continuously and
//! non-ordered** (a random walk over the range), and additionally
//! SIGKILLs a random pod at ≥3 replicas on its own schedule, so nodes
//! come and go in arbitrary order, sometimes without a graceful leave.
//!
//! Sub-quorum discipline: at <2 live replicas the cluster cannot
//! replicate a confirmed write, so the scaler closes the load gate,
//! waits for every soak queue to drain, scales, waits for convergence
//! (pod count, readiness, canary round-trip), and reopens the gate.
//! Protocol sessions stay connected the whole time.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;

use super::check::encode_body;
use super::client;
use super::Ctx;

// ---------------------------------------------------------------------
// In-cluster REST client
// ---------------------------------------------------------------------

pub struct K8s {
    host: String,
    token: String,
    namespace: String,
    tls: tokio_rustls::TlsConnector,
}

impl K8s {
    /// Build from the in-cluster service-account files.
    pub fn in_cluster() -> Result<Self, String> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| "KUBERNETES_SERVICE_HOST not set (not running in-cluster?)" )?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let token = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
            .map_err(|e| format!("sa token: {e}"))?;
        let namespace = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .map_err(|e| format!("sa namespace: {e}"))?;
        let mut roots = RootCertStore::empty();
        let ca_file = std::fs::File::open("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
            .map_err(|e| format!("sa ca: {e}"))?;
        let mut ca_reader = std::io::BufReader::new(ca_file);
        for cert in rustls_pemfile::certs(&mut ca_reader) {
            let cert = cert.map_err(|e| format!("sa ca der: {e}"))?;
            roots.add(cert).map_err(|e| format!("sa ca add: {e}"))?;
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config));
        Ok(K8s { host: format!("{host}:{port}"), token: token.trim().to_string(), namespace: namespace.trim().to_string(), tls })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// One HTTPS request; returns (status, body). New connection per
    /// call — the soak issues a handful of requests per second at most.
    async fn req(
        &self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        body: Option<&[u8]>,
    ) -> Result<(u16, Vec<u8>), String> {
        let tcp = TcpStream::connect(&self.host)
            .await
            .map_err(|e| format!("dial apiserver: {e}"))?;
        tcp.set_nodelay(true).ok();
        let server = tokio_rustls::rustls::pki_types::ServerName::try_from(
            std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default(),
        )
        .map_err(|e| format!("sni: {e}"))?;
        let mut tls = self
            .tls
            .connect(server, tcp)
            .await
            .map_err(|e| format!("tls apiserver: {e}"))?;
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nConnection: close\r\n",
            self.host, self.token
        );
        if let Some(ct) = content_type {
            req.push_str(&format!("Content-Type: {ct}\r\n"));
        }
        let body = body.unwrap_or(&[]);
        req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        tls.write_all(req.as_bytes()).await.map_err(|e| format!("write: {e}"))?;
        if !body.is_empty() {
            tls.write_all(body).await.map_err(|e| format!("write body: {e}"))?;
        }
        tls.flush().await.ok();

        let mut raw = Vec::with_capacity(16 * 1024);
        let mut buf = [0u8; 16 * 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let n = tokio::select! {
                r = tls.read(&mut buf) => r.map_err(|e| format!("read: {e}"))?,
                _ = tokio::time::sleep_until(deadline) => return Err("apiserver read timeout".into()),
            };
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        parse_response(&raw)
    }

    async fn get_json(&self, path: &str) -> Result<Value, String> {
        let (status, body) = self.req("GET", path, None, None).await?;
        if status != 200 {
            return Err(format!("GET {path}: {status}: {}", String::from_utf8_lossy(&body[..body.len().min(300)])));
        }
        serde_json::from_slice(&body).map_err(|e| format!("GET {path}: bad json: {e}"))
    }

    /// All soak broker pods: (name, ip, ready, restarting, terminating).
    pub async fn pods(&self, label: &str) -> Result<Vec<Pod>, String> {
        let v = self
            .get_json(&format!(
                "/api/v1/namespaces/{}/pods?labelSelector={}",
                self.namespace,
                urlencode(label)
            ))
            .await?;
        let mut out = Vec::new();
        for item in v["items"].as_array().cloned().unwrap_or_default() {
            let name = item["metadata"]["name"].as_str().unwrap_or("").to_string();
            let phase = item["status"]["phase"].as_str().unwrap_or("");
            let ip = item["status"]["podIP"].as_str().unwrap_or("").to_string();
            let terminating = !item["metadata"]["deletionTimestamp"].is_null();
            let mut ready = false;
            let mut restarts = 0u64;
            if let Some(cs) = item["status"]["containerStatuses"].as_array() {
                ready = !cs.is_empty() && cs.iter().all(|c| c["ready"].as_bool().unwrap_or(false));
                for c in cs {
                    restarts += c["restartCount"].as_u64().unwrap_or(0);
                }
            }
            out.push(Pod { name, ip, phase: phase.to_string(), ready, restarts, terminating });
        }
        Ok(out)
    }

    /// Last `lines` log lines of a pod (best effort; errors become the
    /// empty string so log scanning can never fail a soak by itself —
    /// the broker's own client-visible behavior is the verdict).
    pub async fn pod_log_tail(&self, pod: &str, lines: u32) -> String {
        let path = format!(
            "/api/v1/namespaces/{}/pods/{pod}/log?tailLines={lines}",
            self.namespace
        );
        match self.req("GET", &path, None, None).await {
            Ok((200, body)) => String::from_utf8_lossy(&body).to_string(),
            _ => String::new(),
        }
    }

    pub async fn scale(&self, deployment: &str, replicas: u32) -> Result<(), String> {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/deployments/{deployment}",
            self.namespace
        );
        let body = json!({"spec": {"replicas": replicas}}).to_string();
        let (status, resp) = self
            .req(
                "PATCH",
                &path,
                Some("application/strategic-merge-patch+json"),
                Some(body.as_bytes()),
            )
            .await?;
        if status != 200 {
            return Err(format!("scale {deployment} -> {replicas}: {status}: {}", String::from_utf8_lossy(&resp[..resp.len().min(300)])));
        }
        Ok(())
    }

    /// Set the pod-deletion-cost annotation (k8s ≥1.21): ReplicaSet
    /// scale-down deletes lowest-cost pods first. The scaler pins meta
    /// voters with a high cost so arbitrary down-moves can never delete
    /// them.
    pub async fn set_deletion_cost(&self, pod: &str, cost: i64) -> Result<(), String> {
        let path = format!("/api/v1/namespaces/{}/pods/{pod}", self.namespace);
        let body = json!({
            "metadata": {"annotations": {"controller.kubernetes.io/pod-deletion-cost": cost.to_string()}}
        })
        .to_string();
        let (status, resp) = self
            .req("PATCH", &path, Some("application/strategic-merge-patch+json"), Some(body.as_bytes()))
            .await?;
        if status != 200 {
            return Err(format!("deletion-cost {pod}: {status}: {}", String::from_utf8_lossy(&resp[..resp.len().min(200)])));
        }
        Ok(())
    }

    /// Delete a pod. `grace_secs = 0` is an unclean SIGKILL (exercises
    /// the dead-node reaper); otherwise a normal SIGTERM leave.
    pub async fn delete_pod(&self, pod: &str, grace_secs: u64) -> Result<(), String> {
        let path = format!("/api/v1/namespaces/{}/pods/{pod}", self.namespace);
        let body = json!({"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": grace_secs}).to_string();
        let (status, resp) = self.req("DELETE", &path, Some("application/json"), Some(body.as_bytes())).await?;
        if !(200..300).contains(&status) {
            return Err(format!("delete {pod}: {status}: {}", String::from_utf8_lossy(&resp[..resp.len().min(300)])));
        }
        Ok(())
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

/// Parse an HTTP/1.1 response (identity or chunked bodies).
pub(crate) fn parse_response(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no response header end")?;
    let head = String::from_utf8_lossy(&raw[..header_end]);
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("bad status line")?;
    let lower = head.to_ascii_lowercase();
    let chunked = lower.contains("transfer-encoding: chunked");
    let content_length: Option<usize> = lower
        .lines()
        .find(|l| l.starts_with("content-length:"))
        .and_then(|l| l[15..].trim().parse().ok());
    let body = &raw[header_end + 4..];
    let out = if chunked {
        dechunk(body)
    } else if let Some(n) = content_length {
        body[..n.min(body.len())].to_vec()
    } else {
        body.to_vec()
    };
    Ok((status, out))
}

fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = body.windows(2).position(|w| w == b"\r\n") else { break };
        let size_str = String::from_utf8_lossy(&body[..line_end]);
        let Ok(size) = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
        else {
            break;
        };
        if size == 0 {
            break;
        }
        let start = line_end + 2;
        if body.len() < start + size {
            out.extend_from_slice(&body[start..]);
            break;
        }
        out.extend_from_slice(&body[start..start + size]);
        body = &body[(start + size + 2).min(body.len())..];
    }
    out
}

#[derive(Debug, Clone)]
pub struct Pod {
    pub name: String,
    pub ip: String,
    pub phase: String,
    pub ready: bool,
    pub restarts: u64,
    pub terminating: bool,
}

impl Pod {
    pub fn client_endpoint(&self) -> Option<String> {
        if self.ready && !self.terminating && !self.ip.is_empty() {
            Some(format!("{}:5672", self.ip))
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------
// Endpoint set: the live broker client endpoints, refreshed
// ---------------------------------------------------------------------

/// Live endpoint provider. Static mode serves the configured hosts;
/// k8s mode lists ready broker pods with a short TTL cache.
pub struct EndpointSet {
    cfg: Arc<super::SoakConfig>,
    k8s: Option<Arc<K8s>>,
    label: String,
    cache: tokio::sync::Mutex<Option<(tokio::time::Instant, Vec<String>)>>,
    rr: AtomicUsize,
}

impl EndpointSet {
    pub fn new(cfg: Arc<super::SoakConfig>) -> Self {
        let (k8s, label) = match &cfg.scale {
            super::ScaleMode::K8s { .. } => {
                let k = K8s::in_cluster().ok();
                let label = std::env::var("SOAK_BROKER_LABEL").unwrap_or_else(|_| "app=switchboard".into());
                (k, label)
            }
            _ => (None, String::new()),
        };
        EndpointSet { cfg, k8s: k8s.map(Arc::new), label, cache: tokio::sync::Mutex::new(None), rr: AtomicUsize::new(0) }
    }

    pub fn k8s(&self) -> Option<Arc<K8s>> {
        self.k8s.clone()
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Current live endpoints (may be empty mid-churn).
    pub async fn snapshot(&self) -> Vec<String> {
        match &self.k8s {
            None => self.cfg.hosts.clone(),
            Some(k) => {
                let mut cache = self.cache.lock().await;
                if let Some((at, eps)) = cache.as_ref() {
                    if at.elapsed() < Duration::from_secs(2) {
                        return eps.clone();
                    }
                }
                let eps = match k.pods(&self.label).await {
                    Ok(pods) => pods.iter().filter_map(|p| p.client_endpoint()).collect(),
                    Err(_) => vec![],
                };
                *cache = Some((tokio::time::Instant::now(), eps.clone()));
                eps
            }
        }
    }

    /// Round-robin over the live endpoints (spreads connections).
    pub async fn random(&self) -> Option<String> {
        let eps = self.snapshot().await;
        if eps.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % eps.len();
        Some(eps[i].clone())
    }
}

// ---------------------------------------------------------------------
// Scaler: continuous non-ordered 1..max churn
// ---------------------------------------------------------------------

pub struct Scaler {
    ctx: Arc<Ctx>,
    k8s: Arc<K8s>,
    deployment: String,
    min: u32,
    max: u32,
    every: Duration,
    kill_every: Duration,
}

impl Scaler {
    pub fn new(ctx: Arc<Ctx>) -> Self {
        let k8s = ctx.endpoints.k8s().expect("k8s scaler needs in-cluster config");
        let (deployment, min, max, every, kill_every) = match &ctx.cfg.scale {
            super::ScaleMode::K8s { deployment, min, max, every, kill_every, .. } => {
                (deployment.clone(), *min, *max, *every, *kill_every)
            }
            _ => unreachable!("scaler only constructed for k8s mode"),
        };
        Scaler { ctx, k8s, deployment, min, max, every, kill_every }
    }

    pub async fn run(self) {
        let token = self.ctx.token.clone();
        let ctx = self.ctx.clone();
        let r = tokio::select! {
            _ = token.cancelled() => Ok(()),
            r = Scaler::run_inner(self) => r,
        };
        if let Err(e) = r {
            ctx.error("scaler", "fatal", "-", e);
        }
    }

    async fn run_inner(self: Self) -> Result<(), String> {
        let ctx = &self.ctx;
        let mut current: u32 = {
            let pods = self.k8s.pods(&ctx.endpoints.label()).await?;
            let live = pods.iter().filter(|p| p.client_endpoint().is_some()).count();
            (live as u32).clamp(self.min, self.max)
        };
        ctx.ledger
            .metrics
            .add("scale.moves", 0);
        let mut next_scale = tokio::time::Instant::now() + self.every;
        let mut next_kill = if self.kill_every.is_zero() {
            tokio::time::Instant::now() + Duration::from_secs(u64::MAX / 2)
        } else {
            tokio::time::Instant::now() + self.kill_every
        };
        loop {
            if ctx.token.is_cancelled() {
                return Ok(());
            }
            let now = tokio::time::Instant::now();
            if now >= next_scale {
                let target = pick_target(current, self.min, self.max);
                if target != current {
                    let result = if target <= 2 {
                        self.quiesce_scale(target).await
                    } else {
                        self.scale_and_wait(target).await
                    };
                    match result {
                        Ok(()) => {
                            current = target;
                            ctx.ledger.metrics.add("scale.moves", 1);
                            println!(
                                "soak: scaler -> {} replica(s) (move #{})",
                                current,
                                ctx.ledger
                                    .metrics
                                    .counter("scale.moves")
                                    .load(Ordering::Relaxed)
                            );
                        }
                        // A failed convergence (pods slow to join, a
                        // churn window that outran the deadline) must
                        // not end the churn: log it and keep walking —
                        // the next move re-converges.
                        Err(e) => {
                            ctx.ledger.metrics.add("scale.failed", 1);
                            ctx.error("scaler", "converge", "-", e);
                        }
                    }
                }
                next_scale = tokio::time::Instant::now() + self.every;
            }
            if now >= next_kill {
                if current >= 3 {
                    self.unclean_kill().await?;
                    ctx.ledger.metrics.add("scale.unclean_kills", 1);
                }
                next_kill = tokio::time::Instant::now() + self.kill_every;
            }
            tokio::select! {
                _ = ctx.token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn scale_and_wait(&self, target: u32) -> Result<(), String> {
        self.k8s.scale(&self.deployment, target).await?;
        self.wait_convergence(target).await
    }

    /// Close the gate, drain every queue, scale below quorum, converge,
    /// reopen. See module docs for why.
    async fn quiesce_scale(&self, target: u32) -> Result<(), String> {
        let ctx = &self.ctx;
        ctx.gate.set(false);
        let drained = drain_all(ctx).await;
        if !drained {
            ctx.error("scaler", "quiesce.drain", "-", "queues did not drain before sub-quorum scale".into());
        }
        let r = async {
            self.k8s.scale(&self.deployment, target).await?;
            self.wait_convergence(target).await
        }
        .await;
        ctx.gate.set(true);
        r
    }

    /// Wait until `target` pods are Ready and a canary round-trip works.
    async fn wait_convergence(&self, target: u32) -> Result<(), String> {
        let ctx = &self.ctx;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        loop {
            if ctx.token.is_cancelled() {
                return Ok(());
            }
            let pods = self.k8s.pods(&ctx.endpoints.label()).await.unwrap_or_default();
            let ready = pods.iter().filter(|p| p.client_endpoint().is_some()).count();
            if ready as u32 >= target.max(1) {
                // Canary: confirm + get on the control queue.
                if canary(ctx).await.is_ok() {
                    // Invalidate the endpoint cache so workloads see the
                    // new pods immediately.
                    *ctx.endpoints.cache.lock().await = None;
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                *ctx.endpoints.cache.lock().await = None;
                return Err(format!(
                    "scale to {target} did not converge in 240s ({ready} ready)"
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Map pod name → broker node id via each ready pod's `/stats`.
    async fn pod_node_ids(&self) -> Vec<(String, u64)> {
        let ctx = &self.ctx;
        let pods = match self.k8s.pods(&ctx.endpoints.label()).await {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let mut out = Vec::new();
        for p in pods {
            if let Some(ep) = p.client_endpoint() {
                if let Some(body) = super::monitor::fetch_stats(&ep).await {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if let Some(id) = v["node"].as_u64() {
                            out.push((p.name.clone(), id));
                        }
                    }
                }
            }
        }
        out
    }

    /// Mark meta-voter pods with a high deletion cost so ReplicaSet
    /// scale-down never picks them, and return the voter node ids.
    async fn protect_voters(&self) -> std::collections::HashSet<u64> {
        let ctx = &self.ctx;
        let mut voters = std::collections::HashSet::new();
        for (pod, node_id) in self.pod_node_ids().await {
            // Ask any single broker for the voter set once.
            if voters.is_empty() {
                if let Some(ep) = ctx.endpoints.random().await {
                    if let Some(body) = super::monitor::fetch_stats(&ep).await {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                            for id in v["voters"].as_array().cloned().unwrap_or_default() {
                                if let Some(id) = id.as_u64() {
                                    voters.insert(id);
                                }
                            }
                        }
                    }
                }
            }
            let cost = if voters.contains(&node_id) { 999 } else { 0 };
            let _ = self.k8s.set_deletion_cost(&pod, cost).await;
        }
        voters
    }

    /// SIGKILL a random NON-VOTER pod: the path that exercises the
    /// dead-node reaper and orphaned-consumer cleanup. Killing meta
    /// voters back-to-back can outrun the controller's re-vote and
    /// freeze the meta group's quorum — voters are protected.
    async fn unclean_kill(&self) -> Result<(), String> {
        let ctx = &self.ctx;
        let voters = self.protect_voters().await;
        let pods = self.k8s.pods(&ctx.endpoints.label()).await?;
        let node_ids: std::collections::HashMap<String, u64> =
            self.pod_node_ids().await.into_iter().collect();
        let candidates: Vec<&Pod> = pods
            .iter()
            .filter(|p| {
                p.client_endpoint().is_some()
                    && node_ids.get(&p.name).is_none_or(|id| !voters.contains(id))
            })
            .collect();
        if candidates.is_empty() {
            // Small cluster: every ready pod may be a voter. Scale moves
            // still churn membership; skip this kill.
            ctx.ledger.metrics.add("scale.kill_skipped_voter", 1);
            return Ok(());
        }
        let i = now_nanos() as usize % candidates.len();
        let victim = candidates[i];
        println!("soak: scaler SIGKILLing pod {}", victim.name);
        self.k8s.delete_pod(&victim.name, 0).await
    }
}

fn pick_target(current: u32, min: u32, max: u32) -> u32 {
    let range = (max - min + 1) as u64;
    let mut v = (now_nanos() % range) as u32 + min;
    if v == current {
        // Force movement: step to the next value (wraps across the range).
        v = if current >= max { min } else { current + 1 };
    }
    v
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
}

/// Drain: every soak queue empty on every live endpoint.
pub async fn drain_all(ctx: &Arc<Ctx>) -> bool {
    let queues = [
        "soak.p.0", "soak.p.1", "soak.p.2", "soak.f.0", "soak.f.1", "soak.f.2", "soak.tx",
        "soak.get", "soak.mand",
    ];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let hosts = ctx.endpoints.snapshot().await;
        let mut backlog = 0i64;
        for q in queues {
            for h in &hosts {
                if let Ok(n) = client::declare_durable_expect(ctx, h, q, "quiesce").await {
                    backlog += n as i64;
                }
            }
        }
        if backlog <= 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Publish + confirm + get one message on the control queue.
async fn canary(ctx: &Arc<Ctx>) -> Result<(), String> {
    let Some(host) = ctx.endpoints.random().await else {
        return Err("no endpoints".into());
    };
    let Some(conn) = client::connect_setup(ctx, &host, "canary").await else {
        return Err("shutting down".into());
    };
    let Some(ch) = client::channel(ctx, &conn, "canary", &host).await else {
        return Err("channel".into());
    };
    if !client::confirm_mode(ctx, &ch, "canary", &host).await {
        return Err("confirm".into());
    }
    let body = encode_body("canary", now_nanos(), 64);
    client::publish_confirmed(ctx, &ch, &host, "canary", "", "soak.control", &body)
        .await
        .filter(|ok| *ok)
        .ok_or_else(|| "canary confirm failed".to_string())?;
    use lapin::options::BasicGetOptions;
    let got = tokio::time::timeout(Duration::from_secs(10), ch.basic_get("soak.control", BasicGetOptions { no_ack: true }))
        .await
        .map_err(|_| "canary get timeout".to_string())?
        .map_err(|e| format!("canary get: {e}"))?;
    match got {
        Some(g) if g.delivery.data == body => Ok(()),
        Some(_) => Err("canary body mismatch".into()),
        None => Err("canary message missing".into()),
    }
}
