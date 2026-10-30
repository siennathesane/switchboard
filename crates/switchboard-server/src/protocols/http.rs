//! Minimal HTTP handling on the client port: a health probe endpoint for
//! load balancers, plus WebSocket-upgrade detection (the upgrade itself
//! lives in [`super::ws`]).
//!
//! # `/stats` and `/metrics` access (code-scanning alert #21)
//!
//! Both endpoints expose operational detail — queue names, message
//! counters, process vitals — so they are access-gated:
//!
//! * Loopback peers need no credentials (the shape of the guest
//!   loopback rule: localhost tooling like `curl 127.0.0.1:5672/stats`
//!   keeps working).
//! * Every other peer must present HTTP basic credentials that
//!   authenticate against the replicated user table (the standard
//!   [`super::shared::authorize`] path, guest-loopback rule included)
//!   AND belong to an admin user — the same admin gate `/api` applies
//!   ([`super::admin::is_admin_user`]). Missing/bad credentials → 401;
//!   authenticated non-admin → 403.
//! * `SWITCHBOARD_STATS_UNAUTHED=1` (any truthy value; **lab-only**)
//!   lets unauthenticated peers scrape from anywhere. The docker/k8s
//!   labs scrape these endpoints from other containers/pods (the soak
//!   monitor and the k8s scaler map pods through `/stats`), and the lab
//!   entrypoint scripts set this env for exactly that; production
//!   deployments should leave it unset.
//!
//! The plain health probes (`/`, `/health`, `/healthz`) stay
//! unauthenticated from any peer: they carry no information beyond
//! "the process is serving".

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

/// Does this request carry a WebSocket upgrade?
pub fn wants_upgrade(prefix: &[u8]) -> bool {
    let lower = prefix.to_ascii_lowercase();
    let window = &lower[..lower.len().min(4096)];
    window.windows(18).any(|w| w == b"upgrade: websocket")
}

/// Read a full HTTP request head (up to `\r\n\r\n`).
pub async fn read_head<S>(io: &mut S, pre: &[u8]) -> std::io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut head = pre.to_vec();
    let _ = pre;
    let mut buf = [0u8; 1024];
    loop {
        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 16 * 1024 {
            break;
        }
        let n = tokio::io::AsyncReadExt::read(io, &mut buf).await?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    Ok(head)
}

/// Dispatch an HTTP request: WebSocket upgrades go to [`super::ws`],
/// everything else gets a tiny JSON health answer (`GET /` or `/health`
/// → 200, otherwise 404).
pub async fn serve<S>(
    mut io: super::gateway::Prefixed<S>,
    _pre: &[u8],
    node: std::sync::Arc<switchboard_cluster::ClusterNode>,
    protocols: super::shared::ProtocolConfig,
    limits: crate::channel::ConnectionLimits,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // `io` already replays the sniffed prefix; start the head empty.
    let head = read_head(&mut io, &[]).await?;
    if protocols.websocket && wants_upgrade(&head) {
        let head = head.clone();
        return super::ws::serve_with_head(io, &head, node, protocols, limits).await;
    }
    let first_line = String::from_utf8_lossy(head.split(|&b| b == b'\n').next().unwrap_or(&[]));
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let ok = method == "GET" && matches!(path, "/" | "/health" | "/healthz");

    // /stats and /metrics are access-gated (code-scanning alert #21;
    // see the module docs): loopback may scrape unauthenticated, any
    // other peer needs admin basic credentials unless the lab-only
    // SWITCHBOARD_STATS_UNAUTHED opt-out is set.
    if method == "GET" && matches!(path, "/stats" | "/metrics") {
        if let Err(denial) = stats_access(&node, &limits, &head).await {
            let (status, extra, body): (&str, Option<&str>, &str) = match denial {
                StatsDenial::Unauthorized => (
                    "401 Unauthorized",
                    Some("WWW-Authenticate: Basic realm=\"switchboard-stats\""),
                    "{\"error\":\"authentication required\"}",
                ),
                StatsDenial::Forbidden => (
                    "403 Forbidden",
                    None,
                    "{\"error\":\"stats require admin credentials\"}",
                ),
            };
            let extra_line = extra.map(|h| format!("{h}\r\n")).unwrap_or_default();
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra_line}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            use tokio::io::AsyncWriteExt;
            io.write_all(response.as_bytes()).await?;
            io.flush().await?;
            let _ = io.shutdown().await;
            return Ok(());
        }
    }

    let (status, content_type, body) = if path == "/stats" && method == "GET" {
        ("200 OK", "application/json", stats_json(&node))
    } else if path == "/metrics" && method == "GET" {
        // Prometheus text exposition (v0.0.4), hand-rolled in
        // [`super::metrics`]; behind the shared /stats access gate.
        (
            "200 OK",
            "text/plain; version=0.0.4",
            super::metrics::render(&node),
        )
    } else if path == "/api" || path.starts_with("/api/") {
        // Admin API (JSON, basic auth): dispatches with its own reader and
        // writer; the head has already been consumed here.
        return super::admin::serve(io, &head, node, limits).await;
    } else {
        let body = if ok {
            "{\"status\":\"ok\"}".to_string()
        } else {
            "{\"status\":\"not found\"}".to_string()
        };
        let status = if ok { "200 OK" } else { "404 Not Found" };
        (status, "application/json", body)
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    use tokio::io::AsyncWriteExt;
    io.write_all(response.as_bytes()).await?;
    io.flush().await?;
    // Connection: close — the gateway drops the socket on return.
    let _ = io.shutdown().await;
    Ok(())
}

/// Why a `/stats` or `/metrics` request was refused
/// (code-scanning alert #21).
enum StatsDenial {
    /// Credentials missing, malformed, or failed to authenticate → 401.
    Unauthorized,
    /// Credentials authenticated but the user is not an admin → 403.
    Forbidden,
}

/// `SWITCHBOARD_STATS_UNAUTHED` (default off; **lab-only**): any truthy
/// value — everything except the empty string, "0", and "false"
/// (case-insensitive) — allows unauthenticated `/stats` and `/metrics`
/// scrapes from ANY peer. Exists because the docker/k8s labs scrape
/// these endpoints from other containers/pods; see the module docs.
fn stats_unauthed_opt_out() -> bool {
    match std::env::var("SWITCHBOARD_STATS_UNAUTHED") {
        Ok(v) => !v.is_empty() && !v.eq_ignore_ascii_case("0") && !v.eq_ignore_ascii_case("false"),
        Err(_) => false,
    }
}

/// May an unauthenticated `/stats`/`/metrics` request from `peer_ip`
/// through? Loopback always — mirroring the shape of the guest
/// loopback rule (a peer whose address is unknown is not refused, the
/// same way `shared::authorize` treats it) so localhost tooling keeps
/// working — and any peer under the lab opt-out. A provably
/// non-loopback peer must authenticate.
fn unauthenticated_stats_allowed(peer_ip: Option<std::net::IpAddr>, lab_opt_out: bool) -> bool {
    if lab_opt_out {
        return true;
    }
    !peer_ip.map_or(false, |ip| !ip.is_loopback())
}

/// Access gate for `/stats` and `/metrics` (code-scanning alert #21).
/// With credentials: they must authenticate through the standard
/// [`super::shared::authorize`] path (guest-loopback rule included) and
/// belong to an admin user — from any peer. Without credentials:
/// loopback or the lab opt-out passes, everything else is a 401.
async fn stats_access(
    node: &std::sync::Arc<switchboard_cluster::ClusterNode>,
    limits: &crate::channel::ConnectionLimits,
    head: &[u8],
) -> Result<(), StatsDenial> {
    let auth_header = super::admin::header_value(head, "authorization");
    match super::admin::parse_basic_auth(auth_header.as_deref()) {
        Some((user, pass)) => {
            super::shared::authorize(
                node,
                &user,
                &pass,
                limits.peer_ip,
                limits.allow_remote_guest,
            )
            .await
            .map_err(|_| StatsDenial::Unauthorized)?;
            let topo = node.topology();
            if super::admin::is_admin_user(&topo, &user) {
                Ok(())
            } else {
                Err(StatsDenial::Forbidden)
            }
        }
        None => {
            if unauthenticated_stats_allowed(limits.peer_ip, stats_unauthed_opt_out()) {
                Ok(())
            } else {
                Err(StatsDenial::Unauthorized)
            }
        }
    }
}

/// One-line JSON resource snapshot for ops/soak monitoring:
/// resident memory, open fds, data-dir size, uptime. Zeros on platforms
/// without `/proc` (macOS dev boxes) rather than an error — monitors
/// trend these numbers, they don't gate on absolute values.
fn stats_json(node: &std::sync::Arc<switchboard_cluster::ClusterNode>) -> String {
    let (rss_kb, fds) = proc_self_stats();
    // Joined = the node is a real cluster member (bootstrap entities
    // applied and a shard layout visible). Orchestrator readiness gates
    // on this so load never lands on a node that is still joining.
    let topo = node.topology();
    // Membership, not visibility: a pending node can fetch (and serve)
    // a peer's topology without having joined. Only this node's own id
    // in the directory means load may land here.
    let joined =
        topo.nodes.contains_key(&node.id) && !topo.vhosts.is_empty() && !topo.groups.is_empty();
    let accounting = node.message_accounting();
    let voters: Vec<String> = node
        .meta_voter_ids()
        .iter()
        .map(|v| v.to_string())
        .collect();
    format!(
        concat!(
            "{{\"status\":\"ok\",\"node\":{},\"pid\":{},\"rss_kb\":{},\"fds\":{},",
            "\"data_dir_kb\":{},\"uptime_s\":{},\"joined\":{},\"voters\":{},",
            "\"messages\":{},\"version\":\"{}\"}}"
        ),
        node.id,
        std::process::id(),
        rss_kb,
        fds,
        dir_size_kb(&node.cfg.data_dir),
        node.uptime_secs(),
        joined,
        serde_json::json!(voters),
        accounting,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Process vitals shared by `/stats` and `/metrics`.
#[cfg(target_os = "linux")]
pub(crate) fn proc_self_stats() -> (u64, u64) {
    let rss_kb = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
            })
        })
        .unwrap_or(0);
    let fds = std::fs::read_dir("/proc/self/fd")
        .map(|d| d.filter_map(|e| e.ok()).count() as u64)
        .unwrap_or(0);
    (rss_kb, fds)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn proc_self_stats() -> (u64, u64) {
    (0, 0)
}

/// Recursive data-dir size in KiB; I/O errors degrade to 0.
fn dir_size_kb(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in rd.filter_map(|e| e.ok()) {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    total / 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_detection() {
        let req = b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: aQ==\r\n\r\n";
        assert!(wants_upgrade(req));
        let plain = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(!wants_upgrade(plain));
    }

    /// Alert #21: the unauthenticated-scrape decision. Loopback passes,
    /// a non-loopback peer is refused, the lab opt-out lets any peer
    /// through, and an unknown peer address mirrors the guest rule
    /// (never the refusal).
    #[test]
    fn unauthenticated_stats_scrape_decision() {
        let loopback: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        let remote: std::net::IpAddr = "192.0.2.10".parse().unwrap();
        assert!(
            unauthenticated_stats_allowed(Some(loopback), false),
            "loopback tooling scrapes without credentials"
        );
        assert!(
            !unauthenticated_stats_allowed(Some(remote), false),
            "non-loopback unauthenticated scrape is a 401"
        );
        assert!(
            unauthenticated_stats_allowed(Some(remote), true),
            "the lab opt-out admits any peer"
        );
        assert!(
            unauthenticated_stats_allowed(None, false),
            "unknown peer mirrors the guest-rule shape (no refusal)"
        );
    }
}
