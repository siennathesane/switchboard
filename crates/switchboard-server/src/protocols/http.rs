//! Minimal HTTP handling on the client port: a health probe endpoint for
//! load balancers, plus WebSocket-upgrade detection (the upgrade itself
//! lives in [`super::ws`]).

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
    pre: &[u8],
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
    let (status, body) = if path == "/stats" && method == "GET" {
        ("200 OK", stats_json(&node))
    } else {
        let body = if ok { "{\"status\":\"ok\"}".to_string() } else { "{\"status\":\"not found\"}".to_string() };
        let status = if ok { "200 OK" } else { "404 Not Found" };
        (status, body)
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    use tokio::io::AsyncWriteExt;
    io.write_all(response.as_bytes()).await?;
    io.flush().await?;
    // Connection: close — the gateway drops the socket on return.
    let _ = io.shutdown().await;
    Ok(())
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
    let joined = topo.nodes.contains_key(&node.id)
        && !topo.vhosts.is_empty()
        && !topo.groups.is_empty();
    let accounting = node.message_accounting();
    let voters: Vec<String> = node.meta_voter_ids().iter().map(|v| v.to_string()).collect();
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

#[cfg(target_os = "linux")]
fn proc_self_stats() -> (u64, u64) {
    let rss_kb = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| {
                l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok())
            })
        })
        .unwrap_or(0);
    let fds = std::fs::read_dir("/proc/self/fd")
        .map(|d| d.filter_map(|e| e.ok()).count() as u64)
        .unwrap_or(0);
    (rss_kb, fds)
}

#[cfg(not(target_os = "linux"))]
fn proc_self_stats() -> (u64, u64) {
    (0, 0)
}

/// Recursive data-dir size in KiB; I/O errors degrade to 0.
fn dir_size_kb(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
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
}
