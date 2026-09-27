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
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // `io` already replays the sniffed prefix; start the head empty.
    let head = read_head(&mut io, &[]).await?;
    if protocols.websocket && wants_upgrade(&head) {
        let head = head.clone();
        return super::ws::serve_with_head(io, &head, node, protocols).await;
    }
    let first_line = String::from_utf8_lossy(head.split(|&b| b == b'\n').next().unwrap_or(&[]));
    let ok = {
        let mut parts = first_line.split_whitespace();
        matches!(parts.next(), Some("GET"))
            && matches!(
                parts.next(),
                Some("/") | Some("/health") | Some("/healthz")
            )
    };
    let body = if ok { "{\"status\":\"ok\"}" } else { "{\"status\":\"not found\"}" };
    let status = if ok { "200 OK" } else { "404 Not Found" };
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
