//! Health + stats endpoint pollers: every endpoint must answer
//! `GET /health` with 200 `{"status":"ok"}` on every probe. This is the
//! load-balancer contract — a ready pod that fails health is exactly
//! the "single error" a month of soak must never produce. During
//! endpoint churn a probe may race a dying pod (we sampled it before
//! k8s noticed) — chaos classification applies.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::soak::client;
use crate::soak::Ctx;

pub async fn run(ctx: Arc<Ctx>) {
    let mut tick = tokio::time::interval(ctx.cfg.health_every);
    tick.reset();
    loop {
        tokio::select! {
            _ = ctx.token.cancelled() => return,
            _ = tick.tick() => {
                let hosts = ctx.endpoints.snapshot().await;
                if hosts.is_empty() {
                    continue; // mid-churn; the scaler owns convergence
                }
                let mut jobs = Vec::new();
                let count = hosts.len() as u64;
                for h in &hosts {
                    let ctx = ctx.clone();
                    let h = h.clone();
                    jobs.push(tokio::spawn(async move { probe(&ctx, &h).await }));
                }
                for j in jobs {
                    let _ = j.await;
                }
                ctx.ledger.metrics.add("health.probes", count);
            }
        }
    }
}

async fn probe(ctx: &Arc<Ctx>, host: &str) {
    match get_health(host).await {
        Some(true) => ctx.ledger.metrics.add("health.ok", 1),
        Some(false) => {
            ctx.error("health", "body", host, "/health answered but not {\"status\":\"ok\"}".into());
        }
        None => {
            ctx.infra_bounce("health", host, "/health unreachable".into());
        }
    }
}

async fn get_health(host: &str) -> Option<bool> {
    let mut s = tokio::net::TcpStream::connect(host).await.ok()?;
    s.set_nodelay(true).ok();
    let req = format!("GET /health HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut raw = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        let n = tokio::select! {
            r = s.read(&mut buf) => r.ok()?,
            _ = tokio::time::sleep_until(deadline) => return None,
        };
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&raw);
    let mut lines = text.split("\r\n");
    let status = lines.next()?.to_string();
    let ok_status = status.contains("200");
    let ok_body = text.contains("{\"status\":\"ok\"}");
    Some(ok_status && ok_body)
}

pub fn spawn_all(ctx: &Arc<Ctx>) -> Vec<tokio::task::JoinHandle<()>> {
    vec![tokio::spawn(run(ctx.clone()))]
}
