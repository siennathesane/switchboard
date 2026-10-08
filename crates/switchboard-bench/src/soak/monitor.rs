//! Resource monitor: per-pod `/stats` sampling (rss, fds, data-dir
//! size, uptime), broker-log error scanning (k8s logs API), pod restart
//! tracking, and leak-trend regression against the configured
//! per-hour thresholds.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;


use super::k8s;
use super::Ctx;

#[derive(Debug, Clone)]
pub struct Sample {
    pub at: std::time::Instant,
    pub rss_kb: u64,
    pub fds: u64,
    pub disk_kb: u64,
    pub uptime_s: u64,
}

#[derive(Debug, Clone, Default)]
pub struct NodeTrend {
    pub endpoint: String,
    pub samples: usize,
    pub rss_kb: u64,
    pub fds: u64,
    pub disk_kb: u64,
    pub uptime_s: u64,
    pub restarts: u64,
    /// Per-hour slopes from a least-squares fit over the window.
    pub rss_mb_h: f64,
    pub fds_h: f64,
    pub disk_mb_h: f64,
}

/// A node's sample history plus its last log-error count.
#[derive(Default)]
struct History {
    samples: Vec<Sample>,
    log_errs: u64,
    restarts: u64,
}

/// Shared history so the final report can read trends after the task
/// is stopped.
fn histories() -> &'static std::sync::Mutex<HashMap<String, History>> {
    static H: std::sync::OnceLock<std::sync::Mutex<HashMap<String, History>>> =
        std::sync::OnceLock::new();
    H.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub async fn run(ctx: Arc<Ctx>, endpoints: Arc<k8s::EndpointSet>) {
    let mut tick = tokio::time::interval(ctx.cfg.monitor_every);
    let mut log_tick = tokio::time::interval(Duration::from_secs(60));
    log_tick.reset(); // don't fire immediately
    let mut monitor_no = 0u64;
    loop {
        tokio::select! {
            _ = ctx.token.cancelled() => return,
            _ = tick.tick() => {
                monitor_no += 1;
                sample_all(&ctx, &endpoints, monitor_no).await;
                if monitor_no % 4 == 0 {
                    check_trends(&ctx);
                }
            }
            _ = log_tick.tick() => {
                scan_logs(&ctx, &endpoints).await;
            }
        }
    }
}

async fn sample_all(ctx: &Arc<Ctx>, endpoints: &Arc<k8s::EndpointSet>, monitor_no: u64) {
    // Pod restarts (k8s mode): an increase is an infrastructure
    // transition, never an error — but always recorded.
    if let Some(k) = endpoints.k8s() {
        if let Ok(pods) = k.pods(endpoints.label()).await {
            let total: u64 = pods.iter().map(|p| p.restarts).sum();
            let mut all = histories().lock().unwrap();
            let h = all.entry("*restarts*".into()).or_default();
            if h.restarts == 0 {
                h.restarts = total;
            }
            if total > h.restarts {
                let n = total - h.restarts;
                ctx.ledger.transition();
                ctx.ledger.metrics.add("pod.restart.transitions", n);
            }
            h.restarts = total;
        }
    }
    for host in endpoints.snapshot().await {
        match get_stats(&host).await {
            Some((s, accounting)) => {
                // Shard message-lifecycle accounting: totals accumulate
                // into metrics; held (unacked) counts on soak queues are
                // the message-loss triage signal.
                if let Some(acc) = accounting {
                    for q in acc["queues"].as_array().cloned().unwrap_or_default() {
                        let name = q["queue"].as_str().unwrap_or("");
                        let held = q["held"].as_u64().unwrap_or(0);
                        if name.starts_with("soak.") && held > 0 {
                            ctx.ledger.metrics.add("shard.held_seen", held);
                        }
                    }
                    if let Some(t) = acc["totals"].as_object() {
                        for (k, v) in t {
                            ctx.ledger
                                .metrics
                                .add(&format!("shard.{k}"), v.as_u64().unwrap_or(0));
                        }
                    }
                }
                let mut all = histories().lock().unwrap();
                let h = all.entry(host.clone()).or_default();
                h.samples.push(s.clone());
                // Bound memory: 2 samples/min for a month is ~86k; cap
                // by decimating to every other sample at 4096.
                if h.samples.len() > 4096 {
                    h.samples = h
                        .samples
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| i % 2 == 0)
                        .map(|(_, s)| s.clone())
                        .collect();
                }
                if monitor_no == 1 {
                    println!(
                        "soak: monitor {} rss={}KiB fds={} data={}KiB",
                        host, s.rss_kb, s.fds, s.disk_kb
                    );
                }
            }
            None => {
                // /stats unreachable: transient churn makes this a
                // transition; persistent unreachability shows up in the
                // workloads anyway.
                ctx.ledger.metrics.add("monitor.stats_miss", 1);
            }
        }
    }
}

/// GET http://{host}/stats and parse the fields we trend.
async fn get_stats(host: &str) -> Option<(Sample, Option<serde_json::Value>)> {
    let body = http_get(host, "/stats").await?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    let sample = Sample {
        at: std::time::Instant::now(),
        rss_kb: v["rss_kb"].as_u64()?,
        fds: v["fds"].as_u64()?,
        disk_kb: v["data_dir_kb"].as_u64()?,
        uptime_s: v["uptime_s"].as_u64()?,
    };
    Some((sample, v.get("messages").cloned()))
}

/// Raw `/stats` body for other modules (scaler voter mapping).
pub(crate) async fn fetch_stats(host: &str) -> Option<String> {
    http_get(host, "/stats").await
}

async fn http_get(host: &str, path: &str) -> Option<String> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let mut s = tokio::net::TcpStream::connect(host).await.ok()?;
    s.set_nodelay(true).ok();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let n = tokio::select! {
            r = s.read(&mut buf) => r.ok()?,
            _ = tokio::time::sleep_until(deadline) => break,
        };
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let (status, body) = k8s::parse_response(&raw).ok()?;
    if status != 200 {
        return None;
    }
    Some(String::from_utf8_lossy(&body).to_string())
}

/// Scan broker logs for new ERROR/panic lines (k8s mode only; docker
/// deployments tail logs out of band).
async fn scan_logs(ctx: &Arc<Ctx>, endpoints: &Arc<k8s::EndpointSet>) {
    let Some(k) = endpoints.k8s() else { return };
    let Ok(pods) = k.pods(endpoints.label()).await else { return };
    for pod in pods {
        if pod.client_endpoint().is_none() {
            continue;
        }
        let text = k.pod_log_tail(&pod.name, 400).await;
        // openraft logs benign ERROR lines for routine election
        // contention ("while requesting vote ... ForwardToLeader"); only
        // count real problems: panics, and non-openraft ERROR lines.
        let interesting = |l: &str| {
            (l.contains("ERROR") && !l.contains("ERROR openraft") && !l.contains("requesting vote"))
                || l.contains("panic")
        };
        let errs = text.lines().filter(|l| interesting(l)).count() as u64;
        let mut all = histories().lock().unwrap();
        let h = all.entry(format!("pod:{}", pod.name)).or_default();
        if errs > h.log_errs {
            let evidence: Vec<String> = text
                .lines()
                .filter(|l| interesting(l))
                .skip(h.log_errs as usize)
                .take(5)
                .map(|s| s.to_string())
                .collect();
            ctx.error(
                "monitor",
                "broker.log",
                &pod.name,
                format!("new broker log errors: {}", evidence.join(" | ")),
            );
        }
        h.log_errs = errs;
    }
}

fn check_trends(ctx: &Arc<Ctx>) {
    // Only judge after a warmup window; early compaction and allocator
    // warmup are not leaks.
    let hist = histories().lock().unwrap();
    for (host, h) in hist.iter() {
        if host.starts_with("pod:") || host.starts_with('*') || h.samples.len() < 8 {
            continue;
        }
        let t = &ctx.cfg.leak;
        let window = Duration::from_secs(3600);
        let newest = h.samples.last().unwrap().at;
        let cutoff = newest.checked_sub(window).unwrap_or(h.samples[0].at);
        let pts: Vec<(f64, Sample)> = h
            .samples
            .iter()
            .filter(|s| s.at >= cutoff)
            .map(|s| (s.at.elapsed().as_secs_f64(), s.clone()))
            .collect();
        if pts.len() < 8 {
            continue;
        }
        let rss_mb_h = slope_h(&pts, |s| s.rss_kb as f64 / 1024.0);
        let fds_h = slope_h(&pts, |s| s.fds as f64);
        let disk_mb_h = slope_h(&pts, |s| s.disk_kb as f64 / 1024.0);
        let mut leak = Vec::new();
        if rss_mb_h > t.rss_mb_h {
            leak.push(format!("rss {rss_mb_h:.1} MiB/h > {}", t.rss_mb_h));
        }
        if fds_h > t.fds_h {
            leak.push(format!("fds {fds_h:.1}/h > {}", t.fds_h));
        }
        if disk_mb_h > t.disk_mb_h {
            leak.push(format!("disk {disk_mb_h:.1} MiB/h > {}", t.disk_mb_h));
        }
        if !leak.is_empty() {
            ctx.error(
                "monitor",
                "leak",
                host,
                format!("resource growth over the last hour: {}", leak.join(", ")),
            );
        }
    }
}

/// Least-squares slope (y per hour) over time-sorted points.
fn slope_h(pts: &[(f64, Sample)], f: impl Fn(&Sample) -> f64) -> f64 {
    let n = pts.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    // x = seconds ago (newest ≈ 0); a growing series has negative
    // slope, so negate into growth-per-hour.
    let xs: Vec<f64> = pts.iter().map(|(t, _)| *t).collect();
    let ys: Vec<f64> = pts.iter().map(|(_, s)| f(s)).collect();
    let (mut sx, mut sy, mut sxx, mut sxy) = (0.0, 0.0, 0.0, 0.0);
    for (x, y) in xs.iter().zip(&ys) {
        sx += x;
        sy += y;
        sxx += x * x;
        sxy += x * y;
    }
    let denom = n * sxx - sx * sx;
    if denom.abs() < f64::EPSILON {
        return 0.0;
    }
    let slope_per_s = (n * sxy - sx * sy) / denom;
    -slope_per_s * 3600.0
}

pub fn trends_snapshot() -> Vec<NodeTrend> {
    let mut out = Vec::new();
    let hist = histories().lock().unwrap();
    for (host, h) in hist.iter() {
        if h.samples.is_empty() {
            continue;
        }
        let last = h.samples.last().unwrap();
        let window = Duration::from_secs(3600);
        let cutoff = last.at.checked_sub(window).unwrap_or(h.samples[0].at);
        let pts: Vec<(f64, Sample)> = h
            .samples
            .iter()
            .filter(|s| s.at >= cutoff)
            .map(|s| (last.at.elapsed().as_secs_f64(), s.clone()))
            .collect();
        out.push(NodeTrend {
            endpoint: host.clone(),
            samples: h.samples.len(),
            rss_kb: last.rss_kb,
            fds: last.fds,
            disk_kb: last.disk_kb,
            uptime_s: last.uptime_s,
            restarts: h.restarts,
            rss_mb_h: if pts.len() >= 8 { slope_h(&pts, |s| s.rss_kb as f64 / 1024.0) } else { 0.0 },
            fds_h: if pts.len() >= 8 { slope_h(&pts, |s| s.fds as f64) } else { 0.0 },
            disk_mb_h: if pts.len() >= 8 { slope_h(&pts, |s| s.disk_kb as f64 / 1024.0) } else { 0.0 },
        });
    }
    out
}

