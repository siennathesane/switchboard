//! Soak reporting: one-line status every `report_every`, and the final
//! verdict (human summary + machine JSON after `=== SOAK RESULTS ===`).

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use serde_json::json;

use super::Ctx;
use super::SoakReport;

#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub expected: u64,
    pub delivered: u64,
    pub ok: bool,
    pub note: String,
}

impl Check {
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "name": self.name,
            "expected": self.expected,
            "delivered": self.delivered,
            "ok": self.ok,
            "note": self.note,
        })
    }
}

/// Per-counter previous values for rate computation between status
/// lines.
fn prev() -> &'static std::sync::Mutex<HashMap<String, (u64, Instant)>> {
    static P: OnceLock<std::sync::Mutex<HashMap<String, (u64, Instant)>>> = OnceLock::new();
    P.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn rate_of(snapshot: &[(String, u64)], name: &str) -> Option<f64> {
    let now = snapshot.iter().find(|(k, _)| k == name)?.1;
    let mut p = prev().lock().unwrap();
    let r = match p.get(name) {
        Some((old, at)) => {
            let dt = at.elapsed().as_secs_f64();
            if dt > 0.2 { Some((now.saturating_sub(*old)) as f64 / dt) } else { None }
        }
        None => None,
    };
    p.insert(name.to_string(), (now, Instant::now()));
    r
}

/// One status line per report tick.
pub async fn status_line(ctx: &Ctx) {
    let snap = ctx.ledger.metrics.snapshot();
    let m = |k: &str| rate_of(&snap, k).unwrap_or(0.0).round() as u64;
    let el = humantime_dur(ctx.started.elapsed());
    let errs = ctx.ledger.error_count();
    let trans = ctx.ledger.transitions();
    let dups = ctx.ledger.legal_dups();
    let backlog: i64 = ctx
        .run_reconcilers()
        .await
        .iter()
        .map(|c| c.expected as i64 - c.delivered as i64)
        .sum();
    println!(
        "soak [{el}] rates/s: pipe+{m} fanout+{} tx+{} get+{} mqtt+{} stomp+{} a10+{} | errs={errs} dups={dups} trans={trans} backlog={backlog}",
        r(&snap, "fanout.confirmed"),
        r(&snap, "tx.commits"),
        r(&snap, "get.delivered"),
        r(&snap, "mqtt.delivered"),
        r(&snap, "stomp.delivered"),
        r(&snap, "amqp10.delivered"),
        m = m("pipeline.confirmed"),
    );
}

fn r(snap: &[(String, u64)], name: &str) -> u64 {
    rate_of(snap, name).unwrap_or(0.0).round() as u64
}

fn humantime_dur(d: Duration) -> String {
    let s = d.as_secs();
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}

/// Final human summary + machine JSON.
pub async fn print_final(ctx: &Ctx, rep: &SoakReport) {
    let snap = ctx.ledger.metrics.snapshot();
    let totals: serde_json::Map<String, serde_json::Value> = snap
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();

    println!();
    println!("================================================================");
    println!(
        " SOAK {}: {} elapsed, {} error(s), {} transition(s), {} legal dup(s)",
        if rep.passed { "PASS" } else { "FAIL" },
        humantime_dur(rep.elapsed),
        rep.error_total,
        rep.transitions,
        rep.legal_dups
    );
    println!("================================================================");
    if rep.errors.is_empty() {
        println!(" no client-visible errors recorded");
    } else {
        println!(" first errors:");
        for e in rep.errors.iter().take(10) {
            println!("  [{}/{}] {}: {}", e.workload, e.kind, e.host, e.detail);
        }
        if rep.error_total > rep.errors.len() as u64 {
            println!("  ... {} more suppressed (totals exact)", rep.error_total - rep.errors.len() as u64);
        }
    }
    let failed: Vec<&Check> = rep.reconcile.iter().filter(|c| !c.ok).collect();
    if !failed.is_empty() {
        println!(" failed reconciliation checks:");
        for c in failed {
            println!("  {}: confirmed {} != delivered {}", c.name, c.expected, c.delivered);
        }
    }
    if !rep.notes.is_empty() {
        for n in &rep.notes {
            println!(" note: {n}");
        }
    }

    println!();
    println!("=== SOAK RESULTS ===");
    println!(
        "{}",
        json!({
            "passed": rep.passed,
            "elapsed_s": rep.elapsed.as_secs(),
            "error_total": rep.error_total,
            "errors": rep.errors.iter().map(|e| e.to_json()).collect::<Vec<_>>(),
            "transitions": rep.transitions,
            "legal_dups": rep.legal_dups,
            "metrics": totals,
            "reconcile": rep.reconcile.iter().map(|c| c.to_json()).collect::<Vec<_>>(),
            "resource": rep.resource.iter().map(|t| json!({
                "endpoint": t.endpoint,
                "samples": t.samples,
                "rss_kb": t.rss_kb,
                "fds": t.fds,
                "disk_kb": t.disk_kb,
                "uptime_s": t.uptime_s,
                "restarts": t.restarts,
                "rss_mb_h": (t.rss_mb_h * 100.0).round() / 100.0,
                "fds_h": (t.fds_h * 100.0).round() / 100.0,
                "disk_mb_h": (t.disk_mb_h * 100.0).round() / 100.0,
            })).collect::<Vec<_>>(),
            "notes": rep.notes,
        })
    );
}
