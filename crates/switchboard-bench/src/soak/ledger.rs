//! The soak error ledger.
//!
//! Every client-visible anomaly goes through [`Ledger::error`]: connect
//! failures, server-driven channel closes, confirm Nacks or timeouts,
//! consume-stream errors, ordering/integrity violations, non-200 health
//! probes, missed heartbeats, and so on. A soak run passes only when the
//! ledger stays empty — "not a single error" is the acceptance bar, so
//! errors are evidence, not noise: the first 200 are recorded verbatim
//! (with per-key suppression counters beyond that) so a month-long run
//! can neither hide a failure nor drown the report in cascade spam.
//!
//! Two adjacent, deliberately separate concepts:
//! * *legal redeliveries* — a repeat delivery carrying
//!   `redelivered = true` is the architecture's documented at-least-once
//!   behavior (failover/requeue); counted, never an error.
//! * *transitions* — under `--chaos`, infrastructure events (node
//!   restarts injected externally) are expected to bounce connections;
//!   those re-establishments are counted, not errored. Message-loss,
//!   duplication, and corruption checks still apply in full.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::SystemTime;

use serde_json::json;
use serde_json::Value;

/// Maximum number of errors recorded verbatim; beyond this, per-key
/// counts keep the totals exact while the report stays readable.
const MAX_RECORDED: usize = 200;

#[derive(Debug, Clone)]
pub struct SoakError {
    pub at: SystemTime,
    /// Seconds since the ledger was created (soak start).
    pub elapsed_s: u64,
    pub workload: String,
    pub kind: String,
    pub host: String,
    pub detail: String,
}

impl SoakError {
    pub fn to_json(&self) -> Value {
        json!({
            "at": humantimeish(self.at),
            "elapsed_s": self.elapsed_s,
            "workload": self.workload,
            "kind": self.kind,
            "host": self.host,
            "detail": self.detail,
        })
    }
}

/// RFC3339-ish timestamp without pulling a date crate in.
fn humantimeish(t: SystemTime) -> String {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let days = secs / 86400;
    // Civil-from-days (Howard Hinnant's algorithm), valid for any date
    // this soak could conceivably run in.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let rem = secs % 86400;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        day,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// A monotonic named counter, created on demand and shared by key.
#[derive(Default)]
pub struct Metrics {
    map: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

type Arc<T> = std::sync::Arc<T>;

impl Metrics {
    pub fn counter(&self, name: &str) -> Arc<AtomicU64> {
        self.map
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    pub fn add(&self, name: &str, n: u64) {
        self.counter(name).fetch_add(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .map
            .lock()
            .unwrap()
            .iter()
            .map(|(k, c)| (k.clone(), c.load(Ordering::Relaxed)))
            .collect();
        v.sort();
        v
    }
}

#[derive(Default)]
struct Inner {
    errors: Vec<SoakError>,
    error_total: u64,
    suppressed: HashMap<(String, String), u64>,
    transitions: u64,
    legal_dups: u64,
}

/// Shared ledger + metrics, cloned freely.
#[derive(Clone)]
pub struct Ledger {
    inner: Arc<Mutex<Inner>>,
    pub metrics: Arc<Metrics>,
    started: Option<Arc<std::time::Instant>>,
}

impl Default for Ledger {
    fn default() -> Self {
        Ledger {
            inner: Default::default(),
            metrics: Default::default(),
            started: Some(Arc::new(std::time::Instant::now())),
        }
    }
}

impl Ledger {
    /// Record an error. Returns true the first time this exact
    /// (workload, kind) pair is seen — useful for one-shot side effects.
    pub fn error(&self, workload: &str, kind: &str, host: &str, detail: String) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.error_total += 1;
        let key = (workload.to_string(), kind.to_string());
        let seen = g
            .errors
            .iter()
            .any(|e| e.workload == key.0 && e.kind == key.1);
        let elapsed_s = self
            .started
            .as_ref()
            .map(|s| s.elapsed().as_secs())
            .unwrap_or(0);
        if g.errors.len() < MAX_RECORDED {
            g.errors.push(SoakError {
                at: SystemTime::now(),
                elapsed_s,
                workload: key.0,
                kind: key.1,
                host: host.to_string(),
                detail,
            });
        } else if !seen {
            *g.suppressed.entry(key.clone()).or_default() += 1;
        } else {
            *g.suppressed.entry(key).or_default() += 1;
        }
        !seen
    }

    /// An infrastructure bounce (node restart, partition) that reconnect
    /// logic absorbed. Only meaningful under `--chaos`; strict-mode
    /// callers convert the same event into `error`.
    pub fn transition(&self) {
        self.inner.lock().unwrap().transitions += 1;
    }

    pub fn legal_dup(&self) {
        self.inner.lock().unwrap().legal_dups += 1;
    }

    pub fn error_count(&self) -> u64 {
        self.inner.lock().unwrap().error_total
    }

    pub fn errors(&self) -> Vec<SoakError> {
        self.inner.lock().unwrap().errors.clone()
    }

    pub fn transitions(&self) -> u64 {
        self.inner.lock().unwrap().transitions
    }

    pub fn legal_dups(&self) -> u64 {
        self.inner.lock().unwrap().legal_dups
    }

    pub fn suppressed_summary(&self) -> HashMap<(String, String), u64> {
        self.inner.lock().unwrap().suppressed.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_counts_and_suppresses() {
        let l = Ledger::default();
        assert!(l.error("w", "k", "h", "one".into()));
        assert!(!l.error("w", "k", "h", "two".into()));
        assert!(l.error("w", "k2", "h", "three".into()));
        assert_eq!(l.error_count(), 3);
        assert_eq!(l.errors().len(), 3);
        assert_eq!(l.transitions(), 0);
        l.transition();
        assert_eq!(l.transitions(), 1);
        assert_eq!(l.legal_dups(), 0);
    }

    #[test]
    fn timestamp_format() {
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(86_400 * 19_723 + 3_723);
        assert_eq!(humantimeish(t), "2024-01-01T01:02:03Z");
    }
}
