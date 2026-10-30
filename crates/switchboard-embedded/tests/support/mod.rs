//! Shared helpers for the embedded crate's integration tests.
//!
//! Test tempo + light hashing params mirror the server suite's support
//! module (tests/support/mod.rs:30-57 there): every wait that goes through
//! `switchboard_core::tempo::scale` runs 50x shorter, and argon2 uses
//! test-grade parameters so dozens of broker starts and handshakes do not
//! starve each other.

#![allow(dead_code)] // each test binary uses a different subset

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Env var marking a self-exec restart child and its phase (embedded
/// design §7). An argv marker would not work: the child is a libtest /
/// nextest binary that parses its arguments and aborts on unrecognized
/// flags.
pub const RESTART_PHASE_ENV: &str = "SWITCHBOARD_EMBEDDED_RESTART_PHASE";
/// Env var carrying the shared `data_dir` from parent to child phases.
pub const RESTART_DIR_ENV: &str = "SWITCHBOARD_EMBEDDED_RESTART_DIR";

static COUNTER: AtomicU64 = AtomicU64::new(1);

/// Install test tempo / hashing env once per process.
pub fn init() {
    static LOG: std::sync::Once = std::sync::Once::new();
    LOG.call_once(|| {
        if std::env::var("SB_TIME_SCALE").is_err() {
            unsafe {
                std::env::set_var("SB_TIME_SCALE", "50");
            }
        }
        if std::env::var("SWITCHBOARD_ARGON2_M_KIB").is_err() {
            unsafe {
                std::env::set_var("SWITCHBOARD_ARGON2_M_KIB", "2048");
            }
            unsafe {
                std::env::set_var("SWITCHBOARD_ARGON2_T", "1");
            }
        }
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .try_init()
            .ok();
    });
}

/// A unique temp directory for one test (or one restart phase pair).
pub fn temp_dir(name: &str) -> PathBuf {
    let unique = format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let dir = std::env::temp_dir().join(format!("switchboard-embedded-{unique}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Timeouts for test brokers: default in every respect except (a) the
/// write budgets, which get a generous ceiling, and (b) the janitor
/// interval, which is widened far beyond any test's duration.
///
/// (a) At SB_TIME_SCALE=50 the effective write ceiling is ~600 ms — the
///     point is to absorb contention spikes when dozens of brokers form in
///     parallel on a loaded runner, where the default 5 s budget (→ 100 ms
///     effective) can starve a raft write.
///
/// (b) The janitor writes `ShardCmd::ExpirePrepared` to every hosted
///     shard group on every tick (node/mod.rs:2274-2277), and the shard
///     state machine's `tx_timeout_ticks` defaults to 0 (derive(Default),
///     shard/mod.rs:263) with cutoff `now - 0` (shard/mod.rs:630) — so an
///     ExpirePrepared landing BETWEEN a client's PrepareTx and CommitTx
///     raft writes silently drops the prepared ops while CommitTx still
///     replies Ok ("Re-commit after recovery: idempotent",
///     shard/mod.rs:600-602). Any broker client — TCP included — can lose
///     an in-flight tx commit to a janitor tick (~every 20 ms at test
///     tempo). This is a PRE-EXISTING broker race, faithfully inherited by
///     embedded clients (embedded design §6: no divergence); it belongs to
///     a separate `fix(broker):` hardening change (give
///     `tx_timeout_ticks` a real default and/or make ExpirePrepared
///     respect it), which this additive-only design may not make. Tests
///     widen the janitor interval so the window is closed here; the
///     inherited race is documented, not fixed.
pub fn test_timeouts() -> switchboard_cluster::node::Timeouts {
    let mut t = switchboard_cluster::node::Timeouts::default();
    t.write_budget = Duration::from_secs(30);
    t.fanout_budget = Duration::from_secs(30);
    t.rpc_reply_budget = Duration::from_secs(30);
    t.reconfigure_budget = Duration::from_secs(30);
    t.janitor_interval = Duration::from_secs(3600);
    t
}

/// Run `fut` with a hard timeout (panics on expiry).
pub async fn with_timeout<F: std::future::Future>(fut: F, secs: u64) -> F::Output {
    tokio::time::timeout(Duration::from_secs(secs), fut)
        .await
        .expect("test timed out")
}

/// Poll `check` every 100 ms for up to `secs`; false on timeout.
pub async fn wait_until(mut check: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    check()
}

/// True when this process is a restart-suite CHILD for `phase`.
pub fn is_child(phase: &str) -> bool {
    std::env::var(RESTART_PHASE_ENV).as_deref() == Ok(phase)
}

/// The data_dir the parent chose for a restart suite (children only).
pub fn restart_dir() -> PathBuf {
    PathBuf::from(
        std::env::var(RESTART_DIR_ENV)
            .unwrap_or_else(|_| panic!("{RESTART_DIR_ENV} must be set in a restart child")),
    )
}

/// Run `phase` as a child process re-exec of THIS test binary running only
/// `test_name`, with the phase + dir env vars set. Returns the exit status.
#[allow(dead_code)]
pub fn spawn_phase(
    test_name: &str,
    phase: &str,
    dir: &std::path::Path,
) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = std::process::Command::new(exe);
    // libtest filters: run ONLY this test. (--exact positional filters are
    // accepted by both plain libtest and nextest-built binaries.)
    cmd.arg("--exact").arg(test_name).arg("--nocapture");
    cmd.env(RESTART_PHASE_ENV, phase);
    cmd.env(RESTART_DIR_ENV, dir);
    // Inherit the parent's tempo env when present (children re-init it in
    // support::init anyway).
    cmd.status().expect("failed to spawn restart phase child")
}

/// Build a fresh single-threaded tokio runtime for a restart phase child.
pub fn child_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("child runtime")
}
