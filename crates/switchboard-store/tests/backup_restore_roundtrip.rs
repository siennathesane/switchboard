//! Backup → restore round-trip, end to end: a durable queue whose
//! messages come back from the archive and survive a subsequent broker
//! restart.
//!
//! Boot pattern modeled on the embedded crate's restart suite
//! (switchboard-embedded/tests/restart.rs): in-process stop-then-start is
//! contractually impossible (the broker keeps the RocksDB store LOCK for
//! its whole process lifetime — the very fact that makes `backup` safe),
//! so each phase is a SEPARATE PROCESS: this test re-execs its own binary
//! with `--exact <test>` plus a phase env var. The BACKUP ITSELF runs in
//! the PARENT process between phases — exactly how an operator runs
//! `switchboard backup` from a separate process against a stopped broker.
//!
//! Phase map:
//! * `a` (child): broker on dir A; declare durable `backup-q`; three
//!   confirmed persistent publishes; graceful stop.
//! * parent: `backup_store(dir A → archive)` (the store lock is free —
//!   the broker was a different, already-exited process); assert v1.
//! * `b` (child): `restore_store(archive → dir B)`; broker on the
//!   restored dir; the durable queue exists again (re-declare with
//!   durable=true is idempotent; a durability mismatch would fail the
//!   declare) and holds all 3 messages.
//! * `c` (child): broker RESTARTED on dir B (new process, same dir); all
//!   3 messages still there — the restore survives a subsequent broker
//!   restart.

use std::path::PathBuf;
use std::time::Duration;

use switchboard_embedded::{Broker, BrokerConfig, MessageProperties, QueueOptions};
use switchboard_store::{backup_store, restore_store, BACKUP_FORMAT_VERSION};

const PHASE_ENV: &str = "SWITCHBOARD_STORE_BACKUP_PHASE";
const DIR_A_ENV: &str = "SWITCHBOARD_STORE_BACKUP_DIR_A";
const DIR_B_ENV: &str = "SWITCHBOARD_STORE_BACKUP_DIR_B";
const ARCHIVE_ENV: &str = "SWITCHBOARD_STORE_BACKUP_ARCHIVE";

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Install test tempo / light hashing params once per process (mirrors
/// switchboard-embedded/tests/support/mod.rs:30-57: 50x shorter waits via
/// `SB_TIME_SCALE`, argon2 test params so broker starts don't starve).
fn init() {
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

fn temp_dir(name: &str) -> PathBuf {
    let unique = format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let dir = std::env::temp_dir().join(format!("switchboard-store-{unique}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Timeouts for test brokers: wide write budgets (contention absorption)
/// and a janitor interval far beyond the test duration (the known
/// ExpirePrepared-vs-tx race documented in the embedded support module).
fn test_timeouts() -> switchboard_cluster::node::Timeouts {
    let mut t = switchboard_cluster::node::Timeouts::default();
    t.write_budget = Duration::from_secs(30);
    t.fanout_budget = Duration::from_secs(30);
    t.rpc_reply_budget = Duration::from_secs(30);
    t.reconfigure_budget = Duration::from_secs(30);
    t.janitor_interval = Duration::from_secs(3600);
    t
}

fn persistent(body: &[u8]) -> (MessageProperties, Vec<u8>) {
    let mut p = MessageProperties::default();
    p.delivery_mode = Some(2);
    (p, body.to_vec())
}

async fn open_channel(broker: &Broker) -> switchboard_embedded::EmbeddedChannel {
    let conn = broker.connect("guest", "guest").await.expect("connect");
    conn.open_channel().await.expect("channel")
}

/// Run `phase` as a child process re-exec of THIS test binary running only
/// `test_name` (self-exec pattern; an argv marker would be rejected by the
/// libtest argument parser).
fn spawn_phase(
    test_name: &str,
    phase: &str,
    dir_a: &std::path::Path,
    dir_b: &std::path::Path,
    archive: &std::path::Path,
) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--exact").arg(test_name).arg("--nocapture");
    cmd.env(PHASE_ENV, phase);
    cmd.env(DIR_A_ENV, dir_a);
    cmd.env(DIR_B_ENV, dir_b);
    cmd.env(ARCHIVE_ENV, archive);
    cmd.status().expect("failed to spawn backup round-trip phase child")
}

fn child_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("child runtime")
}

fn dir_from_env(env: &str) -> PathBuf {
    PathBuf::from(
        std::env::var(env).unwrap_or_else(|_| panic!("{env} must be set in a phase child")),
    )
}

const QUEUE: &str = "backup-q";

#[test]
fn backup_restore_roundtrip_survives_restart() {
    init();
    let test_name = "backup_restore_roundtrip_survives_restart";
    let dir_a = temp_dir("backup-roundtrip-a");
    let dir_b = temp_dir("backup-roundtrip-b");
    let archive = temp_dir("backup-roundtrip-arch").join("node.swbk");

    // ---- phase a: broker + durable queue + 3 persistent messages ----
    match std::env::var(PHASE_ENV).as_deref() {
        Ok("a") => {
            child_runtime().block_on(async {
                let dir_a = dir_from_env(DIR_A_ENV);
                let broker = Broker::start(BrokerConfig::new(dir_a).timeouts(test_timeouts()))
                    .await
                    .expect("phase a broker start");
                let ch = open_channel(&broker).await;
                ch.queue_declare(QUEUE, &QueueOptions::new().durable(true))
                    .await
                    .expect("declare durable queue");
                for i in 0..3u32 {
                    let (props, body) = persistent(format!("m{i}").as_bytes());
                    let outcome = ch
                        .publish_confirmed("", QUEUE, props, body)
                        .await
                        .expect("confirmed publish");
                    assert!(
                        matches!(outcome, switchboard_embedded::ConfirmOutcome::Acked),
                        "publish {i} must be durable (acked), got {outcome:?}"
                    );
                }
                // Graceful stop: quiesced store, ready for the parent's
                // backup.
                broker.stop(Duration::from_secs(60)).await.expect("graceful stop");
            });
            // Explicit exit: lingering join handles must not hang the
            // libtest child after the phase body is done.
            std::process::exit(0);
        }
        Ok("b") => {
            child_runtime().block_on(async {
                let archive = dir_from_env(ARCHIVE_ENV);
                let dir_b = dir_from_env(DIR_B_ENV);
                // RESTORE into a fresh directory (the version gate +
                // digest verification run inside here).
                let header = restore_store(&archive, &dir_b).expect("restore");
                assert_eq!(header.format_version, BACKUP_FORMAT_VERSION);

                // Broker on the RESTORED dir: the durable queue is back
                // with all its messages. A plain durable re-declare is
                // idempotent and reports the ready depth.
                let broker = Broker::start(BrokerConfig::new(dir_b).timeouts(test_timeouts()))
                    .await
                    .expect("phase b broker start on restored dir");
                let ch = open_channel(&broker).await;
                let state = ch
                    .queue_declare(QUEUE, &QueueOptions::new().durable(true))
                    .await
                    .expect("restored queue must re-declare as durable");
                assert_eq!(
                    state.message_count, 3,
                    "all 3 durable messages must come back from the archive"
                );
                broker.stop(Duration::from_secs(60)).await.expect("stop");
            });
            std::process::exit(0);
        }
        Ok("c") => {
            child_runtime().block_on(async {
                let dir_b = dir_from_env(DIR_B_ENV);
                // The SUBSEQUENT restart: new process, same restored dir.
                let broker = Broker::start(BrokerConfig::new(dir_b).timeouts(test_timeouts()))
                    .await
                    .expect("phase c broker restart");
                let ch = open_channel(&broker).await;
                for i in 0..3u32 {
                    let d = ch
                        .get(QUEUE, true)
                        .await
                        .expect("get")
                        .unwrap_or_else(|| panic!("message m{i} lost across the restart"));
                    assert_eq!(d.body, format!("m{i}").into_bytes());
                    assert_eq!(d.properties.delivery_mode, Some(2), "still persistent");
                }
                let empty = ch.get(QUEUE, true).await.expect("get").is_none();
                assert!(empty, "exactly 3 messages survive the restart");
                broker.stop(Duration::from_secs(60)).await.expect("stop");
            });
            std::process::exit(0);
        }
        _ => {}
    }

    // ---- parent: orchestrate the phases; run the BACKUP itself here,
    //      between the stopped phase-a broker and the restore ----
    let a = spawn_phase(test_name, "a", &dir_a, &dir_b, &archive);
    assert!(a.success(), "phase a failed: {a}");

    assert!(dir_a.exists(), "phase a must have left a store at dir A");
    let header = backup_store(&dir_a, &archive).expect("backup of the stopped broker's store");
    assert_eq!(header.format_version, BACKUP_FORMAT_VERSION);
    assert!(!header.files.is_empty(), "the archive must carry the store's files");
    assert!(archive.exists());

    let b = spawn_phase(test_name, "b", &dir_a, &dir_b, &archive);
    assert!(b.success(), "phase b failed: {b}");

    let c = spawn_phase(test_name, "c", &dir_a, &dir_b, &archive);
    assert!(c.success(), "phase c failed: {c}");

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
    let _ = std::fs::remove_dir_all(archive.parent().unwrap());
}
