//! `switchboard backup` / `switchboard restore` — offline store tools.
//!
//! Both commands operate on a **quiesced node**: the store is opened
//! exclusively (the broker holds that lock for its whole process
//! lifetime), so backing up a running broker fails with a clear "stop the
//! broker first" error instead of producing a torn copy. The full
//! consistency guarantee lives in `switchboard_store::backup`'s module
//! docs; in short:
//!
//! * `backup` snapshots the entire store (meta raft state, every shard
//!   group's logs/state machine, the message journal — one RocksDB
//!   instance) via a RocksDB checkpoint into one versioned archive file.
//! * `restore` validates the archive's format version FIRST — newer and
//!   unknown versions are refused with a clear error — verifies every
//!   payload digest, and materializes the store into a fresh data
//!   directory. It never overwrites a non-empty directory.

use clap::Args;
use std::path::PathBuf;

/// Arguments for `switchboard backup`.
#[derive(Debug, Args)]
pub struct BackupArgs {
    /// Broker data directory (RocksDB). The broker on it must be stopped;
    /// the store's exclusive lock enforces this.
    #[arg(long, env = "SWITCHBOARD_DATA", default_value = "./data")]
    pub data: PathBuf,

    /// Output archive file (overwritten atomically on success).
    #[arg(long)]
    pub to: PathBuf,
}

/// Arguments for `switchboard restore`.
#[derive(Debug, Args)]
pub struct RestoreArgs {
    /// Backup archive file to restore from.
    pub from: PathBuf,

    /// Target data directory; it must not exist yet (or must be empty).
    /// Start the broker on it afterwards with `--data`.
    #[arg(long, env = "SWITCHBOARD_DATA", default_value = "./data")]
    pub data: PathBuf,
}

/// The new subcommands. None present → run the broker as before.
#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Write a consistent backup of a STOPPED broker's store to one
    /// archive file (meta raft state + shard logs/state machines + the
    /// message journal).
    Backup(BackupArgs),
    /// Materialize a backup archive into a fresh data directory after
    /// validating its storage-format version.
    Restore(RestoreArgs),
}

/// Run `switchboard backup`.
pub fn backup(args: &BackupArgs) -> anyhow::Result<()> {
    let header = switchboard_store::backup_store(&args.data, &args.to)
        .map_err(|e| anyhow::anyhow!("backup of {} failed: {e}", args.data.display()))?;
    println!(
        "backed up {} store files ({} bytes) from {} to {}",
        header.files.len(),
        header.files.iter().map(|f| f.len).sum::<u64>(),
        args.data.display(),
        args.to.display()
    );
    println!(
        "archive format v{}, written by {}",
        header.format_version, header.writer
    );
    println!(
        "restore into a FRESH directory with: switchboard restore {} --data <new-data-dir>",
        args.to.display()
    );
    Ok(())
}

/// Run `switchboard restore`.
pub fn restore(args: &RestoreArgs) -> anyhow::Result<()> {
    let header = switchboard_store::restore_store(&args.from, &args.data)
        .map_err(|e| anyhow::anyhow!("restore of {} failed: {e}", args.from.display()))?;
    println!(
        "restored {} store files from {} into {}",
        header.files.len(),
        args.from.display(),
        args.data.display()
    );
    println!(
        "archive format v{} (created {}, by {})",
        header.format_version,
        humantime_or_unix_ms(header.created_unix_ms),
        header.writer
    );
    println!(
        "start the restored broker with: switchboard --data {}",
        args.data.display()
    );
    Ok(())
}

/// Render the header timestamp as an ISO-ish local string, falling back to
/// raw ms if the epoch math is out of range.
fn humantime_or_unix_ms(ms: u64) -> String {
    match chrono_compat(ms) {
        Some(s) => s,
        None => format!("{ms}ms-unix"),
    }
}

/// Seconds → `YYYY-MM-DD HH:MM:SS UTC` without pulling in a date crate
/// (proleptic Gregorian civil-date algorithm; backup timestamps only).
/// Years outside 1970..=9999 render as `None` (caller falls back to the
/// raw ms) — a corrupt header timestamp must not print a pseudo-date.
fn chrono_compat(ms: u64) -> Option<String> {
    let secs = i64::try_from(ms / 1000).ok()?;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    if !(1970..=9999).contains(&y) {
        return None;
    }
    Some(format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02} UTC"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_rendering_is_sane() {
        assert_eq!(
            chrono_compat(0),
            Some("1970-01-01 00:00:00 UTC".to_string())
        );
        // 2026-10-30T00:00:00Z is 1793318400 (checked with datetime).
        assert_eq!(
            chrono_compat(1_793_318_400_000),
            Some("2026-10-30 00:00:00 UTC".to_string())
        );
        assert_eq!(chrono_compat(u64::MAX), None, "out-of-i64-range falls back");
        assert_eq!(humantime_or_unix_ms(u64::MAX), format!("{}ms-unix", u64::MAX));
    }
}
