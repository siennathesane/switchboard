//! # switchboard-store backup
//!
//! `switchboard backup` / `switchboard restore`: a consistent, versioned,
//! single-file archive of a node's entire durable broker state, and its
//! restore path with a storage-format version gate.
//!
//! ## What a backup captures
//!
//! A node keeps its ENTIRE durable state in one RocksDB instance opened at
//! the data dir (`RocksKv::open(&cfg.data_dir)`, node/mod.rs:312). One
//! instance covers, per raft group prefix `gN:` (meta group = 0, shard
//! groups = 1..; see the crate root docs):
//!
//! * meta raft state — `g0:vote`, `g0:last-purged`, `g0:log/*`,
//!   `g0:last-applied`, `g0:membership`, `g0:sm` (the replicated
//!   topology), `g0:snap/*`;
//! * shard group raft state — the same key set per shard group;
//! * the message journal — per-message records `gN:m/<len><queue>/<seq>`
//!   written by the state machine alongside every queue mutation
//!   (sm/mod.rs, `msg_key`), so queue contents are inside the same
//!   instance as the logs that produced them.
//!
//! Backing up the one DB therefore captures all of it atomically — there
//! is no second location to tear against.
//!
//! ## The chosen consistency point: a quiesced node, enforced by the store
//!
//! The guarantee is **quiesced-store snapshot**: the backup pipeline opens
//! the data dir with the very same `RocksKv::open` call the broker itself
//! uses. RocksDB guards the directory with an exclusive lock file for the
//! whole process lifetime of the holder (the embedded broker documents the
//! same fact as its restart contract: a second opener fails while the
//! broker lives). Consequences, in order of strength:
//!
//! 1. **A running broker cannot be backed up by this tool** — the open
//!    fails with the RocksDB lock error (surfaced as
//!    [`BackupError::SourceOpen`] with a "stop the broker first" hint).
//!    This is deliberate broker-side coordination by mutual exclusion: an
//!    out-of-process tool cannot quiesce a live broker (there is no
//!    in-process pause/quiesce hook to call), so the only safe consistency
//!    point is a stopped node, and the storage engine enforces it — a
//!    backup either reflects a fully quiesced store or does not happen.
//! 2. On a quiesced store the snapshot is the **complete durable state at
//!    a single instant**: nothing can write concurrently, so there is no
//!    interleave to tear and no uncommitted in-flight state to capture.
//! 3. The snapshot is materialized with **RocksDB's Checkpoint API**
//!    ([`RocksKv::create_checkpoint`]), not by copying files: memtables
//!    are flushed, SSTs are linked, and a fresh MANIFEST/CURRENT are
//!    written for one sequence number. The staging directory is complete
//!    and directly openable before a single byte is archived, so even a
//!    crash of the backup process itself cannot leave a torn archive
//!    behind (the staging dir is discarded; the output file is only
//!    completed after the checkpoint succeeded).
//!
//! Restoring into a fresh (absent or empty) directory reproduces the store
//! exactly; the node opens it like any data dir after a graceful stop and
//! raft resumes from the captured vote/log/last-applied state. Restoring
//! over an existing non-empty directory is refused
//! ([`BackupError::TargetNotEmpty`]) — a restore never clobbers.
//!
//! Operational caveats, stated plainly:
//! * Multi-node clusters: the archive carries ONE member's view. Recover a
//!   cluster by quiescing every node and restoring each member from a
//!   backup taken at that same quiesced instant (or restore a single
//!   member and let raft re-form around it — a stale-but-valid member
//!   catches up or is excluded by the controller's reaper, which is
//!   ordinary raft recovery). Single-node clusters are the simple case:
//!   backup == the whole broker.
//! * Everything outside the store — TLS key material, environment, the
//!   binary itself — is not part of the archive.
//! * Restore requires a binary that understands the archive's format
//!   version (below); the restored directory is opened by the same or a
//!   newer broker build.
//!
//! ## Archive format (versioned single file)
//!
//! ```text
//! offset  size  field
//! 0       4     magic "SWBK"
//! 4       4     format_version: u32 LE            (the version gate)
//! 8       4     header_len: u32 LE
//! 12      ..    header JSON (UTF-8), header_len bytes
//! ..      ..    file records, in header `files` order:
//!                 path_len: u32 LE | path bytes | data_len: u64 LE | data
//! ```
//!
//! The JSON header lists every archived file with its length and sha1
//! digest. [`restore_store`] validates magic and version BEFORE touching
//! the target directory, then verifies each payload digest while writing.
//!
//! ## Format version policy (the gate)
//!
//! [`BACKUP_FORMAT_VERSION`] is the only version this build writes and
//! accepts. On restore:
//!
//! * a NEWER version ([`BackupError::VersionTooNew`]) and any unknown
//!   older/other version ([`BackupError::VersionUnknown`]) are REFUSED
//!   with a clear error, before the target directory is touched;
//! * a bad magic ([`BackupError::NotABackup`]) is refused the same way.
//!
//! Bump the constant only alongside a deliberate format change, and teach
//! the reader to accept every version it actually understands.

use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use sha1::Digest;

/// The archive format version this build writes and accepts. See the
/// module docs for the gate policy: newer and unknown versions are
/// refused on restore.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

/// Archive magic: every switchboard backup starts with these bytes.
pub const BACKUP_MAGIC: [u8; 4] = *b"SWBK";

/// Fixed prefix of every archive: magic + version + header length.
const PREFIX_LEN: u64 = 4 + 4 + 4;

/// Upper bound for the header JSON (defensive: a corrupt length must not
/// turn into a huge allocation).
const MAX_HEADER_LEN: u64 = 64 * 1024 * 1024;

/// Upper bound for one archived path (RocksDB file names are short).
const MAX_PATH_LEN: u64 = 16 * 1024;

/// Chunk size for streaming payloads (keeps restore memory flat
/// regardless of individual file sizes).
const CHUNK: usize = 256 * 1024;

/// One archived file, as described by the header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFileEntry {
    /// Path relative to the store directory, `/`-separated.
    pub path: String,
    /// Payload length in bytes.
    pub len: u64,
    /// sha1 hex digest of the payload (integrity check on restore).
    pub sha1: String,
}

/// The backup archive header. `format_version` is the storage-format
/// version gate; the rest is descriptive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupHeader {
    /// Archive format version (see [`BACKUP_FORMAT_VERSION`]).
    pub format_version: u32,
    /// When the backup was taken, ms since the Unix epoch.
    pub created_unix_ms: u64,
    /// Writer identity (crate + version), informational.
    pub writer: String,
    /// Source data directory, informational only (restore ignores it).
    pub source_dir: String,
    /// Every archived file, in payload order.
    pub files: Vec<BackupFileEntry>,
}

/// Backup/restore failures. The version-gate variants are checked before
/// any output is written.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("not a switchboard backup archive (bad magic {found:?}; expected {expected:?})")]
    NotABackup {
        found: [u8; 4],
        expected: [u8; 4],
    },
    #[error(
        "backup format version {found} is newer than this build supports (supported up to v{supported}); \
         refusing to restore — use a switchboard build that understands format v{found}"
    )]
    VersionTooNew { found: u32, supported: u32 },
    #[error(
        "backup declares unknown format version {found}; this build understands v{supported} only"
    )]
    VersionUnknown { found: u32, supported: u32 },
    #[error("backup archive is corrupt: {0}")]
    Corrupt(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "failed to open the store at {path}: {cause} — a broker on this data dir must be \
         STOPPED first (backups require a quiesced node; the store lock is held while it runs)"
    )]
    SourceOpen { path: PathBuf, cause: String },
    #[error("checkpointing the store at {path} failed: {cause}")]
    Checkpoint { path: PathBuf, cause: String },
    #[error(
        "no RocksDB store at {path} (missing the CURRENT marker); refusing to treat a \
         directory that was never a broker data dir as a backup source"
    )]
    NotAStore { path: PathBuf },
    #[error(
        "target data directory {0} already exists and is not empty; refusing to restore over it"
    )]
    TargetNotEmpty(PathBuf),
    #[error(
        "backup file {path} failed its integrity check for {file}: expected sha1 {expected}, got {actual}"
    )]
    DigestMismatch {
        path: PathBuf,
        file: String,
        expected: String,
        actual: String,
    },
}

/// Take a backup of the broker store at `data_dir` into the single file
/// `out_file`.
///
/// Consistency contract: `data_dir` must be quiesced — this opens the
/// store exclusively, so a running broker makes the call fail (stop the
/// broker first; see the module docs for the full guarantee). The archive
/// is written from a RocksDB checkpoint of the store, so it is internally
/// consistent even if this process dies mid-run.
pub fn backup_store(data_dir: &Path, out_file: &Path) -> Result<BackupHeader, BackupError> {
    // The output's parent must exist before the checkpoint lands (RocksDB
    // stages the checkpoint as a sibling `<dest>.tmp` directory).
    if let Some(parent) = out_file
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(BackupError::Io)?;
    }
    // Refuse a directory that was never a store: opening it with
    // create_if_missing would silently produce a useless backup of an
    // empty database. CURRENT is RocksDB's canonical marker.
    if !data_dir.join("CURRENT").is_file() {
        return Err(BackupError::NotAStore {
            path: data_dir.to_path_buf(),
        });
    }
    let kv = crate::kv::RocksKv::open(data_dir).map_err(|e| BackupError::SourceOpen {
        path: data_dir.to_path_buf(),
        cause: e.to_string(),
    })?;

    // Checkpoint into a staging directory next to the output (same
    // filesystem → hard links where possible). A stale staging dir from a
    // crashed run is ours (fixed name) and is cleared before reuse; the
    // archive itself is only written after the checkpoint is complete.
    let staging = staging_dir_for(out_file);
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(BackupError::Io)?;
    }
    kv.create_checkpoint(&staging).map_err(|e| BackupError::Checkpoint {
        path: data_dir.to_path_buf(),
        cause: e.to_string(),
    })?;
    // The checkpoint dir is now self-contained; release the store lock
    // before the (potentially long) archive copy.
    drop(kv);

    let files = walk_regular_files(&staging)?;
    let header = BackupHeader {
        format_version: BACKUP_FORMAT_VERSION,
        created_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        writer: format!("switchboard-store {}", env!("CARGO_PKG_VERSION")),
        source_dir: data_dir.to_string_lossy().into_owned(),
        files: files
            .iter()
            .map(|f| BackupFileEntry {
                path: f.rel.clone(),
                len: f.len,
                sha1: f.sha1.clone(),
            })
            .collect(),
    };

    let header_json = serde_json::to_vec(&header).map_err(|e| BackupError::Corrupt(format!("header serialize: {e}")))?;
    let header_len = header_json.len() as u64;
    if header_len > MAX_HEADER_LEN {
        return Err(BackupError::Corrupt(format!(
            "header too large: {header_len}"
        )));
    }

    // Write to a temp file and rename, so an interrupted backup never
    // leaves a half-written file at the requested path.
    let tmp_out = tmp_sibling(out_file);
    write_archive(&tmp_out, &header_json, &staging, &files)?;
    std::fs::rename(&tmp_out, out_file).map_err(BackupError::Io)?;

    let _ = std::fs::remove_dir_all(&staging);
    Ok(header)
}

/// Restore a backup archive into `target_dir` (created on demand; must
/// not already contain anything).
///
/// The format-version gate runs FIRST: a newer or unknown archive version
/// is refused before the target is touched. Payload digests are verified
/// while writing, so a corrupted archive fails loudly mid-restore.
pub fn restore_store(backup_file: &Path, target_dir: &Path) -> Result<BackupHeader, BackupError> {
    let (header, mut body) = open_archive(backup_file)?;

    // Target must be absent or empty — a restore never clobbers.
    if target_dir.exists() {
        let mut entries = std::fs::read_dir(target_dir).map_err(BackupError::Io)?;
        if entries.next().is_some() {
            return Err(BackupError::TargetNotEmpty(target_dir.to_path_buf()));
        }
    }
    std::fs::create_dir_all(target_dir).map_err(BackupError::Io)?;

    let file_len = body.get_ref().metadata().map_err(BackupError::Io)?.len();
    let mut consumed = body.stream_position().map_err(BackupError::Io)?;
    for want in &header.files {
        let path = read_u32(&mut body, file_len, &mut consumed)? as u64;
        if path > MAX_PATH_LEN {
            return Err(BackupError::Corrupt(format!(
                "record path length {path} exceeds the {MAX_PATH_LEN}-byte bound"
            )));
        }
        let mut path_bytes = vec![0u8; path as usize];
        read_exact_bounded(&mut body, &mut path_bytes, file_len, &mut consumed)?;
        let rel = std::str::from_utf8(&path_bytes)
            .map_err(|_| BackupError::Corrupt("record path is not UTF-8".into()))?
            .to_string();
        if rel != want.path {
            return Err(BackupError::Corrupt(format!(
                "archive records do not match the header: expected next file {:?}, found {:?}",
                want.path, rel
            )));
        }
        let data_len = read_u64(&mut body, file_len, &mut consumed)?;
        if consumed + data_len > file_len {
            return Err(BackupError::Corrupt(format!(
                "file {:?} declares {data_len} payload bytes past the end of the archive",
                rel
            )));
        }

        let dest = safe_join(target_dir, &rel)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(BackupError::Io)?;
        }
        let mut out = std::fs::File::create(&dest).map_err(BackupError::Io)?;
        let mut hasher = sha1::Sha1::new();
        let mut remaining = data_len;
        let mut chunk = vec![0u8; CHUNK];
        while remaining > 0 {
            let take = remaining.min(chunk.len() as u64) as usize;
            read_exact_bounded(&mut body, &mut chunk[..take], file_len, &mut consumed)?;
            hasher.update(&chunk[..take]);
            out.write_all(&chunk[..take]).map_err(BackupError::Io)?;
            remaining -= take as u64;
        }
        out.flush().map_err(BackupError::Io)?;
        out.sync_all().map_err(BackupError::Io)?;
        let actual = hex(&hasher.finalize());
        if actual != want.sha1 {
            return Err(BackupError::DigestMismatch {
                path: backup_file.to_path_buf(),
                file: rel,
                expected: want.sha1.clone(),
                actual,
            });
        }
    }
    fsync_dir(target_dir);
    Ok(header)
}

/// Read only the header of a backup archive (format-gate + inspection
/// without restoring).
pub fn read_backup_header(backup_file: &Path) -> Result<BackupHeader, BackupError> {
    let (header, _) = open_archive(backup_file)?;
    Ok(header)
}

// ---------------------------------------------------------------------------
// archive reading
// ---------------------------------------------------------------------------

/// Open an archive, validate magic + version (the gate), and parse the
/// header. Returns the header positioned at the first file record.
fn open_archive(
    backup_file: &Path,
) -> Result<(BackupHeader, std::io::BufReader<std::fs::File>), BackupError> {
    let f = std::fs::File::open(backup_file).map_err(BackupError::Io)?;
    let file_len = f.metadata().map_err(BackupError::Io)?.len();
    let mut r = std::io::BufReader::new(f);

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic).map_err(BackupError::Io)?;
    if magic != BACKUP_MAGIC {
        return Err(BackupError::NotABackup {
            found: magic,
            expected: BACKUP_MAGIC,
        });
    }
    let version = read_u32_plain(&mut r)?;
    gate_version(version)?;

    let header_len = read_u32_plain(&mut r)? as u64;
    if header_len > MAX_HEADER_LEN || PREFIX_LEN + header_len > file_len {
        return Err(BackupError::Corrupt(format!(
            "header length {header_len} is implausible"
        )));
    }
    let mut json = vec![0u8; header_len as usize];
    r.read_exact(&mut json).map_err(BackupError::Io)?;
    let header: BackupHeader = serde_json::from_slice(&json)
        .map_err(|e| BackupError::Corrupt(format!("header json: {e}")))?;
    Ok((header, r))
}

/// THE version gate: only the version this build understands passes; a
/// newer or unknown version is refused with a clear error.
fn gate_version(found: u32) -> Result<(), BackupError> {
    if found == BACKUP_FORMAT_VERSION {
        return Ok(());
    }
    if found > BACKUP_FORMAT_VERSION {
        return Err(BackupError::VersionTooNew {
            found,
            supported: BACKUP_FORMAT_VERSION,
        });
    }
    Err(BackupError::VersionUnknown {
        found,
        supported: BACKUP_FORMAT_VERSION,
    })
}

fn read_u32_plain(r: &mut impl Read) -> Result<u32, BackupError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(BackupError::Io)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64(r: &mut impl Read, file_len: u64, consumed: &mut u64) -> Result<u64, BackupError> {
    let mut b = [0u8; 8];
    read_exact_bounded(r, &mut b, file_len, consumed)?;
    Ok(u64::from_le_bytes(b))
}

fn read_u32(r: &mut impl Read, file_len: u64, consumed: &mut u64) -> Result<u32, BackupError> {
    let mut b = [0u8; 4];
    read_exact_bounded(r, &mut b, file_len, consumed)?;
    Ok(u32::from_le_bytes(b))
}

/// `read_exact` with a bounds pre-check so a corrupt length cannot read
/// (or allocate) past the end of the archive.
fn read_exact_bounded(
    r: &mut impl Read,
    buf: &mut [u8],
    file_len: u64,
    consumed: &mut u64,
) -> Result<(), BackupError> {
    if *consumed + buf.len() as u64 > file_len {
        return Err(BackupError::Corrupt(format!(
            "archive truncated: needed {} more bytes at offset {} of {file_len}",
            buf.len(),
            consumed
        )));
    }
    r.read_exact(buf).map_err(BackupError::Io)?;
    *consumed += buf.len() as u64;
    Ok(())
}

/// Resolve an archived relative path under `target_dir`, refusing
/// anything that is not a plain sequence of normal components (no
/// absolute paths, no `..`, no `.`).
fn safe_join(target_dir: &Path, rel: &str) -> Result<PathBuf, BackupError> {
    let rel_path = Path::new(rel);
    for comp in rel_path.components() {
        if !matches!(comp, std::path::Component::Normal(_)) {
            return Err(BackupError::Corrupt(format!(
                "archived path {rel:?} contains a non-normal component"
            )));
        }
    }
    if rel.is_empty() {
        return Err(BackupError::Corrupt("archived path is empty".into()));
    }
    Ok(target_dir.join(rel_path))
}

// ---------------------------------------------------------------------------
// archive writing
// ---------------------------------------------------------------------------

fn write_archive(
    out_path: &Path,
    header_json: &[u8],
    staging: &Path,
    files: &[WalkedFile],
) -> Result<(), BackupError> {
    let f = std::fs::File::create(out_path).map_err(BackupError::Io)?;
    let mut w = std::io::BufWriter::new(f);
    w.write_all(&BACKUP_MAGIC).map_err(BackupError::Io)?;
    w.write_all(&BACKUP_FORMAT_VERSION.to_le_bytes())
        .map_err(BackupError::Io)?;
    w.write_all(&(header_json.len() as u32).to_le_bytes())
        .map_err(BackupError::Io)?;
    w.write_all(header_json).map_err(BackupError::Io)?;
    for file in files {
        let bytes = file.rel.as_bytes();
        w.write_all(&(bytes.len() as u32).to_le_bytes())
            .map_err(BackupError::Io)?;
        w.write_all(bytes).map_err(BackupError::Io)?;
        w.write_all(&file.len.to_le_bytes()).map_err(BackupError::Io)?;
        let mut src = std::fs::File::open(staging.join(&file.rel)).map_err(BackupError::Io)?;
        let mut chunk = vec![0u8; CHUNK];
        loop {
            let n = src.read(&mut chunk).map_err(BackupError::Io)?;
            if n == 0 {
                break;
            }
            w.write_all(&chunk[..n]).map_err(BackupError::Io)?;
        }
    }
    w.flush().map_err(BackupError::Io)?;
    let f = w.into_inner().map_err(|e| BackupError::Io(e.into_error()))?;
    f.sync_all().map_err(BackupError::Io)?;
    Ok(())
}

struct WalkedFile {
    /// Path relative to the staging root, `/`-separated.
    rel: String,
    len: u64,
    sha1: String,
}

/// Collect every regular file under `dir`, in sorted path order
/// (deterministic archives), with length + sha1 per file.
fn walk_regular_files(dir: &Path) -> Result<Vec<WalkedFile>, BackupError> {
    let mut out = Vec::new();
    walk_into(dir, dir, &mut out)?;
    out.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(out)
}

fn walk_into(root: &Path, dir: &Path, out: &mut Vec<WalkedFile>) -> Result<(), BackupError> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)
        .map_err(BackupError::Io)?
        .collect::<Result<_, _>>()
        .map_err(BackupError::Io)?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let ft = entry.file_type().map_err(BackupError::Io)?;
        if ft.is_dir() {
            walk_into(root, &path, out)?;
            continue;
        }
        if !ft.is_file() {
            // Symlinks and other oddities do not occur in a checkpoint
            // dir; skip rather than archive something we cannot restore
            // faithfully.
            continue;
        }
        let mut f = std::fs::File::open(&path).map_err(BackupError::Io)?;
        let mut hasher = sha1::Sha1::new();
        let mut chunk = vec![0u8; CHUNK];
        let mut len = 0u64;
        loop {
            let n = f.read(&mut chunk).map_err(BackupError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&chunk[..n]);
            len += n as u64;
        }
        let rel = path
            .strip_prefix(root)
            .map_err(|_| BackupError::Corrupt("walk escaped its root".into()))?
            .to_string_lossy()
            .replace('\\', "/");
        out.push(WalkedFile {
            rel,
            len,
            sha1: hex(&hasher.finalize()),
        });
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// `<output>.staging-<pid>` next to the archive: same filesystem as the
/// output, unique per process so concurrent backups do not collide.
fn staging_dir_for(out_file: &Path) -> PathBuf {
    let mut name = out_file.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".staging-{}", std::process::id()));
    out_file.with_file_name(name)
}

/// `<output>.tmp-<pid>`: the archive is fully written here and renamed
/// into place, so the requested path only ever holds a complete file.
fn tmp_sibling(out_file: &Path) -> PathBuf {
    let mut name = out_file.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".tmp-{}", std::process::id()));
    out_file.with_file_name(name)
}

fn fsync_dir(dir: &Path) {
    // Best-effort durability of the new directory entries themselves.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            name
        );
        let dir = std::env::temp_dir().join(format!("switchboard-backup-{unique}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Seed a store that looks like a node's: a meta-group blob + log
    /// entry, a shard group's message journal, all left UNflushed so the
    /// checkpoint path must carry memtable content.
    fn seed_node_like_store(data_dir: &Path) -> crate::kv::RocksKv {
        let kv = crate::kv::RocksKv::open(data_dir).expect("open store");
        kv.put(b"g0:sm", b"{\"meta\":\"topology-blob\"}").unwrap();
        kv.put(b"g0:log/00000000000000000000", b"bootstrap-entry").unwrap();
        kv.put(b"g0:vote", b"\x01\x00\x00\x00\x00\x00\x00\x00\x01").unwrap();
        // g1:m/<u16 len="q">/<seq:020> — the sm module's message-journal
        // key shape.
        let mut mk = b"g1:m/".to_vec();
        mk.extend_from_slice(&1u16.to_le_bytes());
        mk.extend_from_slice(b"q/");
        mk.extend_from_slice(format!("{:020}", 0).as_bytes());
        kv.put(&mk, b"message-payload-0").unwrap();
        mk[2 + 2..2 + 2 + 1].copy_from_slice(b"r");
        kv.put(&mk, b"message-payload-1").unwrap();
        kv
    }

    #[test]
    fn roundtrip_preserves_every_group_and_the_message_journal() {
        let data_dir = tmp_dir("rt-src");
        let kv = seed_node_like_store(&data_dir);
        // Deliberately no flush: memtable content must survive via the
        // checkpoint's flush.
        let before: Vec<(String, Vec<Vec<u8>>)> = ["g0:", "g1:"]
            .iter()
            .map(|p| {
                (
                    p.to_string(),
                    kv.prefix_pairs(p.as_bytes()).unwrap().into_iter().map(|(_, v)| v).collect(),
                )
            })
            .collect();

        // Quiesce: drop OUR OWN handle before backing up — the store lock
        // is exclusive even within this process (this is the same
        // exclusive-open behavior backup_store relies on).
        drop(kv);

        let archive = tmp_dir("rt-arch").join("node.swbk");
        let header = backup_store(&data_dir, &archive).expect("backup");
        assert_eq!(header.format_version, BACKUP_FORMAT_VERSION);
        assert!(header.files.len() >= 5, "checkpoint carries its file set, got {:?}", header.files);

        let target = tmp_dir("rt-dst");
        let restored = restore_store(&archive, &target).expect("restore");
        assert_eq!(restored, header, "restore reports the same header");

        let back = crate::kv::RocksKv::open(&target).expect("open restored store");
        for (prefix, values) in &before {
            let got: Vec<Vec<u8>> =
                back.prefix_pairs(prefix.as_bytes()).unwrap().into_iter().map(|(_, v)| v).collect();
            assert_eq!(&got, values, "prefix {prefix} must round-trip byte-for-byte");
        }
    }

    #[test]
    fn restore_refuses_a_newer_format_version_and_touches_nothing() {
        let data_dir = tmp_dir("gate-src");
        seed_node_like_store(&data_dir);
        let archive = tmp_dir("gate-arch").join("node.swbk");
        backup_store(&data_dir, &archive).expect("backup");

        // Doctor the version field (offset 4..8) to FUTURE versions: the
        // next version, and the highest u32 (anything above the known set
        // is "newer", whatever its magnitude).
        for newer in [BACKUP_FORMAT_VERSION + 1, u32::MAX] {
            let mut bytes = std::fs::read(&archive).unwrap();
            bytes[4..8].copy_from_slice(&newer.to_le_bytes());
            let doctored = tmp_dir("gate-doctored").join(format!("node-{newer}.swbk"));
            std::fs::create_dir_all(doctored.parent().unwrap()).unwrap();
            std::fs::write(&doctored, &bytes).unwrap();

            let target = tmp_dir("gate-dst");
            let err = restore_store(&doctored, &target).expect_err("newer version must be refused");
            let msg = err.to_string();
            assert!(msg.contains("newer"), "error must say the version is newer: {msg}");
            assert!(msg.contains("refusing"), "error must be a refusal: {msg}");
            assert!(
                !target.exists(),
                "the version gate must run before the target directory is created"
            );
        }
    }

    #[test]
    fn restore_refuses_an_unknown_format_version() {
        let data_dir = tmp_dir("gateu-src");
        seed_node_like_store(&data_dir);
        let archive = tmp_dir("gateu-arch").join("node.swbk");
        backup_store(&data_dir, &archive).expect("backup");

        for bogus in [0u32] {
            let mut bytes = std::fs::read(&archive).unwrap();
            bytes[4..8].copy_from_slice(&bogus.to_le_bytes());
            let doctored = tmp_dir("gateu-doctored").join(format!("node-{bogus}.swbk"));
            std::fs::create_dir_all(doctored.parent().unwrap()).unwrap();
            std::fs::write(&doctored, &bytes).unwrap();

            let target = tmp_dir("gateu-dst");
            let err = restore_store(&doctored, &target).expect_err("unknown version refused");
            let msg = err.to_string();
            assert!(msg.contains("unknown"), "error must say the version is unknown: {msg}");
            assert!(!target.exists(), "gate runs before touching the target");
        }
    }

    #[test]
    fn backup_refuses_a_directory_that_is_not_a_store() {
        let empty = tmp_dir("not-a-store");
        std::fs::create_dir_all(&empty).unwrap();
        let archive = tmp_dir("not-a-store-arch").join("node.swbk");
        let err = backup_store(&empty, &archive).expect_err("non-store dir refused");
        let msg = err.to_string();
        assert!(msg.contains("CURRENT"), "error must name the marker: {msg}");
        assert!(!archive.exists(), "no archive may be written for a non-store");

        // A completely absent path is refused the same way.
        let missing = tmp_dir("missing-store");
        let err = backup_store(&missing, &archive).expect_err("absent dir refused");
        assert!(err.to_string().contains("CURRENT"), "{err}");
    }

    #[test]
    fn restore_refuses_non_backup_files() {
        let junk = tmp_dir("junk");
        std::fs::create_dir_all(&junk).unwrap();
        let archive = junk.join("not-a-backup.swbk");
        std::fs::write(&archive, b"NOTSWBK....").unwrap();
        let target = junk.join("dst");
        let err = restore_store(&archive, &target).expect_err("bad magic refused");
        let msg = err.to_string();
        assert!(msg.contains("not a switchboard backup"), "{msg}");
        assert!(!target.exists());
    }

    #[test]
    fn restore_refuses_a_corrupt_payload_via_the_digest() {
        let data_dir = tmp_dir("dig-src");
        seed_node_like_store(&data_dir);
        let archive = tmp_dir("dig-arch").join("node.swbk");
        backup_store(&data_dir, &archive).expect("backup");

        // Flip one byte inside the FIRST file payload (skip prefix +
        // header, then path_len + path + data_len).
        let header = read_backup_header(&archive).unwrap();
        let json = serde_json::to_vec(&header).unwrap();
        let first = &header.files[0];
        let payload_start =
            PREFIX_LEN + json.len() as u64 + 4 + first.path.len() as u64 + 8;
        let mut bytes = std::fs::read(&archive).unwrap();
        let idx = payload_start as usize;
        assert!(idx < bytes.len());
        bytes[idx] ^= 0x08;
        std::fs::write(&archive, &bytes).unwrap();

        let target = tmp_dir("dig-dst");
        let err = restore_store(&archive, &target).expect_err("corrupt payload must be caught");
        assert!(
            matches!(err, BackupError::DigestMismatch { .. }),
            "expected a digest mismatch, got: {err}"
        );
    }

    #[test]
    fn restore_refuses_to_clobber_a_non_empty_target() {
        let data_dir = tmp_dir("clob-src");
        seed_node_like_store(&data_dir);
        let archive = tmp_dir("clob-arch").join("node.swbk");
        backup_store(&data_dir, &archive).expect("backup");

        let target = tmp_dir("clob-dst");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("precious"), b"data").unwrap();

        let err = restore_store(&archive, &target).expect_err("non-empty target refused");
        assert!(
            matches!(err, BackupError::TargetNotEmpty(_)),
            "expected TargetNotEmpty, got: {err}"
        );
        assert_eq!(
            std::fs::read(target.join("precious")).unwrap(),
            b"data",
            "existing content must be untouched"
        );
    }

    #[test]
    fn header_roundtrip_is_readable_without_restoring() {
        let data_dir = tmp_dir("hdr-src");
        seed_node_like_store(&data_dir);
        let archive = tmp_dir("hdr-arch").join("node.swbk");
        let wrote = backup_store(&data_dir, &archive).expect("backup");
        let read = read_backup_header(&archive).expect("read header");
        assert_eq!(wrote, read);
        assert_eq!(read.format_version, BACKUP_FORMAT_VERSION);
        assert!(read.files.iter().any(|f| f.path.ends_with(".sst") || f.path.ends_with("CURRENT")));
    }
}
