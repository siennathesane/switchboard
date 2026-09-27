//! RocksDB wrapper: the only module in the workspace that imports `rocksdb`.

use std::path::Path;
use std::sync::Arc;

use rocksdb::{Options, DB};
// Direction used by pairs_from iterator mode.

/// A small, synchronous RocksDB handle. Raft storage layers call into this
/// from async contexts; RocksDB writes at these sizes are sub-millisecond,
/// which keeps the code honest without `spawn_blocking` plumbing.
#[derive(Clone)]
pub struct RocksKv {
    db: Arc<DB>,
}

impl RocksKv {
    /// Open (creating if absent) a database with tuned-for-broker options.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_manual_wal_flush(false);
        let db = DB::open(&opts, path)?;
        Ok(RocksKv { db: Arc::new(db) })
    }

    /// Open for tests: a fresh temporary directory.
    pub fn open_temp(name: &str) -> Self {
        let unique = format!(
            "{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            name
        );
        let dir = std::env::temp_dir().join(format!("switchboard-kv-{unique}"));
        Self::open(&dir).expect("open temp rocksdb")
    }

    /// Stable identity of this database instance (for single-instance
    /// registries keyed by storage).
    pub fn identity(&self) -> usize {
        Arc::as_ptr(&self.db) as usize
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.db.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.delete(key)
    }

    /// Delete every key under `prefix`.
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<(), rocksdb::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        for item in self.db.prefix_iterator(prefix) {
            let (k, _) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            batch.delete(k.as_ref());
        }
        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// All values whose keys start with `prefix`, in key order.
    pub fn prefix_values(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, rocksdb::Error> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push(v.to_vec());
        }
        Ok(out)
    }

    /// Key-value pairs under `prefix`, in key order.
    pub fn prefix_pairs(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rocksdb::Error> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    /// Key-value pairs starting at key `start` (inclusive), stopping when
    /// keys no longer start with `bound_prefix`. Unlike [`Self::prefix_pairs`]
    /// this seeks to `start` rather than bounding to it.
    pub fn pairs_from(
        &self,
        start: &[u8],
        bound_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, rocksdb::Error> {
        let mut out = Vec::new();
        let it = self
            .db
            .iterator(rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward));
        for item in it {
            let (k, v) = item?;
            if !k.starts_with(bound_prefix) {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    /// Put many pairs atomically.
    pub fn write_batch(
        &self,
        pairs: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        for (k, v) in pairs {
            batch.put(k, v);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.db.write(batch)
    }

    /// Atomically apply writes and deletes.
    pub fn write_mixed(
        &self,
        puts: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        dels: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut any = false;
        for (k, v) in puts {
            batch.put(k, v);
            any = true;
        }
        for k in dels {
            batch.delete(k);
            any = true;
        }
        if any {
            self.db.write(batch)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        self.db.flush_wal(true)
    }

    /// Drop all data (tests).
    pub fn clear_all(&self) -> Result<(), rocksdb::Error> {
        let mut it = self.db.raw_iterator();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        it.seek_to_first();
        while it.valid() {
            keys.push(it.key().expect("valid key").to_vec());
            it.next();
        }
        let mut batch = rocksdb::WriteBatch::default();
        for k in keys {
            batch.delete(k);
        }
        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete_roundtrip() {
        let kv = RocksKv::open_temp("kv-basic");
        assert_eq!(kv.get(b"k").unwrap(), None);
        kv.put(b"k", b"v1").unwrap();
        assert_eq!(kv.get(b"k").unwrap(), Some(b"v1".to_vec()));
        kv.put(b"k", b"v2").unwrap();
        assert_eq!(kv.get(b"k").unwrap(), Some(b"v2".to_vec()));
        kv.delete(b"k").unwrap();
        assert_eq!(kv.get(b"k").unwrap(), None);
    }

    #[test]
    fn prefix_scans_are_ordered() {
        let kv = RocksKv::open_temp("kv-prefix");
        for i in [3u64, 1, 10, 2] {
            kv.put(format!("p/{i:03}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
        kv.put(b"other", b"x").unwrap();
        let vs = kv.prefix_values(b"p/").unwrap();
        assert_eq!(vs, vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec(), b"v10".to_vec()]);
        let pairs = kv.prefix_pairs(b"p/").unwrap();
        assert_eq!(pairs.len(), 4);
        kv.delete_prefix(b"p/").unwrap();
        assert!(kv.prefix_values(b"p/").unwrap().is_empty());
        assert_eq!(kv.get(b"other").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn batches_are_atomic_and_seek_scans_work() {
        let kv = RocksKv::open_temp("kv-batch");
        kv.write_batch([(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())]).unwrap();
        // Empty batch is a no-op, not an error.
        kv.write_batch([]).unwrap();
        kv.write_mixed([(b"c".to_vec(), b"3".to_vec())], [b"a".to_vec()]).unwrap();
        assert_eq!(kv.get(b"a").unwrap(), None);
        assert_eq!(kv.get(b"c").unwrap(), Some(b"3".to_vec()));
        // Seek-scan from "b" bounded by "b" prefix: only key "b".
        let pairs = kv.pairs_from(b"b", b"b").unwrap();
        assert_eq!(pairs, vec![(b"b".to_vec(), b"2".to_vec())]);
        kv.flush().unwrap();
        kv.clear_all().unwrap();
        assert_eq!(kv.get(b"c").unwrap(), None);
    }
}
