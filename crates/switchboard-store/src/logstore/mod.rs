//! `RaftLogStorage` over RocksDB, one key prefix per raft group.
//!
//! Layout under the group prefix:
//! * `vote` — the current term/vote (`openraft::Vote`).
//! * `last-purged` — the highest purged log id.
//! * `log/{index:020}` — log entries, ordered by zero-padded index so
//!   RocksDB prefix iteration yields log order.

use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use serde::de::DeserializeOwned;

use crate::kv::RocksKv;
use crate::typ::NodeId;
use crate::typ::SwitchboardTypeConfig;

fn log_key(prefix: &[u8], index: u64) -> Vec<u8> {
    let mut k = logs_prefix(prefix);
    k.extend_from_slice(format!("{index:020}").as_bytes());
    k
}

/// Prefix covering every log entry of the group.
fn logs_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"log/");
    k
}

fn vote_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"vote");
    k
}

fn last_purged_key(prefix: &[u8]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(b"last-purged");
    k
}

fn msg_err(msg: &str) -> AnyError {
    AnyError::new(&std::io::Error::other(msg.to_string()))
}

fn any_err(e: impl std::error::Error + Send + Sync + 'static) -> AnyError {
    AnyError::new(&e)
}

fn io_fail(subject: openraft::ErrorSubject<NodeId>, verb: openraft::ErrorVerb, e: impl std::error::Error + Send + Sync + 'static) -> StorageError<NodeId> {
    StorageIOError::new(subject, verb, AnyError::new(&e)).into()
}

fn ser<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(v).map_err(|e| StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Write, any_err(e)).into())
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<NodeId>> {
    bincode::deserialize(bytes).map_err(|e| StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, any_err(e)).into())
}

/// Per-group raft log storage.
#[derive(Clone)]
pub struct LogStore {
    kv: RocksKv,
    prefix: Vec<u8>,
}

impl LogStore {
    /// `group_id` namespaces this store inside the shared database: group 0
    /// is the meta group, 1.. the shard groups.
    pub fn new(kv: RocksKv, group_id: u32) -> Self {
        LogStore { kv, prefix: format!("g{group_id}:").into_bytes() }
    }

    fn log_state_raw(&self) -> Result<(Option<LogId<NodeId>>, Option<LogId<NodeId>>), StorageError<NodeId>> {
        let purged = self
            .kv
            .get(&last_purged_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e))?
            .and_then(|b| de::<LogId<NodeId>>(&b).ok());
        let last = self
            .kv
            .prefix_pairs(&logs_prefix(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e))?
            .pop()
            .and_then(|(_, v)| de::<openraft::Entry<SwitchboardTypeConfig>>(&v).ok())
            .map(|e| e.log_id);
        Ok((purged, last))
    }

    /// Persist a batch of entries; split out of the trait method so tests
    /// can drive it without openraft's flush callback.
    async fn append_batch<I>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<SwitchboardTypeConfig>> + Send,
    {
        let mut pairs = Vec::new();
        for e in entries {
            pairs.push((log_key(&self.prefix, e.log_id.index), ser(&e)?));
        }
        self.kv
            .write_batch(pairs)
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e))
    }

    /// Read the last entry's log id without decoding (used by tests).
    pub fn last_entry_index(&self) -> Option<u64> {
        self.kv
            .prefix_pairs(&logs_prefix(&self.prefix))
            .ok()?
            .pop()
            .and_then(|(k, _)| {
                let s = std::str::from_utf8(&k).ok()?;
                s.rsplit('/').next()?.parse().ok()
            })
    }
}

impl RaftLogReader<SwitchboardTypeConfig> for LogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<SwitchboardTypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        // Seek to the start index (zero-padded keys keep order) and scan
        // while keys remain under the group's log/ namespace.
        let pairs = self
            .kv
            .pairs_from(&log_key(&self.prefix, start), &logs_prefix(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Read, e))?;
        let mut out = Vec::new();
        for (k, v) in pairs {
            let parsed = std::str::from_utf8(&k)
                .ok()
                .and_then(|s| s.rsplit('/').next()?.parse::<u64>().ok());
            let idx = match parsed {
                Some(i) => i,
                None => {
                    return Err(StorageIOError::new(
                        openraft::ErrorSubject::Logs,
                        openraft::ErrorVerb::Read,
                        msg_err("bad log key"),
                    )
                    .into());
                }
            };
            if idx >= end {
                break;
            }
            out.push(de(&v)?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<SwitchboardTypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<SwitchboardTypeConfig>, StorageError<NodeId>> {
        let (last_purged, last) = self.log_state_raw()?;
        // If the log has no entries, the "last" is the purged id (its
        // index), per openraft's LogState contract.
        let last = match last {
            Some(l) => Some(l),
            None => last_purged,
        };
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(vote)?;
        self.kv
            .put(&vote_key(&self.prefix), &bytes)
            .map_err(|e| io_fail(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let v = self
            .kv
            .get(&vote_key(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e))?
            .map(|b| de(&b))
            .transpose()?;
        if let Some(_v) = &v {
        }
        Ok(v)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<SwitchboardTypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<SwitchboardTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let r = self.append_batch(entries).await;
        match r {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete every entry from `log_id.index` upwards.
        let dels: Vec<Vec<u8>> = self
            .kv
            .pairs_from(&log_key(&self.prefix, log_id.index), &logs_prefix(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        self.kv
            .write_mixed([], dels)
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let dels: Vec<Vec<u8>> = self
            .kv
            .prefix_pairs(&logs_prefix(&self.prefix))
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))?
            .into_iter()
            .take_while(|(k, _)| {
                std::str::from_utf8(k)
                    .ok()
                    .and_then(|s| s.rsplit('/').next()?.parse::<u64>().ok())
                    .map(|i| i <= log_id.index)
                    .unwrap_or(false)
            })
            .map(|(k, _)| k)
            .collect();
        let watermark = ser(&log_id)?;
        self.kv
            .write_mixed(
                [(last_purged_key(&self.prefix), watermark)],
                dels,
            )
            .map_err(|e| io_fail(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Delete, e))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;

    fn store(name: &str) -> LogStore {
        LogStore::new(RocksKv::open_temp(name), 7)
    }

    fn entry(index: u64, term: u64) -> openraft::Entry<SwitchboardTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn log_state_and_reads() {
        let mut st = store("logstore-basic");
        assert_eq!(st.get_log_state().await.unwrap().last_log_id, None);

        st.append_batch([entry(0, 1), entry(1, 1), entry(2, 2)]).await.unwrap();

        let state = st.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id.unwrap().index, 2);
        assert_eq!(st.last_entry_index(), Some(2));

        let ents = st.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(ents.len(), 3);
        assert_eq!(ents[2].log_id.leader_id.term, 2);

        let ents = st.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(ents.len(), 2);
        assert_eq!(ents[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn truncate_and_purge() {
        let mut st = store("logstore-trunc");
        st.append_batch([entry(0, 1), entry(1, 1), entry(2, 1), entry(3, 1)])
            .await
            .unwrap();

        // Truncate removes [2..): a conflicting suffix from another leader.
        st.truncate(LogId::new(CommittedLeaderId::new(1, 1), 2)).await.unwrap();
        let ents = st.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(ents.len(), 2);

        // Purge removes [..=1] and records the purged watermark.
        st.purge(LogId::new(CommittedLeaderId::new(1, 1), 1)).await.unwrap();
        let state = st.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);
        let ents = st.try_get_log_entries(0..u64::MAX).await.unwrap();
        assert_eq!(ents.len(), 0, "entries 0 and 1 purged, 2 and 3 truncated");
        assert_eq!(state.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn vote_roundtrip() {
        let mut st = store("logstore-vote");
        assert_eq!(st.read_vote().await.unwrap(), None);
        let vote = Vote::new(3, 5);
        st.save_vote(&vote).await.unwrap();
        assert_eq!(st.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn group_prefixes_are_isolated() {
        let kv = RocksKv::open_temp("logstore-iso");
        let mut a = LogStore::new(kv.clone(), 0);
        let mut b = LogStore::new(kv, 1);
        a.append_batch([entry(0, 1)]).await.unwrap();
        assert_eq!(a.try_get_log_entries(0..u64::MAX).await.unwrap().len(), 1);
        assert_eq!(b.try_get_log_entries(0..u64::MAX).await.unwrap().len(), 0);
    }
}

#[cfg(test)]
mod bounds_tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;

    fn store(name: &str) -> LogStore {
        LogStore::new(RocksKv::open_temp(name), 7)
    }

    fn entry(index: u64, term: u64) -> openraft::Entry<SwitchboardTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn range_bound_variants_all_resolve() {
        let mut st = store("logstore-bounds");
        st.append_batch([entry(0, 1), entry(1, 1), entry(2, 2), entry(3, 2)])
            .await
            .unwrap();

        // Unbounded start / Unbounded end.
        let all = st.try_get_log_entries(..).await.unwrap();
        assert_eq!(all.len(), 4);
        // Excluded start skips the boundary index.
        let excl = st.try_get_log_entries((std::ops::Bound::Excluded(1), std::ops::Bound::Unbounded)).await.unwrap();
        assert_eq!(excl.len(), 2, "indices 2 and 3");
        // Included end is inclusive.
        let incl = st.try_get_log_entries((std::ops::Bound::Unbounded, std::ops::Bound::Included(1))).await.unwrap();
        assert_eq!(incl.len(), 2, "indices 0 and 1");
        // Excluded end is exclusive.
        let excl_end = st.try_get_log_entries((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(2))).await.unwrap();
        assert_eq!(excl_end.len(), 2, "indices 0 and 1");
    }

    #[tokio::test]
    async fn a_corrupt_log_key_is_a_storage_read_error() {
        let mut st = store("logstore-corrupt");
        // Plant a key inside the group's log namespace that does not
        // decode to a log index (log keys are "<prefix>log/<zero-padded
        // index>").
        let mut bad = st.prefix.clone();
        bad.extend_from_slice(b"log/not-a-number");
        st.kv.put(&bad, b"junk").unwrap();

        let r = st.try_get_log_entries(0..u64::MAX).await;
        assert!(r.is_err(), "corrupt key must surface as a storage error");
        let msg = format!("{}", r.unwrap_err());
        assert!(msg.contains("bad log key"), "{msg}");
    }
}
