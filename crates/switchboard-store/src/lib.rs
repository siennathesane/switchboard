//! # switchboard-store
//!
//! Durable state for a Switchboard node:
//! * [`kv`] — a thin, dependency-friendly wrapper over RocksDB (the only
//!   place that touches the rocksdb crate directly).
//! * [`typ`] — the raft type configuration shared by every group: one
//!   log-entry payload type ([`typ::Command`]) covering both the meta group
//!   (topology commands) and shard groups (queue commands).
//! * [`logstore`] — `RaftLogStorage` over RocksDB, one prefix per group.
//! * [`sm`] — `RaftStateMachine` over the same RocksDB: the replicated
//!   [`switchboard_core::topology::MetaState`] and
//!   [`switchboard_core::shard::ShardState`], plus snapshot install/emit.
//!
//! Storage layout: a single RocksDB instance per node; each raft group owns
//! the key prefix `g{group_id}:`. Under it:
//! `vote`, `last-purged`, `log/{index:020}`, `sm` (state machine blob),
//! `snap/meta`, `snap/data`. The state machine is stored as one serialized
//! blob — simple and correct; a per-queue column-family layout is the
//! documented optimization path.

pub mod kv;
pub mod logstore;
pub mod sm;
pub mod typ;

pub use kv::RocksKv;
pub use logstore::LogStore;
pub use sm::StateMachine;
pub use typ::{BrokerCommand, BrokerReply, Effect, SwitchboardTypeConfig};
