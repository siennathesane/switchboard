//! # switchboard-core
//!
//! The AMQ model (§2.1) as pure, testable state machines.
//!
//! This crate contains the broker's *semantics* with no I/O at all:
//! * [`model`] — entities: exchanges, queues, bindings, messages (§2.1.1).
//! * [`topic`] — the topic-exchange matching algorithm (§3.1.3.3).
//! * [`routing`] — direct / fanout / topic / headers routing (§3.1.3).
//! * [`error`] — protocol exceptions mapped to reply codes (§4.8).
//! * [`topology`] — the vhost control-plane state machine: exchanges,
//!   queues, bindings, users, vhosts. This is what the *meta* raft group
//!   replicates.
//! * [`shard`] — the data-plane state machine for replicated queues:
//!   message storage, subscriptions, unacked tracking, credit-based
//!   delivery, transactions. This is what *shard* raft groups replicate.
//! * [`auth`] — SASL `PLAIN` / `AMQPLAIN` credential checks (§2.2.4).
//!
//! Keeping the state machines pure lets the same code drive single-node
//! tests, raft replicas, and the conformance suite.

pub mod auth;
pub mod error;
pub mod model;
pub mod routing;
pub mod shard;
pub mod topic;
pub mod tempo;
pub mod topology;
