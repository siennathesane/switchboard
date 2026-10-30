//! # switchboard-cluster
//!
//! The multi-raft layer that turns N independent nodes into one broker:
//!
//! * [`proto`] — the internal management protocol (framed bincode over
//!   TCP/TLS): raft RPC tunneling, command forwarding, join/admin traffic.
//! * [`transport`] — length-prefixed frames over TCP, optionally wrapped in
//!   TLS (rustls with the aws-lc-rs / aws-lc-sys crypto provider).
//! * [`net`] — openraft's `RaftNetwork` tunneled through the internal
//!   protocol, with a node directory for address lookup.
//! * [`node`] — [`node::ClusterNode`]: raft group lifecycle, the
//!   multi-master write path (local raft or one-hop forward), the join
//!   protocol, cluster formation (sliding-window shard groups), effect
//!   pumps, and the internal listener.
//! * [`internal_auth`] — the optional shared-secret proof that gates the
//!   internal listener (code-scanning alert #5).

pub mod discovery;
pub mod internal_auth;
pub mod net;

pub use node::now_ms;
pub mod node;
pub mod proto;
pub mod transport;
pub mod typ;

pub use node::ClusterError;
pub use node::ClusterNode;
pub use node::ConsumerSink;
pub use node::Delivery;
pub use node::MAX_VOTERS;
pub use node::META_GROUP;
pub use node::NodeConfig;
pub use typ::BrokerCommand;
pub use typ::BrokerReply;
pub use typ::Effect;
pub use typ::NodeId;
pub use typ::SwitchboardTypeConfig;
