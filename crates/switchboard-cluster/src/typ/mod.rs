//! Re-exports of the shared raft types (defined once in
//! `switchboard-store::typ` so the log payload is identical everywhere).

pub use switchboard_store::typ::BrokerCommand;
pub use switchboard_store::typ::BrokerReply;
pub use switchboard_store::typ::Effect;
pub use switchboard_store::typ::NodeId;
pub use switchboard_store::typ::RequestId;
pub use switchboard_store::typ::SwitchboardTypeConfig;

/// The RPC error shape openraft expects for this type config.
pub type RpcError = openraft::error::RPCError<NodeId, openraft::impls::BasicNode>;

/// The raft error shape for this type config.
pub type RaftErrorShape = openraft::error::RaftError<NodeId, openraft::impls::BasicNode>;
