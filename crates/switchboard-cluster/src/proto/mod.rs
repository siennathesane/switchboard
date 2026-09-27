//! The internal management protocol: messages exchanged on the internal
//! network between Switchboard nodes.
//!
//! Transport: TCP with a 4-byte little-endian length prefix followed by a
//! bincode-encoded [`InternalMessage`]. TLS (aws-lc-rs) wraps the stream
//! when the node is configured with certificates.
//!
//! Three families of traffic share the internal port:
//!
//! * **Raft** ([`RaftPayload`]): openraft RPCs (vote, append-entries,
//!   install-snapshot) between members of a group.
//! * **Forward** ([`ForwardRequest`]): "apply this command to group `g`",
//!   sent to any member of `g`; the receiver runs it through its local
//!   raft instance, which returns `ForwardToLeader` hints when it is not
//!   the leader. This is how *every node accepts writes* (multi-master): a
//!   publish that arrives on any node hops at most once to reach the
//!   owning shard's leader.
//! * **Admin** ([`AdminRequest`]): the join protocol, directory lookups,
//!   topology snapshots, and inter-node consumer deliveries.

use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Deserialize;
use serde::Serialize;

use switchboard_core::model::ConnectionId;
use switchboard_core::model::StoredMessage;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardCmd;
use switchboard_core::topology::GroupId;
use switchboard_core::topology::MetaState;
use switchboard_core::topology::NodeInfo;

use crate::typ::BrokerCommand;
use crate::typ::BrokerReply;
use crate::typ::NodeId;

/// One request-response exchange on the internal network.
#[derive(Debug, Serialize, Deserialize)]
pub enum InternalMessage {
    Request(InternalRequest),
    Response(InternalResponse),
}

/// The node-level identity accompanying every request.
#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub message: InternalMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InternalRequest {
    /// openraft RPC between members of `group`.
    Raft { group: GroupId, payload: RaftPayload },
    /// Apply `command` on `group` (used for cross-group writes and reads).
    Forward { group: GroupId, command: BrokerCommand },
    Admin(AdminRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InternalResponse {
    Raft(RaftResponse),
    Forward(BrokerReply),
    Admin(AdminResponse),
    /// A forwarded write could not be applied on the receiving node; the
    /// typed outcome lets the forwarder retry (transient) or surface the
    /// broker error verbatim instead of string-matching.
    ForwardFailed(ForwardError),
    Error(String),
}

/// The failure of a single forwarded-write attempt, in routable terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForwardError {
    /// The receiving node could not reach a leader for the group (yet).
    Transient,
    /// The receiving node cannot serve the group at all.
    Unreachable,
    NotJoined,
    /// The group's state machine rejected the command.
    Broker(switchboard_core::error::BrokerError),
    Other(String),
}

impl From<&crate::node::ClusterError> for ForwardError {
    fn from(e: &crate::node::ClusterError) -> Self {
        match e {
            crate::node::ClusterError::Transient => ForwardError::Transient,
            crate::node::ClusterError::Unreachable { .. } => ForwardError::Unreachable,
            crate::node::ClusterError::NotJoined => ForwardError::NotJoined,
            crate::node::ClusterError::Broker(b) => ForwardError::Broker(b.clone()),
            other => ForwardError::Other(other.to_string()),
        }
    }
}

/// The three openraft RPCs, verbatim.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftPayload {
    AppendEntries(Box<AppendEntriesRequest<crate::typ::SwitchboardTypeConfig>>),
    Vote(Box<VoteRequest<NodeId>>),
    InstallSnapshot(Box<InstallSnapshotRequest<crate::typ::SwitchboardTypeConfig>>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RaftResponse {
    AppendEntries(Box<AppendEntriesResponse<NodeId>>),
    Vote(Box<VoteResponse<NodeId>>),
    InstallSnapshot(Box<InstallSnapshotResponse<NodeId>>),
}

/// Management-plane requests. These make up the join protocol and the
/// consumer data path between the shard leader and the consumer's node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminRequest {
    /// "I am node `node` at these addresses; please add me to the cluster
    /// directory (and to my assigned groups)."
    Join { node: NodeId, info: NodeInfo, bootstrap: bool },
    /// Hand a delivered message to the node hosting the consumer.
    Deliver {
        sub: SubscriptionId,
        queue: String,
        seq: u64,
        message: StoredMessage,
        redelivered: bool,
        deleted: bool,
    },
    /// The consumer's queue was deleted: cancel the consumer.
    CancelConsumer { sub: SubscriptionId, consumer_tag: String },
    /// Register a local channel as the recipient for `sub`'s deliveries.
    AttachConsumer { sub: SubscriptionId, conn: ConnectionId, reply: bool },
    /// Liveness signal (in-memory on the receiving side, not raft).
    Ping,
    /// Ask for the peer's local applied meta state (eventually consistent
    /// topology view for routing).
    Topology,
    /// Bring the group's raft membership in line with `voters` (the meta
    /// member list): missing nodes are added as learners and promoted, and
    /// the voter set is replaced wholesale. Sent by the meta controller
    /// after a departure heals a group; `forwarded` bounds the leader hop.
    ReconfigureGroup { group: GroupId, voters: Vec<NodeId>, forwarded: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    Joined {
        groups: Vec<(GroupId, Vec<NodeId>)>,
        /// The seed's view of the node directory, so the joiner can reach
        /// every peer immediately.
        directory: Vec<(NodeId, String)>,
    },
    Delivered,
    Cancelled,
    Attached,
    Pong,
    NotFound,
    Reconfigured,
    Topology(TopologySnapshot),
}

/// Everything a Forward receiver needs to route to the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub group: GroupId,
    pub command: BrokerCommand,
}

/// Best-effort leader hint bookkeeping (opaque to the wire; used by the
/// forwarder caches).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LeaderHints {
    pub hints: Vec<(GroupId, Option<NodeId>)>,
}

/// The shard command variant a channel uses when it wants shard semantics.
pub fn shard_cmd(cmd: ShardCmd) -> BrokerCommand {
    BrokerCommand::Shard(cmd)
}

/// A topology snapshot as served by any meta member from its local applied
/// state (routing data may be slightly stale; §4.4 visibility still holds
/// for synchronous replies because declares go through raft).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologySnapshot {
    pub meta: MetaState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_roundtrip_through_bincode() {
        let msg = InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping));
        let bytes = bincode::serialize(&msg).unwrap();
        let back: InternalMessage = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(back, InternalMessage::Request(InternalRequest::Admin(AdminRequest::Ping))));

        let msg = InternalMessage::Request(InternalRequest::Forward {
            group: 2,
            command: BrokerCommand::Bootstrap {
                vhost: "/".into(),
                user: "u".into(),
                password: "p".into(),
            },
        });
        let bytes = bincode::serialize(&msg).unwrap();
        let back: InternalMessage = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(
            back,
            InternalMessage::Request(InternalRequest::Forward { group: 2, .. })
        ));
    }
}
