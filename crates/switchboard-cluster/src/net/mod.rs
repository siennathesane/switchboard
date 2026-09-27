//! `RaftNetwork` over the internal management protocol.
//!
//! One [`NetworkFactory`] belongs to a group's raft instance; openraft asks
//! it for a client per target member. RPCs are tunneled as
//! [`InternalRequest::Raft`] frames to the peer's internal address, which
//! the node directory provides (populated from the meta group at join time
//! and refreshed by the reconciliation loop).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RaftError;
use openraft::error::RPCError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::MessageSummary;

use switchboard_core::topology::GroupId;

use crate::proto::Envelope;
use crate::proto::InternalMessage;
use crate::proto::InternalRequest;
use crate::proto::InternalResponse;
use crate::proto::RaftPayload;
use crate::proto::RaftResponse;
use crate::transport::PeerChannel;
use crate::typ::NodeId;
use crate::typ::SwitchboardTypeConfig;

/// Node id → internal address. Shared, mutable, cheap.
#[derive(Debug, Default)]
pub struct Directory {
    addrs: RwLock<HashMap<NodeId, String>>,
}

impl Directory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn addr_of(&self, node: NodeId) -> Option<String> {
        self.addrs.read().expect("dir lock").get(&node).cloned()
    }

    /// Insert `addr`, returning true when the address for `node` changed
    /// (a new node or a different address). Callers that keep connection
    /// caches keyed by node id must drop the stale entry on `true` — a
    /// cached connection targets the previous address.
    pub fn update(&self, node: NodeId, addr: String) -> bool {
        let mut addrs = self.addrs.write().expect("dir lock");
        match addrs.get(&node) {
            Some(existing) if *existing == addr => false,
            _ => {
                addrs.insert(node, addr);
                true
            }
        }
    }

    pub fn remove(&self, node: NodeId) {
        self.addrs.write().expect("dir lock").remove(&node);
    }

    pub fn all(&self) -> Vec<(NodeId, String)> {
        self.addrs.read().expect("dir lock").iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

/// Per-group factory handed to `Raft::new`.
#[derive(Clone)]
pub struct NetworkFactory {
    pub group: GroupId,
    pub from: NodeId,
    pub dir: Arc<Directory>,
    pub channel: Arc<PeerChannel>,
}

impl RaftNetworkFactory<SwitchboardTypeConfig> for NetworkFactory {
    type Network = RaftNetClient;

    async fn new_client(&mut self, target: NodeId, _node: &openraft::BasicNode) -> Self::Network {
        RaftNetClient {
            group: self.group,
            from: self.from,
            target,
            dir: self.dir.clone(),
            channel: self.channel.clone(),
        }
    }
}

pub struct RaftNetClient {
    group: GroupId,
    #[allow(dead_code)] // identity of the dialer, kept for diagnostics
    from: NodeId,
    target: NodeId,
    dir: Arc<Directory>,
    channel: Arc<PeerChannel>,
}

impl RaftNetClient {
    fn net_err(msg: String) -> RaftNetErr {
        RPCError::Network(NetworkError::new(&ConnectionError::new(msg)))
    }

    async fn rpc(&mut self, payload: RaftPayload, summary: &str) -> Result<RaftResponse, RaftNetErr> {
        let Some(addr) = self.dir.addr_of(self.target) else {
            return Err(RPCError::Unreachable(Unreachable::new(&ConnectionError::new(format!(
                "no address known for node {}",
                self.target
            )))));
        };
        // The envelope wrapper is mandatory: receivers deserialize
        // `Envelope { from, message }` (the join path wraps; so must this).
        let req = Envelope {
            from: self.from,
            message: InternalMessage::Request(InternalRequest::Raft {
                group: self.group,
                payload,
            }),
        };
        let bytes = bincode::serialize(&req)
            .map_err(|e| RPCError::Network(NetworkError::new(&ConnectionError::new(e.to_string()))))?;
        // Pooled request→reply: the raft heartbeat cadence must not churn
        // fresh TCP connections (ephemeral-port exhaustion under load).
        let back = self
            .channel
            .rpc(&addr, &bytes)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&ConnectionError::new(format!(
                "rpc {addr}: {e}"
            )))))?;
        let msg: InternalMessage = bincode::deserialize(&back)
            .map_err(|e| RPCError::Network(NetworkError::new(&ConnectionError::new(e.to_string()))))?;
        match msg {
            InternalMessage::Response(InternalResponse::Raft(r)) => Ok(r),
            InternalMessage::Response(InternalResponse::Error(e)) => {
                // Raft-level rejections arrive as response payloads; an
                // InternalResponse::Error is transport/handler trouble, so
                // we surface it as a retryable network condition.
                Err(Self::net_err(format!("{e} (rpc: {summary})")))
            }
            other => Err(Self::net_err(format!(
                "unexpected response to {summary}: {other:?}"
            ))),
        }
    }
}

/// An error carrying a peer-side message; openraft requires concrete
/// `std::error::Error` payloads.
#[derive(Debug)]
pub struct PeerError(pub String);

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PeerError {}

/// A displayable wrapper so io/errors fit openraft's `AnyError`-shaped
/// constructors.
#[derive(Debug)]
struct ConnectionError {
    msg: String,
}

impl ConnectionError {
    fn new(msg: String) -> Self {
        ConnectionError { msg }
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for ConnectionError {}

impl RaftNetwork<SwitchboardTypeConfig> for RaftNetClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<SwitchboardTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftNetErr> {
        let summary = req.summary();
        let resp = self.rpc(RaftPayload::AppendEntries(Box::new(req)), &summary).await?;
        match resp {
            RaftResponse::AppendEntries(r) => Ok(*r),
            other => Err(Self::net_err(mismatched(other, "append_entries")))
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<SwitchboardTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, SnapNetErr> {
        // Snapshot transfers are idempotent; every failure maps onto the
        // shared (E-free) variants so openraft retries the whole transfer.
        let resp = match self
            .rpc(RaftPayload::InstallSnapshot(Box::new(req)), "install_snapshot")
            .await
        {
            Ok(r) => r,
            Err(e) => return Err(snap_of(e)),
        };
        match resp {
            RaftResponse::InstallSnapshot(r) => Ok(*r),
            other => Err(snap_of(Self::net_err(mismatched(other, "install_snapshot"))))
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RaftNetErr> {
        let summary = req.summary();
        let resp = self.rpc(RaftPayload::Vote(Box::new(req)), &summary).await?;
        match resp {
            RaftResponse::Vote(r) => Ok(*r),
            other => Err(Self::net_err(mismatched(other, "vote")))
        }
    }
}

/// Error type of `append_entries`/`vote` for this type config.
type RaftNetErr = RPCError<NodeId, openraft::impls::BasicNode, RaftError<NodeId>>;
/// Error type of `install_snapshot` for this type config.
type SnapNetErr = RPCError<
    NodeId,
    openraft::impls::BasicNode,
    RaftError<NodeId, InstallSnapshotError>,
>;

fn snap_of(e: RaftNetErr) -> SnapNetErr {
    match e {
        RPCError::Timeout(t) => RPCError::Timeout(t),
        RPCError::Unreachable(u) => RPCError::Unreachable(u),
        RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
        RPCError::Network(n) => RPCError::Network(n),
        // A peer-side raft failure during snapshot transfer: report as a
        // network condition; openraft restarts the transfer.
        RPCError::RemoteError(r) => {
            RPCError::Network(NetworkError::new(&ConnectionError::new(format!(
                "peer {} failed snapshot transfer: {}",
                r.target, r.source
            ))))
        }
    }
}

fn mismatched(other: RaftResponse, rpc: &'static str) -> String {
    format!("mismatched {rpc} response: {other:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typ::NodeId;
    use openraft::Vote;
    use std::time::Duration;

    fn client(dir: Arc<Directory>, target: NodeId) -> RaftNetClient {
        RaftNetClient {
            group: 0,
            from: 1,
            target,
            dir,
            channel: Arc::new(PeerChannel::new(None, "sb".into())),
        }
    }

    fn vote_payload(term: u64) -> RaftPayload {
        RaftPayload::Vote(Box::new(openraft::raft::VoteRequest::new(
            Vote::new(term, 1),
            None,
        )))
    }

    async fn fake_peer(reply: InternalMessage) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = crate::transport::accept(tcp, None).await.unwrap();
            let _frame = conn.read_frame_full().await.unwrap();
            conn.write_frame(&bincode::serialize(&reply).unwrap()).await.unwrap();
        });
        addr
    }

    #[test]
    fn directory_adds_removes_and_lists() {
        let dir = Directory::new();
        dir.update(7, "127.0.0.1:1".into());
        assert_eq!(dir.addr_of(7).as_deref(), Some("127.0.0.1:1"));
        dir.remove(7);
        assert_eq!(dir.addr_of(7), None);
        assert!(dir.all().is_empty());
    }

    #[tokio::test]
    async fn rpc_without_a_known_address_is_unreachable() {
        let mut c = client(Arc::new(Directory::new()), 42);
        let r = c.rpc(vote_payload(1), "vote").await;
        let err = format!("{:?}", r.unwrap_err());
        assert!(err.contains("no address known for node 42"), "{err}");
    }

    #[tokio::test]
    async fn rpc_error_and_unexpected_replies_map_to_network_errors() {
        let dir = Arc::new(Directory::new());
        // A peer that answers with a bare error string.
        let peer = fake_peer(InternalMessage::Response(InternalResponse::Error("boom".into()))).await;
        dir.update(9, peer);
        let mut c = client(dir.clone(), 9);
        let err = format!("{}", c.rpc(vote_payload(2), "vote").await.unwrap_err());
        assert!(err.contains("boom"), "{err}");

        // A peer that answers a vote with something that is not a raft
        // response at all.
        let peer = fake_peer(InternalMessage::Response(InternalResponse::Admin(
            crate::proto::AdminResponse::Pong,
        )))
        .await;
        dir.update(9, peer);
        let mut c = client(dir.clone(), 9);
        let err = format!("{}", c.rpc(vote_payload(3), "vote").await.unwrap_err());
        assert!(err.contains("unexpected response to vote"), "{err}");

        // A peer that answers a vote with the WRONG raft response kind
        // (the mismatch mapping lives in the RaftNetwork trait methods).
        let wrong: InternalMessage = InternalMessage::Response(InternalResponse::Raft(
            RaftResponse::InstallSnapshot(Box::new(openraft::raft::InstallSnapshotResponse {
                vote: Vote::new(1, 1),
            })),
        ));
        let peer = fake_peer(wrong).await;
        dir.update(9, peer);
        let mut c = client(dir.clone(), 9);
        let req = openraft::raft::VoteRequest::new(Vote::new(4, 1), None);
        let err = format!("{}", c.vote(req, RPCOption::new(Duration::from_secs(5))).await.unwrap_err());
        assert!(err.contains("mismatched vote response"), "{err}");
    }

    #[tokio::test]
    async fn append_entries_rejects_mismatched_replies() {
        let dir = Arc::new(Directory::new());
        let wrong: InternalMessage = InternalMessage::Response(InternalResponse::Raft(
            RaftResponse::Vote(Box::new(openraft::raft::VoteResponse::new(
                Vote::new(1, 1),
                None,
                false,
            ))),
        ));
        let peer = fake_peer(wrong).await;
        dir.update(9, peer);
        let mut c = client(dir, 9);
        let req = openraft::raft::AppendEntriesRequest {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        };
        let err = format!(
            "{}",
            c.append_entries(req, RPCOption::new(Duration::from_secs(5)))
                .await
                .unwrap_err()
        );
        assert!(err.contains("mismatched append_entries response"), "{err}");
    }

    #[tokio::test]
    async fn install_snapshot_maps_mismatched_replies() {
        let dir = Arc::new(Directory::new());
        let wrong: InternalMessage = InternalMessage::Response(InternalResponse::Raft(
            RaftResponse::Vote(Box::new(openraft::raft::VoteResponse::new(
                Vote::new(1, 1),
                None,
                false,
            ))),
        ));
        let peer = fake_peer(wrong).await;
        dir.update(9, peer);
        let mut c = client(dir, 9);
        let req = openraft::raft::InstallSnapshotRequest {
            vote: Vote::new(1, 1),
            meta: openraft::SnapshotMeta::default(),
            offset: 0,
            data: Vec::new(),
            done: true,
        };
        let r = c.install_snapshot(req, RPCOption::new(Duration::from_secs(5))).await;
        let err = format!("{:?}", r.unwrap_err());
        assert!(err.contains("mismatched") || err.contains("install_snapshot"), "{err}");
    }

    #[test]
    fn snap_of_maps_remote_errors_to_network_errors() {
        use openraft::error::RemoteError;
        let e: RPCError<u64, openraft::impls::BasicNode, RaftError<u64>> =
            RPCError::RemoteError(RemoteError::new(
                9u64,
                RaftError::Fatal(openraft::error::Fatal::Stopped),
            ));
        let mapped = snap_of(e);
        let err = format!("{mapped:?}");
        assert!(err.contains("snapshot transfer"), "{err}");
    }
}
