//! Per-channel broker state: the `Channel` is the AMQP session — it owns
//! unacked deliveries, prefetch windows, transactions, publisher confirms,
//! and the content-assembly machine (§4.2.6, §4.3).
//!
//! # Content assembly
//!
//! After a content-carrying method (`Basic.Publish`/`Get-Ok`/`Deliver`/
//! `Return`), §4.2.6 requires exactly one header frame then the announced
//! number of body bytes. `pending_content` tracks this; any other method
//! frame implicitly aborts the content ("any non-content frame explicitly
//! marks the end of the content").
//!
//! # Outbound deliveries
//!
//! `basic.deliver`/`basic.return` frames are written by a dedicated writer
//! task fed through [`Channel::outbound`]; delivery tags are allocated by
//! the channel, credits flow back to the shard group as
//! `Credit`/`Ack` commands.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;
use switchboard_cluster::BrokerReply;
use switchboard_core::error::BrokerError;
use switchboard_core::model::ConnectionId;
use switchboard_core::model::StoredMessage;
use switchboard_core::model::SubscriptionId;
use switchboard_core::shard::ShardReply;
use switchboard_core::topology::GroupId;
use switchboard_wire::constants::reply;
use switchboard_wire::method::Method;
use switchboard_wire::properties::ContentHeader;
use switchboard_wire::BasicProperties;
use tokio::sync::mpsc;

use crate::outbound::OutboundFrame;

/// Server capability/behaviour knobs negotiated per connection.
#[derive(Debug, Clone)]
pub struct ConnectionLimits {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    /// The client's IP, stamped per connection by the listener; None for
    /// transports that cannot see one. Drives the guest-loopback rule.
    pub peer_ip: Option<std::net::IpAddr>,
    /// Allow the well-known `guest` user from non-loopback addresses.
    pub allow_remote_guest: bool,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        ConnectionLimits {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
            peer_ip: None,
            allow_remote_guest: false,
        }
    }
}

/// One consumer registered by this channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConsumer {
    pub tag: String,
    pub queue: String,
    pub sub: SubscriptionId,
    pub shard: GroupId,
    pub no_ack: bool,
    pub exclusive: bool,
    /// Prefetch for this consumer (0 = unlimited).
    pub prefetch: u32,
    pub flow_active: bool,
}

/// An outstanding (unacked) delivery on this channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unacked {
    pub queue: String,
    pub shard: GroupId,
    pub seq: u64,
    pub sub: Option<SubscriptionId>,
    pub consumer_tag: Option<String>,
}

/// A buffered operation inside a server-local transaction (§2.2.9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxOp {
    /// Publish routed to a concrete queue (content already assembled).
    /// `confirm_seq` carries the publisher-confirm number allocated at
    /// publish time (None outside confirm mode); confirms fire at commit.
    Publish {
        queue: String,
        shard: GroupId,
        message: StoredMessage,
        confirm_seq: Option<u64>,
    },
    Ack { queue: String, shard: GroupId, seq: u64 },
}

/// Coordinator id for transactions started by this connection.
pub type TxId = (u64, u64);

/// Mutable channel state shared between the reader loop and delivery
/// writers. All access is through the mutex; operations are quick.
pub struct ChannelInner {
    pub open: bool,
    pub conn: ConnectionId,
    pub node_id: u64,

    /// Content assembly (§4.2.6).
    pub pending_method: Option<Method>,
    pub pending_header: Option<ContentHeader>,
    pub pending_body: Vec<u8>,

    /// Delivery-tag allocator (per channel, starts at 1).
    pub next_delivery_tag: u64,

    /// Publisher-confirm sequence (per channel, starts at 1). AMQP
    /// confirms number *publishes* in their own sequence — sharing the
    /// delivery-tag counter would orphan client-side confirm tracking
    /// the moment a basic.get or consumer delivery consumed a tag.
    pub confirm_seq: u64,
    /// delivery-tag → what to do when acked.
    pub unacked: BTreeMap<u64, Unacked>,

    /// QoS (§3.1.7): applied to new consumers (global=false) or the whole
    /// channel's refill accounting (global=true).
    pub prefetch_size: u64,
    pub prefetch_count: u32,
    pub prefetch_global: bool,

    /// Consumers started on this channel.
    pub consumers: BTreeMap<String, LocalConsumer>,
    /// Sub id → shard for credit bookkeeping.
    pub sub_shards: BTreeMap<SubscriptionId, GroupId>,
    /// Flow control (channel.flow, §3.1.9).
    pub flow_active: bool,

    /// Transactions (tx class).
    pub tx: Option<TxState>,

    /// Publisher confirms (confirm class; deployed extension).
    pub confirm: Option<ConfirmState>,

    pub vhost: String,
    pub limits: ConnectionLimits,
}

/// Server-local transaction state.
#[derive(Debug, Default)]
pub struct TxState {
    pub ops: Vec<TxOp>,
    /// Acks of already-delivered messages must survive rollback as "still
    /// unacked" (rollback does not requeue, §2.2.9) — buffered acks are
    /// simply dropped with the buffer on rollback.
    pub counter: u64,
}

/// Publisher-confirm state: per-channel monotonically increasing
/// publish-sequence numbers, acked once every routed queue has persisted
/// the message.
#[derive(Debug, Default)]
pub struct ConfirmState {
    pub next_publish_seq: u64,
    /// publish-seq → number of queue enqueues still outstanding.
    pub outstanding: BTreeMap<u64, usize>,
    /// publish-seq of the message being assembled with the current publish
    /// method (used to file the ack when the enqueue replies land).
    pub pending_publish: Option<u64>,
    /// unroutable-and-mandatory messages awaiting their return.
    pub pending_returns: VecDeque<(u64, String, String)>, // (seq, exchange, routing-key)
}

impl ChannelInner {
    pub fn new(conn: ConnectionId, node_id: u64, vhost: String, limits: ConnectionLimits) -> Self {
        ChannelInner {
            open: true,
            conn,
            node_id,
            pending_method: None,
            pending_header: None,
            pending_body: Vec::new(),
            next_delivery_tag: 1,
            confirm_seq: 0,
            unacked: BTreeMap::new(),
            prefetch_size: 0,
            prefetch_count: 0,
            prefetch_global: false,
            consumers: BTreeMap::new(),
            sub_shards: BTreeMap::new(),
            flow_active: true,
            tx: None,
            confirm: None,
            vhost,
            limits,
        }
    }

    pub fn allocate_delivery_tag(&mut self) -> u64 {
        let t = self.next_delivery_tag;
        self.next_delivery_tag += 1;
        t
    }

    pub fn next_sub_id(&self, counter: u64) -> SubscriptionId {
        SubscriptionId { node: self.node_id, sub: counter }
    }

    /// Content assembly accepted a whole message?
    pub fn take_content(&mut self) -> Option<(Method, BasicProperties, Vec<u8>)> {
        let method = self.pending_method.take()?;
        let header = self.pending_header.take()?;
        let body = std::mem::take(&mut self.pending_body);
        let props = header.properties.clone();
        Some((method, props, body))
    }

    pub fn unacked_count(&self) -> u64 {
        self.unacked.len() as u64
    }
}

/// Errors a channel handler can produce, mapped onto §4.8 exceptions.
pub type ChannelResult<T> = Result<T, BrokerError>;

/// Shared channel handle. `id` and `outbound` live outside the lock: they
/// are immutable per channel, and reply paths must never re-take the inner
/// lock (std `Mutex` is not reentrant).
#[derive(Clone)]
pub struct Channel {
    pub id: u16,
    pub outbound: mpsc::UnboundedSender<OutboundFrame>,
    pub inner: Arc<Mutex<ChannelInner>>,
    /// Sub-id allocation (connection-scoped monotonic).
    pub sub_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl Channel {
    pub fn new(
        id: u16,
        conn: ConnectionId,
        node_id: u64,
        vhost: String,
        limits: ConnectionLimits,
        outbound: mpsc::UnboundedSender<OutboundFrame>,
    ) -> Self {
        Channel {
            id,
            outbound,
            inner: Arc::new(Mutex::new(ChannelInner::new(
                conn, node_id, vhost, limits,
            ))),
            sub_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Queue a reply method without touching the inner lock.
    pub fn reply(&self, m: Method) {
        let _ = self.outbound.send(OutboundFrame::Method { channel: self.id, method: m });
    }

    /// Map a shard reply onto either success or a broker exception.
    pub fn shard_reply(reply: BrokerReply) -> ChannelResult<ShardReply> {
        match reply {
            BrokerReply::Shard(r) => Ok(r),
            BrokerReply::Error(e) => Err(e),
            other => Err(BrokerError::resource_error(format!(
                "unexpected reply: {other:?}"
            ))),
        }
    }

    pub fn meta_reply(reply: BrokerReply) -> ChannelResult<switchboard_core::topology::MetaReply> {
        match reply {
            BrokerReply::Meta(r) => Ok(r),
            BrokerReply::Error(e) => Err(e),
            other => Err(BrokerError::resource_error(format!(
                "unexpected reply: {other:?}"
            ))),
        }
    }

    /// Which fields of a Queue.Declare must match for equivalence (§4.8's
    /// assertion model)? Queue-level: passive declares never compare.
    pub fn reply_code_of(e: &BrokerError) -> u16 {
        e.code
    }

    /// Basic.Consume consumer tag: server-generated when the client leaves
    /// the field empty (`ct-<n>`), verbatim otherwise.
    pub fn consumer_tag_for(requested: &str, n: &mut u64) -> String {
        if requested.is_empty() {
            let t = format!("ct-{n}");
            *n += 1;
            t
        } else {
            requested.to_string()
        }
    }

    /// Compute the credit to grant a fresh consumer given its (or the
    /// channel's, when global) prefetch count. Unlimited consumers pull in
    /// bounded batches to keep flow control meaningful.
    pub fn initial_credit(prefetch: u32) -> u32 {
        if prefetch == 0 {
            1000
        } else {
            prefetch
        }
    }

    /// Credit refill after `acked` messages were acknowledged.
    pub fn refill(prefetch: u32, acked: u32) -> u32 {
        if prefetch == 0 {
            // Unlimited: re-credit the standing batch.
            Channel::initial_credit(0)
        } else {
            acked
        }
    }

    /// Basic.Ack with `multiple`: everything up to and including `tag`.
    pub fn multiple_range(unacked: &BTreeMap<u64, Unacked>, tag: u64) -> Vec<u64> {
        unacked.range(..=tag).map(|(t, _)| *t).collect()
    }
}

/// Convert a broker error into the wire-level Close pair.
pub fn close_for(e: &BrokerError) -> (u16, String, u16, u16) {
    (e.code, e.text.clone(), e.class_id, e.method_id)
}

/// Reply-code text used by Basic.Return for unroutable mandatory messages
/// (§3.1.2.1: "the exchange may drop it silently or return it").
pub fn return_text_no_route() -> String {
    "NO_ROUTE".to_string()
}

/// Delivery-mode extraction for persistence decisions ("Persistent",
/// §1.4.3): delivery-mode 2 means persistent.
pub fn is_persistent(props: &BasicProperties) -> bool {
    props.delivery_mode == Some(2)
}

/// Build a StoredMessage from assembled content + publish coordinates.
pub fn stored_message(
    exchange: String,
    routing_key: String,
    props: BasicProperties,
    body: Vec<u8>,
) -> StoredMessage {
    StoredMessage {
        persistent: is_persistent(&props),
        properties: props,
        body,
        exchange,
        routing_key,
    }
}

/// Validate that a method may appear on a connection-level channel (§4.2.3:
/// channel 0 is connection-class only).
pub fn method_allowed_on_channel_zero(m: &Method) -> bool {
    m.class_id() == switchboard_wire::method::CONNECTION_CLASS_ID
}

/// Is `m` one of the four content-carrying methods (§4.2.6)?
pub fn expects_content(m: &Method) -> bool {
    m.carries_content()
}

/// Error when a frame arrives for an unknown channel (§4.3: channel error).
pub fn unknown_channel(id: u16) -> BrokerError {
    BrokerError::channel_error(format!("unknown channel {id}"))
}

/// 504 for a second Channel.Open on an open channel (§4.8: structural).
pub fn channel_already_open(id: u16) -> BrokerError {
    BrokerError::channel_error(format!("channel {id} already open"))
}

/// 504 for content on a channel that is not expecting it.
pub fn unexpected_content() -> BrokerError {
    BrokerError {
        code: reply::UNEXPECTED_FRAME,
        text: "content without a content-bearing method".into(),
        level: switchboard_core::error::Level::Connection,
        class_id: 0,
        method_id: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel_for_test() -> Channel {
        let (tx, _rx) = mpsc::unbounded_channel();
        Channel::new(
            1,
            ConnectionId { node: 1, conn: 1 },
            1,
            "/".into(),
            ConnectionLimits::default(),
            tx,
        )
    }

    #[test]
    fn delivery_tags_start_at_one_and_increment() {
        let inner = channel_for_test();
        let mut inner = inner.inner.lock().unwrap();
        assert_eq!(inner.allocate_delivery_tag(), 1);
        assert_eq!(inner.allocate_delivery_tag(), 2);
    }

    #[test]
    fn consumer_tag_generation() {
        let mut n = 1u64;
        assert_eq!(Channel::consumer_tag_for("", &mut n), "ct-1");
        assert_eq!(Channel::consumer_tag_for("", &mut n), "ct-2");
        assert_eq!(Channel::consumer_tag_for("mine", &mut n), "mine");
    }

    #[test]
    fn credit_rules() {
        // Unlimited consumers pull bounded batches.
        assert_eq!(Channel::initial_credit(0), 1000);
        assert_eq!(Channel::initial_credit(5), 5);
        // Refill: unlimited keeps the batch; windowed gets exactly the acks.
        assert_eq!(Channel::refill(0, 3), 1000);
        assert_eq!(Channel::refill(10, 3), 3);
    }

    #[test]
    fn multiple_ack_range_is_inclusive() {
        let mut unacked = BTreeMap::new();
        for t in [1u64, 2, 5] {
            unacked.insert(
                t,
                Unacked {
                    queue: "q".into(),
                    shard: 1,
                    seq: t,
                    sub: None,
                    consumer_tag: None,
                },
            );
        }
        let mut tags = Channel::multiple_range(&unacked, 5);
        tags.sort_unstable();
        assert_eq!(tags, vec![1, 2, 5]);
        let tags = Channel::multiple_range(&unacked, 2);
        assert_eq!(tags, vec![1, 2]);
    }

    #[test]
    fn persistence_flag_follows_delivery_mode() {
        let mut p = BasicProperties::new();
        assert!(!is_persistent(&p));
        p.delivery_mode = Some(1);
        assert!(!is_persistent(&p));
        p.delivery_mode = Some(2);
        assert!(is_persistent(&p));
    }

    #[test]
    fn stored_message_carries_coordinates() {
        let m = stored_message(
            "amq.direct".into(),
            "rk".into(),
            BasicProperties::new(),
            b"body".to_vec(),
        );
        assert_eq!(m.exchange, "amq.direct");
        assert_eq!(m.routing_key, "rk");
        assert!(!m.persistent);
    }

    #[test]
    fn connection_class_on_channel_zero_only() {
        let m = Method::ChannelOpen { out_of_band: String::new() };
        assert!(!method_allowed_on_channel_zero(&m));
        let m = Method::ConnectionClose {
            reply_code: 200,
            reply_text: String::new(),
            class_id: 0,
            method_id: 0,
        };
        assert!(method_allowed_on_channel_zero(&m));
    }

    #[test]
    fn content_expectations_follow_the_registry() {
        let publish = Method::BasicPublish {
            ticket: 0,
            exchange: String::new(),
            routing_key: String::new(),
            mandatory: false,
            immediate: false,
        };
        assert!(expects_content(&publish));
        let ack = Method::BasicAck { delivery_tag: 1, multiple: false };
        assert!(!expects_content(&ack));
    }

    #[test]
    fn channel_zero_violations_are_connection_level() {
        assert_eq!(unknown_channel(9).code, 504);
        assert_eq!(channel_already_open(3).code, 504);
        let e = unexpected_content();
        assert_eq!(e.code, 505);
        assert_eq!(e.level, switchboard_core::error::Level::Connection);
    }

    #[test]
    fn close_pair_carries_code_text_and_method() {
        let e = BrokerError::not_found("no exchange 'x'").for_method(40, 20);
        let (code, text, class, method) = close_for(&e);
        assert_eq!((code, class, method), (404, 40, 20));
        assert_eq!(text, "no exchange 'x'");
    }

    #[test]
    fn content_assembly_take() {
        let ch = channel_for_test();
        let mut inner = ch.inner.lock().unwrap();
        inner.pending_method = Some(Method::BasicPublish {
            ticket: 0,
            exchange: "".into(),
            routing_key: "k".into(),
            mandatory: true,
            immediate: false,
        });
        let mut props = BasicProperties::new();
        props.delivery_mode = Some(2);
        inner.pending_header = Some(ContentHeader::new(4, props));
        inner.pending_body = b"body".to_vec();

        let (m, props, body) = inner.take_content().unwrap();
        assert!(matches!(m, Method::BasicPublish { mandatory: true, .. }));
        assert_eq!(props.delivery_mode, Some(2));
        assert_eq!(body, b"body");
        assert!(inner.pending_method.is_none());
        assert!(inner.take_content().is_none());
    }
}

#[cfg(test)]
mod reply_arms {
    use super::*;

    #[test]
    fn shard_reply_maps_error_and_unexpected() {
        let ok = Channel::shard_reply(BrokerReply::Shard(ShardReply::Ok)).unwrap();
        assert!(matches!(ok, ShardReply::Ok));
        let err = Channel::shard_reply(BrokerReply::Error(BrokerError::not_found("x"))).unwrap_err();
        assert_eq!(err.code, 404);
        let bad = Channel::shard_reply(BrokerReply::Bootstrapped).unwrap_err();
        assert!(bad.code >= 500);
    }

    #[test]
    fn meta_reply_maps_ok_and_error() {
        let ok = Channel::meta_reply(BrokerReply::Meta(switchboard_core::topology::MetaReply::Ok)).unwrap();
        assert!(matches!(ok, switchboard_core::topology::MetaReply::Ok));
        let err = Channel::meta_reply(BrokerReply::Error(BrokerError::not_found("v"))).unwrap_err();
        assert_eq!(err.code, 404);
        let bad = Channel::meta_reply(BrokerReply::Bootstrapped).unwrap_err();
        assert!(bad.code >= 500);
    }

    #[test]
    fn reply_code_of_extracts_code() {
        assert_eq!(
            Channel::reply_code_of(&BrokerError::not_found("x")),
            404
        );
    }
}
