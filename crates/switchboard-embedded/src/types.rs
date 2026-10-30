//! The embedded client's data types: delivered messages, returns, publish
//! and confirm outcomes, and queue state.
//!
//! The client's [`Delivery`] is deliberately UNRELATED to
//! `switchboard_cluster::Delivery` (the node-internal effect payload,
//! cluster/src/lib.rs:25), which is not part of this surface.

use switchboard_wire::BasicProperties;

/// A message delivered to this client (by [`get`](crate::EmbeddedChannel::get)
/// or a [`Consumer`](crate::Consumer)).
///
/// The AMQP content format is the durable contract, so the wire data types
/// are re-exported under crate aliases: [`MessageProperties`] is
/// `switchboard_wire::BasicProperties`.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The consumer that received this message; empty for
    /// [`get`](crate::EmbeddedChannel::get).
    pub consumer_tag: String,
    /// Channel-scoped delivery tag used for ack/nack.
    pub delivery_tag: u64,
    /// True when this is a redelivery (nack-requeue, hold release,
    /// restart redelivery).
    pub redelivered: bool,
    /// The exchange the publisher addressed.
    pub exchange: String,
    /// The routing key the publisher used.
    pub routing_key: String,
    /// The message's content-header properties.
    pub properties: MessageProperties,
    /// The opaque body octets.
    pub body: Vec<u8>,
}

/// A mandatory-unroutable message returned by the broker (basic.return,
/// methods/publish.rs:213-234), retrieved from
/// [`EmbeddedConnection::take_returns`](crate::EmbeddedConnection::take_returns).
#[derive(Debug, Clone)]
pub struct ReturnedMessage {
    /// Reply code (312 NO_ROUTE / NO_CONSUMERS).
    pub code: u16,
    /// Reply text (e.g. `"NO_ROUTE"`).
    pub text: String,
    /// The exchange the publisher addressed.
    pub exchange: String,
    /// The routing key the publisher used.
    pub routing_key: String,
    /// The message's content-header properties.
    pub properties: MessageProperties,
    /// The opaque body octets.
    pub body: Vec<u8>,
}

/// Outcome of a mandatory publish
/// ([`publish_mandatory`](crate::EmbeddedChannel::publish_mandatory)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The message routed to at least one destination (or was silently
    /// dropped as unroutable-uninteresting — mandatory=false semantics do
    /// not apply here: `publish_mandatory` always sets the mandatory flag).
    Routed,
    /// Unroutable: the broker returned the message; it was not enqueued.
    Returned { code: u16, text: String },
}

/// Outcome of a confirmed publish
/// ([`publish_confirmed`](crate::EmbeddedChannel::publish_confirmed)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmOutcome {
    /// The broker acked the publish sequence: every routed destination has
    /// persisted the message (methods/publish.rs:149-151).
    Acked,
    /// Unroutable: the message was returned instead of enqueued, and the
    /// broker confirmed it (methods/publish.rs:83-99) — it will NOT be
    /// delivered.
    Returned { code: u16, text: String },
}

/// State of a declared queue, from `Queue.Declare-Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueState {
    /// Final queue name (server-named queues are `amq.gen-…`).
    pub queue: String,
    /// Ready (not held) message count reported by the owning shard.
    pub message_count: u32,
    /// Active consumer count.
    pub consumer_count: u32,
}

/// Stable alias for the AMQP Basic-class content-header property set
/// (`switchboard_wire::BasicProperties`). The AMQP message format is the
/// stable contract, so this wire data type is safe to expose.
pub type MessageProperties = BasicProperties;

/// Stable alias for the AMQP field table used in declare/bind/consume
/// arguments (`switchboard_wire::field::FieldTable`).
pub type Arguments = switchboard_wire::field::FieldTable;
