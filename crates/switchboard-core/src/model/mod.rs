//! AMQ model entities (§2.1.1): exchanges, queues, bindings, messages.
//!
//! All types are `serde`-serializable: they travel inside raft log entries
//! and live inside the replicated state machines.

use serde::{Deserialize, Serialize};
use switchboard_wire::field::{FieldValue, FieldTable};

/// The four standard exchange types (§3.1.3). The optional `system` type of
/// §3.1.3.5 is not implemented; unknown types are rejected with 540
/// (NOT_IMPLEMENTED) at declare time, as §3.1.3.6 reserves non-`x-` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// §3.1.3.1: route when the binding key equals the routing key.
    Direct,
    /// §3.1.3.2: route to every bound queue unconditionally.
    Fanout,
    /// §3.1.3.3: route when the key matches the binding pattern.
    Topic,
    /// §3.1.3.4: route on the message's `headers` property.
    Headers,
}

impl ExchangeKind {
    /// Parse the `type` argument of Exchange.Declare.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "direct" => Some(ExchangeKind::Direct),
            "fanout" => Some(ExchangeKind::Fanout),
            "topic" => Some(ExchangeKind::Topic),
            "headers" => Some(ExchangeKind::Headers),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeKind::Direct => "direct",
            ExchangeKind::Fanout => "fanout",
            ExchangeKind::Topic => "topic",
            ExchangeKind::Headers => "headers",
        }
    }
}

/// An exchange instance (§2.1.1.2): a named matching/routing engine that
/// never stores messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    /// Empty name is the default exchange (§3.1.3.1).
    pub name: String,
    pub kind: ExchangeKind,
    /// Survives server restart (§1.4.3 "Durable").
    pub durable: bool,
    /// Deleted when its last binding is removed.
    pub auto_delete: bool,
    /// Internal exchanges accept no publishes.
    pub internal: bool,
    /// Declare arguments; compared on re-declare for 406 assertions.
    pub arguments: FieldTable,
}

/// Queue properties selected at declare time (§2.1.4.1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueOptions {
    pub durable: bool,
    /// Owned by one connection; deleted when it closes.
    pub exclusive: bool,
    /// Deleted when the last consumer detaches.
    pub auto_delete: bool,
    pub arguments: FieldTable,
}

/// Queue metadata, replicated in the meta group. Message contents live in
/// the owning shard group's state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    pub options: QueueOptions,
    /// For exclusive queues: the owning connection (node, conn id).
    pub owner: Option<ConnectionId>,
    /// Shard group that holds this queue's messages.
    pub shard: u32,
}

/// Identifies one client connection cluster-wide: the node serving it plus
/// that node's local connection counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectionId {
    pub node: u64,
    pub conn: u64,
}

/// A binding (§2.1.1): the routing criterion connecting a queue to an
/// exchange.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub exchange: String,
    pub queue: String,
    /// Routing key (direct/topic) or pattern (topic); unused by fanout/headers.
    pub routing_key: String,
    /// Binding arguments (headers match table incl. `x-match`).
    pub arguments: FieldTable,
}

/// A message body as stored in a queue: the content header properties plus
/// the opaque body octets. "The server MUST NOT modify message content
/// bodies" (§3.1.1) — we store them verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub properties: switchboard_wire::BasicProperties,
    pub body: Vec<u8>,
    /// The exchange the publisher addressed (replayed in Deliver/Get-Ok).
    pub exchange: String,
    /// The routing key the publisher used.
    pub routing_key: String,
    /// True when delivery-mode was 2 ("Persistent", §1.4.3).
    pub persistent: bool,
}

impl StoredMessage {
    /// Delivery order payload size (header + body), useful for limits.
    pub fn size(&self) -> u64 {
        self.body.len() as u64
    }
}

/// Consumer subscription identity, stable across the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SubscriptionId {
    /// Node that hosts the client channel.
    pub node: u64,
    /// Per-node monotonic counter.
    pub sub: u64,
}

/// One standard queue name generator: server-named queues are `amq.gen-`
/// followed by 22 random base-64-ish characters (RabbitMQ-compatible shape,
/// §3.1.10 reserves the `amq.` prefix for the server).
pub fn generate_queue_name() -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let suffix: String = (0..22)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("amq.gen-{suffix}")
}

/// Binding identity: the spec allows binding the same (exchange, queue,
/// key, args) set; we key bindings by their full tuple, so an exact
/// re-bind is idempotent.
pub fn binding_key(b: &Binding) -> String {
    // Deterministic serialization: exchange | queue | key | sorted arg names.
    let mut arg_names: Vec<&str> = b.arguments.iter().map(|(k, _)| k.as_str()).collect();
    arg_names.sort_unstable();
    format!(
        "{}|{}|{}|{}",
        b.exchange,
        b.queue,
        b.routing_key,
        arg_names.join(",")
    )
}

/// Convenience constructors for tests and bootstrap.
pub fn exchange(name: &str, kind: ExchangeKind, durable: bool) -> Exchange {
    Exchange {
        name: name.to_string(),
        kind,
        durable,
        auto_delete: false,
        internal: false,
        arguments: FieldTable::new(),
    }
}

/// Extract the boolean value of a flag argument from a table.
pub fn flag_arg(args: &FieldTable, name: &str) -> Option<bool> {
    match args.get(name) {
        Some(FieldValue::Boolean(b)) => Some(*b),
        _ => None,
    }
}

#[cfg(test)]
mod kind_tests {
    use super::*;

    #[test]
    fn from_str_accepts_all_kinds_and_rejects_other() {
        assert_eq!(ExchangeKind::from_str("direct"), Some(ExchangeKind::Direct));
        assert_eq!(ExchangeKind::from_str("fanout"), Some(ExchangeKind::Fanout));
        assert_eq!(ExchangeKind::from_str("topic"), Some(ExchangeKind::Topic));
        assert_eq!(ExchangeKind::from_str("headers"), Some(ExchangeKind::Headers));
        assert_eq!(ExchangeKind::from_str("banana"), None);
    }

    #[test]
    fn as_str_roundtrips_from_str() {
        for kind in [
            ExchangeKind::Direct,
            ExchangeKind::Fanout,
            ExchangeKind::Topic,
            ExchangeKind::Headers,
        ] {
            assert_eq!(ExchangeKind::from_str(kind.as_str()), Some(kind));
        }
    }

    #[test]
    fn flag_arg_reads_booleans_only() {
        let mut t = FieldTable::new();
        t.insert("flag", FieldValue::Boolean(true));
        t.insert("num", FieldValue::SignedInt(3));
        assert_eq!(crate::model::flag_arg(&t, "flag"), Some(true));
        assert_eq!(crate::model::flag_arg(&t, "num"), None);
        assert_eq!(crate::model::flag_arg(&t, "missing"), None);
    }
}
