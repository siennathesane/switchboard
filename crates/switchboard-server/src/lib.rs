//! # switchboard-server
//!
//! The AMQP 0-9-1 front end: TCP/TLS listeners, the connection handshake,
//! per-channel state machines, heartbeats, and the method handlers that
//! bridge wire methods onto the cluster.
//!
//! Layering (bottom-up): [`switchboard_wire`] (bytes ↔ methods) →
//! [`switchboard_core`] (model semantics) → [`switchboard_cluster`]
//! (replication and forwarding) → this crate (sessions).
//!
//! Notable conformance points implemented here:
//! * protocol-header validation and the reject path (§4.2.2),
//! * negotiation to the lowest agreed limits (§2.3.3),
//! * channel-0/class-0 frame rules (§4.2.3),
//! * content assembly with the §4.2.6 framing rules,
//! * heartbeats with the §4.2.7 send/monitor discipline,
//! * Close/Close-Ok handshaking (§2.3.7),
//! * channel teardown redelivering unacked messages (§4.5).

pub mod channel;
pub mod listener;
pub mod methods;
pub mod protocols;
pub mod outbound;
pub mod session;
pub mod tls;

pub use channel::{Channel, ChannelInner, ConnectionLimits, LocalConsumer, Unacked};
pub use protocols::ProtocolConfig;
pub use session::serve;
