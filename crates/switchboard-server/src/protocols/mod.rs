//! Client-port protocol gateway.
//!
//! One client listener carries every supported wire protocol, selected by
//! sniffing the first bytes of the connection (the same trick RabbitMQ's
//! direct listener uses):
//!
//! | first bytes                                   | protocol |
//! |-----------------------------------------------|----------|
//! | `AMQP\0\0\9\1`                                | AMQP 0-9-1 (this crate's primary session) |
//! | `AMQP\0\1\0\0`                                | AMQP 1.0 (minimal bridge) |
//! | `0x10 …` (MQTT CONNECT)                       | MQTT 3.1.1 |
//! | `STOMP\n` / `CONNECT\n`                       | STOMP |
//! | `GET /…` (+ `Upgrade: websocket`)             | WebSocket (→ inner protocol) or HTTP health |
//!
//! Everything except plain HTTP then runs to completion on the same
//! socket; TLS wraps the socket *before* sniffing, so every protocol is
//! available with and without TLS.

pub mod admin;
pub mod amqp10;
pub mod connstats;
pub mod detect;
mod gateway;
pub mod http;
pub mod metrics;
pub mod mqtt;
pub mod shared;
pub mod stomp;
pub mod ws;

pub use detect::Detected;
pub use detect::classify;
pub use gateway::serve_client;
pub use shared::BridgeContext;
pub use shared::ProtocolConfig;

/// Defensive ceiling on a single *pre-authentication* frame for the
/// bridged protocols: the STOMP body announced by `content-length`, the
/// AMQP 1.0 frame size declared in the transport header, and the MQTT
/// remaining-length (code-scanning alerts #10/#11/#12).
///
/// The native AMQP 0-9-1 session enforces the configured/negotiated
/// `ConnectionLimits::frame_max` (default 128 KiB) on every read, but
/// each bridge reads its first wire frame — STOMP `CONNECT`, the AMQP 1.0
/// pre-SASL frame, the MQTT `CONNECT` — *before any credentials have
/// been verified*, so on that path a peer-controlled length must never
/// drive the allocation. Eight MiB sits far above any legitimate
/// handshake frame (the AMQP 0-9-1 default frame budget is 128 KiB) and
/// far below memory-exhaustion territory, and gives one consistent,
/// documented bound for every bridge.
pub(crate) const PREAUTH_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
