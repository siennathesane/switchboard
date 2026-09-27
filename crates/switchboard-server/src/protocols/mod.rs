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

pub mod amqp10;
pub mod detect;
mod gateway;
pub mod http;
pub mod mqtt;
pub mod shared;
pub mod stomp;
pub mod ws;

pub use detect::{classify, Detected};
pub use gateway::serve_client;
pub use shared::BridgeContext;
pub use shared::ProtocolConfig;
