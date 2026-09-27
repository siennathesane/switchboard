//! # switchboard-wire
//!
//! AMQP 0-9-1 wire-level codec, implemented from the protocol specification
//! (`docs/amqp0-9-1.pdf`, chapter 4 "Technical Specifications" and the
//! normative method registry it defers to in §3.2.2).
//!
//! The crate is deliberately transport-agnostic: it turns bytes into frames,
//! frames into methods/content, and back. Connection and channel state
//! machines live in `switchboard-server`.
//!
//! Layout mirrors the spec:
//! * [`constants`] — frame types, protocol constants, reply codes (§4.2, §4.8).
//! * [`field`] — field tables and the field-value grammar of §4.2.1.
//! * [`method`] — every class/method of 0-9-1 with its exact argument list.
//! * [`properties`] — the Basic-class content header property list (§4.2.6).
//! * [`frame`] — general frame format, protocol header, heartbeats (§4.2.2–§4.2.7).

pub mod constants;
pub mod error;
pub mod field;
pub mod frame;
pub mod method;
pub mod properties;
pub mod wireio;

pub use error::CodecError;
pub use field::{FieldValue, FieldTable};
pub use constants::FrameType;
pub use frame::{parse_protocol_header, Frame, FrameReader, PROTOCOL_HEADER};
pub use method::{
    BASIC_CLASS_ID, CHANNEL_CLASS_ID, CONFIRM_CLASS_ID, CONNECTION_CLASS_ID, EXCHANGE_CLASS_ID,
    Method, QUEUE_CLASS_ID, TX_CLASS_ID,
};
pub use properties::{BasicProperties, ContentHeader};

/// The protocol version this implementation speaks: 0-9-1.
pub const VERSION_MAJOR: u8 = 0;
pub const VERSION_MINOR: u8 = 9;
pub const VERSION_REVISION: u8 = 1;
