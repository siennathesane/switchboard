//! The complete AMQP 0-9-1 method set: every class, method, argument, and
//! argument order, defined in one declarative table (§3.2.2 / §4.2.4).
//!
//! Argument types map one-to-one onto the native data fields of §4.2.5:
//!
//! | wire type      | Rust type    |
//! |----------------|--------------|
//! | octet          | `u8`         |
//! | short-uint     | `u16`        |
//! | long-uint      | `u32`        |
//! | long-long-uint | `u64`        |
//! | bit            | `bool`       |
//! | short-string   | `String`     |
//! | long-string    | `Vec<u8>`    |
//! | field-table    | `FieldTable` |
//!
//! Because the map is injective, encode/decode can be derived from the Rust
//! types alone, and the conformance test in
//! `tests/registry_conformance.rs` checks this table, byte for byte, against
//! the normative method registry.

use serde::{Deserialize, Serialize};

use crate::error::CodecError;
use crate::field::FieldTable;
use crate::wireio::{Decoder, Encoder};

/// Class 10: connection.
pub const CONNECTION_CLASS_ID: u16 = 10;
/// Class 20: channel.
pub const CHANNEL_CLASS_ID: u16 = 20;
/// Class 40: exchange.
pub const EXCHANGE_CLASS_ID: u16 = 40;
/// Class 50: queue.
pub const QUEUE_CLASS_ID: u16 = 50;
/// Class 60: basic.
pub const BASIC_CLASS_ID: u16 = 60;
/// Class 90: tx.
pub const TX_CLASS_ID: u16 = 90;
/// Class 85: confirm (publisher confirmations; the deployed extension for
/// tracking asynchronous success, complementing §2.2.3's "no confirmations").
pub const CONFIRM_CLASS_ID: u16 = 85;

/// A method argument: the Rust/wire type pairing of the table above.
pub trait AmqpArg: Sized {
    fn encode_arg(&self, e: &mut Encoder);
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError>;
}

impl AmqpArg for u8 {
    fn encode_arg(&self, e: &mut Encoder) { e.u8(*self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.u8() }
}
impl AmqpArg for u16 {
    fn encode_arg(&self, e: &mut Encoder) { e.u16(*self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.u16() }
}
impl AmqpArg for u32 {
    fn encode_arg(&self, e: &mut Encoder) { e.u32(*self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.u32() }
}
impl AmqpArg for u64 {
    fn encode_arg(&self, e: &mut Encoder) { e.u64(*self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.u64() }
}
impl AmqpArg for bool {
    fn encode_arg(&self, e: &mut Encoder) { e.bit(*self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.bit() }
}
impl AmqpArg for String {
    fn encode_arg(&self, e: &mut Encoder) { e.short_str(self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.short_str() }
}
impl AmqpArg for Vec<u8> {
    fn encode_arg(&self, e: &mut Encoder) { e.long_str(self); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { d.long_str() }
}
impl AmqpArg for FieldTable {
    fn encode_arg(&self, e: &mut Encoder) { self.encode(e); }
    fn decode_arg(d: &mut Decoder<'_>) -> Result<Self, CodecError> { FieldTable::decode(d) }
}

macro_rules! define_methods {
    ( $( $class_id:literal, $class_const:ident, $class_name:literal => {
        $( $method_id:literal, $variant:ident, $sync:expr, $content:expr,
           [ $( $field:ident : $ty:ty ),* $(,)? ] ),* $(,)?
    } ),* $(,)? ) => {
        /// Every method of the implemented protocol dialect.
        ///
        /// The six standard classes (connection, channel, exchange, queue,
        /// basic, tx) follow §3.2.2 exactly; `confirm` and `basic.nack` /
        /// `connection.blocked` are the ubiquitous deployment extensions,
        /// included because real-world clients negotiate them.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[non_exhaustive]
        pub enum Method {
            $(
                $(
                    $variant { $( $field: $ty ),* }
                ),*
                // Trailing comma closes the gap between the last variant of
                // one class and the first of the next.
                ,
            )*
        }

        impl Method {
            $(
                #[doc = concat!("Class id of the `", $class_name, "` class.")]
                pub const $class_const: u16 = $class_id;
            )*

            /// Class id of this method (§4.2.4).
            pub fn class_id(&self) -> u16 {
                match self { $( $( Self::$variant { .. } => $class_id, )* )* }
            }

            /// Method id within its class (§4.2.4).
            pub fn method_id(&self) -> u16 {
                match self { $( $( Self::$variant { .. } => $method_id, )* )* }
            }

            /// Human-readable name, e.g. `"QueueDeclare"` for logs and
            /// error texts.
            pub fn name(&self) -> &'static str {
                match self { $( $( Self::$variant { .. } => stringify!($variant), )* )* }
            }

            /// True for synchronous *request* methods: "The sending peer
            /// SHOULD wait for the specific reply method" (§3.2.1).
            pub fn is_sync_request(&self) -> bool {
                match self { $( $( Self::$variant { .. } => $sync, )* )* }
            }

            /// True for methods that "carry content... do so unconditionally"
            /// (§4.2.6): the method frame is always followed by a content
            /// header and zero or more body frames.
            pub fn carries_content(&self) -> bool {
                match self { $( $( Self::$variant { .. } => $content, )* )* }
            }

            /// Encode class-id, method-id and arguments.
            pub fn encode(&self, e: &mut Encoder) {
                match self {
                    $(
                        $(
                            Self::$variant { $( $field ),* } => {
                                e.u16($class_id);
                                e.u16($method_id);
                                $( AmqpArg::encode_arg($field, e); )*
                            }
                        )*
                    )*
                }
            }

            /// Decode class-id, method-id and arguments. Callers must follow
            /// with [`Decoder::finish`] when the payload is exactly one
            /// method — use [`Method::decode_payload`].
            pub fn decode(d: &mut Decoder<'_>) -> Result<Self, CodecError> {
                let class_id = d.u16()?;
                let method_id = d.u16()?;
                match class_id {
                    $(
                        $class_id => match method_id {
                            $(
                                $method_id => Ok(Self::$variant {
                                    $( $field: AmqpArg::decode_arg(d)? ),*
                                }),
                            )*
                            _ => Err(CodecError::UnknownMethod(class_id, method_id)),
                        },
                    )*
                    _ => Err(CodecError::UnknownClass(class_id)),
                }
            }

            /// Decode a full method-frame payload: one method, nothing else.
            pub fn decode_payload(payload: &[u8]) -> Result<Self, CodecError> {
                let mut d = Decoder::new(payload);
                let m = Self::decode(&mut d)?;
                d.finish()?;
                Ok(m)
            }
        }
    };
}

define_methods! {
    10, CONNECTION_CLASS_ID, "connection" => {
        10, ConnectionStart, true, false, [
            version_major: u8, version_minor: u8, server_properties: FieldTable,
            mechanisms: Vec<u8>, locales: Vec<u8>,
        ],
        11, ConnectionStartOk, false, false, [
            client_properties: FieldTable, mechanism: String, response: Vec<u8>, locale: String,
        ],
        20, ConnectionSecure, true, false, [challenge: Vec<u8>],
        21, ConnectionSecureOk, false, false, [response: Vec<u8>],
        30, ConnectionTune, true, false, [
            channel_max: u16, frame_max: u32, heartbeat: u16,
        ],
        31, ConnectionTuneOk, false, false, [
            channel_max: u16, frame_max: u32, heartbeat: u16,
        ],
        40, ConnectionOpen, true, false, [
            virtual_host: String, capabilities: String, insist: bool,
        ],
        41, ConnectionOpenOk, false, false, [known_hosts: String],
        50, ConnectionClose, true, false, [
            reply_code: u16, reply_text: String, class_id: u16, method_id: u16,
        ],
        51, ConnectionCloseOk, false, false, [],
        60, ConnectionBlocked, false, false, [reason: String],
        61, ConnectionUnblocked, false, false, [],
    },
    20, CHANNEL_CLASS_ID, "channel" => {
        10, ChannelOpen, true, false, [out_of_band: String],
        11, ChannelOpenOk, false, false, [channel_id: Vec<u8>],
        20, ChannelFlow, true, false, [active: bool],
        21, ChannelFlowOk, false, false, [active: bool],
        40, ChannelClose, true, false, [
            reply_code: u16, reply_text: String, class_id: u16, method_id: u16,
        ],
        41, ChannelCloseOk, false, false, [],
    },
    40, EXCHANGE_CLASS_ID, "exchange" => {
        10, ExchangeDeclare, true, false, [
            ticket: u16, exchange: String, exchange_type: String, passive: bool,
            durable: bool, auto_delete: bool, internal: bool, nowait: bool,
            arguments: FieldTable,
        ],
        11, ExchangeDeclareOk, false, false, [],
        20, ExchangeDelete, true, false, [
            ticket: u16, exchange: String, if_unused: bool, nowait: bool,
        ],
        21, ExchangeDeleteOk, false, false, [],
        30, ExchangeBind, true, false, [
            ticket: u16, destination: String, source: String, routing_key: String,
            nowait: bool, arguments: FieldTable,
        ],
        31, ExchangeBindOk, false, false, [],
        40, ExchangeUnbind, true, false, [
            ticket: u16, destination: String, source: String, routing_key: String,
            nowait: bool, arguments: FieldTable,
        ],
        // 51, not 41: the registry numbers exchange.unbind-ok 51, matching the
        // deployed wire behaviour.
        51, ExchangeUnbindOk, false, false, [],
    },
    50, QUEUE_CLASS_ID, "queue" => {
        10, QueueDeclare, true, false, [
            ticket: u16, queue: String, passive: bool, durable: bool,
            exclusive: bool, auto_delete: bool, nowait: bool, arguments: FieldTable,
        ],
        11, QueueDeclareOk, false, false, [
            queue: String, message_count: u32, consumer_count: u32,
        ],
        20, QueueBind, true, false, [
            ticket: u16, queue: String, exchange: String, routing_key: String,
            nowait: bool, arguments: FieldTable,
        ],
        21, QueueBindOk, false, false, [],
        30, QueuePurge, true, false, [ticket: u16, queue: String, nowait: bool],
        31, QueuePurgeOk, false, false, [message_count: u32],
        40, QueueDelete, true, false, [
            ticket: u16, queue: String, if_unused: bool, if_empty: bool, nowait: bool,
        ],
        41, QueueDeleteOk, false, false, [message_count: u32],
        50, QueueUnbind, true, false, [
            ticket: u16, queue: String, exchange: String, routing_key: String,
            arguments: FieldTable,
        ],
        51, QueueUnbindOk, false, false, [],
    },
    60, BASIC_CLASS_ID, "basic" => {
        10, BasicQos, true, false, [
            prefetch_size: u32, prefetch_count: u16, global_: bool,
        ],
        11, BasicQosOk, false, false, [],
        20, BasicConsume, true, false, [
            ticket: u16, queue: String, consumer_tag: String, no_local: bool,
            no_ack: bool, exclusive: bool, nowait: bool, arguments: FieldTable,
        ],
        21, BasicConsumeOk, false, false, [consumer_tag: String],
        30, BasicCancel, true, false, [consumer_tag: String, nowait: bool],
        31, BasicCancelOk, false, false, [consumer_tag: String],
        40, BasicPublish, false, true, [
            ticket: u16, exchange: String, routing_key: String,
            mandatory: bool, immediate: bool,
        ],
        50, BasicReturn, false, true, [
            reply_code: u16, reply_text: String, exchange: String, routing_key: String,
        ],
        60, BasicDeliver, false, true, [
            consumer_tag: String, delivery_tag: u64, redelivered: bool,
            exchange: String, routing_key: String,
        ],
        70, BasicGet, true, false, [ticket: u16, queue: String, no_ack: bool],
        71, BasicGetOk, false, true, [
            delivery_tag: u64, redelivered: bool, exchange: String,
            routing_key: String, message_count: u32,
        ],
        72, BasicGetEmpty, false, false, [cluster_id: String],
        80, BasicAck, false, false, [delivery_tag: u64, multiple: bool],
        90, BasicReject, false, false, [delivery_tag: u64, requeue: bool],
        100, BasicRecoverAsync, false, false, [requeue: bool],
        110, BasicRecover, true, false, [requeue: bool],
        111, BasicRecoverOk, false, false, [],
        120, BasicNack, false, false, [
            delivery_tag: u64, multiple: bool, requeue: bool,
        ],
    },
    90, TX_CLASS_ID, "tx" => {
        10, TxSelect, true, false, [],
        11, TxSelectOk, false, false, [],
        20, TxCommit, true, false, [],
        21, TxCommitOk, false, false, [],
        30, TxRollback, true, false, [],
        31, TxRollbackOk, false, false, [],
    },
    85, CONFIRM_CLASS_ID, "confirm" => {
        10, ConfirmSelect, true, false, [nowait: bool],
        11, ConfirmSelectOk, false, false, [],
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal byte-level check against the spec examples: class-id and
    /// method-id lead every method payload (§4.2.4).
    #[test]
    fn ids_lead_the_payload() {
        let m = Method::QueuePurge { ticket: 0, queue: "q".into(), nowait: false };
        let mut e = Encoder::new();
        m.encode(&mut e);
        let buf = e.finish();
        assert_eq!(&buf[..4], &[0, 50, 0, 30]);
        assert_eq!(Method::decode_payload(&buf).unwrap(), m);
    }

    #[test]
    fn unknown_class_and_method_are_distinct_errors() {
        let mut d = Decoder::new(&[0, 99, 0, 1]);
        assert!(matches!(Method::decode(&mut d), Err(CodecError::UnknownClass(99))));

        let mut d = Decoder::new(&[0, 50, 0, 99]);
        assert!(matches!(
            Method::decode(&mut d),
            Err(CodecError::UnknownMethod(50, 99))
        ));
    }

    #[test]
    fn class_ids_match_the_spec() {
        assert_eq!(Method::CONNECTION_CLASS_ID, 10);
        assert_eq!(Method::CHANNEL_CLASS_ID, 20);
        assert_eq!(Method::EXCHANGE_CLASS_ID, 40);
        assert_eq!(Method::QUEUE_CLASS_ID, 50);
        assert_eq!(Method::BASIC_CLASS_ID, 60);
        assert_eq!(Method::CONFIRM_CLASS_ID, 85);
        assert_eq!(Method::TX_CLASS_ID, 90);
    }

    /// Every variant must report the ids its table row declares.
    #[test]
    fn id_accessors_agree_with_encoding() {
        let samples = [
            Method::ConnectionStart {
                version_major: 0,
                version_minor: 9,
                server_properties: FieldTable::new(),
                mechanisms: b"PLAIN".to_vec(),
                locales: b"en_US".to_vec(),
            },
            Method::ExchangeUnbindOk {},
            Method::BasicNack { delivery_tag: 1, multiple: false, requeue: true },
            Method::ConfirmSelect { nowait: true },
        ];
        for m in samples {
            let mut e = Encoder::new();
            m.encode(&mut e);
            let buf = e.finish();
            assert_eq!(&buf[..2], &m.class_id().to_be_bytes());
            assert_eq!(&buf[2..4], &m.method_id().to_be_bytes());
            let back = Method::decode_payload(&buf).unwrap();
            assert_eq!(back, m);
        }
    }

    #[test]
    fn sync_and_content_flags() {
        // Synchronous requests wait for a specific reply (§3.2.1).
        assert!(Method::QueueDeclare {
            ticket: 0,
            queue: String::new(),
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: FieldTable::new(),
        }
        .is_sync_request());
        assert!(!Method::BasicPublish {
            ticket: 0,
            exchange: String::new(),
            routing_key: String::new(),
            mandatory: false,
            immediate: false,
        }
        .is_sync_request());

        // Content is unconditional for these four methods (§4.2.6).
        for (m, wants) in [
            (
                Method::BasicPublish {
                    ticket: 0,
                    exchange: String::new(),
                    routing_key: String::new(),
                    mandatory: false,
                    immediate: false,
                },
                true,
            ),
            (
                Method::BasicGetOk {
                    delivery_tag: 0,
                    redelivered: false,
                    exchange: String::new(),
                    routing_key: String::new(),
                    message_count: 0,
                },
                true,
            ),
            (
                Method::BasicReturn {
                    reply_code: 0,
                    reply_text: String::new(),
                    exchange: String::new(),
                    routing_key: String::new(),
                },
                true,
            ),
            (
                Method::BasicDeliver {
                    consumer_tag: String::new(),
                    delivery_tag: 0,
                    redelivered: false,
                    exchange: String::new(),
                    routing_key: String::new(),
                },
                true,
            ),
            (Method::BasicAck { delivery_tag: 0, multiple: false }, false),
        ] {
            assert_eq!(m.carries_content(), wants, "{}", m.name());
        }
    }
}

#[cfg(test)]
mod name_tests {
    use super::*;

    #[test]
    fn method_name_is_the_variant() {
        let m = Method::ChannelOpen { out_of_band: String::new() };
        assert_eq!(m.name(), "ChannelOpen");
    }
}

#[cfg(test)]
mod ack_encoding_tests {
    use super::*;

    /// Go/RabbitMQ clients parse method fields strictly: Basic.Ack is
    /// [delivery-tag: longlong][multiple: bit] — the bit packs into one
    /// trailing octet. Encoding it as any other constructor breaks
    /// interop (the Go client tears the channel down on a parse error).
    #[test]
    fn basic_ack_wire_bytes_match_the_spec() {
        let m = Method::BasicAck { delivery_tag: 3, multiple: false };
        let mut e = crate::wireio::Encoder::new();
        m.encode(&mut e);
        let bytes = e.finish();
        // class 60 + method 80 + tag(8) + flag octet
        assert_eq!(bytes.len(), 2 + 2 + 8 + 1);
        assert_eq!(&bytes[..4], &[0x00, 0x3C, 0x00, 0x50]);
        assert_eq!(&bytes[4..12], &[0, 0, 0, 0, 0, 0, 0, 3]);
        assert_eq!(bytes[12], 0x00, "multiple=false packs as 0x00 octet");
    }

    #[test]
    fn basic_ack_roundtrips_with_multiple() {
        let m = Method::BasicAck { delivery_tag: 9, multiple: true };
        let mut e = crate::wireio::Encoder::new();
        m.encode(&mut e);
        let bytes = e.finish();
        assert_eq!(bytes[12], 0x01);
        let mut d = crate::wireio::Decoder::new(&bytes);
        let back = Method::decode(&mut d).unwrap();
        assert_eq!(back, Method::BasicAck { delivery_tag: 9, multiple: true });
    }
}
