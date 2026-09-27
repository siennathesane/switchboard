//! Codec errors. Every variant corresponds to a protocol violation the
//! specification tells us to reject, so the server can map each one onto the
//! right reply code (§4.8).

use thiserror::Error;

/// Errors raised while decoding (or, rarely, encoding) AMQP wire data.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("unexpected end of input: needed {needed} bytes, had {had}")]
    Eof { needed: usize, had: usize },
    #[error("trailing bytes after complete structure: {count}")]
    TrailingBytes { count: usize },
    #[error("frame-end octet was {actual:#04x}, expected 0xCE")]
    FrameEnd { actual: u8 },
    #[error("frame type {0} is not defined by the protocol")]
    FrameType(u8),
    #[error("frame payload of {size} bytes exceeds the agreed frame-max")]
    OversizedFrame { size: u32 },
    #[error("malformed protocol header")]
    ProtocolHeader,
    #[error("short string length {len} overflows the remaining input ({had} bytes)")]
    ShortStrOverflow { len: usize, had: usize },
    #[error("short strings MUST NOT contain binary zero octets")]
    ShortStrNul,
    #[error("short string is not valid UTF-8")]
    ShortStrUtf8,
    #[error("unknown field-table value type {0:#04x}")]
    FieldType(u8),
    #[error("field name {0:?} is invalid (must start with a letter, '$' or '#', max 128 chars)")]
    FieldName(String),
    #[error("decimal scale {0} exceeds 255 digits of precision")]
    #[allow(dead_code)]
    DecimalScale(u8),
    #[error("content header class-id {actual} does not match method class-id {expected}")]
    ContentClassMismatch { actual: u16, expected: u16 },
    #[error("content header weight must be zero, got {0}")]
    ContentWeight(u16),
    #[error("unknown method class {0}")]
    UnknownClass(u16),
    #[error("unknown method {1} in class {0}")]
    UnknownMethod(u16, u16),
    #[error("method arguments are malformed: {0}")]
    MalformedMethod(&'static str),
    #[error("property flags overflow: continuation bit set after 2048 properties")]
    PropertyFlagsOverflow,
}
