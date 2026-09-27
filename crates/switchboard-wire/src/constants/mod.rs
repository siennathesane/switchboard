//! Protocol constants from the AMQP 0-9-1 specification.
//!
//! Frame types and the protocol header come from §4.2; reply codes from §4.8.2
//! and the exception rules of §2.3.6 / §3.2.2. Constant names follow the
//! naming convention of §1.4.1 ("Protocol constants are shown as upper-case
//! names").

/// General frame format frame types (§4.2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameType {
    /// Type = 1: method frame.
    Method = 1,
    /// Type = 2: content header frame.
    Header = 2,
    /// Type = 3: content body frame.
    Body = 3,
    /// Type = 8: heartbeat frame (formal grammar §4.2.1: `heartbeat = %d8 ...`).
    Heartbeat = 8,
}

impl FrameType {
    /// Map a wire octet onto a frame type. `None` for undefined types, which
    /// §4.2.3 requires peers to treat as a fatal protocol error.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(FrameType::Method),
            2 => Some(FrameType::Header),
            3 => Some(FrameType::Body),
            8 => Some(FrameType::Heartbeat),
            _ => None,
        }
    }
}

/// The `frame-end` octet: MUST always be `%xCE` (§4.2.3).
pub const FRAME_END: u8 = 0xCE;

/// Size of the frame header: type (octet) + channel (short) + size (long).
pub const FRAME_HEADER_SIZE: usize = 7;

/// Largest possible frame payload (32-bit size field, §4.9).
pub const FRAME_PAYLOAD_MAX: u32 = u32::MAX - 1;

/// Heartbeat frames MUST use channel zero (§4.2.7).
pub const HEARTBEAT_FRAME: [u8; 4] = [FrameType::Heartbeat as u8, 0, 0, FRAME_END];

/// The `Connection.Start` server properties key identifying this product.
pub const PRODUCT_NAME: &str = "switchboard";
/// Implementation version reported in `Connection.Start`.
pub const PRODUCT_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Default virtual host created at bootstrap (§3.1.2: vhosts are created
/// outside the protocol; brokers conventionally provide "/").
pub const DEFAULT_VHOST: &str = "/";
/// Default user created at bootstrap (development default, like RabbitMQ's
/// `guest`).
pub const DEFAULT_USER: &str = "guest";
/// Password of the default user.
pub const DEFAULT_PASSWORD: &str = "guest";

/// Reply codes (§4.8.2, RFC 2821 style three-digit codes).
pub mod reply {
    /// 200: normal reply.
    pub const REPLY_SUCCESS: u16 = 200;
    /// 311: content too large for the receiving peer (channel exception).
    pub const CONTENT_TOO_LARGE: u16 = 311;
    /// 312: reached the end of the internal limit on consumers.
    pub const NO_CONSUMERS: u16 = 312;
    /// 320: an operator intervened, closing the connection.
    pub const CONNECTION_FORCED: u16 = 320;
    /// 402: the client tried to work with an unknown virtual host.
    pub const INVALID_PATH: u16 = 402;
    /// 403: access refused by the server (connection- and channel-level).
    pub const ACCESS_REFUSED: u16 = 403;
    /// 404: the client attempted to work with a server entity that does not exist.
    pub const NOT_FOUND: u16 = 404;
    /// 405: the client attempted to work with a server entity to which it has
    /// no access because another client is working with it.
    pub const RESOURCE_LOCKED: u16 = 405;
    /// 406: the client attempted to work with a server entity that was
    /// re-declared with different arguments.
    pub const PRECONDITION_FAILED: u16 = 406;
    /// 501: the sender sent a malformed frame.
    pub const FRAME_ERROR: u16 = 501;
    /// 502: the sender sent a frame with malformed content.
    pub const SYNTAX_ERROR: u16 = 502;
    /// 503: the client sent an unsupported or out-of-order command.
    pub const COMMAND_INVALID: u16 = 503;
    /// 504: the client sent a second channel while it was already open, or
    /// content on a non-open channel.
    pub const CHANNEL_ERROR: u16 = 504;
    /// 505: the client sent a frame that is unexpected in the current context.
    pub const UNEXPECTED_FRAME: u16 = 505;
    /// 506: a resource could not be allocated (out of disk, memory, ...).
    pub const RESOURCE_ERROR: u16 = 506;
    /// 530: the client attempted to transfer content larger than the server's
    /// configured limit, or work with a disallowed entity.
    pub const NOT_ALLOWED: u16 = 530;
    /// 540: the client tried to work with a method not implemented by the peer.
    pub const NOT_IMPLEMENTED: u16 = 540;
}

/// Well-known exchange names pre-declared in every vhost (§3.1.3).
pub mod exchanges {
    /// The nameless direct exchange used by `Basic.Publish` when the exchange
    /// field is empty (§3.1.3.1).
    pub const DEFAULT: &str = "";
    /// Mandatory pre-declared direct exchange.
    pub const AMQ_DIRECT: &str = "amq.direct";
    /// Mandatory pre-declared fanout exchange.
    pub const AMQ_FANOUT: &str = "amq.fanout";
    /// Pre-declared topic exchange (required when the topic type is supported).
    pub const AMQ_TOPIC: &str = "amq.topic";
    /// Pre-declared headers exchange (required when the headers type is supported).
    pub const AMQ_MATCH: &str = "amq.match";
}

/// Reserved prefixes (§3.1.10 naming conventions).
pub mod naming {
    /// Standard exchange/queue instances are prefixed by `amq.`.
    pub const RESERVED_PREFIX: &str = "amq.";
    /// User defined exchange *types* MUST be prefixed by `x-`.
    pub const CUSTOM_TYPE_PREFIX: &str = "x-";
}
