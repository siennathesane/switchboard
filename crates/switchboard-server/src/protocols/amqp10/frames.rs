//! AMQP 1.0 transport frames and performatives.
//!
//! Frame layout: 8-byte header (`doff=2, type, channel:u16, size:u32`)
//! followed by a body — for AMQP frames a described performative list,
//! for `transfer` additionally the message payload bytes. SASL frames use
//! type byte `0x01`.

use super::types::described_list;
use super::types::encode;
use super::types::decode;
use super::types::Value;

pub const FRAME_TYPE_AMQP: u8 = 0x00;
pub const FRAME_TYPE_SASL: u8 = 0x01;

/// Performatives (ulong descriptors).
pub mod codes {
    pub const OPEN: u64 = 0x10;
    pub const BEGIN: u64 = 0x11;
    pub const ATTACH: u64 = 0x12;
    pub const FLOW: u64 = 0x13;
    pub const TRANSFER: u64 = 0x14;
    pub const DISPOSITION: u64 = 0x15;
    pub const DETACH: u64 = 0x16;
    pub const END: u64 = 0x17;
    pub const CLOSE: u64 = 0x18;
    pub const SASL_MECHANISMS: u64 = 0x40;
    pub const SASL_INIT: u64 = 0x41;
    pub const SASL_CHALLENGE: u64 = 0x42;
    pub const SASL_RESPONSE: u64 = 0x43;
    pub const SASL_OUTCOME: u64 = 0x44;
    /// Message sections and delivery states.
    pub const SECTION_HEADER: u64 = 0x70;
    pub const SECTION_PROPERTIES: u64 = 0x73;
    pub const SECTION_APPLICATION_PROPERTIES: u64 = 0x74;
    pub const SECTION_DATA: u64 = 0x75;
    pub const SECTION_AMQP_VALUE: u64 = 0x77;
    pub const STATE_ACCEPTED: u64 = 0x24;
}

/// A decoded transport frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub channel: u16,
    pub frame_type: u8,
    /// The described performative (descriptor ulong + field list).
    pub performative: Value,
    /// For `transfer`: the message payload bytes following the list.
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn code(&self) -> Option<u64> {
        match &self.performative {
            Value::Described(d, _) => match **d {
                Value::ULong(n) => Some(n),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn fields(&self) -> &[Value] {
        static EMPTY: [Value; 0] = [];
        match &self.performative {
            Value::Described(_, v) => match v.as_ref() {
                Value::List(items) => items,
                _ => &EMPTY,
            },
            _ => &EMPTY,
        }
    }

    pub fn field(&self, i: usize) -> Option<&Value> {
        self.fields().get(i)
    }
}

/// Encode a performative frame (no payload).
pub fn encode_frame(channel: u16, code: u64, fields: Vec<Value>) -> Vec<u8> {
    encode_frame_with_payload(channel, code, fields, &[])
}

/// Encode a performative frame plus raw payload (transfer).
pub fn encode_frame_with_payload(
    channel: u16,
    code: u64,
    fields: Vec<Value>,
    payload: &[u8],
) -> Vec<u8> {
    let described = described_list(code, fields);
    let mut body = Vec::new();
    encode(&described, &mut body);
    body.extend_from_slice(payload);
    let size = 8 + body.len();
    let mut out = Vec::with_capacity(size);
    // §2.3 frame header: size (4), doff (1), type (1), channel (2).
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.push(2); // doff
    out.push(FRAME_TYPE_AMQP);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// Encode a SASL frame.
pub fn encode_sasl_frame(code: u64, fields: Vec<Value>) -> Vec<u8> {
    let described = described_list(code, fields);
    let mut body = Vec::new();
    encode(&described, &mut body);
    let size = 8 + body.len();
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.push(2);
    out.push(FRAME_TYPE_SASL);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// Decode one frame from `buf`. Returns the frame and bytes consumed.
pub fn decode_frame(buf: &[u8]) -> Result<(Frame, usize), String> {
    if buf.len() < 8 {
        return Ok(want_more());
    }
    // §2.3 frame header: size (4), doff (1), type (1), channel (2).
    let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let doff = buf[4] as usize;
    if doff < 2 {
        return Err("amqp1: frame offset < 2".into());
    }
    let header_len = doff * 4;
    if size < 8 || size < header_len {
        return Err("amqp1: frame size smaller than header".into());
    }
    if buf.len() < size {
        return Ok(want_more());
    }
    let frame_type = buf[5];
    let channel = u16::from_be_bytes([buf[6], buf[7]]);
    let body = &buf[header_len..size];
    if !body.is_empty() && (frame_type == FRAME_TYPE_AMQP || frame_type == FRAME_TYPE_SASL) {
        // Both frame types carry a described performative; transfer adds
        // payload bytes after it.
        let (performative, used) = decode(body)?;
        let payload = body[used..].to_vec();
        Ok((
            Frame { channel, frame_type, performative, payload },
            size,
        ))
    } else {
        Ok((
            Frame { channel, frame_type, performative: Value::Null, payload: body.to_vec() },
            size,
        ))
    }
}

fn want_more() -> (Frame, usize) {
    // Sentinel: a zero-size frame means "need more bytes" to the caller.
    (
        Frame {
            channel: 0,
            frame_type: 0xFF,
            performative: Value::Null,
            payload: Vec::new(),
        },
        0,
    )
}

pub fn is_more(consumed: usize) -> bool {
    consumed == 0
}

// ---- performatives the bridge sends ----

pub fn open(container_id: &str) -> Vec<u8> {
    encode_frame(
        0,
        codes::OPEN,
        vec![
            Value::String(container_id.into()), // container-id
            Value::UShort(0),                   // channel-max (0 = only ch 0)
            Value::UInt(1_048_576),             // max-frame-size
            Value::UInt(60_000),                // idle-time-out (ms)
            Value::Null,                        // outgoing-locales
            Value::Null,                        // incoming-locales
            Value::Null,                        // offered-capabilities
            Value::Null,                        // desired-capabilities
            Value::Null,                        // properties
        ],
    )
}

pub fn begin(remote_channel: Option<u16>, next_outgoing_id: u32) -> Vec<u8> {
    encode_frame(
        0,
        codes::BEGIN,
        vec![
            remote_channel.map(Value::UShort).unwrap_or(Value::Null),
            Value::UInt(next_outgoing_id), // next-outgoing-id
            Value::UInt(100_000),          // incoming-window
            Value::UInt(100_000),          // outgoing-window
            Value::ULong(10),              // handle-max
            Value::Null,                   // offered-capabilities
            Value::Null,                   // desired-capabilities
            Value::Null,                   // properties
        ],
    )
}

/// Attach as receiver (role=true) — we deliver to the client.
pub fn attach_receiver(name: &str, handle: u32, address: &str) -> Vec<u8> {
    // role = receiver: the SOURCE (field 5) carries the address whose
    // content we want.
    let source = Value::Map(vec![(
        Value::Symbol("address".into()),
        Value::String(address.into()),
    )]);
    encode_frame(
        0,
        codes::ATTACH,
        vec![
            Value::String(name.into()), // name
            Value::UInt(handle),        // handle
            Value::Bool(true),          // role: receiver
            Value::UByte(1),            // snd-settle-mode: settled
            Value::Null,                // rcv-settle-mode
            source,                     // source
            Value::Null,                // target
            Value::Null,                // unsettled
            Value::Bool(false),         // incomplete-unsettled
            Value::Null,                // offered-capabilities
            Value::Null,                // desired-capabilities
            Value::Null,                // properties
        ],
    )
}

/// Attach as sender (role=false) — the client delivers to us, via the
/// given target address.
pub fn attach_sender(name: &str, handle: u32, address: Option<&str>) -> Vec<u8> {
    // role = sender: the TARGET (field 6) carries the delivery address.
    let target = match address {
        Some(addr) => Value::Map(vec![
            (Value::Symbol("address".into()), Value::String(addr.into()))
        ]),
        None => Value::Null,
    };
    encode_frame(
        0,
        codes::ATTACH,
        vec![
            Value::String(name.into()), // name
            Value::UInt(handle),        // handle
            Value::Bool(false),         // role: sender
            Value::UByte(1),            // snd-settle-mode: settled
            Value::Null,                // rcv-settle-mode
            Value::Null,                // source
            target,                     // target
            Value::Null,                // unsettled
            Value::Bool(false),         // incomplete-unsettled
            Value::Null,                // offered-capabilities
            Value::Null,                // desired-capabilities
            Value::Null,                // properties
        ],
    )
}

/// Attach as sender with an explicit settle mode (`settled = true` →
/// snd-settle-mode 1, else 0 = unsettled).
pub fn attach_sender_mode(name: &str, handle: u32, settled: bool) -> Vec<u8> {
    let target: Value = Value::Null; // our attach's target is moot for delivery
    encode_frame(
        0,
        codes::ATTACH,
        vec![
            Value::String(name.into()),
            Value::UInt(handle),
            Value::Bool(false),                            // role: sender
            Value::UByte(if settled { 1 } else { 0 }), // snd-settle-mode
            Value::Null,                               // rcv-settle-mode
            Value::Null,                               // source
            target,                                    // target
            Value::Null,                               // unsettled
            Value::Bool(false),
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    )
}

pub fn transfer(
    channel: u16,
    handle: u32,
    delivery_id: u32,
    delivery_tag: &[u8],
    settled: bool,
    payload: &[u8],
) -> Vec<u8> {
    encode_frame_with_payload(
        channel,
        codes::TRANSFER,
        vec![
            Value::UInt(handle),
            Value::UInt(delivery_id),
            Value::Binary(delivery_tag.to_vec()),
            Value::UInt(0), // message-format
            Value::Bool(settled),
            Value::Bool(false), // more
            Value::Null,        // rcv-settle-mode
            Value::Null,        // state
        ],
        payload,
    )
}

pub fn flow(channel: u16, next_outgoing_id: u32) -> Vec<u8> {
    encode_frame(
        channel,
        codes::FLOW,
        vec![
            Value::Null,              // next-incoming-id
            Value::UInt(100_000),     // incoming-credit
            Value::UInt(next_outgoing_id),
            Value::Null,              // credit (echo)
            Value::Null,              // available
            Value::Bool(false),       // drain
            Value::Bool(false),       // echo
            Value::Null,              // properties
        ],
    )
}

/// Grant link credit to the peer (flow with `handle` + `credit` set).
pub fn flow_credit(channel: u16, handle: u32, credit: u32) -> Vec<u8> {
    encode_frame(
        channel,
        codes::FLOW,
        vec![
            Value::Null,          // next-incoming-id
            Value::UInt(100_000), // incoming-credit
            Value::UInt(0),       // next-outgoing-id
            Value::UInt(credit),  // credit
            Value::Null,          // available
            Value::Bool(false),   // drain
            Value::Bool(false),   // echo
            Value::Null,          // properties
            Value::UInt(handle),  // handle
        ],
    )
}

pub fn disposition_accepted(channel: u16, first: u32, last: u32) -> Vec<u8> {
    let state = described_list(codes::STATE_ACCEPTED, vec![]);
    encode_frame(
        channel,
        codes::DISPOSITION,
        vec![
            Value::Bool(true), // role: receiver (we received the transfer)
            Value::UInt(first),
            Value::UInt(last),
            Value::Bool(true), // settled
            state,
        ],
    )
}

pub fn detach(handle: u32) -> Vec<u8> {
    encode_frame(
        0,
        codes::DETACH,
        vec![Value::UInt(handle), Value::Bool(false), Value::Null],
    )
}

pub fn end() -> Vec<u8> {
    encode_frame(0, codes::END, vec![Value::Null])
}

pub fn close() -> Vec<u8> {
    encode_frame(0, codes::CLOSE, vec![Value::Null])
}

pub fn sasl_mechanisms(mechs: &[&str]) -> Vec<u8> {
    encode_sasl_frame(
        codes::SASL_MECHANISMS,
        vec![Value::List(mechs.iter().map(|m| Value::Symbol((*m).into())).collect())],
    )
}

pub fn sasl_outcome(code: u8) -> Vec<u8> {
    encode_sasl_frame(codes::SASL_OUTCOME, vec![Value::UByte(code), Value::Null])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_frame_roundtrips() {
        let bytes = open("switchboard");
        let (frame, used) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(frame.frame_type, FRAME_TYPE_AMQP);
        assert_eq!(frame.code(), Some(codes::OPEN));
        assert_eq!(frame.field(0).and_then(Value::as_str), Some("switchboard"));
    }

    #[test]
    fn transfer_frame_keeps_payload() {
        let bytes = transfer(0, 3, 42, b"tag", true, b"hello body");
        let (frame, used) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(frame.code(), Some(codes::TRANSFER));
        assert_eq!(frame.field(1).and_then(Value::as_uint), Some(42));
        assert_eq!(frame.payload, b"hello body");
    }

    #[test]
    fn partial_frames_need_more() {
        let bytes = open("x");
        let (half, _) = bytes.split_at(bytes.len() / 2);
        let (_, used) = decode_frame(half).unwrap();
        assert!(is_more(used));
    }

    #[test]
    fn sasl_frame_type_roundtrips() {
        let bytes = sasl_mechanisms(&["PLAIN", "ANONYMOUS"]);
        let (frame, used) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(frame.frame_type, FRAME_TYPE_SASL);
        assert_eq!(frame.code(), Some(codes::SASL_MECHANISMS));
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;

    fn roundtrip(bytes: &[u8]) -> Frame {
        let (f, used) = decode_frame(bytes).unwrap();
        assert_eq!(used, bytes.len());
        f
    }

    #[test]
    fn begin_frame_roundtrips() {
        let f = roundtrip(&begin(Some(3), 42));
        assert_eq!(f.code(), Some(codes::BEGIN));
        assert_eq!(f.field(0).cloned(), Some(Value::UShort(3)));
        assert_eq!(f.field(1).cloned(), Some(Value::UInt(42)));
    }

    #[test]
    fn attach_sender_roundtrips_with_target() {
        let f = roundtrip(&attach_sender("link", 4, Some("/queue/jobs")));
        assert_eq!(f.code(), Some(codes::ATTACH));
        assert_eq!(f.field(0).and_then(Value::as_str), Some("link"));
        assert_eq!(f.field(1).and_then(Value::as_uint), Some(4));
        assert_eq!(f.field(2).and_then(Value::as_bool), Some(false));
        assert_eq!(
            f.field(6).and_then(|v| v.map_get("address").and_then(Value::as_str)),
            Some("/queue/jobs"),
            "sender attach carries the target address"
        );
    }

    #[test]
    fn attach_receiver_roundtrips() {
        let f = roundtrip(&attach_receiver("rlink", 2, ""));
        assert_eq!(f.code(), Some(codes::ATTACH));
        assert_eq!(f.field(2).and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn flow_credit_roundtrips() {
        let f = roundtrip(&flow_credit(0, 9, 25));
        assert_eq!(f.code(), Some(codes::FLOW));
        assert_eq!(f.field(3).and_then(Value::as_uint), Some(25));
        assert_eq!(f.field(8).and_then(Value::as_uint), Some(9));
    }

    #[test]
    fn disposition_accepted_roundtrips() {
        let f = roundtrip(&disposition_accepted(0, 3, 7));
        assert_eq!(f.code(), Some(codes::DISPOSITION));
        assert_eq!(f.field(1).and_then(Value::as_uint), Some(3));
        assert_eq!(f.field(2).and_then(Value::as_uint), Some(7));
        // State is a described accepted (0x24).
        match f.field(4) {
            Some(Value::Described(d, _)) => match **d {
                Value::ULong(c) => assert_eq!(c, codes::STATE_ACCEPTED),
                ref o => panic!("bad descriptor {o:?}"),
            },
            other => panic!("bad state {other:?}"),
        }
    }

    #[test]
    fn detach_end_close_roundtrip() {
        for (bytes, code, handle) in [
            (detach(5), codes::DETACH, Some(5u32)),
        ] {
            let f = roundtrip(&bytes);
            assert_eq!(f.code(), Some(code));
            assert_eq!(f.field(0).and_then(Value::as_uint), handle);
        }
        let f = roundtrip(&end());
        assert_eq!(f.code(), Some(codes::END));
        let f = roundtrip(&close());
        assert_eq!(f.code(), Some(codes::CLOSE));
    }

    #[test]
    fn sasl_outcome_roundtrips() {
        let f = roundtrip(&sasl_outcome(0));
        assert_eq!(f.frame_type, FRAME_TYPE_SASL);
        assert_eq!(f.code(), Some(codes::SASL_OUTCOME));
        assert_eq!(f.field(0).cloned(), Some(Value::UByte(0)));
    }

    #[test]
    fn garbage_frames_error_cleanly() {
        // §2.3 layout: size (0..4), doff (4), type (5), channel (6..8).
        // Frame offset < 2.
        let mut b = vec![0u8, 0, 0, 16, 1, 0, 0, 0];
        b.extend_from_slice(&[0; 8]);
        assert!(decode_frame(&b).is_err());
        // size below the 8-byte fixed header → error.
        assert!(decode_frame(&[0u8, 0, 0, 7, 2, 0, 0, 0]).is_err());
        // Shorter than the announced size → want-more sentinel, not an
        // error.
        let (f, used) = decode_frame(&[0u8, 0, 0, 64, 2, 0, 0, 0]).unwrap();
        assert_eq!(used, 0);
        assert_eq!(f.frame_type, 0xFF);
    }
}
