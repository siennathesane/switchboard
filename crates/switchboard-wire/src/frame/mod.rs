//! General frame format, protocol header, and heartbeats (§4.2.2–§4.2.7).
//!
//! ```text
//! 0      1        3             7                        size+7   size+8
//! +------+---------+-------------+ ----------------------+--------+
//! | type | channel |     size    |       payload         |  0xCE  |
//! +------+---------+-------------+ ----------------------+--------+
//!   octet    short      long             'size' octets         octet
//! ```

use bytes::{Buf, Bytes, BytesMut};

use crate::constants::{FrameType, FRAME_END, FRAME_HEADER_SIZE};
use crate::error::CodecError;
use crate::method::Method;
use crate::properties::ContentHeader;
use crate::wireio::{Decoder, Encoder};

/// The 8-octet protocol header of §4.2.2: `"AMQP"` + id 0 + version 0.9.1.
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";

/// Parse a protocol header from the first octets on a new connection.
///
/// Returns `Ok(0)` when the full 8-octet header is present and correct,
/// `Err(CodecError::Eof)` when more octets are needed, and
/// `Err(CodecError::ProtocolHeader)` when the header is malformed or names an
/// unsupported protocol — in which case §4.2.2 requires the server to write a
/// valid protocol header to the socket and close it.
pub fn parse_protocol_header(buf: &[u8]) -> Result<usize, CodecError> {
    if buf.len() < 8 {
        if PROTOCOL_HEADER.starts_with(buf) {
            Err(CodecError::Eof { needed: 8, had: buf.len() })
        } else {
            Err(CodecError::ProtocolHeader)
        }
    } else if buf[..8] == PROTOCOL_HEADER {
        Ok(8)
    } else {
        Err(CodecError::ProtocolHeader)
    }
}

/// A complete wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub channel: u16,
    pub payload: Bytes,
}

impl Frame {
    pub fn method(channel: u16, m: &Method) -> Self {
        let mut e = Encoder::new();
        m.encode(&mut e);
        Frame { frame_type: FrameType::Method, channel, payload: e.finish().freeze() }
    }

    pub fn header(channel: u16, header: &ContentHeader) -> Self {
        let mut e = Encoder::new();
        header.encode(&mut e);
        Frame { frame_type: FrameType::Header, channel, payload: e.finish().freeze() }
    }

    pub fn body(channel: u16, data: &[u8]) -> Self {
        Frame {
            frame_type: FrameType::Body,
            channel,
            payload: Bytes::copy_from_slice(data),
        }
    }

    /// Heartbeat frames MUST use channel zero and carry no payload (§4.2.7).
    pub fn heartbeat() -> Self {
        Frame { frame_type: FrameType::Heartbeat, channel: 0, payload: Bytes::new() }
    }

    pub fn encode(&self, out: &mut BytesMut) {
        use bytes::BufMut;
        out.reserve(FRAME_HEADER_SIZE + self.payload.len() + 1);
        out.put_u8(self.frame_type as u8);
        out.put_u16(self.channel);
        out.put_u32(self.payload.len() as u32);
        out.put_slice(&self.payload);
        out.put_u8(FRAME_END);
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut out = BytesMut::new();
        self.encode(&mut out);
        out
    }

    /// Decode the payload of a method frame.
    pub fn decode_method(&self) -> Result<Method, CodecError> {
        debug_assert!(matches!(self.frame_type, FrameType::Method));
        Method::decode_payload(&self.payload)
    }

    /// Decode the payload of a content header frame against the pending
    /// method's class id (§4.2.6.1).
    pub fn content_header(&self, expected_class: u16) -> Result<ContentHeader, CodecError> {
        debug_assert!(matches!(self.frame_type, FrameType::Header));
        let mut d = Decoder::new(&self.payload);
        let h = ContentHeader::decode(expected_class, &mut d)?;
        d.finish()?;
        Ok(h)
    }
}

/// Incremental frame parser for a byte stream.
///
/// `next_frame` returns `Ok(None)` while more octets are needed, which keeps
/// the TCP read loop trivial: feed everything, drain frames.
#[derive(Debug, Default)]
pub struct FrameReader {
    buf: BytesMut,
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read octets.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// True when the buffer begins with a well-formed protocol header.
    pub fn take_protocol_header(&mut self) -> Result<bool, CodecError> {
        match parse_protocol_header(&self.buf) {
            Ok(8) => {
                let _ = self.buf.split_to(8);
                Ok(true)
            }
            Ok(_) => unreachable!("parse_protocol_header only returns Ok(8)"),
            Err(CodecError::Eof { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Pull one complete frame, or `None` if more octets are needed.
    ///
    /// Enforces §4.2.3: known frame type, valid frame-end octet, and the
    /// agreed `frame_max` limit (oversized frames are a 501 connection
    /// exception).
    pub fn next_frame(&mut self, frame_max: u32) -> Result<Option<Frame>, CodecError> {
        if self.buf.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }
        let type_byte = self.buf[0];
        let frame_type = FrameType::from_u8(type_byte)
            .ok_or(CodecError::FrameType(type_byte))?;
        let channel = u16::from_be_bytes([self.buf[1], self.buf[2]]);
        let size = u32::from_be_bytes([self.buf[3], self.buf[4], self.buf[5], self.buf[6]]);

        // The agreed frame-max covers header, payload and frame-end octet;
        // zero disables the limit (the negotiated "no limit").
        let total = FRAME_HEADER_SIZE as u64 + size as u64 + 1;
        if frame_max != 0 && total > frame_max as u64 {
            return Err(CodecError::OversizedFrame { size });
        }
        let total = FRAME_HEADER_SIZE + size as usize + 1;
        if self.buf.len() < total {
            return Ok(None);
        }

        let mut frame_buf = self.buf.split_to(total);
        let _ = frame_buf.advance(FRAME_HEADER_SIZE);
        let payload = frame_buf.split_to(size as usize);
        let end = frame_buf.first().copied().unwrap_or(0);
        if end != FRAME_END {
            return Err(CodecError::FrameEnd { actual: end });
        }
        Ok(Some(Frame { frame_type, channel, payload: payload.freeze() }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::FieldTable;
    use crate::method::Method;

    #[test]
    fn protocol_header_is_exact() {
        assert_eq!(&PROTOCOL_HEADER, b"AMQP\x00\x00\x09\x01");
        assert_eq!(parse_protocol_header(&PROTOCOL_HEADER).unwrap(), 8);
        assert_eq!(
            parse_protocol_header(&PROTOCOL_HEADER[..4]),
            Err(CodecError::Eof { needed: 8, had: 4 })
        );
        assert_eq!(parse_protocol_header(b"HTTP/1.1"), Err(CodecError::ProtocolHeader));
        assert_eq!(parse_protocol_header(&[]), Err(CodecError::Eof { needed: 8, had: 0 }));
        // Wrong version: magic ok, version bad -> reject outright.
        assert_eq!(
            parse_protocol_header(b"AMQP\x00\x00\x09\x00"),
            Err(CodecError::ProtocolHeader)
        );
    }

    #[test]
    fn heartbeat_frame_bytes() {
        // §4.2.1: heartbeat = %d8 %d0 %d0 frame-end (with the 4-octet size
        // field zero, per the general format).
        let f = Frame::heartbeat();
        assert_eq!(f.to_bytes().as_ref(), &[8, 0, 0, 0, 0, 0, 0, 0xCE]);
    }

    fn sample_method() -> Method {
        Method::BasicQos { prefetch_size: 0, prefetch_count: 10, global_: false }
    }

    #[test]
    fn frame_roundtrip_through_reader() {
        let mut r = FrameReader::new();
        let m = sample_method();
        let bytes = Frame::method(3, &m).to_bytes();
        // Feed in odd chunks to exercise the incremental parser.
        for chunk in bytes.chunks(3) {
            r.feed(chunk);
            if let Some(f) = r.next_frame(0).unwrap() {
                assert_eq!(f.channel, 3);
                assert_eq!(f.decode_method().unwrap(), m);
                return;
            }
        }
        panic!("frame never completed");
    }

    #[test]
    fn multiple_frames_drain_in_order() {
        let mut r = FrameReader::new();
        r.feed(&Frame::method(1, &sample_method()).to_bytes());
        r.feed(&Frame::heartbeat().to_bytes());
        r.feed(&Frame::body(9, b"payload").to_bytes());

        let f1 = r.next_frame(0).unwrap().unwrap();
        assert!(matches!(f1.frame_type, FrameType::Method));
        let f2 = r.next_frame(0).unwrap().unwrap();
        assert!(matches!(f2.frame_type, FrameType::Heartbeat));
        let f3 = r.next_frame(0).unwrap().unwrap();
        assert_eq!(f3.payload.as_ref(), b"payload");
        assert!(r.next_frame(0).unwrap().is_none());
    }

    #[test]
    fn bad_frame_end_is_fatal() {
        let mut r = FrameReader::new();
        let mut bytes = Frame::heartbeat().to_bytes().to_vec();
        let last = bytes.len() - 1;
        bytes[last] = 0x00;
        r.feed(&bytes);
        assert!(matches!(
            r.next_frame(0),
            Err(CodecError::FrameEnd { actual: 0x00 })
        ));
    }

    #[test]
    fn unknown_frame_type_is_fatal() {
        let mut r = FrameReader::new();
        r.feed(&[7, 0, 0, 0, 0, 0, 0, 0xCE]);
        assert!(matches!(r.next_frame(0), Err(CodecError::FrameType(7))));
    }

    #[test]
    fn oversized_frame_is_rejected_early() {
        let mut r = FrameReader::new();
        // Announce a 1000-byte payload; frame_max = 128 covers the whole
        // frame, so 1000 + 7 + 1 is far over the limit.
        r.feed(&[1, 0, 1, 0, 0, 3, 232, 0xCE]);
        assert!(matches!(
            r.next_frame(128),
            Err(CodecError::OversizedFrame { size: 1000 })
        ));

        // A frame that exactly fills frame_max is legal (fresh reader: the
        // rejected frame's header is still buffered above).
        let mut r2 = FrameReader::new();
        let exactly = Frame::body(0, &[0u8; 128 - FRAME_HEADER_SIZE - 1]);
        r2.feed(&exactly.to_bytes());
        assert!(r2.next_frame(128).unwrap().is_some());
    }

    #[test]
    fn content_header_roundtrip_through_frame() {
        use crate::properties::BasicProperties;
        let mut p = BasicProperties::new();
        p.delivery_mode = Some(2);
        let h = ContentHeader::new(5, p);
        let mut r = FrameReader::new();
        r.feed(&Frame::header(2, &h).to_bytes());
        let f = r.next_frame(0).unwrap().unwrap();
        assert_eq!(f.content_header(60).unwrap(), h);
    }

    #[test]
    fn method_frame_roundtrip() {
        let m = Method::QueueDeclare {
            ticket: 0,
            queue: "q1".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: FieldTable::new(),
        };
        let mut r = FrameReader::new();
        r.feed(&Frame::method(7, &m).to_bytes());
        let f = r.next_frame(0).unwrap().unwrap();
        assert_eq!(f.decode_method().unwrap(), m);
    }
}
