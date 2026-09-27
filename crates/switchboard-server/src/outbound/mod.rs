//! Outbound frame pipeline: an ordered queue of frames per connection,
//! drained by the writer task with heartbeat injection.

use bytes::BytesMut;
use switchboard_wire::method::Method;
use switchboard_wire::properties::ContentHeader;

/// One frame (or control) waiting to go out on the socket.
#[derive(Debug)]
pub enum OutboundFrame {
    Method { channel: u16, method: Method },
    Header { channel: u16, header: ContentHeader },
    Body { channel: u16, data: Vec<u8> },
    Heartbeat,
    /// Terminate the writer (connection teardown).
    Shutdown,
}

/// Encode an outbound frame into wire bytes.
pub fn encode(frame: &OutboundFrame, out: &mut BytesMut) {
    match frame {
        OutboundFrame::Method { channel, method } => {
            switchboard_wire::Frame::method(*channel, method).encode(out);
        }
        OutboundFrame::Header { channel, header } => {
            switchboard_wire::Frame::header(*channel, header).encode(out);
        }
        OutboundFrame::Body { channel, data } => {
            switchboard_wire::Frame::body(*channel, data).encode(out);
        }
        OutboundFrame::Heartbeat => {
            switchboard_wire::Frame::heartbeat().encode(out);
        }
        OutboundFrame::Shutdown => {}
    }
}

/// Max body bytes per content-body frame given the agreed frame-max
/// (frame = 7-byte header + payload + 1-byte end).
pub fn max_body_size(frame_max: u32) -> usize {
    if frame_max == 0 {
        128 * 1024
    } else {
        (frame_max as usize).saturating_sub(8).max(1)
    }
}

/// Method + content header + body frames for a message delivery.
pub fn message_frames(
    channel: u16,
    method: Method,
    props: switchboard_wire::BasicProperties,
    body: &[u8],
    frame_max: u32,
) -> Vec<OutboundFrame> {
    let mut frames = vec![
        OutboundFrame::Method { channel, method },
        OutboundFrame::Header {
            channel,
            header: ContentHeader::new(body.len() as u64, props),
        },
    ];
    let chunk = max_body_size(frame_max);
    if body.is_empty() {
        return frames; // zero body size: no body frames (§4.2.6.1)
    }
    for piece in body.chunks(chunk.max(1)) {
        frames.push(OutboundFrame::Body { channel, data: piece.to_vec() });
    }
    frames
}

#[cfg(test)]
mod tests {
    use super::*;
    use switchboard_wire::field::FieldTable;
    use switchboard_wire::wireio::Decoder;

    #[test]
    fn method_frame_encoding_roundtrip() {
        let m = Method::BasicAck { delivery_tag: 9, multiple: true };
        let f = OutboundFrame::Method { channel: 2, method: m.clone() };
        let mut buf = BytesMut::new();
        encode(&f, &mut buf);
        let mut r = switchboard_wire::FrameReader::new();
        r.feed(&buf);
        let frame = r.next_frame(0).unwrap().unwrap();
        assert_eq!(frame.channel, 2);
        assert_eq!(frame.decode_method().unwrap(), m);
    }

    #[test]
    fn heartbeat_encodes() {
        let mut buf = BytesMut::new();
        encode(&OutboundFrame::Heartbeat, &mut buf);
        assert_eq!(&buf[..], &[8, 0, 0, 0, 0, 0, 0, 0xCE]);
    }

    #[test]
    fn bodies_split_to_frame_max() {
        let frames = message_frames(
            1,
            Method::BasicDeliver {
                consumer_tag: "c".into(),
                delivery_tag: 1,
                redelivered: false,
                exchange: "".into(),
                routing_key: String::new(),
            },
            switchboard_wire::BasicProperties::new(),
            &vec![7u8; 500],
            128,
        );
        // method + header + ceil(500/120) bodies
        assert_eq!(frames.len(), 2 + 5);
        let bodies: Vec<&OutboundFrame> =
            frames.iter().filter(|f| matches!(f, OutboundFrame::Body { .. })).collect();
        let total: usize = bodies
            .iter()
            .map(|f| match f {
                OutboundFrame::Body { data, .. } => data.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(total, 500);
        for f in &frames {
            if let OutboundFrame::Body { data, .. } = f {
                assert!(data.len() <= 120);
            }
        }
    }

    #[test]
    fn empty_body_has_no_body_frames() {
        let frames = message_frames(
            1,
            Method::BasicGetOk {
                delivery_tag: 1,
                redelivered: false,
                exchange: "".into(),
                routing_key: String::new(),
                message_count: 0,
            },
            switchboard_wire::BasicProperties::new(),
            &[],
            131_072,
        );
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn header_roundtrip_through_wire() {
        let mut props = switchboard_wire::BasicProperties::new();
        props.delivery_mode = Some(2);
        props.headers = Some(FieldTable::new());
        let frames = message_frames(
            3,
            Method::BasicDeliver {
                consumer_tag: "c".into(),
                delivery_tag: 4,
                redelivered: false,
                exchange: "e".into(),
                routing_key: "k".into(),
            },
            props.clone(),
            b"data",
            0,
        );
        // Encode the header frame and decode it back.
        let mut buf = BytesMut::new();
        encode(&frames[1], &mut buf);
        let mut r = switchboard_wire::FrameReader::new();
        r.feed(&buf);
        let frame = r.next_frame(0).unwrap().unwrap();
        let header = frame.content_header(60).unwrap();
        assert_eq!(header.body_size, 4);
        assert_eq!(header.properties, props);
        let _ = Decoder::new(&[]);
    }
}
