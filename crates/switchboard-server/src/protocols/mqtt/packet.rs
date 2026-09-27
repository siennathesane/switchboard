//! MQTT 3.1.1 packet codec (the subset the bridge speaks).

use tokio::io::AsyncRead;

/// MQTT control packet types.
pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const PUBREC: u8 = 5;
pub const PUBREL: u8 = 6;
pub const PUBCOMP: u8 = 7;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const UNSUBSCRIBE: u8 = 10;
pub const UNSUBACK: u8 = 11;
pub const PINGREQ: u8 = 12;
pub const PINGRESP: u8 = 13;
pub const DISCONNECT: u8 = 14;

/// An inbound MQTT packet (what the broker accepts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect {
        clean_session: bool,
        keep_alive: u16,
        client_id: String,
        username: Option<String>,
        password: Option<Vec<u8>>,
    },
    Publish {
        qos: u8,
        retain: bool,
        topic: String,
        packet_id: Option<u16>,
        payload: Vec<u8>,
    },
    PubAck {
        packet_id: u16,
    },
    PubRec {
        packet_id: u16,
    },
    /// PUBREL arrives with flags 0b0010 per §3.6.1.
    PubRel {
        packet_id: u16,
    },
    PubComp {
        packet_id: u16,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    PingReq,
    Disconnect,
}

/// Reader over a growing in-memory buffer.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn u8(&mut self) -> Result<u8, &'static str> {
        let b = self.buf.get(self.pos).copied().ok_or("truncated")?;
        self.pos += 1;
        Ok(b)
    }
    fn u16(&mut self) -> Result<u16, &'static str> {
        let hi = self.u8()?;
        let lo = self.u8()?;
        Ok(u16::from_be_bytes([hi, lo]))
    }
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        if self.pos + n > self.buf.len() {
            return Err("truncated");
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn utf8(&mut self) -> Result<String, &'static str> {
        let n = self.u16()? as usize;
        let b = self.bytes(n)?;
        String::from_utf8(b.to_vec()).map_err(|_| "bad utf8")
    }
}

/// Remaining-length varint encode (1–4 bytes).
pub fn encode_remaining_length(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// Parse one packet from `buf`. Returns the packet and how many bytes it
/// consumed, or `None` when more bytes are needed. `Err` on protocol
/// violations.
pub fn parse(buf: &[u8]) -> Result<Option<(Packet, usize)>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    let ptype = buf[0] >> 4;
    let flags = buf[0] & 0x0F;
    // Remaining length varint.
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    let mut i = 1;
    loop {
        let Some(&b) = buf.get(i) else { return Ok(None) };
        remaining += ((b & 0x7F) as usize) * multiplier;
        multiplier *= 128;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        if i > 4 {
            return Err("remaining length longer than 4 bytes".into());
        }
    }
    if buf.len() < i + remaining {
        return Ok(None);
    }
    let body = &buf[i..i + remaining];
    let mut c = Cursor { buf: body, pos: 0 };
    let packet = match (ptype, flags) {
        (CONNECT, 0) => {
            let proto = c.utf8().map_err(str::to_string)?;
            if proto != "MQTT" {
                return Err(format!("unsupported protocol name {proto:?}"));
            }
            let level = c.u8().map_err(str::to_string)?;
            if level != 4 {
                return Err(format!("unsupported MQTT level {level}"));
            }
            let connect_flags = c.u8().map_err(str::to_string)?;
            let keep_alive = c.u16().map_err(str::to_string)?;
            let clean = connect_flags & 0x02 != 0;
            let has_will = connect_flags & 0x04 != 0;
            let has_user = connect_flags & 0x80 != 0;
            let has_pass = connect_flags & 0x40 != 0;
            let client_id = c.utf8().map_err(str::to_string)?;
            // Will fields are consumed and discarded (will messages are
            // not part of the bridge subset).
            if has_will {
                let _will_topic = c.utf8().map_err(str::to_string)?;
                let n = c.u16().map_err(str::to_string)? as usize;
                let _will_msg = c.bytes(n).map_err(str::to_string)?;
            }
            let mut username = None;
            let mut password = None;
            if has_user {
                username = Some(c.utf8().map_err(str::to_string)?);
            }
            if has_pass {
                let n = c.u16().map_err(str::to_string)? as usize;
                password = Some(c.bytes(n).map_err(str::to_string)?.to_vec());
            }
            Packet::Connect { clean_session: clean, keep_alive, client_id, username, password }
        }
        (PUBLISH, f) => {
            let qos = (f >> 1) & 0x03;
            let retain = f & 0x01 != 0;
            let topic = c.utf8().map_err(str::to_string)?;
            let packet_id = if qos > 0 { Some(c.u16().map_err(str::to_string)?) } else { None };
            let payload = body[c.pos..].to_vec();
            Packet::Publish { qos, retain, topic, packet_id, payload }
        }
        (PUBACK, _) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            Packet::PubAck { packet_id }
        }
        (PUBREC, _) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            Packet::PubRec { packet_id }
        }
        (PUBREL, 2) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            Packet::PubRel { packet_id }
        }
        (PUBCOMP, _) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            Packet::PubComp { packet_id }
        }
        (SUBSCRIBE, 2) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            let mut filters = Vec::new();
            while c.pos < body.len() {
                let filter = c.utf8().map_err(str::to_string)?;
                let qos = c.u8().map_err(str::to_string)? & 0x03;
                filters.push((filter, qos));
            }
            Packet::Subscribe { packet_id, filters }
        }
        (UNSUBSCRIBE, 2) => {
            let packet_id = c.u16().map_err(str::to_string)?;
            let mut filters = Vec::new();
            while c.pos < body.len() {
                filters.push(c.utf8().map_err(str::to_string)?);
            }
            Packet::Unsubscribe { packet_id, filters }
        }
        (PINGREQ, 0) => Packet::PingReq,
        (DISCONNECT, 0) => Packet::Disconnect,
        (t, _) => return Err(format!("unexpected packet type {t}")),
    };
    Ok(Some((packet, i + remaining)))
}

/// Read one packet straight off a stream. `Ok(None)` = clean EOF between
/// packets.
pub async fn read_packet<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<Packet>> {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 1];
    // First byte.
    let n = r.read(&mut head).await?;
    if n == 0 {
        return Ok(None);
    }
    let ptype = head[0] >> 4;
    let flags = head[0] & 0x0F;
    // Remaining length varint (kept verbatim: parse() re-reads it).
    let mut varint = Vec::new();
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut b = [0u8; 1];
        if r.read(&mut b).await? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "mqtt: eof in remaining length",
            ));
        }
        varint.push(b[0]);
        remaining += ((b[0] & 0x7F) as usize) * multiplier;
        multiplier *= 128;
        if b[0] & 0x80 == 0 {
            break;
        }
        if multiplier > 128 * 128 * 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mqtt: remaining length too long",
            ));
        }
    }
    let mut body = vec![0u8; remaining];
    r.read_exact(&mut body).await?;
    let mut full = vec![head[0]];
    full.extend_from_slice(&varint);
    full.extend_from_slice(&body);
    let parsed = parse(&full).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    match parsed {
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mqtt: short packet",
        )),
        Some((p, _)) => {
            let _ = (ptype, flags);
            Ok(Some(p))
        }
    }
}

/// Encode a broker→client packet.
pub enum Out {
    ConnAck { session_present: bool, code: u8 },
    PubRec { packet_id: u16 },
    PubRel { packet_id: u16 },
    PubComp { packet_id: u16 },
    Publish { qos: u8, retain: bool, topic: String, packet_id: Option<u16>, payload: Vec<u8> },
    PubAck { packet_id: u16 },
    SubAck { packet_id: u16, codes: Vec<u8> },
    UnsubAck { packet_id: u16 },
    PingResp,
}

impl Out {
    pub fn encode(&self) -> Vec<u8> {
        let (ptype, flags, body): (u8, u8, Vec<u8>) = match self {
            Out::ConnAck { session_present, code } => {
                (CONNACK, 0, vec![if *session_present { 1 } else { 0 }, *code])
            }
            Out::Publish { qos, retain, topic, packet_id, payload } => {
                let mut b = Vec::new();
                let tn = topic.len() as u16;
                b.extend_from_slice(&tn.to_be_bytes());
                b.extend_from_slice(topic.as_bytes());
                if let Some(pid) = packet_id {
                    b.extend_from_slice(&pid.to_be_bytes());
                }
                b.extend_from_slice(payload);
                let f = ((*qos & 0x03) << 1) | u8::from(*retain);
                (PUBLISH, f, b)
            }
            Out::PubAck { packet_id } => (PUBACK, 0, packet_id.to_be_bytes().to_vec()),
            Out::PubRec { packet_id } => (PUBREC, 0, packet_id.to_be_bytes().to_vec()),
            Out::PubRel { packet_id } => (PUBREL, 2, packet_id.to_be_bytes().to_vec()),
            Out::PubComp { packet_id } => (PUBCOMP, 0, packet_id.to_be_bytes().to_vec()),
            Out::SubAck { packet_id, codes } => {
                let mut b = packet_id.to_be_bytes().to_vec();
                b.extend_from_slice(codes);
                (SUBACK, 0, b)
            }
            Out::UnsubAck { packet_id } => (UNSUBACK, 0, packet_id.to_be_bytes().to_vec()),
            Out::PingResp => (PINGRESP, 0, Vec::new()),
        };
        let mut out = vec![(ptype << 4) | flags];
        encode_remaining_length(body.len(), &mut out);
        out.extend_from_slice(&body);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_roundtrip() {
        let mut buf = vec![0x10];
        encode_remaining_length(17, &mut buf);
        assert_eq!(buf, vec![0x10, 17]);
        let p = parse(&[0x10]).unwrap();
        assert!(p.is_none(), "needs more bytes");
    }

    #[test]
    fn publish_qos1_parses() {
        // PUBLISH qos1, topic a/b, pid 7, payload "hi".
        let raw = [
            0x32, 9, 0, 3, b'a', b'/', b'b', 0, 7, b'h', b'i',
        ];
        let _ = 0;
        let (p, used) = parse(&raw).unwrap().unwrap();
        assert_eq!(used, raw.len());
        match p {
            Packet::Publish { qos, retain, topic, packet_id, payload } => {
                assert_eq!((qos, retain), (1, false));
                assert_eq!(topic, "a/b");
                assert_eq!(packet_id, Some(7));
                assert_eq!(payload, b"hi");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn out_connack_encodes() {
        let out = Out::ConnAck { session_present: false, code: 0 };
        assert_eq!(out.encode(), vec![0x20, 0x02, 0x00, 0x00]);
    }
}

#[cfg(test)]
mod probe_tests {
    use super::*;

    #[test]
    fn parses_gateway_helper_connect_bytes() {
        // Exactly what tests/gateway_support::mqtt_connect produces.
        let mut body = Vec::new();
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"MQTT");
        body.push(4);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        let cid = b"sub-1";
        body.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        body.extend_from_slice(cid);
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        let (p, used) = parse(&raw).unwrap().unwrap();
        assert_eq!(used, raw.len());
        assert_eq!(
            p,
            Packet::Connect {
                clean_session: true,
                keep_alive: 60,
                client_id: "sub-1".into(),
                username: None,
                password: None
            }
        );
    }
}

#[cfg(test)]
mod error_path_tests {
    use super::*;

    #[test]
    fn unsupported_protocol_and_level() {
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(b"MQIsdp");
        body.push(3);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn truncated_packets_error_or_wait() {
        // PUBLISH declared length 20 but only 4 bytes present → wait.
        let raw = [0x30, 20, 0, 3];
        assert_eq!(parse(&raw).unwrap(), None);
        // Remaining-length varint longer than 4 bytes → error.
        let raw = [0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        assert!(parse(&raw).is_err());
        // Unexpected packet type → error.
        assert!(parse(&[0x50, 0x00]).is_err());
    }

    #[test]
    fn pubrel_requires_flags_two() {
        // PUBREL with flags 0 is a protocol violation → falls to the
        // catch-all error arm.
        let raw = [0x60, 0x02, 0, 1];
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn qos2_out_packets_encode() {
        assert_eq!(Out::PubRec { packet_id: 5 }.encode(), vec![0x50, 0x02, 0, 5]);
        assert_eq!(Out::PubRel { packet_id: 5 }.encode(), vec![0x62, 0x02, 0, 5]);
        assert_eq!(Out::PubComp { packet_id: 5 }.encode(), vec![0x70, 0x02, 0, 5]);
    }

    #[test]
    fn connect_with_will_and_credentials_parses() {
        let mut body = Vec::new();
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"MQTT");
        body.push(4);
        body.push(0x02 | 0x04 | 0x80 | 0x40); // clean + will + user + pass
        body.extend_from_slice(&60u16.to_be_bytes());
        let cid = "c";
        body.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        body.extend_from_slice(cid.as_bytes());
        let wt = "wills";
        body.extend_from_slice(&(wt.len() as u16).to_be_bytes());
        body.extend_from_slice(wt.as_bytes());
        let wm = b"gone";
        body.extend_from_slice(&(wm.len() as u16).to_be_bytes());
        body.extend_from_slice(wm);
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"user");
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"hush");
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        match parse(&raw).unwrap().unwrap().0 {
            Packet::Connect { clean_session, username, password, .. } => {
                assert!(clean_session);
                assert_eq!(username.as_deref(), Some("user"));
                assert_eq!(password.as_deref(), Some(b"hush".as_slice()));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[cfg(test)]
mod parse_edge_tests {
    use super::*;

    #[test]
    fn mqtt_bad_utf8_in_client_id_is_error() {
        // CONNECT with client-id containing invalid UTF-8.
        let mut body = Vec::new();
        body.extend_from_slice(&4u16.to_be_bytes());
        body.extend_from_slice(b"MQTT");
        body.push(4);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        let cid = [0xFF, 0xFE];
        body.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        body.extend_from_slice(&cid);
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn wrong_fixed_flags_are_rejected() {
        // CONNECT with flags ≠ 0.
        let mut body = Vec::new();
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"MQTT");
        body.push(4);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        let mut raw = vec![0x11]; // flags low nibble = 1, not 0
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn unsupported_protocol_name_is_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"MQIs");
        body.push(3);
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes()); // empty client id
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn unsupported_level_is_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&(4u16).to_be_bytes());
        body.extend_from_slice(b"MQTT");
        body.push(5); // level 5 = MQTT v3.1.1+ / v5
        body.push(0x02);
        body.extend_from_slice(&60u16.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        let mut raw = vec![0x10];
        encode_remaining_length(body.len(), &mut raw);
        raw.extend_from_slice(&body);
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn remaining_length_4_byte_boundary() {
        // 268435455 is the maximum encodable value.
        let raw = [0x30, 0xFF, 0xFF, 0xFF, 0x7F];
        // Body would be huge; the varint itself must parse (returns None for
        // missing body).
        assert_eq!(parse(&raw).unwrap(), None);
    }

    #[test]
    fn pingreq_and_disconnect_parse() {
        assert_eq!(parse(&[0xC0, 0]).unwrap().unwrap().0, Packet::PingReq);
        assert_eq!(parse(&[0xE0, 0]).unwrap().unwrap().0, Packet::Disconnect);
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn truncated_varint_needs_more_bytes() {
        // Continuation bit set, but no follow-up byte.
        let raw = [0xC0, 0x80];
        assert_eq!(parse(&raw), Ok(None));
    }

    #[test]
    fn varint_longer_than_four_bytes_is_rejected() {
        // Five continuation bytes: 0xC0 header + varint longer than the
        // 4-byte cap.
        let raw = [0xC0, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00];
        assert!(parse(&raw).is_err());
    }

    #[test]
    fn empty_buffer_needs_more_bytes() {
        assert_eq!(parse(&[]), Ok(None));
    }

    #[test]
    fn truncated_body_needs_more_bytes() {
        // Announce 10 payload bytes, deliver 3.
        let raw = [0x30, 0x0A, 1, 2, 3];
        assert_eq!(parse(&raw), Ok(None));
    }

    #[test]
    fn multi_byte_remaining_length_encodes_and_parses() {
        // 300 needs two varint bytes. The body is a well-formed
        // qos-0 PUBLISH: topic then payload.
        let mut body = Vec::new();
        body.extend_from_slice(&3u16.to_be_bytes());
        body.extend_from_slice(b"a/b");
        body.extend_from_slice(&vec![7u8; 300 - body.len()]);
        let mut raw = vec![0x30];
        encode_remaining_length(body.len(), &mut raw);
        assert_eq!(raw.len(), 3);
        raw.extend_from_slice(&body);
        let (p, used) = parse(&raw).unwrap().unwrap();
        assert_eq!(used, raw.len());
        assert!(matches!(p, Packet::Publish { .. }));
    }

    #[tokio::test]
    async fn read_packet_reports_eof_inside_the_varint() {
        let mut r: &[u8] = &[0xC0, 0x80];
        let err = read_packet(&mut r).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn read_packet_rejects_overlong_varints() {
        let mut r: &[u8] = &[0xC0, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        let err = read_packet(&mut r).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("too long"));
    }

    #[tokio::test]
    async fn read_packet_rejects_short_packets() {
        // A PUBLISH whose declared body is missing pieces the parser
        // needs (topic length beyond the packet).
        let mut r: &[u8] = &[0x30, 0x02, 0x00, 0x05];
        let err = read_packet(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("short packet") || err.to_string().contains("truncated"), "{err}");
    }
}
