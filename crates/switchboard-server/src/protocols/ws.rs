//! WebSocket transport (RFC 6455), server side.
//!
//! The handshake runs on the sniffed connection; afterwards the socket is
//! wrapped in [`WsStream`], which implements `AsyncRead`/`AsyncWrite` on
//! top of WebSocket frames — binary payloads only. Inner protocols see an
//! ordinary duplex stream, so the very same MQTT/STOMP/AMQP 1.0 servers
//! run over TCP and over WebSocket.
//!
//! Protocol selection inside WebSocket, in priority order:
//! 1. the `Sec-WebSocket-Protocol` header (`mqtt`, `stomp`, `amqp`),
//! 2. a `/mqtt`, `/stomp`, `/amqp10` request path,
//! 3. sniffing the first frame's payload (same classifier as raw TCP).

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use sha1::{Digest, Sha1};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use switchboard_cluster::ClusterNode;

use super::detect::classify;
use super::detect::Classify;
use super::detect::Detected;
use super::gateway::Prefixed;
use super::shared::ProtocolConfig;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// Refuse absurd frames (protection against runaway peers).
const MAX_FRAME: usize = 32 * 1024 * 1024;

fn accept_key(client_key: &str) -> String {
    let mut h = Sha1::new();
    h.update(client_key.trim().as_bytes());
    h.update(WS_GUID.as_bytes());
    B64.encode(h.finalize())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opcode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl Opcode {
    fn from_u8(v: u8) -> Option<Opcode> {
        Some(match v {
            0x0 => Opcode::Continuation,
            0x1 => Opcode::Text,
            0x2 => Opcode::Binary,
            0x8 => Opcode::Close,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            _ => return None,
        })
    }
}

/// Parsed handshake request (what the selector needs from it).
struct Handshake {
    path: String,
    subprotocol: Option<String>,
    accept_key: String,
}

fn parse_handshake(head: &[u8]) -> Option<Handshake> {
    let text = std::str::from_utf8(head).ok()?;
    let mut lines = text.split("\r\n");
    let request = lines.next()?;
    let path = request.split_whitespace().nth(1)?.to_string();
    let mut subprotocol = None;
    let mut key = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else { continue };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name == "sec-websocket-protocol" {
            subprotocol = value.split(',').map(str::trim).next().map(str::to_string);
        } else if name == "sec-websocket-key" {
            key = Some(value.to_string());
        }
    }
    Some(Handshake { path, subprotocol, accept_key: key? })
}

/// The selected inner protocol for a WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inner {
    Mqtt,
    Stomp,
    Amqp10,
}

fn select_inner(path: &str, subprotocol: Option<&str>) -> Option<Inner> {
    if let Some(p) = subprotocol {
        match p.to_ascii_lowercase().as_str() {
            "mqtt" | "mqttv3.1" | "mqttv3.1.1" => return Some(Inner::Mqtt),
            "stomp" => return Some(Inner::Stomp),
            "amqp" | "amqp10" | "amqp1.0" => return Some(Inner::Amqp10),
            _ => {}
        }
    }
    let path = path.to_ascii_lowercase();
    if path.starts_with("/mqtt") {
        Some(Inner::Mqtt)
    } else if path.starts_with("/stomp") {
        Some(Inner::Stomp)
    } else if path.starts_with("/amqp10") || path.starts_with("/amqp1") {
        Some(Inner::Amqp10)
    } else {
        None
    }
}

/// Encode one outbound frame (server frames are unmasked).
fn encode_frame(opcode: Opcode, payload: &[u8], out: &mut Vec<u8>) {
    out.push(0x80 | opcode as u8);
    let len = payload.len();
    if len < 126 {
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
    out.extend_from_slice(payload);
}

/// One frame pulled off the wire.
#[derive(Debug, PartialEq)]
struct Frame {
    opcode: Opcode,
    payload: Vec<u8>,
}

/// A duplex WebSocket stream: `AsyncRead` yields payload bytes
/// (reassembling frames; answering pings; turning peer close frames into
/// EOF), `AsyncWrite` coalesces writes into binary frames on flush.
pub struct WsStream<S> {
    inner: S,
    /// Partially received frame bytes (header/payload not yet complete).
    raw: Vec<u8>,
    /// Payload bytes read from frames but not yet consumed by the inner
    /// protocol.
    inbox: Vec<u8>,
    inbox_pos: usize,
    /// Control/data frames queued for the peer.
    out_frames: Vec<u8>,
    /// Offset into `out_frames` up to which the inner stream is written.
    out_written: usize,
    /// Set when a peer close frame (or EOF) was seen.
    eof: bool,
    /// Set after a close reply was queued.
    close_sent: bool,
    /// Pre-framed close frame pending a write to the inner stream.
    close_frame: Option<Vec<u8>>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsStream<S> {
    fn new(inner: S) -> Self {
        WsStream {
            inner,
            raw: Vec::new(),
            inbox: Vec::new(),
            inbox_pos: 0,
            out_frames: Vec::new(),
            out_written: 0,
            eof: false,
            close_sent: false,
            close_frame: None,
        }
    }

}

fn parse_frame_free(raw: &mut Vec<u8>) -> Result<Option<Frame>, std::io::Error> {
    if raw.len() < 2 {
        return Ok(None);
    }
    let b0 = raw[0];
    let b1 = raw[1];
    let opcode = Opcode::from_u8(b0 & 0x0F).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "ws: bad opcode")
    })?;
    if b1 & 0x80 == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ws: client frames must be masked",
        ));
    }
    let mut len = (b1 & 0x7F) as usize;
    let mut offset = 2usize;
    if len == 126 {
        if raw.len() < offset + 2 {
            return Ok(None);
        }
        len = u16::from_be_bytes([raw[2], raw[3]]) as usize;
        offset += 2;
    } else if len == 127 {
        if raw.len() < offset + 8 {
            return Ok(None);
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&raw[offset..offset + 8]);
        len = usize::try_from(u64::from_be_bytes(b)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "ws: frame too large")
        })?;
        offset += 8;
    }
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ws: frame too large",
        ));
    }
    if raw.len() < offset + 4 + len {
        return Ok(None);
    }
    let mask = [raw[offset], raw[offset + 1], raw[offset + 2], raw[offset + 3]];
    let payload_start = offset + 4;
    let mut payload = raw[payload_start..payload_start + len].to_vec();
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    raw.drain(..payload_start + len);
    Ok(Some(Frame { opcode, payload }))
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsStream<S> {
    /// Drain complete frames from `raw`, updating inbox/queue state.
    /// Returns true when the connection should be treated as EOF.
    fn absorb_frames(&mut self) -> Result<bool, std::io::Error> {
        loop {
            let Some(frame) = parse_frame_free(&mut self.raw)? else {
                return Ok(false);
            };
            match frame.opcode {
                Opcode::Binary | Opcode::Text | Opcode::Continuation => {
                    self.inbox.extend_from_slice(&frame.payload);
                }
                Opcode::Ping => {
                    if !self.close_sent {
                        encode_frame(Opcode::Pong, &frame.payload, &mut self.out_frames);
                    }
                }
                Opcode::Pong => {}
                Opcode::Close => {
                    if !self.close_sent {
                        self.close_sent = true;
                        encode_frame(Opcode::Close, &frame.payload, &mut self.out_frames);
                    }
                    return Ok(true);
                }
            }
        }
    }

    fn inbox_bytes(&self) -> &[u8] {
        &self.inbox[self.inbox_pos.min(self.inbox.len())..]
    }

    /// Push bytes back to the front of the read stream (used by the
    /// gateway after sniffing, so the inner protocol sees the whole
    /// conversation from byte 0).
    fn unconsume(&mut self, bytes: &[u8]) {
        let rest = self.inbox[self.inbox_pos.min(self.inbox.len())..].to_vec();
        self.inbox.clear();
        self.inbox.extend_from_slice(bytes);
        self.inbox.extend_from_slice(&rest);
        self.inbox_pos = 0;
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        loop {
            if !this.inbox_bytes().is_empty() {
                let src = this.inbox_bytes();
                let n = src.len().min(buf.remaining());
                buf.put_slice(&src[..n]);
                this.inbox_pos += n;
                if this.inbox_pos >= this.inbox.len() {
                    this.inbox.clear();
                    this.inbox_pos = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if this.eof {
                return Poll::Ready(Ok(())); // 0 bytes = EOF for the reader
            }
            // Need more wire bytes.
            let mut tmp = [0u8; 4096];
            let mut tmp_buf = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.inner).poll_read(cx, &mut tmp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = tmp_buf.filled().len();
                    if n == 0 {
                        this.eof = true;
                        return Poll::Ready(Ok(()));
                    }
                    this.raw.extend_from_slice(tmp_buf.filled());
                    match this.absorb_frames() {
                        Ok(true) => {
                            this.eof = true;
                            // Loop once more: frames before the close
                            // belong to the reader.
                        }
                        Ok(false) => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // Coalesce; bytes hit the wire on flush (protocols flush per
        // message, so latency is bounded by the protocol itself).
        self.out_frames.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        // Any buffered-but-unwritten payload must be turned into exactly
        // one binary frame before it touches the wire.
        if this.out_written < this.out_frames.len() {
            let payload = std::mem::take(&mut this.out_frames);
            let mut framed = Vec::with_capacity(payload.len() + 10);
            encode_frame(Opcode::Binary, &payload, &mut framed);
            this.out_frames = framed;
            this.out_written = 0;
        } else if this.out_frames.is_empty() {
            this.out_written = 0;
            return Pin::new(&mut this.inner).poll_flush(cx);
        }
        // Drain the framed bytes into the inner stream.
        while this.out_written < this.out_frames.len() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.out_frames[this.out_written..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "ws: inner wrote 0",
                    )));
                }
                Poll::Ready(Ok(n)) => this.out_written += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        this.out_frames.clear();
        this.out_written = 0;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if !this.close_sent {
            this.close_sent = true;
            // `out_frames` holds RAW payload until poll_flush frames it;
            // a close frame must reach the wire already framed, so it is
            // appended pre-framed and marked as written-past framing
            // (out_written covers the framed region below by flushing
            // first, then queueing the encoded close frame directly).
            this.close_frame = Some({
                let mut f = Vec::new();
                encode_frame(Opcode::Close, &[], &mut f);
                f
            });
        }
        // Flush any raw payload first, then the close frame itself.
        let _ = Pin::new(&mut *this).poll_flush(cx);
        if let Some(close) = this.close_frame.take() {
            let mut written = 0usize;
            while written < close.len() {
                match Pin::new(&mut this.inner).poll_write(cx, &close[written..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "ws: inner wrote 0",
                        )));
                    }
                    Poll::Ready(Ok(n)) => written += n,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            Pin::new(&mut this.inner).poll_flush(cx)?;
        }
        match Pin::new(&mut *this).poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

/// Run the handshake (the request head is already read), select the
/// inner protocol, and serve it over the framed stream.
pub async fn serve_with_head<S>(
    io: Prefixed<S>,
    head: &[u8],
    node: Arc<ClusterNode>,
    protocols: ProtocolConfig,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut io = io;
    let Some(handshake) = parse_handshake(head) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ws: malformed upgrade request",
        ));
    };
    let inner = select_inner(&handshake.path, handshake.subprotocol.as_deref());
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(&handshake.accept_key)
    );
    use tokio::io::AsyncWriteExt;
    io.write_all(response.as_bytes()).await?;
    io.flush().await?;

    let mut stream = WsStream::new(io);
    let mut sniff_buffer: Vec<u8> = Vec::new();
    let selected = match inner {
        Some(i) => Some(i),
        None => {
            // No hint from path or subprotocol: sniff the first payload.
            // The sniffed bytes are pushed back below so the inner
            // protocol reads its stream from byte 0.
            loop {
                let mut one = [0u8; 256];
                let n = tokio::io::AsyncReadExt::read(&mut stream, &mut one).await?;
                if n == 0 {
                    return Ok(());
                }
                sniff_buffer.extend_from_slice(&one[..n]);
                match classify(&sniff_buffer) {
                    Classify::Yes(Detected::Mqtt) => break Some(Inner::Mqtt),
                    Classify::Yes(Detected::Stomp) => break Some(Inner::Stomp),
                    Classify::Yes(Detected::Amqp10) => break Some(Inner::Amqp10),
                    Classify::Yes(Detected::Amqp091) => break None,
                    Classify::Yes(Detected::Http) | Classify::Unknown => break None,
                    Classify::NeedMore => continue,
                }
            }
        }
    };
    stream.unconsume(&sniff_buffer);
    match selected {
        Some(Inner::Mqtt) if protocols.mqtt => super::mqtt::serve(stream, node).await,
        Some(Inner::Stomp) if protocols.stomp => super::stomp::serve(stream, node).await,
        Some(Inner::Amqp10) if protocols.amqp10 => super::amqp10::serve(stream, node).await,
        _ => Ok(()), // unrecognized or disabled inner protocol
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_is_rfc6455_example() {
        // The example from RFC 6455 §1.3.
        assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn handshake_parsing() {
        let head = b"GET /mqtt HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: aQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n";
        let hs = parse_handshake(head).unwrap();
        assert_eq!(hs.path, "/mqtt");
        assert_eq!(hs.subprotocol.as_deref(), Some("mqtt"));
        assert_eq!(hs.accept_key, "aQ==");
    }

    #[test]
    fn inner_selection() {
        assert_eq!(select_inner("/mqtt", None), Some(Inner::Mqtt));
        assert_eq!(select_inner("/x", Some("stomp")), Some(Inner::Stomp));
        assert_eq!(select_inner("/amqp10", None), Some(Inner::Amqp10));
        assert_eq!(select_inner("/ws", None), None);
    }

    #[test]
    fn frame_parse_roundtrip_with_mask() {
        // Client-style masked frame parsed through the free helper.
        let mut raw = vec![0x82, 0x85]; // FIN|binary, len 5|mask bit
        raw.extend_from_slice(&[1, 2, 3, 4]); // mask
        raw.extend_from_slice(&[b'h' ^ 1, b'i' ^ 2, b'!' ^ 3, b'!' ^ 4, b'!' ^ 1]);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.opcode, Opcode::Binary);
        assert_eq!(f.payload, b"hi!!!");
        assert!(raw.is_empty());
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::io::duplex;

    /// In-memory duplex satisfying the WsStream bounds.
    async fn test_stream() -> WsStream<tokio::io::DuplexStream> {
        let (a, _b) = duplex(1024);
        WsStream::new(a)
    }

    #[test]
    fn parse_rejects_unmasked_client_frames() {
        let mut raw = vec![0x82, 0x02, b'h', b'i']; // no mask bit
        assert!(parse_frame_free(&mut raw).is_err());
    }

    #[test]
    fn parse_waits_for_partial_frames() {
        // Header says 4 masked bytes; only the mask arrived.
        let mut raw = vec![0x82, 0x84, 1, 2, 3, 4];
        assert_eq!(parse_frame_free(&mut raw).unwrap(), None);
        // Complete it with 4 masked payload bytes.
        raw.extend_from_slice(&[1 ^ 1, 2 ^ 2, 3 ^ 3, 4 ^ 4]);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload, vec![1, 2, 3, 4], "mask XOR unmasked zero bytes");
    }

    #[test]
    fn parse_rejects_bad_opcode() {
        let mut raw = vec![0x83, 0x80, 0, 0, 0, 0];
        assert!(parse_frame_free(&mut raw).is_err());
    }

    #[test]
    fn absorb_frames_queues_ping_and_close() {
        let mut s = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(test_stream());
        // Ping (masked, empty).
        s.raw = vec![0x89, 0x80, 0, 0, 0, 0];
        assert!(!s.absorb_frames().unwrap());
        assert!(!s.out_frames.is_empty(), "pong queued");
        // Close (masked, empty) — absorb reports EOF to the poller.
        s.raw = vec![0x88, 0x80, 0, 0, 0, 0];
        assert!(s.absorb_frames().unwrap());
        assert!(s.close_sent);
    }


    #[test]
    fn select_inner_recognizes_amqp_subprotocols() {
        assert_eq!(select_inner("/x", Some("amqp")), Some(Inner::Amqp10));
        assert_eq!(select_inner("/x", Some("amqp10")), Some(Inner::Amqp10));
        assert_eq!(select_inner("/x", Some("amqp1.0")), Some(Inner::Amqp10));
    }

    fn enc(opcode: Opcode, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_frame(opcode, payload, &mut out);
        out
    }

    /// Build a masked client frame of arbitrary length (126/127 paths).
    fn masked_frame_long(opcode: u8, payload_len: usize) -> Vec<u8> {
        let mut out = vec![0x80 | opcode];
        let mask = [1u8, 2, 3, 4];
        if payload_len < 126 {
            out.push(0x80 | payload_len as u8);
        } else if payload_len <= 0xFFFF {
            out.push(0x80 | 126);
            out.extend_from_slice(&(payload_len as u16).to_be_bytes());
        } else {
            out.push(0x80 | 127);
            out.extend_from_slice(&(payload_len as u64).to_be_bytes());
        }
        out.extend_from_slice(&mask);
        for i in 0..payload_len {
            out.push(0 ^ mask[i % 4]);
        }
        out
    }

    #[test]
    fn frame_length_encodes_16bit_and_64bit_paths() {
        // Server-side encoding picks the extended lengths.
        let medium = enc(Opcode::Binary, &vec![7u8; 300]);
        assert_eq!(medium[1] & 0x7F, 126);
        let huge = enc(Opcode::Binary, &vec![9u8; 70_000]);
        assert_eq!(huge[1] & 0x7F, 127);
        // Client-side masked frames of both shapes parse losslessly.
        let mut raw = masked_frame_long(0x2, 300);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload.len(), 300);
        let mut raw = masked_frame_long(0x2, 70_000);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload.len(), 70_000);
    }

    #[test]
    fn parse_reads_16bit_length_header() {
        // Hand-built: FIN+binary, 16-bit length = 0x0102, mask, payload.
        let mut raw = vec![0x82, 0x80 | 126, 0x01, 0x02, 1, 2, 3, 4];
        // Only header + mask so far: needs more.
        assert_eq!(parse_frame_free(&mut raw.clone()).unwrap(), None);
        raw.extend_from_slice(&[0u8; 0x0102]);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload.len(), 0x0102);
    }

    #[test]
    fn parse_reads_64bit_length_header() {
        let mut raw = vec![0x82, 0x80 | 127];
        raw.extend_from_slice(&70_000u64.to_be_bytes());
        raw.extend_from_slice(&[1, 2, 3, 4]); // mask
        assert_eq!(parse_frame_free(&mut raw.clone()).unwrap(), None);
        raw.extend_from_slice(&[0u8; 70_000]);
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload.len(), 70_000);
    }

    #[test]
    fn unknown_subprotocol_falls_back_to_the_path() {
        assert_eq!(select_inner("/mqtt", Some("bogus")), Some(Inner::Mqtt));
        assert_eq!(select_inner("/ws", Some("bogus")), None);
    }

    #[test]
    fn masked_short_frames_roundtrip() {
        let mut raw = masked_frame(0x2, b"tiny");
        let f = parse_frame_free(&mut raw).unwrap().unwrap();
        assert_eq!(f.payload, b"tiny");
    }

    #[test]
    fn truncated_extended_lengths_need_more_bytes() {
        // 16-bit length announced but truncated inside the length field.
        let mut raw = vec![0x82, 0x80 | 126, 0x01];
        assert_eq!(parse_frame_free(&mut raw).unwrap(), None);
        // 64-bit length announced but truncated inside the length field.
        let mut raw = vec![0x82, 0x80 | 127, 1, 2, 3];
        assert_eq!(parse_frame_free(&mut raw).unwrap(), None);
    }

    #[test]
    fn peer_vanish_surfaces_as_a_write_error() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (a, b) = duplex(64);
            let mut ws = WsStream::new(a);
            drop(b);
            // Payload buffers fine; the flush fails because the peer is
            // gone.
            use tokio::io::AsyncWriteExt;
            ws.write_all(b"payload").await.unwrap();
            assert!(ws.flush().await.is_err(), "flush must surface the peer's disappearance");
        });
    }

    #[test]
    fn parse_rejects_frames_beyond_the_size_cap() {
        let mut raw = vec![0x82, 0x80 | 127];
        raw.extend_from_slice(&u64::MAX.to_be_bytes());
        raw.extend_from_slice(&[1, 2, 3, 4]);
        assert!(parse_frame_free(&mut raw).is_err(), "absurd 64-bit length must be rejected");
    }

    /// Build a masked client frame (helper local to this module).
    fn masked_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x80 | opcode, 0x80 | payload.len() as u8, 1, 2, 3, 4];
        for (i, b) in payload.iter().enumerate() {
            out.push(b ^ [1u8, 2, 3, 4][i % 4]);
        }
        out
    }

    #[test]
    fn ping_is_answered_with_pong_and_pong_is_ignored() {
        let mut s = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(test_stream());
        // PING (masked) → PONG queued.
        s.raw = masked_frame(0x9, b"hb");
        assert!(!s.absorb_frames().unwrap());
        let pong = s.out_frames.clone();
        assert!(!pong.is_empty() && pong[0] & 0x0F == 0xA, "pong queued");
        // PONG from the peer is absorbed silently.
        s.raw = masked_frame(0xA, b"hb");
        assert!(!s.absorb_frames().unwrap());
        assert_eq!(s.out_frames.len(), pong.len(), "no reply to pong");
    }

    #[test]
    fn shutdown_sends_a_close_frame_then_shuts_the_inner_stream() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (a, mut b) = duplex(4096);
            let mut ws = WsStream::new(a);
            use tokio::io::AsyncWriteExt;
            ws.shutdown().await.unwrap();
            // The peer sees a close frame (retry reads: the flush may
            // need a couple of polls to move the queued frame).
            let mut buf = vec![0u8; 64];
            let n = tokio::io::AsyncReadExt::read(&mut b, &mut buf).await.unwrap_or(0);
            assert!(
                n > 0 && buf[0] & 0x0F == 0x8,
                "close frame written (n={n}, buf={:?})",
                &buf[..n.min(buf.len())]
            );
        });
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use tokio::io::duplex;

    fn masked_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x80 | opcode, 0x80 | payload.len() as u8, 1, 2, 3, 4];
        for (i, b) in payload.iter().enumerate() {
            out.push(b ^ [1u8, 2, 3, 4][i % 4]);
        }
        out
    }

    #[tokio::test]
    async fn ws_stream_delivers_binary_payload() {
        let (mut client, server) = duplex(4096);
        let mut ws = WsStream::new(server);
        // Write a masked binary frame into the client end.
        let frame = {
            let mut f = vec![0x82, 0x80 | 5, 1, 2, 3, 4];
            for (i, b) in b"hello".iter().enumerate() {
                f.push(b ^ [1u8, 2, 3, 4][i % 4]);
            }
            f
        };
        use tokio::io::AsyncWriteExt;
        client.write_all(&frame).await.unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::io::AsyncReadExt::read(&mut ws, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    #[tokio::test]
    async fn ws_stream_writes_are_framed_for_the_peer() {
        let (mut client, server) = duplex(4096);
        let mut ws = WsStream::new(server);
        tokio::io::AsyncWriteExt::write_all(&mut ws, b"payload").await.unwrap();
        tokio::io::AsyncWriteExt::flush(&mut ws).await.unwrap();
        // The peer sees a framed binary message.
        let mut hdr = [0u8; 2];
        tokio::io::AsyncReadExt::read_exact(&mut client, &mut hdr).await.unwrap();
        assert_eq!(hdr[0] & 0x80, 0x80, "FIN set");
        assert_eq!(hdr[0] & 0x0F, 2, "binary opcode");
        assert_eq!(hdr[1] & 0x80, 0, "server frames are unmasked");
        let len = (hdr[1] & 0x7F) as usize;
        let mut payload = vec![0u8; len];
        tokio::io::AsyncReadExt::read_exact(&mut client, &mut payload).await.unwrap();
        assert_eq!(payload, b"payload");
    }

    #[tokio::test]
    async fn ws_close_frame_yields_eof_to_reader() {
        let (mut client, server) = duplex(4096);
        let mut ws = WsStream::new(server);
        // Close frame with a two-byte status (masked by the client).
        tokio::io::AsyncWriteExt::write_all(
            &mut client,
            &[0x88, 0x82, 9, 9, 9, 9, 0x03, 0xE8 ^ 9 ^ 9 ^ 9 ^ 9],
        )
        .await
        .unwrap();
        let mut buf = [0u8; 32];
        // First read may see queued data; the loop must terminate.
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut ws, &mut buf).await.unwrap();
            if n == 0 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn fragmented_frames_reassemble() {
        let (mut client, server) = duplex(4096);
        let mut ws = WsStream::new(server);
        // FIN=0 text fragment + FIN=1 continuation, each masked.
        tokio::io::AsyncWriteExt::write_all(&mut client, &[0x01, 0x83, 1, 2, 3, 4]).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, &[b'a' ^ 1, b'b' ^ 2, b'c' ^ 3])
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, &[0x80, 0x82, 5, 6, 7, 8]).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, &[b'd' ^ 5, b'e' ^ 6]).await.unwrap();
        let mut buf = [0u8; 16];
        let n = tokio::io::AsyncReadExt::read(&mut ws, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"abcde");
    }

    #[test]
    fn select_inner_uses_path_or_subprotocol() {
        assert_eq!(select_inner("/mqtt", None), Some(Inner::Mqtt));
        assert_eq!(select_inner("/stomp", None), Some(Inner::Stomp));
        assert_eq!(select_inner("/amqp10", None), Some(Inner::Amqp10));
        assert_eq!(select_inner("/", Some("mqtt")), Some(Inner::Mqtt));
        assert_eq!(select_inner("/", None), None);
    }
}
