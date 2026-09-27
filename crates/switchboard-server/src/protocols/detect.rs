//! First-byte protocol detection for the client gateway.

/// The protocol a client connection speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detected {
    /// AMQP 0-9-1 (`AMQP\0\0\9\1`).
    Amqp091,
    /// AMQP 1.0 (`AMQP\0\1\0\0`).
    Amqp10,
    /// MQTT 3.1.1 (CONNECT packet, `0x10 …`).
    Mqtt,
    /// STOMP (`STOMP\n` or a leading `CONNECT\n` frame).
    Stomp,
    /// An HTTP request — either a WebSocket upgrade or a health probe.
    Http,
}

/// How many bytes the classifier may need at most (the full AMQP header).
pub const SNIFF_LEN: usize = 8;

/// Outcome of classifying the first bytes of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classify {
    /// Enough bytes: this is the protocol.
    Yes(Detected),
    /// Read more bytes (up to [`SNIFF_LEN`]) before deciding.
    NeedMore,
    /// Unrecognizable: the gateway should close the connection.
    Unknown,
}

/// Classify `buf` (the first bytes received on a fresh connection).
pub fn classify(buf: &[u8]) -> Classify {
    if buf.is_empty() {
        return Classify::NeedMore;
    }
    match buf[0] {
        b'A' => {
            if buf.len() < SNIFF_LEN {
                // Could still become any AMQP header.
                return if "AMQP".as_bytes().iter().enumerate().all(|(i, b)| buf.get(i) == Some(b))
                {
                    Classify::NeedMore
                } else {
                    Classify::Unknown
                };
            }
            if buf[..8] == *b"AMQP\0\0\x09\x01" {
                Classify::Yes(Detected::Amqp091)
            } else if buf[..8] == *b"AMQP\0\x01\0\0" {
                Classify::Yes(Detected::Amqp10)
            } else {
                Classify::Unknown
            }
        }
        0x10 => {
            // MQTT CONNECT: type 1 (high nibble), flags must be 0. One byte
            // is enough to route (the MQTT parser validates the rest).
            Classify::Yes(Detected::Mqtt)
        }
        b'S' => {
            if buf.len() < 6 {
                return Classify::NeedMore;
            }
            if &buf[..6] == b"STOMP\n" {
                Classify::Yes(Detected::Stomp)
            } else {
                Classify::Unknown
            }
        }
        b'C' => {
            // "CONNECT\n…" is a STOMP CONNECT frame; "CONNECT …" (space) is
            // the rare HTTP CONNECT method; "POST"/"OPTIONS" handled below.
            if buf.len() < 8 {
                return if "CONNECT".as_bytes().iter().enumerate().all(|(i, b)| buf.get(i) == Some(b))
                {
                    Classify::NeedMore
                } else {
                    Classify::Unknown
                };
            }
            if buf[..8] == *b"CONNECT\n" {
                Classify::Yes(Detected::Stomp)
            } else if buf[0..7] == *b"CONNECT" && buf[7] == b' ' {
                Classify::Yes(Detected::Http)
            } else {
                Classify::Unknown
            }
        }
        b'G' | b'H' | b'P' | b'D' | b'O' | b'T' => {
            // HTTP methods: GET, HEAD, POST, PUT, DELETE, OPTIONS, TRACE,
            // PATCH. All are method-name SP … route on method-prefix SP.
            let method_end = buf.iter().position(|&b| b == b' ');
            match method_end {
                Some(i) if i <= 7 => {
                    if buf[..i].iter().all(|b| b.is_ascii_uppercase()) {
                        Classify::Yes(Detected::Http)
                    } else {
                        Classify::Unknown
                    }
                }
                Some(_) => Classify::Unknown,
                None => {
                    if buf.len() >= SNIFF_LEN {
                        Classify::Unknown
                    } else {
                        Classify::NeedMore
                    }
                }
            }
        }
        _ => Classify::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yes(b: &[u8]) -> Detected {
        match classify(b) {
            Classify::Yes(d) => d,
            other => panic!("expected Yes, got {other:?}"),
        }
    }

    #[test]
    fn known_protocols_are_detected() {
        assert_eq!(yes(b"AMQP\0\0\x09\x01"), Detected::Amqp091);
        assert_eq!(yes(b"AMQP\0\x01\0\0"), Detected::Amqp10);
        assert_eq!(yes(&[0x10, 0x10, 0x00, 0x00]), Detected::Mqtt);
        assert_eq!(yes(b"STOMP\naccept-version:1.2\n"), Detected::Stomp);
        assert_eq!(yes(b"CONNECT\naccept-version:1.2\n"), Detected::Stomp);
        assert_eq!(yes(b"GET /health HTTP/1.1\r\n"), Detected::Http);
        assert_eq!(yes(b"CONNECT host:443 HTTP/1.1\r\n"), Detected::Http);
        assert_eq!(yes(b"POST / HTTP/1.1\r\n"), Detected::Http);
    }

    #[test]
    fn partial_and_unknown_traffic() {
        assert_eq!(classify(b""), Classify::NeedMore);
        assert_eq!(classify(b"AMQP"), Classify::NeedMore);
        assert_eq!(classify(b"AMQP\0"), Classify::NeedMore);
        assert_eq!(classify(b"CONNECT"), Classify::NeedMore);
        assert_eq!(classify(b"STOM"), Classify::NeedMore);
        assert_eq!(classify(b"NONSENSE!!!"), Classify::Unknown);
        assert_eq!(classify(b"AMQP\xff\xff\xff\xff\xff"), Classify::Unknown);
    }
}
