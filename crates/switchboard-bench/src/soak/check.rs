//! Message integrity and per-publisher FIFO stream checking.
//!
//! Every soak message carries a fixed 20-byte header inside its body:
//!
//! ```text
//! [0..8]   publisher tag, ASCII, space-padded
//! [8..16]  sequence number, u64 big-endian
//! [16..20] CRC-32 (IEEE) over the whole remainder of the body
//! ```
//!
//! The checker decodes and verifies the CRC on every delivery, and a
//! [`FifoStream`] tracks one publisher's sequence space to classify each
//! delivery: next-in-order, legal redelivery (`redelivered = true`),
//! unauthorized duplicate, or gap.
//!
//! Memory is O(1) per publisher (plus the bounded reorder window in
//! chaos mode), so a month at high rate never grows the driver.

/// CRC-32 (IEEE 802.3), bitwise — table-free, fast enough for soak rates.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

pub const HEADER_LEN: usize = 20;

/// Encode `publisher`/`seq` plus filler into a soak message body.
pub fn encode_body(publisher: &str, seq: u64, size: usize) -> Vec<u8> {
    let size = size.max(HEADER_LEN);
    let mut body = vec![0u8; size];
    let tag = publisher.as_bytes();
    let n = tag.len().min(8);
    body[..n].copy_from_slice(&tag[..n]);
    for b in body[..8].iter_mut().skip(n) {
        *b = b' ';
    }
    body[8..16].copy_from_slice(&seq.to_be_bytes());
    // Filler: a cheap deterministic pattern so bodies differ by position
    // and any broker-side corruption shifts the CRC.
    for (i, b) in body[HEADER_LEN..].iter_mut().enumerate() {
        *b = ((i ^ seq as usize) % 251) as u8;
    }
    let crc = crc32(&body[HEADER_LEN..]);
    body[16..20].copy_from_slice(&crc.to_be_bytes());
    body
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    TooSmall(usize),
    Corrupt { publisher: String, seq: u64 },
}

/// Decode and CRC-verify a soak body; returns (publisher tag, seq).
pub fn decode_body(body: &[u8]) -> Result<(String, u64), BodyError> {
    if body.len() < HEADER_LEN {
        return Err(BodyError::TooSmall(body.len()));
    }
    let publisher: String = String::from_utf8_lossy(&body[..8]).trim().to_string();
    let seq = u64::from_be_bytes(body[8..16].try_into().unwrap());
    let crc = u32::from_be_bytes(body[16..20].try_into().unwrap());
    if crc32(&body[HEADER_LEN..]) != crc {
        return Err(BodyError::Corrupt { publisher, seq });
    }
    Ok((publisher, seq))
}

/// Classification of one delivery against a publisher's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Exactly the next expected sequence.
    InOrder,
    /// A repeat of an already-delivered sequence. Legal only when the
    /// broker flagged it `redelivered`.
    Repeat,
    /// A repeat that arrived *without* the redelivered flag — the
    /// broker handed out the same message twice as fresh. Always a
    /// violation.
    RepeatUnflagged,
    /// A sequence ahead of the cursor (gap). Legal while in flight
    /// under chaos; in strict steady state this is a lost message.
    Gap(u64),
}

/// Per-publisher FIFO stream tracker. `next` is the smallest sequence
/// not yet observed; `window` holds out-of-order arrivals (chaos mode
/// only) so runs with node restarts can still verify exactly-once.
pub struct FifoStream {
    pub next: u64,
    window: std::collections::HashSet<u64>,
    window_max: u64,
    pub repeats: u64,
}

impl FifoStream {
    pub fn new() -> Self {
        FifoStream { next: 0, window: Default::default(), window_max: 0, repeats: 0 }
    }

    /// Enable out-of-order tolerance (chaos mode) with a bounded window.
    pub fn with_window(max: u64) -> Self {
        FifoStream { next: 0, window: Default::default(), window_max: max, repeats: 0 }
    }

    pub fn observe(&mut self, seq: u64, redelivered: bool) -> Delivery {
        if seq >= self.next {
            if self.window.remove(&seq) {
                // Was a gap arrival, now formally delivered in cursory
                // order terms; count it like any in-order observation.
                self.advance();
                return Delivery::InOrder;
            }
            if seq == self.next {
                self.next += 1;
                self.advance();
                return Delivery::InOrder;
            }
            // seq > next: a gap. Strict mode reports; chaos mode buffers.
            if self.window_max == 0 {
                return Delivery::Gap(seq - self.next);
            }
            if !self.window.insert(seq) {
                self.repeats += 1;
                return if redelivered { Delivery::Repeat } else { Delivery::RepeatUnflagged };
            }
            if self.window.len() as u64 > self.window_max {
                return Delivery::Gap(self.window.len() as u64);
            }
            self.advance();
            return Delivery::InOrder;
        }
        // seq < next: a repeat.
        self.repeats += 1;
        if redelivered {
            Delivery::Repeat
        } else {
            Delivery::RepeatUnflagged
        }
    }

    fn advance(&mut self) {
        while self.window.remove(&self.next) {
            self.next += 1;
        }
    }

    /// Distinct sequences delivered so far (cursor + buffered window).
    pub fn distinct(&self) -> u64 {
        self.next + self.window.len() as u64
    }
}

impl Default for FifoStream {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_roundtrip() {
        let body = encode_body("pub-1", 42, 256);
        assert_eq!(decode_body(&body).unwrap(), ("pub-1".to_string(), 42));
        let mut bad = body.clone();
        bad[100] ^= 0xff;
        assert!(matches!(decode_body(&bad), Err(BodyError::Corrupt { .. })));
        assert!(matches!(decode_body(&body[..10]), Err(BodyError::TooSmall(10))));
    }

    #[test]
    fn fifo_in_order() {
        let mut s = FifoStream::new();
        assert_eq!(s.observe(0, false), Delivery::InOrder);
        assert_eq!(s.observe(1, false), Delivery::InOrder);
        assert_eq!(s.observe(2, false), Delivery::InOrder);
        assert_eq!(s.next, 3);
        assert_eq!(s.distinct(), 3);
    }

    #[test]
    fn fifo_repeats() {
        let mut s = FifoStream::new();
        s.observe(0, false);
        s.observe(1, false);
        assert_eq!(s.observe(1, true), Delivery::Repeat);
        assert_eq!(s.observe(1, false), Delivery::RepeatUnflagged);
        assert_eq!(s.observe(0, true), Delivery::Repeat);
    }

    #[test]
    fn fifo_gap_strict() {
        let mut s = FifoStream::new();
        s.observe(0, false);
        assert_eq!(s.observe(2, false), Delivery::Gap(1));
        // Stream stays at 1: the gap is reported, not papered over.
        assert_eq!(s.next, 1);
    }

    #[test]
    fn fifo_window_chaos() {
        let mut s = FifoStream::with_window(16);
        s.observe(0, false);
        assert_eq!(s.observe(2, false), Delivery::InOrder);
        assert_eq!(s.observe(3, false), Delivery::InOrder);
        assert_eq!(s.observe(1, false), Delivery::InOrder);
        assert_eq!(s.next, 4);
        assert_eq!(s.observe(2, false), Delivery::RepeatUnflagged);
        assert_eq!(s.observe(2, true), Delivery::Repeat);
    }
}
