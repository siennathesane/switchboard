//! Basic-class content header properties (§4.2.6).
//!
//! A content header carries `class-id`, `weight`, `body size`, then the
//! property flags and list:
//!
//! ```text
//! property-flags = 15*BIT %b0 / 15*BIT %b1 property-flags
//! ```
//!
//! Bit 15 of the first flags word is the first property; a set bit 0 signals
//! a further flags word (§4.2.6.1). "Bit properties are indicated ONLY by
//! their respective property flag and are never present in the property
//! list."

use serde::{Deserialize, Serialize};

use crate::error::CodecError;
use crate::field::FieldTable;
use crate::method::BASIC_CLASS_ID;
use crate::wireio::{Decoder, Encoder};

/// The property set of the Basic class, in wire order.
///
/// `None` = absent (flag 0). Present optional properties are `Some`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BasicProperties {
    /// MIME content type, e.g. `application/json`.
    pub content_type: Option<String>,
    /// MIME content encoding.
    pub content_encoding: Option<String>,
    /// Application-defined header table (used by the headers exchange, §3.1.3.4).
    pub headers: Option<FieldTable>,
    /// Delivery mode: transient (1) or persistent (2) — see §2.1.3 "Persistent".
    pub delivery_mode: Option<u8>,
    /// Message priority, 0..=9.
    pub priority: Option<u8>,
    /// Application correlation identifier.
    pub correlation_id: Option<String>,
    /// Reply queue name.
    pub reply_to: Option<String>,
    /// Expiration as an AMQP short string, e.g. `"60000"` (milliseconds).
    pub expiration: Option<String>,
    /// Application message identifier.
    pub message_id: Option<String>,
    /// POSIX timestamp (§4.2.5.4).
    pub timestamp: Option<u64>,
    /// Application message type name.
    pub message_type: Option<String>,
    /// Creating user id — servers may validate this.
    pub user_id: Option<String>,
    /// Creating application id.
    pub app_id: Option<String>,
    /// Reserved/ignored cluster id (deprecated, kept for wire fidelity).
    pub cluster_id: Option<String>,
}

impl BasicProperties {
    /// Number of properties in the Basic class set.
    pub const COUNT: usize = 14;

    pub fn new() -> Self {
        Self::default()
    }

    /// Iterate `(position, is_present)` per the property-flags semantics.
    fn flags(&self) -> Vec<bool> {
        vec![
            self.content_type.is_some(),
            self.content_encoding.is_some(),
            self.headers.is_some(),
            self.delivery_mode.is_some(),
            self.priority.is_some(),
            self.correlation_id.is_some(),
            self.reply_to.is_some(),
            self.expiration.is_some(),
            self.message_id.is_some(),
            self.timestamp.is_some(),
            self.message_type.is_some(),
            self.user_id.is_some(),
            self.app_id.is_some(),
            self.cluster_id.is_some(),
        ]
    }

    /// Encode the property-flags words and property list for the Basic class.
    pub fn encode(&self, e: &mut Encoder) {
        let flags = self.flags();
        // 14 properties fit in one 16-bit word (bit 15 first); the grammar's
        // continuation rule still applies, so implement it generally.
        let mut words: Vec<u16> = Vec::new();
        let mut current: u16 = 0;
        let mut bit = 15u32;
        for (i, present) in flags.iter().enumerate() {
            if *present {
                current |= 1 << bit;
            }
            if bit == 0 {
                let last = i == flags.len() - 1;
                if !last {
                    // Continuation: low bit set means "another word follows".
                    current |= 1;
                    words.push(current);
                    current = 0;
                } else {
                    words.push(current);
                }
                bit = 15;
            } else {
                bit -= 1;
            }
        }
        if words.is_empty() || current != 0 || bit != 15 {
            words.push(current);
        }

        for w in &words {
            e.u16(*w);
        }

        if let Some(v) = &self.content_type { e.short_str(v); }
        if let Some(v) = &self.content_encoding { e.short_str(v); }
        if let Some(v) = &self.headers { v.encode(e); }
        if let Some(v) = &self.delivery_mode { e.u8(*v); }
        if let Some(v) = &self.priority { e.u8(*v); }
        if let Some(v) = &self.correlation_id { e.short_str(v); }
        if let Some(v) = &self.reply_to { e.short_str(v); }
        if let Some(v) = &self.expiration { e.short_str(v); }
        if let Some(v) = &self.message_id { e.short_str(v); }
        if let Some(v) = &self.timestamp { e.u64(*v); }
        if let Some(v) = &self.message_type { e.short_str(v); }
        if let Some(v) = &self.user_id { e.short_str(v); }
        if let Some(v) = &self.app_id { e.short_str(v); }
        if let Some(v) = &self.cluster_id { e.short_str(v); }
    }

    /// Decode property flags + list from a content-header payload whose
    /// fixed fields have already been consumed.
    pub fn decode(d: &mut Decoder<'_>) -> Result<Self, CodecError> {
        let mut p = BasicProperties::new();
        let mut remaining = Self::COUNT;

        // Read flags words until a word with a clear continuation bit.
        let mut flag_bits: Vec<bool> = Vec::new();
        loop {
            let word = d.u16()?;
            for bit in (1..=15).rev() {
                if remaining == 0 {
                    break;
                }
                flag_bits.push(word & (1 << bit) != 0);
                remaining -= 1;
            }
            if word & 1 == 0 {
                break;
            }
            // Continuation bit set: another flags word follows (§4.2.6.1).
            if remaining == 0 {
                // All known properties consumed but the peer insists there is
                // more — the flags list is unbounded, so keep reading words
                // and skipping unknown properties (there are none to skip;
                // this is a protocol violation in practice).
                return Err(CodecError::PropertyFlagsOverflow);
            }
        }

        let mut it = flag_bits.into_iter();
        let mut next_flag = || Ok(it.next().unwrap_or(false));
        if next_flag()? { p.content_type = Some(d.short_str()?); }
        if next_flag()? { p.content_encoding = Some(d.short_str()?); }
        if next_flag()? { p.headers = Some(FieldTable::decode(d)?); }
        if next_flag()? { p.delivery_mode = Some(d.u8()?); }
        if next_flag()? { p.priority = Some(d.u8()?); }
        if next_flag()? { p.correlation_id = Some(d.short_str()?); }
        if next_flag()? { p.reply_to = Some(d.short_str()?); }
        if next_flag()? { p.expiration = Some(d.short_str()?); }
        if next_flag()? { p.message_id = Some(d.short_str()?); }
        if next_flag()? { p.timestamp = Some(d.u64()?); }
        if next_flag()? { p.message_type = Some(d.short_str()?); }
        if next_flag()? { p.user_id = Some(d.short_str()?); }
        if next_flag()? { p.app_id = Some(d.short_str()?); }
        if next_flag()? { p.cluster_id = Some(d.short_str()?); }
        Ok(p)
    }
}

/// A parsed content header frame payload (§4.2.6.1).
#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    /// Must match the class id of the method that carries the content;
    /// mismatches are a 501 frame error (§4.2.6.1).
    pub class_id: u16,
    /// "The weight field is unused and must be zero" (§4.2.6.1).
    pub weight: u16,
    /// Total body size; zero means no body frames follow.
    pub body_size: u64,
    /// Basic-class properties.
    pub properties: BasicProperties,
}

impl ContentHeader {
    pub fn new(body_size: u64, properties: BasicProperties) -> Self {
        ContentHeader { class_id: BASIC_CLASS_ID, weight: 0, body_size, properties }
    }

    pub fn encode(&self, e: &mut Encoder) {
        e.u16(self.class_id);
        e.u16(self.weight);
        e.u64(self.body_size);
        self.properties.encode(e);
    }

    /// Decode with the method's class id for the §4.2.6.1 mismatch check.
    pub fn decode(expected_class: u16, d: &mut Decoder<'_>) -> Result<Self, CodecError> {
        let class_id = d.u16()?;
        if class_id != expected_class {
            return Err(CodecError::ContentClassMismatch { actual: class_id, expected: expected_class });
        }
        let weight = d.u16()?;
        if weight != 0 {
            return Err(CodecError::ContentWeight(weight));
        }
        let body_size = d.u64()?;
        let properties = BasicProperties::decode(d)?;
        Ok(ContentHeader { class_id, weight, body_size, properties })
    }
}

/// Convenience: turn a field table into header properties (used when the
/// broker re-publishes internally).
pub fn headers_of(p: &BasicProperties) -> Option<&FieldTable> {
    p.headers.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::FieldValue;

    fn rt(p: BasicProperties) -> BasicProperties {
        let mut e = Encoder::new();
        p.encode(&mut e);
        let buf = e.finish();
        let mut d = Decoder::new(&buf);
        let back = BasicProperties::decode(&mut d).unwrap();
        d.finish().unwrap();
        back
    }

    #[test]
    fn empty_properties_are_two_zero_bytes() {
        let p = BasicProperties::new();
        let mut e = Encoder::new();
        p.encode(&mut e);
        assert_eq!(&e.finish()[..], &[0, 0]);
        assert_eq!(rt(p.clone()), p);
    }

    #[test]
    fn first_property_sets_bit_15() {
        let p = BasicProperties { content_type: Some("text/plain".into()), ..Default::default() };
        let mut e = Encoder::new();
        p.encode(&mut e);
        let buf = e.finish();
        // flags word then shortstr "text/plain"
        assert_eq!(&buf[..2], &[0x80, 0x00]);
        assert_eq!(&buf[2..3], &[10]);
        assert_eq!(rt(p.clone()), p);
    }

    #[test]
    fn last_property_sets_final_bit() {
        let p = BasicProperties { cluster_id: Some("c".into()), ..Default::default() };
        let mut e = Encoder::new();
        p.encode(&mut e);
        let buf = e.finish();
        // 14 properties, last one at bit 15 - 13 = 2.
        assert_eq!(&buf[..2], &[0x00, 0x04]);
        assert_eq!(&buf[2..], &[1, b'c']);
        assert_eq!(rt(p.clone()), p);
    }

    #[test]
    fn bit_properties_use_flags_only() {
        // delivery_mode & priority are plain octets here, but a property set
        // with only middle properties must leave earlier flags clear.
        let p = BasicProperties {
            priority: Some(4),
            timestamp: Some(42),
            ..Default::default()
        };
        let mut e = Encoder::new();
        p.encode(&mut e);
        let buf = e.finish();
        // priority is 5th property -> bit 11; timestamp is 10th -> bit 6.
        let word = u16::from_be_bytes([buf[0], buf[1]]);
        assert_eq!(word, (1 << 11) | (1 << 6));
        assert_eq!(rt(p.clone()), p);
    }

    #[test]
    fn all_properties_roundtrip() {
        let mut headers = FieldTable::new();
        headers.insert("k", FieldValue::LongString(b"v".to_vec()));
        let p = BasicProperties {
            content_type: Some("application/json".into()),
            content_encoding: Some("utf-8".into()),
            headers: Some(headers),
            delivery_mode: Some(2),
            priority: Some(9),
            correlation_id: Some("corr".into()),
            reply_to: Some("reply.q".into()),
            expiration: Some("10000".into()),
            message_id: Some("m-1".into()),
            timestamp: Some(1_700_000_000),
            message_type: Some("event".into()),
            user_id: Some("guest".into()),
            app_id: Some("tester".into()),
            cluster_id: Some("sb".into()),
        };
        assert_eq!(rt(p.clone()), p);
    }

    #[test]
    fn content_header_roundtrip_and_checks() {
        let mut p = BasicProperties::new();
        p.delivery_mode = Some(2);
        let h = ContentHeader::new(1234, p.clone());
        let mut e = Encoder::new();
        h.encode(&mut e);
        let buf = e.finish().to_vec();
        let mut d = Decoder::new(&buf);
        let back = ContentHeader::decode(BASIC_CLASS_ID, &mut d).unwrap();
        d.finish().unwrap();
        assert_eq!(back, h);
        assert_eq!(back.body_size, 1234);

        // class mismatch -> 501 (§4.2.6.1)
        let mut d = Decoder::new(&buf);
        assert!(matches!(
            ContentHeader::decode(99, &mut d),
            Err(CodecError::ContentClassMismatch { actual: 60, expected: 99 })
        ));

        // nonzero weight -> rejected
        let bad = [0, 60, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut d = Decoder::new(&bad);
        assert!(matches!(
            ContentHeader::decode(60, &mut d),
            Err(CodecError::ContentWeight(7))
        ));
    }
}

#[cfg(test)]
mod flag_tests {
    use super::*;
    use crate::field::FieldValue;

    #[test]
    fn continuation_flag_overflow_is_an_error() {
        // Full header: class 60, weight 0, body_size 0, then flags word
        // 0x8001 = content-type bit + continuation bit, shortstr "text" —
        // then the payload ENDS while the continuation bit demands another
        // flags word → PropertyFlagsOverflow.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&60u16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(&0x8001u16.to_be_bytes());
        bytes.extend_from_slice(&4u8.to_be_bytes());
        bytes.extend_from_slice(b"text");
        let mut d = crate::wireio::Decoder::new(&bytes);
        let result = ContentHeader::decode(60, &mut d);
        assert!(matches!(result, Err(CodecError::PropertyFlagsOverflow)));
    }

    #[test]
    fn headers_of_returns_table() {
        let mut p = BasicProperties::new();
        assert!(headers_of(&p).is_none());
        let mut t = FieldTable::new();
        t.insert("k".to_string(), FieldValue::LongString(b"v".to_vec()));
        p.headers = Some(t);
        assert!(headers_of(&p).is_some());
    }
}
