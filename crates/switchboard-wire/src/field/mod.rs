//! Field tables and field values — the `field-table` grammar of §4.2.1/§4.2.5.5.
//!
//! The 0-9-1 grammar defines these value types:
//!
//! ```text
//! field-value = 't' boolean / 'b' short-short-int / 'B' short-short-uint
//!             / 'U' short-int     / 'u' short-uint      / 'I' long-int
//!             / 'i' long-uint     / 'L' long-long-int   / 'l' long-long-uint
//!             / 'f' float         / 'd' double          / 'D' decimal-value
//!             / 's' short-string  / 'S' long-string     / 'A' field-array
//!             / 'T' timestamp     / 'F' field-table     / 'V'
//! ```
//!
//! We additionally accept `'x'` (byte array), the widely deployed extension
//! used by RabbitMQ clients, so that interop clients can round-trip tables
//! through the broker unchanged.
//!
//! Field names MUST start with a letter, `$` or `#`, continue with letters,
//! `$`, `#`, digits or underscores, and be at most 128 characters (§4.2.5.5).
//! [`FieldTable::validate`] enforces this so the server can raise 503.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::CodecError;
use crate::wireio::{Decoder, Encoder};

/// A typed field-table value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// `t`
    Boolean(bool),
    /// `b` — signed 8-bit
    SignedByte(i8),
    /// `B` — unsigned 8-bit
    UnsignedByte(u8),
    /// `U` — signed 16-bit
    SignedShort(i16),
    /// `u` — unsigned 16-bit
    UnsignedShort(u16),
    /// `I` — signed 32-bit
    SignedInt(i32),
    /// `i` — unsigned 32-bit
    UnsignedInt(u32),
    /// `L` — signed 64-bit
    SignedLongLong(i64),
    /// `l` — unsigned 64-bit
    UnsignedLongLong(u64),
    /// `f` — IEEE-754 single precision
    Float(f32),
    /// `d` — IEEE-754 double precision ("rfc1832 XDR double")
    Double(f64),
    /// `D` — fixed-point decimal: scale (decimal digits) + signed value.
    Decimal { scale: u8, value: i32 },
    /// `s` — short string (extension spelling of the native shortstr)
    ShortString(String),
    /// `S` — long string (may hold binary data)
    LongString(Vec<u8>),
    /// `x` — byte array (RabbitMQ extension; distinct from `S` in semantics)
    ByteArray(Vec<u8>),
    /// `A` — array of field values
    Array(Vec<FieldValue>),
    /// `T` — 64-bit POSIX timestamp, one-second accuracy (§4.2.5.4)
    Timestamp(u64),
    /// `F` — nested field table
    FieldTable(FieldTable),
    /// `V` — void / no value
    Void,
}

/// An ordered map of field names to values.
///
/// Field tables are semantically unordered (§4.2.5.5 makes duplicate fields
/// illegal and leaves peer behaviour undefined), so a sorted map keeps
/// serialization deterministic — the same logical table always encodes to the
/// same bytes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FieldTable(pub BTreeMap<String, FieldValue>);

impl FieldTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, value: FieldValue) {
        self.0.insert(name.into(), value);
    }

    pub fn get(&self, name: &str) -> Option<&FieldValue> {
        self.0.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &FieldValue)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Validate every field name (recursively) against the rules of §4.2.5.5.
    pub fn validate(&self) -> Result<(), CodecError> {
        for (name, value) in self.iter() {
            validate_field_name(name)?;
            if let FieldValue::Array(items) = value {
                // Arrays hold bare values without names; nothing to validate.
                for item in items {
                    if let FieldValue::FieldTable(t) = item {
                        t.validate()?;
                    }
                }
            }
            if let FieldValue::FieldTable(t) = value {
                t.validate()?;
            }
        }
        Ok(())
    }

    /// Serialize the full table (with its 32-bit length prefix).
    pub fn encode(&self, e: &mut Encoder) {
        let mut body = Encoder::new();
        for (name, value) in self.iter() {
            body.short_str(name);
            value.encode(&mut body);
        }
        let body = body.finish();
        e.u32(body.len() as u32);
        e.raw(&body);
    }

    /// Decode a table including its 32-bit length prefix.
    pub fn decode(d: &mut Decoder<'_>) -> Result<Self, CodecError> {
        let len = d.u32()? as usize;
        if len > d.remaining() {
            return Err(CodecError::Eof { needed: len, had: d.remaining() });
        }
        let inner = d.bytes(len)?;
        let mut td = Decoder::new(inner);
        let mut table = FieldTable::new();
        while !td.is_empty() {
            let name = td.short_str()?;
            let value = FieldValue::decode(&mut td)?;
            if table.0.insert(name.clone(), value).is_some() {
                // Duplicate fields are illegal (§4.2.5.5); treat as syntax error.
                return Err(CodecError::FieldName(name));
            }
        }
        Ok(table)
    }
}

/// `Field names MUST start with a letter, '$' or '#' and may continue with
/// letters, '$' or '#', digits, or underlines, to a maximum length of 128
/// characters.` (§4.2.5.5)
///
/// Erratum: the same specification mandates the `x-match` binding argument for
/// the headers exchange (§3.1.3.4), whose name contains a hyphen — a character
/// the field-name rule does not allow. We resolve the conflict permissively by
/// accepting `-` as a continuation character.
pub fn validate_field_name(name: &str) -> Result<(), CodecError> {
    let ok_first = |c: char| c.is_ascii_alphabetic() || c == '$' || c == '#';
    let ok_rest = |c: char| ok_first(c) || c.is_ascii_digit() || c == '_' || c == '-';
    let valid = !name.is_empty()
        && name.chars().count() <= 128
        && name.chars().next().is_some_and(ok_first)
        && name.chars().skip(1).all(ok_rest);
    if valid {
        Ok(())
    } else {
        Err(CodecError::FieldName(name.to_string()))
    }
}

impl FieldValue {
    /// The wire type octet for this value.
    pub fn type_char(&self) -> u8 {
        match self {
            FieldValue::Boolean(_) => b't',
            FieldValue::SignedByte(_) => b'b',
            FieldValue::UnsignedByte(_) => b'B',
            FieldValue::SignedShort(_) => b'U',
            FieldValue::UnsignedShort(_) => b'u',
            FieldValue::SignedInt(_) => b'I',
            FieldValue::UnsignedInt(_) => b'i',
            FieldValue::SignedLongLong(_) => b'L',
            FieldValue::UnsignedLongLong(_) => b'l',
            FieldValue::Float(_) => b'f',
            FieldValue::Double(_) => b'd',
            FieldValue::Decimal { .. } => b'D',
            FieldValue::ShortString(_) => b's',
            FieldValue::LongString(_) => b'S',
            FieldValue::ByteArray(_) => b'x',
            FieldValue::Array(_) => b'A',
            FieldValue::Timestamp(_) => b'T',
            FieldValue::FieldTable(_) => b'F',
            FieldValue::Void => b'V',
        }
    }

    /// Encode the value without a name or outer length prefix.
    pub fn encode(&self, e: &mut Encoder) {
        e.u8(self.type_char());
        match self {
            FieldValue::Boolean(v) => e.u8(*v as u8),
            FieldValue::SignedByte(v) => e.u8(*v as u8),
            FieldValue::UnsignedByte(v) => e.u8(*v),
            FieldValue::SignedShort(v) => e.u16(*v as u16),
            FieldValue::UnsignedShort(v) => e.u16(*v),
            FieldValue::SignedInt(v) => e.i32(*v),
            FieldValue::UnsignedInt(v) => e.u32(*v),
            FieldValue::SignedLongLong(v) => e.i64(*v),
            FieldValue::UnsignedLongLong(v) => e.u64(*v),
            FieldValue::Float(v) => e.f32(*v),
            FieldValue::Double(v) => e.f64(*v),
            FieldValue::Decimal { scale, value } => {
                e.u8(*scale);
                e.i32(*value);
            }
            FieldValue::ShortString(v) => e.short_str(v),
            FieldValue::LongString(v) => e.long_str(v),
            FieldValue::ByteArray(v) => {
                e.u32(v.len() as u32);
                e.raw(v);
            }
            FieldValue::Array(items) => {
                // `field-array = long-int *field-value`
                let mut body = Encoder::new();
                for item in items {
                    item.encode(&mut body);
                }
                let body = body.finish();
                e.u32(body.len() as u32);
                e.raw(&body);
            }
            FieldValue::Timestamp(v) => e.u64(*v),
            FieldValue::FieldTable(t) => t.encode(e),
            FieldValue::Void => {}
        }
    }

    /// Decode a value from its type octet.
    pub fn decode(d: &mut Decoder<'_>) -> Result<Self, CodecError> {
        let tag = d.u8()?;
        let v = match tag {
            b't' => FieldValue::Boolean(d.u8()? != 0),
            b'b' => FieldValue::SignedByte(d.u8()? as i8),
            b'B' => FieldValue::UnsignedByte(d.u8()?),
            b'U' => FieldValue::SignedShort(d.u16()? as i16),
            b'u' => FieldValue::UnsignedShort(d.u16()?),
            b'I' => FieldValue::SignedInt(d.i32()?),
            b'i' => FieldValue::UnsignedInt(d.u32()?),
            b'L' => FieldValue::SignedLongLong(d.i64()?),
            b'l' => FieldValue::UnsignedLongLong(d.u64()?),
            b'f' => FieldValue::Float(d.f32()?),
            b'd' => FieldValue::Double(d.f64()?),
            b'D' => {
                let scale = d.u8()?;
                let value = d.i32()?;
                FieldValue::Decimal { scale, value }
            }
            b's' => FieldValue::ShortString(d.short_str()?),
            b'S' => FieldValue::LongString(d.long_str()?),
            b'x' => {
                let len = d.u32()? as usize;
                FieldValue::ByteArray(d.bytes(len)?.to_vec())
            }
            b'A' => {
                let len = d.u32()? as usize;
                if len > d.remaining() {
                    return Err(CodecError::Eof { needed: len, had: d.remaining() });
                }
                let inner = d.bytes(len)?;
                let mut ad = Decoder::new(inner);
                let mut items = Vec::new();
                while !ad.is_empty() {
                    items.push(FieldValue::decode(&mut ad)?);
                }
                FieldValue::Array(items)
            }
            b'T' => FieldValue::Timestamp(d.u64()?),
            b'F' => FieldValue::FieldTable(FieldTable::decode(d)?),
            b'V' => FieldValue::Void,
            other => return Err(CodecError::FieldType(other)),
        };
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &FieldValue) -> Vec<u8> {
        let mut e = Encoder::new();
        v.encode(&mut e);
        e.finish().to_vec()
    }

    fn dec(bytes: &[u8]) -> FieldValue {
        let mut d = Decoder::new(bytes);
        let v = FieldValue::decode(&mut d).unwrap();
        d.finish().unwrap();
        v
    }

    #[test]
    fn every_field_type_roundtrips() {
        let values = vec![
            FieldValue::Boolean(true),
            FieldValue::SignedByte(-1),
            FieldValue::UnsignedByte(255),
            FieldValue::SignedShort(-300),
            FieldValue::UnsignedShort(400),
            FieldValue::SignedInt(-70000),
            FieldValue::UnsignedInt(80000),
            FieldValue::SignedLongLong(-1 << 40),
            FieldValue::UnsignedLongLong(1 << 50),
            FieldValue::Float(1.5),
            FieldValue::Double(-2.25),
            FieldValue::Decimal { scale: 2, value: -1099 },
            FieldValue::ShortString("short".into()),
            FieldValue::LongString(vec![0xFF, 0x00]),
            FieldValue::ByteArray(vec![1, 2, 3]),
            FieldValue::Array(vec![FieldValue::Void, FieldValue::SignedInt(7)]),
            FieldValue::Timestamp(1_234_567_890),
            FieldValue::FieldTable(FieldTable::new()),
            FieldValue::Void,
        ];
        for v in values {
            assert_eq!(dec(&enc(&v)), v, "roundtrip failed for {:?}", v);
        }
    }

    #[test]
    fn wire_type_chars_match_the_grammar() {
        assert_eq!(FieldValue::Boolean(false).type_char(), b't');
        assert_eq!(FieldValue::SignedByte(0).type_char(), b'b');
        assert_eq!(FieldValue::UnsignedByte(0).type_char(), b'B');
        assert_eq!(FieldValue::SignedShort(0).type_char(), b'U');
        assert_eq!(FieldValue::UnsignedShort(0).type_char(), b'u');
        assert_eq!(FieldValue::SignedInt(0).type_char(), b'I');
        assert_eq!(FieldValue::UnsignedInt(0).type_char(), b'i');
        assert_eq!(FieldValue::SignedLongLong(0).type_char(), b'L');
        assert_eq!(FieldValue::UnsignedLongLong(0).type_char(), b'l');
        assert_eq!(FieldValue::Float(0.).type_char(), b'f');
        assert_eq!(FieldValue::Double(0.).type_char(), b'd');
        assert_eq!(FieldValue::Decimal { scale: 0, value: 0 }.type_char(), b'D');
        assert_eq!(FieldValue::ShortString(String::new()).type_char(), b's');
        assert_eq!(FieldValue::LongString(vec![]).type_char(), b'S');
        assert_eq!(FieldValue::ByteArray(vec![]).type_char(), b'x');
        assert_eq!(FieldValue::Array(vec![]).type_char(), b'A');
        assert_eq!(FieldValue::Timestamp(0).type_char(), b'T');
        assert_eq!(FieldValue::FieldTable(FieldTable::new()).type_char(), b'F');
        assert_eq!(FieldValue::Void.type_char(), b'V');
    }

    #[test]
    fn boolean_true_is_nonzero_octet() {
        // §4.2.1: boolean = OCTET ; 0 = FALSE, else TRUE
        assert_eq!(enc(&FieldValue::Boolean(true)), vec![b't', 1]);
        assert_eq!(enc(&FieldValue::Boolean(false)), vec![b't', 0]);
        // Any nonzero octet decodes TRUE.
        assert_eq!(dec(&[b't', 0x7F]), FieldValue::Boolean(true));
    }

    #[test]
    fn decimal_is_scale_then_signed_long() {
        assert_eq!(
            enc(&FieldValue::Decimal { scale: 4, value: -12345 }),
            vec![b'D', 4, 0xFF, 0xFF, 0xCF, 0xC7]
        );
    }

    #[test]
    fn nested_tables_roundtrip() {
        let mut inner = FieldTable::new();
        inner.insert("x", FieldValue::UnsignedLongLong(9));
        let mut outer = FieldTable::new();
        outer.insert("nested", FieldValue::FieldTable(inner));
        outer.insert("list", FieldValue::Array(vec![FieldValue::ShortString("a".into())]));

        let mut e = Encoder::new();
        outer.encode(&mut e);
        let bytes = e.finish().to_vec();

        let mut d = Decoder::new(&bytes);
        let back = FieldTable::decode(&mut d).unwrap();
        d.finish().unwrap();
        assert_eq!(back, outer);
    }

    #[test]
    fn duplicate_fields_are_rejected() {
        // "Duplicate fields are illegal. The behaviour of a peer with respect
        // to a table containing duplicate fields is undefined." (§4.2.5.5)
        // We define ours: syntax error.
        let bytes = [0, 0, 0, 8, 1, b'a', b't', 1, 1, b'a', b't', 0];
        let mut d = Decoder::new(&bytes);
        assert!(matches!(FieldTable::decode(&mut d), Err(CodecError::FieldName(_))));
    }

    #[test]
    fn field_name_validation() {
        for good in ["a", "x-match", "$flag", "#hash", "A_b9", "z", "$", "#"] {
            assert!(validate_field_name(good).is_ok(), "{good} should be valid");
        }
        let too_long = "x".repeat(129);
        for bad in ["", "9lead", "_lead", "has space", "bad!", too_long.as_str()] {
            assert!(validate_field_name(bad).is_err(), "{bad:?} should be invalid");
        }
        assert!(validate_field_name(&"x".repeat(128)).is_ok(), "128 chars is the limit");
    }

    #[test]
    fn table_validate_walks_nesting() {
        let mut bad = FieldTable::new();
        bad.insert("9bad", FieldValue::Void);
        assert!(bad.validate().is_err());

        let mut deep = FieldTable::new();
        deep.insert("ok", FieldValue::FieldTable(bad.clone()));
        assert!(deep.validate().is_err());

        let mut in_array = FieldTable::new();
        in_array.insert("ok", FieldValue::Array(vec![FieldValue::FieldTable(bad)]));
        assert!(in_array.validate().is_err());

        let mut fine = FieldTable::new();
        fine.insert("#$", FieldValue::Array(vec![FieldValue::Void]));
        assert!(fine.validate().is_ok());
    }

    #[test]
    fn unknown_type_tag_is_rejected() {
        let mut d = Decoder::new(&[b'Z']);
        assert!(matches!(FieldValue::decode(&mut d), Err(CodecError::FieldType(0x5A))));
    }

    #[test]
    fn truncated_table_length_is_rejected() {
        let mut d = Decoder::new(&[0, 0, 0, 200, 1, b'a', b't']);
        assert!(matches!(
            FieldTable::decode(&mut d),
            Err(CodecError::Eof { .. })
        ));
    }
}

#[cfg(test)]
mod access_tests {
    use super::*;

    #[test]
    fn table_accessors_work() {
        let mut t = FieldTable::new();
        assert!(t.is_empty());
        t.insert("a".to_string(), FieldValue::Boolean(true));
        t.insert("b".to_string(), FieldValue::SignedInt(7));
        assert_eq!(t.len(), 2);
        assert!(!t.is_empty());
        assert!(t.get("a").is_some());
        assert_eq!(t.iter().count(), 2);
    }

    #[test]
    fn array_value_recursion_validates() {
        let mut inner = FieldTable::new();
        inner.insert("bad name!".to_string(), FieldValue::Boolean(true));
        let mut t = FieldTable::new();
        t.insert(
            "arr".to_string(),
            FieldValue::Array(vec![FieldValue::FieldTable(inner)]),
        );
        assert!(t.validate().is_err(), "nested bad name must be rejected");
    }
}
