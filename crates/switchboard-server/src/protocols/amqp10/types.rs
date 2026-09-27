//! AMQP 1.0 type system codec (the subset the bridge needs): primitive
//! encodings, described values, and the constructor used by
//! performatives and message sections.

/// An AMQP 1.0 value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    UByte(u8),
    UShort(u16),
    UInt(u32),
    ULong(u64),
    /// UTF-8 string.
    String(String),
    /// Symbol (restricted string, used for capability names).
    Symbol(String),
    Binary(Vec<u8>),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    /// Described value: descriptor (ulong/symbol) + value.
    Described(Box<Value>, Box<Value>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) | Value::Symbol(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_uint(&self) -> Option<u32> {
        match self {
            Value::UInt(n) => Some(*n),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }
    /// Map lookup by string key (symbol or string).
    pub fn map_get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(entries) => entries.iter().find_map(|(k, v)| {
                k.as_str().and_then(|s| if s == key { Some(v) } else { None })
            }),
            _ => None,
        }
    }
}

pub fn encode(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(0x40),
        Value::Bool(b) => out.push(if *b { 0x41 } else { 0x42 }),
        Value::UByte(b) => {
            out.push(0x50);
            out.push(*b);
        }
        Value::UShort(n) => {
            out.push(0x60);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Value::UInt(n) => match n {
            0 => out.push(0x43),
            n if *n <= u8::MAX as u32 => {
                out.push(0x52);
                out.push(*n as u8);
            }
            n => {
                out.push(0x70);
                out.extend_from_slice(&n.to_be_bytes());
            }
        },
        Value::ULong(n) => match n {
            0 => out.push(0x44),
            n if *n <= u8::MAX as u64 => {
                out.push(0x53);
                out.push(*n as u8);
            }
            n => {
                out.push(0x80);
                out.extend_from_slice(&n.to_be_bytes());
            }
        },
        Value::String(s) => encode_vbin8_or_32(0xA1, 0xB1, s.as_bytes(), out),
        Value::Symbol(s) => encode_vbin8_or_32(0xA3, 0xB3, s.as_bytes(), out),
        Value::Binary(b) => encode_vbin8_or_32(0xA0, 0xB0, b, out),
        Value::List(items) => {
            let mut body = Vec::new();
            for item in items {
                encode(item, &mut body);
            }
            if body.len() <= u8::MAX as usize && items.len() <= u8::MAX as usize {
                out.push(0xC0);
                out.push(body.len() as u8);
                out.push(items.len() as u8);
            } else {
                out.push(0xD0);
                out.extend_from_slice(&(body.len() as u32).to_be_bytes());
                out.extend_from_slice(&(items.len() as u32).to_be_bytes());
            }
            out.extend_from_slice(&body);
        }
        Value::Map(entries) => {
            let mut body = Vec::new();
            for (k, v) in entries {
                encode(k, &mut body);
                encode(v, &mut body);
            }
            let count = (entries.len() as u32) * 2;
            if body.len() <= u8::MAX as usize && count <= u8::MAX as u32 {
                out.push(0xC1);
                out.push(body.len() as u8);
                out.push(count as u8);
            } else {
                out.push(0xD1);
                out.extend_from_slice(&(body.len() as u32).to_be_bytes());
                out.extend_from_slice(&count.to_be_bytes());
            }
            out.extend_from_slice(&body);
        }
        Value::Described(descriptor, value) => {
            out.push(0x00);
            encode(descriptor, out);
            encode(value, out);
        }
    }
}

fn encode_vbin8_or_32(short: u8, long: u8, bytes: &[u8], out: &mut Vec<u8>) {
    if bytes.len() <= u8::MAX as usize {
        out.push(short);
        out.push(bytes.len() as u8);
    } else {
        out.push(long);
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Decode one value. Returns the value and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<(Value, usize), String> {
    let mut pos = 0usize;
    let v = decode_inner(buf, &mut pos)?;
    Ok((v, pos))
}

fn need(buf: &[u8], pos: usize, n: usize) -> Result<(), String> {
    if pos + n > buf.len() {
        Err("truncated amqp value".into())
    } else {
        Ok(())
    }
}

fn decode_inner(buf: &[u8], pos: &mut usize) -> Result<Value, String> {
    need(buf, *pos, 1)?;
    let constructor = buf[*pos];
    *pos += 1;
    let value = match constructor {
        0x40 => Value::Null,
        0x41 => Value::Bool(true),
        0x42 => Value::Bool(false),
        0x50 => {
            need(buf, *pos, 1)?;
            let b = buf[*pos];
            *pos += 1;
            Value::UByte(b)
        }
        0x60 => {
            need(buf, *pos, 2)?;
            let n = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
            *pos += 2;
            Value::UShort(n)
        }
        0x43 => Value::UInt(0),
        0x52 => {
            need(buf, *pos, 1)?;
            let n = buf[*pos];
            *pos += 1;
            Value::UInt(n as u32)
        }
        0x70 => {
            need(buf, *pos, 4)?;
            let n = u32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
            *pos += 4;
            Value::UInt(n)
        }
        0x44 => Value::ULong(0),
        0x53 => {
            need(buf, *pos, 1)?;
            let n = buf[*pos];
            *pos += 1;
            Value::ULong(n as u64)
        }
        0x80 => {
            need(buf, *pos, 8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[*pos..*pos + 8]);
            *pos += 8;
            Value::ULong(u64::from_be_bytes(b))
        }
        0xA1 | 0xB1 => {
            let (len, w) = read_vlen(buf, *pos, constructor == 0xB1)?;
            *pos += w;
            need(buf, *pos, len)?;
            let s = std::str::from_utf8(&buf[*pos..*pos + len])
                .map_err(|_| "bad utf8 in string")?;
            *pos += len;
            Value::String(s.to_string())
        }
        0xA3 | 0xB3 => {
            let (len, w) = read_vlen(buf, *pos, constructor == 0xB3)?;
            *pos += w;
            need(buf, *pos, len)?;
            let s = std::str::from_utf8(&buf[*pos..*pos + len])
                .map_err(|_| "bad utf8 in symbol")?;
            *pos += len;
            Value::Symbol(s.to_string())
        }
        0xA0 | 0xB0 => {
            let (len, w) = read_vlen(buf, *pos, constructor == 0xB0)?;
            *pos += w;
            need(buf, *pos, len)?;
            let b = buf[*pos..*pos + len].to_vec();
            *pos += len;
            Value::Binary(b)
        }
        0xC0 | 0xD0 => {
            let (size, count, w) = read_compound(buf, *pos, constructor == 0xD0)?;
            *pos += w;
            need(buf, *pos, size)?;
            let end = *pos + size;
            let mut items = Vec::new();
            while *pos < end {
                items.push(decode_inner(buf, pos)?);
            }
            if items.len() != count as usize {
                return Err("list count mismatch".into());
            }
            Value::List(items)
        }
        0xC1 | 0xD1 => {
            let (size, count, w) = read_compound(buf, *pos, constructor == 0xD1)?;
            *pos += w;
            need(buf, *pos, size)?;
            let end = *pos + size;
            let mut entries = Vec::new();
            while *pos < end {
                let k = decode_inner(buf, pos)?;
                let v = decode_inner(buf, pos)?;
                entries.push((k, v));
            }
            if entries.len() * 2 != count as usize {
                return Err("map count mismatch".into());
            }
            Value::Map(entries)
        }
        0x00 => {
            let descriptor = decode_inner(buf, pos)?;
            let value = decode_inner(buf, pos)?;
            Value::Described(Box::new(descriptor), Box::new(value))
        }
        other => return Err(format!("unsupported amqp constructor 0x{other:02X}")),
    };
    Ok(value)
}

fn read_vlen(buf: &[u8], pos: usize, long: bool) -> Result<(usize, usize), String> {
    if long {
        need(buf, pos, 4)?;
        let n = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        Ok((n, 4))
    } else {
        need(buf, pos, 1)?;
        Ok((buf[pos] as usize, 1))
    }
}

fn read_compound(buf: &[u8], pos: usize, long: bool) -> Result<(usize, u32, usize), String> {
    if long {
        need(buf, pos, 8)?;
        let size = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        let count = u32::from_be_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
        Ok((size, count, 8))
    } else {
        need(buf, pos, 2)?;
        Ok((buf[pos] as usize, buf[pos + 1] as u32, 2))
    }
}

/// A described list with a ulong descriptor (performatives, sections).
pub fn described_list(code: u64, fields: Vec<Value>) -> Value {
    Value::Described(
        Box::new(Value::ULong(code)),
        Box::new(Value::List(fields)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalars_roundtrip() {
        for v in [
            Value::Null,
            Value::Bool(true),
            Value::UByte(7),
            Value::UShort(1000),
            Value::UInt(0),
            Value::UInt(70_000),
            Value::ULong(0),
            Value::ULong(1 << 40),
            Value::String("héllo".into()),
            Value::Symbol("amqp".into()),
            Value::Binary(vec![1, 2, 3, 250]),
        ] {
            let mut out = Vec::new();
            encode(&v, &mut out);
            let (back, used) = decode(&out).unwrap();
            assert_eq!(used, out.len());
            assert_eq!(back, v);
        }
    }

    #[test]
    fn described_list_roundtrip() {
        let v = described_list(0x10, vec![Value::String("c".into()), Value::Null, Value::UInt(5)]);
        let mut out = Vec::new();
        encode(&v, &mut out);
        let (back, used) = decode(&out).unwrap();
        assert_eq!(used, out.len());
        assert_eq!(back, v);
    }

    #[test]
    fn map_roundtrip_and_lookup() {
        let v = Value::Map(vec![
            (Value::Symbol("address".into()), Value::String("/topic/news".into())),
            (Value::Symbol("durable".into()), Value::Bool(true)),
        ]);
        let mut out = Vec::new();
        encode(&v, &mut out);
        let (back, used) = decode(&out).unwrap();
        assert_eq!(used, out.len());
        assert_eq!(back.map_get("address").and_then(Value::as_str), Some("/topic/news"));
        assert_eq!(back.map_get("durable").and_then(Value::as_bool), Some(true));
    }
}

#[cfg(test)]
mod decode_error_tests {
    use super::*;

    #[test]
    fn truncated_values_error() {
        // u64 declared but only 3 bytes present.
        assert!(decode(&[0x80, 1, 2, 3]).is_err());
        // String length exceeds the buffer.
        assert!(decode(&[0xA1, 20, b'a']).is_err());
        // List count mismatch.
        assert!(decode(&[0xD0, 0, 0, 0, 4, 0, 0, 0, 9]).is_err());
        // Unknown constructor.
        assert!(decode(&[0x3F]).is_err());
        // Empty input.
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn null_and_bool_forms() {
        let (v, used) = decode(&[0x40]).unwrap();
        assert_eq!(v, Value::Null);
        assert_eq!(used, 1);
    }
}

#[cfg(test)]
mod wide_tests {
    use super::*;

    #[test]
    fn accessor_none_arms() {
        assert!(Value::Null.as_uint().is_none());
        assert!(Value::Null.as_bool().is_none());
        assert!(Value::Null.as_str().is_none());
        assert!(Value::Null.as_list().is_none());
        assert_eq!(Value::List(vec![]).as_list(), Some(&[][..]));
    }

    #[test]
    fn wide_list_and_map_roundtrip() {
        // >255 items force the 32-bit list form.
        let items: Vec<Value> = (0..300).map(Value::UInt).collect();
        let mut out = Vec::new();
        encode(&Value::List(items.clone()), &mut out);
        let (back, used) = decode(&out).unwrap();
        assert_eq!(used, out.len());
        assert_eq!(back.as_list().unwrap().len(), 300);

        // >255 entries force the 32-bit map form.
        let mut map = Vec::new();
        for i in 0..300u32 {
            map.push((Value::UInt(i), Value::Bool(true)));
        }
        let mut out = Vec::new();
        encode(&Value::Map(map.clone()), &mut out);
        let (back, used) = decode(&out).unwrap();
        assert_eq!(used, out.len());
        assert_eq!(back.as_list().map(|l| l.len()), None);
        if let Value::Map(m) = back {
            assert_eq!(m.len(), 300);
        } else {
            panic!("expected a map");
        }
    }

    #[test]
    fn wide_binary_roundtrip() {
        let blob = vec![9u8; 300];
        let mut out = Vec::new();
        encode(&Value::Binary(blob.clone()), &mut out);
        let (back, used) = decode(&out).unwrap();
        assert_eq!(used, out.len());
        assert_eq!(back, Value::Binary(blob));
    }

    #[test]
    fn declared_collection_counts_must_match() {
        // A list8 claiming 3 items but encoding 1.
        let mut raw = vec![0xC0, 3, 0x52, 1];
        assert!(decode(&raw).is_err());
        raw = vec![0xC8, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x52, 1];
        assert!(decode(&raw).is_err());
    }
}
