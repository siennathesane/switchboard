//! Low-level encode/decode primitives for AMQP data fields (§4.2.5).
//!
//! Integers are unsigned and big-endian ("held in network byte order").
//! Bits accumulate into octets starting from the low bit (§4.2.5.2); a
//! partially-used bit octet is padded out as soon as a non-bit field follows.

use bytes::{BufMut, BytesMut};

use crate::error::CodecError;

/// Cursor-based decoder over a method/content payload.
#[derive(Debug)]
pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
    /// Current bit-packing octet; `bit_mask == 0` means "no bits in flight".
    bit_byte: u8,
    bit_mask: u8,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Decoder { data, pos: 0, bit_byte: 0, bit_mask: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Assert the payload has been fully consumed. A method frame carries
    /// exactly one method, so trailing bytes are a syntax error (502).
    pub fn finish(&self) -> Result<(), CodecError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes { count: self.remaining() })
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.remaining() < n {
            return Err(CodecError::Eof { needed: n, had: self.remaining() });
        }
        // A non-bit read implicitly ends any bit run: the encoder pads the
        // partial octet, so the tail bits are padding, not data.
        self.bit_mask = 0;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, CodecError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> Result<u64, CodecError> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    pub fn i32(&mut self) -> Result<i32, CodecError> {
        Ok(self.u32()? as i32)
    }

    pub fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(self.u64()? as i64)
    }

    pub fn f32(&mut self) -> Result<f32, CodecError> {
        Ok(f32::from_bits(self.u32()?))
    }

    pub fn f64(&mut self) -> Result<f64, CodecError> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// A raw block of `n` octets.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        self.take(n)
    }

    /// Consume and validate the frame-end octet.
    pub fn frame_end(&mut self) -> Result<(), CodecError> {
        let b = self.u8()?;
        if b == crate::constants::FRAME_END {
            Ok(())
        } else {
            Err(CodecError::FrameEnd { actual: b })
        }
    }

    /// One packed bit. Bits arrive low-bit-first within each octet.
    pub fn bit(&mut self) -> Result<bool, CodecError> {
        if self.bit_mask == 0 {
            self.bit_byte = self.u8()?;
            self.bit_mask = 0b0000_0001;
        }
        let v = self.bit_byte & self.bit_mask != 0;
        self.bit_mask <<= 1;
        Ok(v)
    }

    /// Short string: one length octet + UTF-8 data, no NUL octets (§4.2.5.3).
    pub fn short_str(&mut self) -> Result<String, CodecError> {
        let len = self.u8()? as usize;
        let raw = self.take(len)?;
        if raw.contains(&0) {
            return Err(CodecError::ShortStrNul);
        }
        String::from_utf8(raw.to_vec()).map_err(|_| CodecError::ShortStrUtf8)
    }

    /// Long string: 32-bit length + raw octets (§4.2.5.3).
    pub fn long_str(&mut self) -> Result<Vec<u8>, CodecError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }
}

/// Buffered encoder emitting AMQP field primitives.
#[derive(Debug, Default)]
pub struct Encoder {
    pub buf: BytesMut,
    bit_byte: u8,
    bit_count: u8,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Finish any partially-packed bit octet. Called automatically by the
    /// non-bit primitives and by [`Encoder::finish`].
    pub fn flush_bits(&mut self) {
        if self.bit_count > 0 {
            self.buf.put_u8(self.bit_byte);
            self.bit_byte = 0;
            self.bit_count = 0;
        }
    }

    pub fn u8(&mut self, v: u8) {
        self.flush_bits();
        self.buf.put_u8(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.flush_bits();
        self.buf.put_u16(v);
    }

    pub fn u32(&mut self, v: u32) {
        self.flush_bits();
        self.buf.put_u32(v);
    }

    pub fn u64(&mut self, v: u64) {
        self.flush_bits();
        self.buf.put_u64(v);
    }

    pub fn i32(&mut self, v: i32) {
        self.u32(v as u32);
    }

    pub fn i64(&mut self, v: i64) {
        self.u64(v as u64);
    }

    pub fn f32(&mut self, v: f32) {
        self.u32(v.to_bits());
    }

    pub fn f64(&mut self, v: f64) {
        self.u64(v.to_bits());
    }

    pub fn raw(&mut self, v: &[u8]) {
        self.flush_bits();
        self.buf.put_slice(v);
    }

    pub fn frame_end(&mut self) {
        self.flush_bits();
        self.buf.put_u8(crate::constants::FRAME_END);
    }

    pub fn bit(&mut self, v: bool) {
        if v {
            self.bit_byte |= 1 << self.bit_count;
        }
        self.bit_count += 1;
        if self.bit_count == 8 {
            self.flush_bits();
        }
    }

    pub fn short_str(&mut self, v: &str) {
        debug_assert!(v.len() <= 255, "short string overflow");
        debug_assert!(!v.as_bytes().contains(&0), "short string with NUL");
        self.u8(v.len() as u8);
        self.raw(v.as_bytes());
    }

    pub fn long_str(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.raw(v);
    }

    /// Hand back the finished buffer.
    pub fn finish(mut self) -> BytesMut {
        self.flush_bits();
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<F, G>(enc: F, dec: G, expected_bytes: &[u8])
    where
        F: FnOnce(&mut Encoder),
        G: FnOnce(&mut Decoder) -> Result<(), CodecError>,
    {
        let mut e = Encoder::new();
        enc(&mut e);
        let buf = e.finish();
        assert_eq!(&buf[..], expected_bytes);
        let mut d = Decoder::new(&buf);
        dec(&mut d).unwrap();
        d.finish().unwrap();
    }

    #[test]
    fn integers_are_big_endian() {
        roundtrip(
            |e| {
                e.u8(0x01);
                e.u16(0x0102);
                e.u32(0x0102_0304);
                e.u64(0x0102_0304_0506_0708);
            },
            |d| {
                assert_eq!(d.u8()?, 1);
                assert_eq!(d.u16()?, 0x0102);
                assert_eq!(d.u32()?, 0x0102_0304);
                assert_eq!(d.u64()?, 0x0102_0304_0506_0708);
                Ok(())
            },
            &[
                0x01, 0x01, 0x02, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08,
            ],
        );
    }

    #[test]
    fn signed_and_float_internals() {
        let mut e = Encoder::new();
        e.i32(-2);
        e.i64(-2);
        e.f32(-1.5);
        e.f64(3.25);
        let buf = e.finish();
        let mut d = Decoder::new(&buf);
        assert_eq!(d.i32().unwrap(), -2);
        assert_eq!(d.i64().unwrap(), -2);
        assert_eq!(d.f32().unwrap(), -1.5);
        assert_eq!(d.f64().unwrap(), 3.25);
        d.finish().unwrap();
    }

    #[test]
    fn bits_pack_low_bit_first_and_pad() {
        // 0.9.1 pack: two bits then a u8. The bits take the low bits of the
        // first octet, the u8 forces a new octet.
        roundtrip(
            |e| {
                e.bit(true);
                e.bit(false);
                e.u8(0xFF);
                e.bit(true);
                e.bit(true);
                e.bit(false);
            },
            |d| {
                assert!(d.bit()?);
                assert!(!d.bit()?);
                assert_eq!(d.u8()?, 0xFF);
                assert!(d.bit()?);
                assert!(d.bit()?);
                assert!(!d.bit()?);
                Ok(())
            },
            &[0b0000_0001, 0xFF, 0b0000_0011],
        );
    }

    #[test]
    fn eight_bits_fill_exactly_one_octet() {
        roundtrip(
            |e| {
                for i in 0..8 {
                    e.bit(i % 2 == 0);
                }
                e.bit(true);
            },
            |d| {
                for i in 0..8 {
                    assert_eq!(d.bit()?, i % 2 == 0);
                }
                assert!(d.bit()?);
                Ok(())
            },
            // bits T,F,T,F,T,F,T,F pack low-first into 0b0101_0101; the
            // ninth bit starts a fresh octet.
            &[0b0101_0101, 0b0000_0001],
        );
    }

    #[test]
    fn short_strings_roundtrip_and_reject_nul() {
        let mut e = Encoder::new();
        e.short_str("hello");
        let buf = e.finish();
        assert_eq!(&buf[..], &[5, b'h', b'e', b'l', b'l', b'o']);
        let mut d = Decoder::new(&buf);
        assert_eq!(d.short_str().unwrap(), "hello");

        // NUL octets are illegal in short strings (§4.2.5.3).
        let mut d = Decoder::new(&[3, b'a', 0, b'b']);
        assert!(matches!(d.short_str(), Err(CodecError::ShortStrNul)));
    }

    #[test]
    fn long_strings_carry_binary() {
        let mut e = Encoder::new();
        e.long_str(&[0, 1, 2, 0xCE]);
        let buf = e.finish();
        let mut d = Decoder::new(&buf);
        assert_eq!(d.long_str().unwrap(), vec![0, 1, 2, 0xCE]);
        d.finish().unwrap();
    }

    #[test]
    fn eof_and_trailing_are_reported() {
        let mut d = Decoder::new(&[0x01]);
        assert!(matches!(d.u16(), Err(CodecError::Eof { needed: 2, had: 1 })));

        let mut d = Decoder::new(&[0x01, 0x02]);
        assert_eq!(d.u8().unwrap(), 1);
        assert!(matches!(d.finish(), Err(CodecError::TrailingBytes { count: 1 })));
    }

    #[test]
    fn frame_end_validation() {
        let mut e = Encoder::new();
        e.frame_end();
        assert_eq!(&e.finish()[..], &[0xCE]);
        let mut d = Decoder::new(&[0x00]);
        assert!(matches!(d.frame_end(), Err(CodecError::FrameEnd { actual: 0x00 })));
    }
}
