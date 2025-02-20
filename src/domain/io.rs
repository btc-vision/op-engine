use crate::domain::generic::errors::{OpNetError, OpNetResult};
use std::io::{Read, Write};

/// Provides an interface for writing raw bytes with typed methods.
///
/// This enhanced version tracks a `position` field (i.e. how many bytes have
/// been written so far). This allows you to do `writer.position()` without flushing.
///
pub struct ByteWriter<W: Write> {
    pub inner: W,
    position: u64, // how many bytes we've written
}

impl<W: Write> ByteWriter<W> {
    /// Create a new ByteWriter
    pub fn new(inner: W) -> Self {
        Self { inner, position: 0 }
    }

    /// Return the number of bytes written so far.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Write raw bytes, updating the position. Private helper for all typed methods.
    fn write_all_bytes(&mut self, data: &[u8]) -> OpNetResult<()> {
        self.inner.write_all(data)?;
        self.position += data.len() as u64;
        Ok(())
    }

    /// Flush the underlying writer
    pub fn flush(&mut self) -> OpNetResult<()> {
        self.inner.flush()?;
        Ok(())
    }

    //-----------------------------------------------------------------------
    //  Unsigned integers (little-endian)
    //-----------------------------------------------------------------------
    pub fn write_u8(&mut self, value: u8) -> OpNetResult<()> {
        self.write_all_bytes(&[value])
    }

    pub fn write_u16(&mut self, value: u16) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_u32(&mut self, value: u32) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_u64(&mut self, value: u64) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_u128(&mut self, value: u128) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    //-----------------------------------------------------------------------
    //  Signed integers (little-endian)
    //-----------------------------------------------------------------------
    pub fn write_i8(&mut self, value: i8) -> OpNetResult<()> {
        self.write_all_bytes(&[value as u8])
    }

    pub fn write_i16(&mut self, value: i16) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_i32(&mut self, value: i32) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_i64(&mut self, value: i64) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_i128(&mut self, value: i128) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    //-----------------------------------------------------------------------
    //  Floating point (little-endian)
    //-----------------------------------------------------------------------
    pub fn write_f32(&mut self, value: f32) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    pub fn write_f64(&mut self, value: f64) -> OpNetResult<()> {
        let bytes = value.to_le_bytes();
        self.write_all_bytes(&bytes)
    }

    //-----------------------------------------------------------------------
    //  Boolean
    //-----------------------------------------------------------------------
    /// Writes a single byte: `1` if true, `0` if false.
    pub fn write_bool(&mut self, value: bool) -> OpNetResult<()> {
        self.write_u8(if value { 1 } else { 0 })
    }

    //-----------------------------------------------------------------------
    //  Bytes, strings, and var-length
    //-----------------------------------------------------------------------
    /// Write a fixed slice of bytes
    pub fn write_bytes(&mut self, data: &[u8]) -> OpNetResult<()> {
        self.write_all_bytes(data)
    }

    /// Write variable-length bytes: first the length (as u64), then the bytes
    pub fn write_var_bytes(&mut self, data: &[u8]) -> OpNetResult<()> {
        self.write_u64(data.len() as u64)?;
        self.write_all_bytes(data)
    }

    /// Writes a UTF-8 string in two parts: (u64 length) + raw bytes.
    pub fn write_string(&mut self, s: &str) -> OpNetResult<()> {
        let bytes = s.as_bytes();
        self.write_var_bytes(bytes)
    }

    /// Writes a “varint” with LEB128 encoding for smaller integers.
    /// (Optional example; may or may not be relevant to your design.)
    pub fn write_varint(&mut self, mut value: u64) -> OpNetResult<()> {
        while value >= 0x80 {
            let byte = (value as u8) | 0x80;
            self.write_u8(byte)?;
            value >>= 7;
        }
        self.write_u8(value as u8)
    }
}

pub struct ByteReader<R: Read> {
    pub inner: R,
    pub offset: usize, // track how many bytes read
}

impl<R: Read> ByteReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, offset: 0 }
    }

    fn read_exact_buf(&mut self, buf: &mut [u8]) -> OpNetResult<()> {
        let mut total_read = 0;
        while total_read < buf.len() {
            let n = self.inner.read(&mut buf[total_read..])?;
            if n == 0 {
                // Reached EOF unexpectedly before filling `buf`
                return Err(OpNetError::new("Not enough bytes to read"));
            }
            total_read += n;
        }

        self.offset += total_read;
        Ok(())
    }

    /// Return how many bytes have been read so far
    pub fn position(&self) -> usize {
        self.offset
    }

    //-----------------------------------------------------------------------
    //  Unsigned integers (little-endian)
    //-----------------------------------------------------------------------
    pub fn read_u8(&mut self) -> OpNetResult<u8> {
        let mut buf = [0u8; 1];
        self.read_exact_buf(&mut buf)?;
        Ok(buf[0])
    }

    pub fn read_u16(&mut self) -> OpNetResult<u16> {
        let mut buf = [0u8; 2];
        self.read_exact_buf(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    pub fn read_u32(&mut self) -> OpNetResult<u32> {
        let mut buf = [0u8; 4];
        self.read_exact_buf(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    pub fn read_u64(&mut self) -> OpNetResult<u64> {
        let mut buf = [0u8; 8];
        self.read_exact_buf(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn read_u128(&mut self) -> OpNetResult<u128> {
        let mut buf = [0u8; 16];
        self.read_exact_buf(&mut buf)?;
        Ok(u128::from_le_bytes(buf))
    }

    //-----------------------------------------------------------------------
    //  Signed integers (little-endian)
    //-----------------------------------------------------------------------
    pub fn read_i8(&mut self) -> OpNetResult<i8> {
        let val = self.read_u8()?;
        Ok(val as i8)
    }

    pub fn read_i16(&mut self) -> OpNetResult<i16> {
        let val = self.read_u16()?;
        Ok(val as i16)
    }

    pub fn read_i32(&mut self) -> OpNetResult<i32> {
        let val = self.read_u32()?;
        Ok(val as i32)
    }

    pub fn read_i64(&mut self) -> OpNetResult<i64> {
        let val = self.read_u64()?;
        Ok(val as i64)
    }

    pub fn read_i128(&mut self) -> OpNetResult<i128> {
        let val = self.read_u128()?;
        Ok(val as i128)
    }

    //-----------------------------------------------------------------------
    //  Floating point (little-endian)
    //-----------------------------------------------------------------------
    pub fn read_f32(&mut self) -> OpNetResult<f32> {
        let bits = self.read_u32()?;
        Ok(f32::from_le_bytes(bits.to_le_bytes()))
    }

    pub fn read_f64(&mut self) -> OpNetResult<f64> {
        let bits = self.read_u64()?;
        Ok(f64::from_le_bytes(bits.to_le_bytes()))
    }

    //-----------------------------------------------------------------------
    //  Boolean
    //-----------------------------------------------------------------------
    /// Reads a single byte: returns true if 1, false if 0. Any other value => error.
    pub fn read_bool(&mut self) -> OpNetResult<bool> {
        let val = self.read_u8()?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(OpNetError::new("Invalid boolean byte")),
        }
    }

    //-----------------------------------------------------------------------
    //  Bytes, strings, var-length
    //-----------------------------------------------------------------------
    /// Read an exact number of raw bytes.
    pub fn read_bytes(&mut self, length: usize) -> OpNetResult<Vec<u8>> {
        let mut buf = vec![0u8; length];
        self.read_exact_buf(&mut buf)?;
        Ok(buf)
    }

    /// Read variable-length bytes: first a u64 (little-endian) length, then that many bytes.
    pub fn read_var_bytes(&mut self) -> OpNetResult<Vec<u8>> {
        let length = self.read_u64()? as usize;
        let mut buf = vec![0u8; length];
        self.read_exact_buf(&mut buf)?;
        Ok(buf)
    }

    /// Read a length-prefixed UTF-8 string. (u64 length + raw bytes)
    pub fn read_string(&mut self) -> OpNetResult<String> {
        let bytes = self.read_var_bytes()?;
        let s = String::from_utf8(bytes)
            .map_err(|_| OpNetError::new("Invalid UTF-8 data for string"))?;
        Ok(s)
    }

    /// Reads a “varint” in LEB128 form, up to 64 bits.
    pub fn read_varint(&mut self) -> OpNetResult<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;

        loop {
            let byte = self.read_u8()?;
            let value = (byte & 0x7F) as u64;
            result |= value << shift;

            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(OpNetError::new("varint too large for u64"));
            }
        }
        Ok(result)
    }
}

/// Trait for custom serialization without JSON or Protobuf.
pub trait CustomSerialize: Sized {
    fn serialize<W: Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()>;
    fn deserialize<R: Read>(reader: &mut ByteReader<R>) -> OpNetResult<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    #[test]
    fn test_write_read_u8() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u8(123).unwrap();
        assert_eq!(writer.position(), 1);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_u8().unwrap();
        assert_eq!(val, 123);
        assert_eq!(reader.position(), 1);
    }

    #[test]
    fn test_write_read_u16() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u16(0xABCD).unwrap();
        assert_eq!(writer.position(), 2);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_u16().unwrap();
        assert_eq!(val, 0xABCD);
        assert_eq!(reader.position(), 2);
    }

    #[test]
    fn test_write_read_u32() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u32(0xDEADBEEF).unwrap();
        assert_eq!(writer.position(), 4);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_u32().unwrap();
        assert_eq!(val, 0xDEADBEEF);
        assert_eq!(reader.position(), 4);
    }

    #[test]
    fn test_write_read_u64() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u64(0x12345678ABCDEF01).unwrap();
        assert_eq!(writer.position(), 8);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_u64().unwrap();
        assert_eq!(val, 0x12345678ABCDEF01);
        assert_eq!(reader.position(), 8);
    }

    #[test]
    fn test_write_read_u128() {
        let mut writer = ByteWriter::new(Vec::new());
        writer
            .write_u128(0x123456789ABCDEF0_123456789ABCDEF0)
            .unwrap();
        assert_eq!(writer.position(), 16);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_u128().unwrap();
        assert_eq!(val, 0x123456789ABCDEF0_123456789ABCDEF0);
        assert_eq!(reader.position(), 16);
    }

    #[test]
    fn test_write_read_i8() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_i8(-123).unwrap();
        assert_eq!(writer.position(), 1);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_i8().unwrap();
        assert_eq!(val, -123);
        assert_eq!(reader.position(), 1);
    }

    #[test]
    fn test_write_read_i16() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_i16(-12345).unwrap();
        assert_eq!(writer.position(), 2);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_i16().unwrap();
        assert_eq!(val, -12345);
        assert_eq!(reader.position(), 2);
    }

    #[test]
    fn test_write_read_i32() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_i32(-123456789).unwrap();
        assert_eq!(writer.position(), 4);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_i32().unwrap();
        assert_eq!(val, -123456789);
        assert_eq!(reader.position(), 4);
    }

    #[test]
    fn test_write_read_i64() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_i64(-0x12345678ABCDEF0).unwrap();
        assert_eq!(writer.position(), 8);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_i64().unwrap();
        assert_eq!(val, -0x12345678ABCDEF0);
        assert_eq!(reader.position(), 8);
    }

    #[test]
    fn test_write_read_i128() {
        let mut writer = ByteWriter::new(Vec::new());
        writer
            .write_i128(-0x123456789ABCDEF0_123456789ABCDEF0)
            .unwrap();
        assert_eq!(writer.position(), 16);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_i128().unwrap();
        assert_eq!(val, -0x123456789ABCDEF0_123456789ABCDEF0);
        assert_eq!(reader.position(), 16);
    }

    #[test]
    fn test_write_read_f32() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_f32(123.456).unwrap();
        assert_eq!(writer.position(), 4);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_f32().unwrap();
        // Compare within a small epsilon if necessary, but direct bit equivalence should work here
        assert_eq!(val, 123.456_f32);
        assert_eq!(reader.position(), 4);
    }

    #[test]
    fn test_write_read_f64() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_f64(98765.4321).unwrap();
        assert_eq!(writer.position(), 8);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val = reader.read_f64().unwrap();
        // Compare within a small epsilon if necessary
        assert_eq!(val, 98765.4321_f64);
        assert_eq!(reader.position(), 8);
    }

    #[test]
    fn test_write_read_bool() {
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_bool(true).unwrap();
        writer.write_bool(false).unwrap();
        assert_eq!(writer.position(), 2);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let val_true = reader.read_bool().unwrap();
        let val_false = reader.read_bool().unwrap();
        assert!(val_true);
        assert!(!val_false);
        assert_eq!(reader.position(), 2);
    }

    #[test]
    fn test_write_read_bool_error() {
        // Write an invalid boolean byte
        let mut data = vec![2]; // 2 is not valid for boolean
        let mut reader = ByteReader::new(Cursor::new(&mut data));

        let err = reader.read_bool().unwrap_err();
        assert_eq!(err.to_string(), "OpNetError: Invalid boolean byte");
    }

    #[test]
    fn test_write_read_bytes() {
        let test_data = vec![1, 2, 3, 4, 5];
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_bytes(&test_data).unwrap();
        assert_eq!(writer.position(), 5);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let read_data = reader.read_bytes(5).unwrap();
        assert_eq!(read_data, test_data);
        assert_eq!(reader.position(), 5);
    }

    #[test]
    fn test_write_read_var_bytes() {
        let test_data = vec![10, 20, 30, 40];
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_var_bytes(&test_data).unwrap();
        // 8 bytes for the length (u64) + 4 bytes of data
        assert_eq!(writer.position(), 12);

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let read_data = reader.read_var_bytes().unwrap();
        assert_eq!(read_data, test_data);
        // 8 + 4 = 12
        assert_eq!(reader.position(), 12);
    }

    #[test]
    fn test_write_read_string() {
        let s = "Hello, world! Привет, мир! こんにちは世界！";
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_string(s).unwrap();

        let buffer = writer.inner;
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let read_s = reader.read_string().unwrap();
        assert_eq!(read_s, s);
    }

    #[test]
    fn test_write_read_varint() {
        // test small varint
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_varint(127).unwrap(); // single byte
        writer.write_varint(128).unwrap(); // boundary: two bytes
        writer.write_varint(300).unwrap(); // multi-byte
        let buffer = writer.inner;

        let mut reader = ByteReader::new(Cursor::new(buffer));
        assert_eq!(reader.read_varint().unwrap(), 127);
        assert_eq!(reader.read_varint().unwrap(), 128);
        assert_eq!(reader.read_varint().unwrap(), 300);
    }

    #[test]
    fn test_varint_too_large_for_u64() {
        // Construct a sequence of bytes that will overflow a 64-bit varint
        // e.g., 10 bytes with the high bit set: LEB128 would exceed 64 bits
        let invalid_varint_data = vec![0xFF; 10];
        let mut reader = ByteReader::new(Cursor::new(invalid_varint_data));
        let result = reader.read_varint();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "OpNetError: varint too large for u64"
        );
    }

    #[test]
    fn test_flush() {
        // For Vec<u8>, flush is a no-op, but we can still call it
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u8(42).unwrap();
        writer.flush().unwrap(); // Should succeed
        assert_eq!(writer.position(), 1);
    }

    #[test]
    fn test_partial_read_error() {
        // Attempt to read more bytes than available
        let mut writer = ByteWriter::new(Vec::new());
        writer.write_u8(42).unwrap();
        let buffer = writer.inner;

        // Now read 2 bytes from a buffer containing only 1
        let mut reader = ByteReader::new(Cursor::new(buffer));
        let result = reader.read_u16();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "OpNetError: Not enough bytes to read"
        );
    }

    #[test]
    fn test_reader_offset_increments() {
        let test_data = vec![0x01, 0x02, 0x03, 0x04];
        let mut reader = ByteReader::new(Cursor::new(test_data));
        assert_eq!(reader.position(), 0);

        let b1 = reader.read_u8().unwrap();
        assert_eq!(b1, 0x01);
        assert_eq!(reader.position(), 1);

        let b2 = reader.read_u8().unwrap();
        assert_eq!(b2, 0x02);
        assert_eq!(reader.position(), 2);

        let b3 = reader.read_u16().unwrap();
        assert_eq!(b3, 0x0403); // little-endian => 0x03 is LSB, 0x04 is MSB
        assert_eq!(reader.position(), 4);
    }

    #[test]
    fn test_writer_position_increments() {
        let mut writer = ByteWriter::new(Vec::new());
        assert_eq!(writer.position(), 0);
        writer.write_u8(0xAA).unwrap();
        assert_eq!(writer.position(), 1);
        writer.write_u16(0xBBCC).unwrap();
        assert_eq!(writer.position(), 3);
        writer.write_bytes(&[1, 2, 3]).unwrap();
        assert_eq!(writer.position(), 6);
    }
}
