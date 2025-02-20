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
        let read_bytes = self.inner.read(buf)?;
        if read_bytes < buf.len() {
            return Err(OpNetError::new("Not enough bytes to read"));
        }
        self.offset += read_bytes;
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
