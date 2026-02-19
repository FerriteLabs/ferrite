//! RESP2 protocol encoder and decoder.
//!
//! Implements the Redis Serialization Protocol (RESP2) wire format
//! for communication with Ferrite/Redis servers.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;

use crate::error::{Error, Result};
use crate::types::Value;

/// Encode a command as a RESP2 array of bulk strings.
///
/// This is the standard format for sending commands to a Redis-compatible server.
///
/// # Example wire format
/// ```text
/// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
/// ```
pub fn encode_command(args: &[Bytes], buf: &mut BytesMut) {
    // *<count>\r\n
    buf.put_u8(b'*');
    buf.put_slice(args.len().to_string().as_bytes());
    buf.put_slice(b"\r\n");

    for arg in args {
        // $<len>\r\n<data>\r\n
        buf.put_u8(b'$');
        buf.put_slice(arg.len().to_string().as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(arg);
        buf.put_slice(b"\r\n");
    }
}

/// Attempt to decode a RESP2 value from the buffer.
///
/// Returns `Ok(Some(value))` if a complete frame was parsed,
/// `Ok(None)` if more data is needed, or `Err` if the data is malformed.
pub fn decode_value(buf: &mut BytesMut) -> Result<Option<Value>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(&buf[..]);

    match check_complete(&mut cursor) {
        Ok(len) => {
            cursor.set_position(0);
            let value = parse_value(&mut cursor)?;
            buf.advance(len);
            Ok(Some(value))
        }
        Err(Error::Protocol(ref s)) if s == "incomplete" => Ok(None),
        Err(e) => Err(e),
    }
}

/// Check if a complete frame is available, returning its byte length.
fn check_complete(cursor: &mut Cursor<&[u8]>) -> Result<usize> {
    match peek_byte(cursor)? {
        b'+' | b'-' => {
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b':' => {
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b'$' => {
            cursor.advance(1);
            let len = read_decimal(cursor)?;
            if len == -1 {
                Ok(cursor.position() as usize)
            } else if len < -1 {
                Err(Error::Protocol("negative bulk string length".into()))
            } else {
                let total = cursor.position() as usize + len as usize + 2;
                if cursor.get_ref().len() < total {
                    Err(Error::Protocol("incomplete".into()))
                } else {
                    cursor.set_position(total as u64);
                    Ok(total)
                }
            }
        }
        b'*' => {
            cursor.advance(1);
            let count = read_decimal(cursor)?;
            if count == -1 {
                Ok(cursor.position() as usize)
            } else if count < -1 {
                Err(Error::Protocol("negative array length".into()))
            } else {
                for _ in 0..count {
                    check_complete(cursor)?;
                }
                Ok(cursor.position() as usize)
            }
        }
        byte => Err(Error::Protocol(format!("unexpected byte: 0x{:02x}", byte))),
    }
}

/// Parse a RESP value from the cursor (assumes complete data).
fn parse_value(cursor: &mut Cursor<&[u8]>) -> Result<Value> {
    match get_byte(cursor)? {
        b'+' => {
            let line = read_line(cursor)?;
            let s = String::from_utf8(line.to_vec())
                .map_err(|e| Error::Protocol(format!("invalid UTF-8: {}", e)))?;
            Ok(Value::Status(s))
        }
        b'-' => {
            let line = read_line(cursor)?;
            let s = String::from_utf8(line.to_vec())
                .map_err(|e| Error::Protocol(format!("invalid UTF-8: {}", e)))?;
            Err(Error::Server(s))
        }
        b':' => {
            let n = read_decimal(cursor)?;
            Ok(Value::Integer(n))
        }
        b'$' => {
            let len = read_decimal(cursor)?;
            if len == -1 {
                Ok(Value::Nil)
            } else {
                let len = len as usize;
                let data = read_bytes(cursor, len)?;
                let val = Bytes::copy_from_slice(data);
                skip_crlf(cursor)?;
                Ok(Value::String(val))
            }
        }
        b'*' => {
            let count = read_decimal(cursor)?;
            if count == -1 {
                Ok(Value::Nil)
            } else {
                let count = count as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(parse_value(cursor)?);
                }
                Ok(Value::Array(values))
            }
        }
        byte => Err(Error::Protocol(format!("unexpected byte: 0x{:02x}", byte))),
    }
}

// ── Low-level cursor helpers ────────────────────────────────────────────────

fn incomplete() -> Error {
    Error::Protocol("incomplete".into())
}

fn peek_byte(cursor: &Cursor<&[u8]>) -> Result<u8> {
    let pos = cursor.position() as usize;
    if pos >= cursor.get_ref().len() {
        return Err(incomplete());
    }
    Ok(cursor.get_ref()[pos])
}

fn get_byte(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let pos = cursor.position() as usize;
    if pos >= cursor.get_ref().len() {
        return Err(incomplete());
    }
    let byte = cursor.get_ref()[pos];
    cursor.advance(1);
    Ok(byte)
}

fn find_line(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let start = cursor.position() as usize;
    let buf = cursor.get_ref();
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(());
        }
    }
    Err(incomplete())
}

fn read_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let start = cursor.position() as usize;
    let buf: &'a [u8] = *cursor.get_ref();
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(&buf[start..i]);
        }
    }
    Err(incomplete())
}

fn read_decimal(cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let line = read_line(cursor)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| Error::Protocol("invalid UTF-8 in integer".into()))?;
    s.parse::<i64>()
        .map_err(|_| Error::Protocol(format!("invalid integer: {}", s)))
}

fn read_bytes<'a>(cursor: &mut Cursor<&'a [u8]>, n: usize) -> Result<&'a [u8]> {
    let start = cursor.position() as usize;
    let buf: &'a [u8] = *cursor.get_ref();
    if start + n > buf.len() {
        return Err(incomplete());
    }
    cursor.set_position((start + n) as u64);
    Ok(&buf[start..start + n])
}

fn skip_crlf(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let pos = cursor.position() as usize;
    let buf = cursor.get_ref();
    if pos + 2 > buf.len() {
        return Err(incomplete());
    }
    if buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return Err(Error::Protocol("expected CRLF".into()));
    }
    cursor.advance(2);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_command() {
        let args = vec![
            Bytes::from("SET"),
            Bytes::from("key"),
            Bytes::from("value"),
        ];
        let mut buf = BytesMut::new();
        encode_command(&args, &mut buf);
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn test_decode_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(val, Value::Status("OK".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_error() {
        let mut buf = BytesMut::from("-ERR unknown\r\n");
        let err = decode_value(&mut buf).unwrap_err();
        match err {
            Error::Server(msg) => assert_eq!(msg, "ERR unknown"),
            _ => panic!("expected Server error"),
        }
    }

    #[test]
    fn test_decode_integer() {
        let mut buf = BytesMut::from(":42\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(val, Value::String(Bytes::from("hello")));
    }

    #[test]
    fn test_decode_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(val, Value::Nil);
    }

    #[test]
    fn test_decode_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(
            val,
            Value::Array(vec![
                Value::String(Bytes::from("foo")),
                Value::String(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn test_decode_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let val = decode_value(&mut buf).unwrap().unwrap();
        assert_eq!(val, Value::Nil);
    }

    #[test]
    fn test_decode_incomplete() {
        let mut buf = BytesMut::from("+OK");
        assert!(decode_value(&mut buf).unwrap().is_none());

        let mut buf = BytesMut::from("$5\r\nhel");
        assert!(decode_value(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_empty() {
        let mut buf = BytesMut::new();
        assert!(decode_value(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_roundtrip() {
        let args = vec![Bytes::from("PING")];
        let mut buf = BytesMut::new();
        encode_command(&args, &mut buf);
        let val = decode_value(&mut buf).unwrap().unwrap();
        match val {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Value::String(Bytes::from("PING")));
            }
            _ => panic!("expected array"),
        }
    }
}
