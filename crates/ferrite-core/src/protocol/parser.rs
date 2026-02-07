//! RESP protocol parser
//!
//! This module implements a streaming parser for RESP2/RESP3 protocol.

use std::collections::HashMap;

use bytes::{Buf, BytesMut};
use std::io::Cursor;

use super::Frame;

/// Parse error types
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Not enough data to parse a complete frame
    Incomplete,

    /// Invalid protocol format
    Invalid(String),

    /// Invalid UTF-8 in string data
    InvalidUtf8,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Incomplete => write!(f, "incomplete data"),
            ParseError::Invalid(msg) => write!(f, "invalid protocol: {}", msg),
            ParseError::InvalidUtf8 => write!(f, "invalid UTF-8"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Helper to create invalid byte error (marked cold)
#[cold]
#[inline(never)]
fn invalid_byte_error(byte: u8) -> ParseError {
    ParseError::Invalid(format!("unexpected byte: {:02x}", byte))
}

/// Helper to create invalid integer error (marked cold)
#[cold]
#[inline(never)]
fn invalid_integer_error(s: &str) -> ParseError {
    ParseError::Invalid(format!("invalid integer: {}", s))
}

/// Helper to create invalid double error (marked cold)
#[cold]
#[inline(never)]
fn invalid_double_error(s: &str) -> ParseError {
    ParseError::Invalid(format!("invalid double: {}", s))
}

/// Helper to create invalid boolean error (marked cold)
#[cold]
#[inline(never)]
fn invalid_boolean_error(b: u8) -> ParseError {
    ParseError::Invalid(format!("invalid boolean: {:02x}", b))
}

/// Helper to create verbatim string too short error (marked cold)
#[cold]
#[inline(never)]
fn verbatim_string_too_short_error() -> ParseError {
    ParseError::Invalid("verbatim string too short".to_string())
}

/// Helper to create map key must be string error (marked cold)
#[cold]
#[inline(never)]
fn map_key_must_be_string_error() -> ParseError {
    ParseError::Invalid("map key must be string".to_string())
}

/// Helper to create expected CRLF error (marked cold)
#[cold]
#[inline(never)]
fn expected_crlf_error() -> ParseError {
    ParseError::Invalid("expected CRLF".to_string())
}

/// Parse a RESP frame from the buffer
///
/// Returns Ok(Some(frame)) if a complete frame was parsed,
/// Ok(None) if more data is needed, or Err if the data is invalid.
pub fn parse_frame(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Use a cursor to peek without consuming
    let mut cursor = Cursor::new(&buf[..]);

    match check_frame(&mut cursor) {
        Ok(len) => {
            // We have a complete frame, parse it
            cursor.set_position(0);
            let frame = parse_frame_internal(&mut cursor)?;
            // Advance the buffer
            buf.advance(len);
            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Check if a complete frame is available and return its length
fn check_frame(cursor: &mut Cursor<&[u8]>) -> Result<usize, ParseError> {
    match peek_byte(cursor)? {
        b'+' | b'-' => {
            // Simple string or error: read until \r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b':' => {
            // Integer: read until \r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b'$' => {
            // Bulk string
            cursor.advance(1);
            let len = read_decimal(cursor)?;
            if len == -1 {
                // Null bulk string
                Ok(cursor.position() as usize)
            } else if len < -1 {
                // Invalid negative length
                Err(ParseError::Invalid("negative bulk string length".into()))
            } else {
                let len = len as usize;
                // Skip the data plus final \r\n
                let total = cursor.position() as usize + len + 2;
                if cursor.get_ref().len() < total {
                    Err(ParseError::Incomplete)
                } else {
                    // Advance cursor to end of bulk string for array parsing
                    cursor.set_position(total as u64);
                    Ok(total)
                }
            }
        }
        b'*' => {
            // Array
            cursor.advance(1);
            let count = read_decimal(cursor)?;
            if count == -1 {
                // Null array
                Ok(cursor.position() as usize)
            } else if count < -1 {
                Err(ParseError::Invalid("negative array length".into()))
            } else {
                // Check each element
                for _ in 0..count {
                    check_frame(cursor)?;
                }
                Ok(cursor.position() as usize)
            }
        }
        // RESP3 types
        b'_' => {
            // Null: _\r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b'#' => {
            // Boolean: #t\r\n or #f\r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b',' => {
            // Double: ,3.14\r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b'(' => {
            // Big number: (12345...\r\n
            find_line(cursor)?;
            Ok(cursor.position() as usize)
        }
        b'!' => {
            // Bulk error: !<len>\r\n<error>\r\n
            cursor.advance(1);
            let len = read_decimal(cursor)?;
            if len < 0 {
                return Err(ParseError::Invalid("negative bulk error length".into()));
            }
            let len = len as usize;
            let total = cursor.position() as usize + len + 2;
            if cursor.get_ref().len() < total {
                Err(ParseError::Incomplete)
            } else {
                cursor.set_position(total as u64);
                Ok(total)
            }
        }
        b'=' => {
            // Verbatim string: =<len>\r\n<encoding>:<data>\r\n
            cursor.advance(1);
            let len = read_decimal(cursor)?;
            if len < 0 {
                return Err(ParseError::Invalid(
                    "negative verbatim string length".into(),
                ));
            }
            let len = len as usize;
            let total = cursor.position() as usize + len + 2;
            if cursor.get_ref().len() < total {
                Err(ParseError::Incomplete)
            } else {
                cursor.set_position(total as u64);
                Ok(total)
            }
        }
        b'%' => {
            // Map: %<count>\r\n<key><value>...
            cursor.advance(1);
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(cursor.position() as usize);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative map length".into()));
            }
            for _ in 0..count {
                check_frame(cursor)?; // key
                check_frame(cursor)?; // value
            }
            Ok(cursor.position() as usize)
        }
        b'~' => {
            // Set: ~<count>\r\n<element>...
            cursor.advance(1);
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(cursor.position() as usize);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative set length".into()));
            }
            for _ in 0..count {
                check_frame(cursor)?;
            }
            Ok(cursor.position() as usize)
        }
        b'>' => {
            // Push: ><count>\r\n<element>...
            cursor.advance(1);
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(cursor.position() as usize);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative push length".into()));
            }
            for _ in 0..count {
                check_frame(cursor)?;
            }
            Ok(cursor.position() as usize)
        }
        byte => Err(invalid_byte_error(byte)),
    }
}

/// Parse a frame from the cursor (assumes complete data is available)
fn parse_frame_internal(cursor: &mut Cursor<&[u8]>) -> Result<Frame, ParseError> {
    match get_byte(cursor)? {
        b'+' => {
            // Simple string
            let line = read_line(cursor)?;
            Ok(Frame::Simple(bytes::Bytes::copy_from_slice(line)))
        }
        b'-' => {
            // Error
            let line = read_line(cursor)?;
            Ok(Frame::Error(bytes::Bytes::copy_from_slice(line)))
        }
        b':' => {
            // Integer
            let n = read_decimal(cursor)?;
            Ok(Frame::Integer(n))
        }
        b'$' => {
            // Bulk string
            let len = read_decimal(cursor)?;
            if len == -1 {
                Ok(Frame::null())
            } else {
                let len = len as usize;
                let data = read_bytes(cursor, len)?;
                // Skip trailing \r\n
                skip_crlf(cursor)?;
                Ok(Frame::Bulk(Some(bytes::Bytes::copy_from_slice(data))))
            }
        }
        b'*' => {
            // Array
            let count = read_decimal(cursor)?;
            if count == -1 {
                Ok(Frame::null_array())
            } else if count < -1 {
                Err(ParseError::Invalid("negative array length".into()))
            } else {
                let mut frames = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    frames.push(parse_frame_internal(cursor)?);
                }
                Ok(Frame::Array(Some(frames)))
            }
        }
        // RESP3 types
        b'_' => {
            // Null: _\r\n
            skip_crlf(cursor)?;
            Ok(Frame::Null)
        }
        b'#' => {
            // Boolean: #t\r\n or #f\r\n
            let b = get_byte(cursor)?;
            skip_crlf(cursor)?;
            match b {
                b't' => Ok(Frame::Boolean(true)),
                b'f' => Ok(Frame::Boolean(false)),
                _ => Err(invalid_boolean_error(b)),
            }
        }
        b',' => {
            // Double: ,3.14\r\n or ,inf\r\n or ,-inf\r\n or ,nan\r\n
            let line = read_line(cursor)?;
            let s = std::str::from_utf8(line).map_err(|_| ParseError::InvalidUtf8)?;
            let d = match s {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                _ => s.parse().map_err(|_| invalid_double_error(s))?,
            };
            Ok(Frame::Double(d))
        }
        b'(' => {
            // Big number: (12345...\r\n
            let line = read_line(cursor)?;
            let s = String::from_utf8(line.to_vec()).map_err(|_| ParseError::InvalidUtf8)?;
            Ok(Frame::BigNumber(s))
        }
        b'!' => {
            // Bulk error: !<len>\r\n<error>\r\n
            let len = read_decimal(cursor)?;
            if len < 0 {
                return Err(ParseError::Invalid("negative bulk error length".into()));
            }
            let len = len as usize;
            let data = read_bytes(cursor, len)?;
            skip_crlf(cursor)?;
            let s = String::from_utf8(data.to_vec()).map_err(|_| ParseError::InvalidUtf8)?;
            Ok(Frame::BulkError(s))
        }
        b'=' => {
            // Verbatim string: =<len>\r\n<encoding>:<data>\r\n
            let len = read_decimal(cursor)?;
            if len < 0 {
                return Err(ParseError::Invalid(
                    "negative verbatim string length".into(),
                ));
            }
            let len = len as usize;
            let data = read_bytes(cursor, len)?;
            skip_crlf(cursor)?;
            // Format is "txt:actual data" or "mkd:markdown"
            if len < 4 {
                return Err(verbatim_string_too_short_error());
            }
            let encoding =
                String::from_utf8(data[..3].to_vec()).map_err(|_| ParseError::InvalidUtf8)?;
            let content = bytes::Bytes::copy_from_slice(&data[4..]);
            Ok(Frame::VerbatimString {
                encoding,
                data: content,
            })
        }
        b'%' => {
            // Map: %<count>\r\n<key><value>...
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative map length".into()));
            }
            let mut map = HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let key_frame = parse_frame_internal(cursor)?;
                let value_frame = parse_frame_internal(cursor)?;
                // Convert key to bytes
                let key = match key_frame {
                    Frame::Simple(b) | Frame::Bulk(Some(b)) => b,
                    _ => return Err(map_key_must_be_string_error()),
                };
                map.insert(key, value_frame);
            }
            Ok(Frame::Map(map))
        }
        b'~' => {
            // Set: ~<count>\r\n<element>...
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative set length".into()));
            }
            let mut elements = Vec::with_capacity(count as usize);
            for _ in 0..count {
                elements.push(parse_frame_internal(cursor)?);
            }
            Ok(Frame::Set(elements))
        }
        b'>' => {
            // Push: ><count>\r\n<element>...
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < -1 {
                return Err(ParseError::Invalid("negative push length".into()));
            }
            let mut elements = Vec::with_capacity(count as usize);
            for _ in 0..count {
                elements.push(parse_frame_internal(cursor)?);
            }
            Ok(Frame::Push(elements))
        }
        byte => Err(invalid_byte_error(byte)),
    }
}

/// Peek at the next byte without advancing
#[inline]
fn peek_byte(cursor: &Cursor<&[u8]>) -> Result<u8, ParseError> {
    if cursor.position() as usize >= cursor.get_ref().len() {
        return Err(ParseError::Incomplete);
    }
    Ok(cursor.get_ref()[cursor.position() as usize])
}

/// Get the next byte and advance
#[inline]
fn get_byte(cursor: &mut Cursor<&[u8]>) -> Result<u8, ParseError> {
    if cursor.position() as usize >= cursor.get_ref().len() {
        return Err(ParseError::Incomplete);
    }
    let byte = cursor.get_ref()[cursor.position() as usize];
    cursor.advance(1);
    Ok(byte)
}

/// Find the end of a line (\r\n) and position cursor after it
#[inline]
fn find_line(cursor: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
    let start = cursor.position() as usize;
    let buf = cursor.get_ref();

    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(());
        }
    }

    Err(ParseError::Incomplete)
}

/// Read a line (excluding \r\n)
#[inline]
fn read_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    let start = cursor.position() as usize;
    // Dereference to get &'a [u8] directly, avoiding borrow of cursor
    let buf: &'a [u8] = cursor.get_ref();

    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(&buf[start..i]);
        }
    }

    Err(ParseError::Incomplete)
}

/// Read a decimal number (possibly negative) followed by \r\n
#[inline]
fn read_decimal(cursor: &mut Cursor<&[u8]>) -> Result<i64, ParseError> {
    let line = read_line(cursor)?;
    let s = std::str::from_utf8(line).map_err(|_| ParseError::InvalidUtf8)?;
    s.parse().map_err(|_| invalid_integer_error(s))
}

/// Read exactly n bytes
#[inline]
fn read_bytes<'a>(cursor: &mut Cursor<&'a [u8]>, n: usize) -> Result<&'a [u8], ParseError> {
    let start = cursor.position() as usize;
    // Dereference to get &'a [u8] directly, avoiding borrow of cursor
    let buf: &'a [u8] = cursor.get_ref();

    if start + n > buf.len() {
        return Err(ParseError::Incomplete);
    }

    cursor.set_position((start + n) as u64);
    Ok(&buf[start..start + n])
}

/// Skip \r\n
#[inline]
fn skip_crlf(cursor: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
    let pos = cursor.position() as usize;
    let buf = cursor.get_ref();

    if pos + 2 > buf.len() {
        return Err(ParseError::Incomplete);
    }

    if buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return Err(expected_crlf_error());
    }

    cursor.advance(2);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Simple(Bytes::from("OK")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Error(Bytes::from("ERR unknown command")));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":1000\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(1000));

        let mut buf = BytesMut::from(":-500\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(-500));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::bulk("hello"));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::null());
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut buf = BytesMut::from("$0\r\n\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::bulk(""));
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::array(vec![Frame::bulk("foo"), Frame::bulk("bar")])
        );
    }

    #[test]
    fn test_parse_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::null_array());
    }

    #[test]
    fn test_parse_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::array(vec![]));
    }

    #[test]
    fn test_parse_nested_array() {
        let mut buf = BytesMut::from("*2\r\n*2\r\n+a\r\n+b\r\n*1\r\n:42\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::array(vec![
                Frame::array(vec![
                    Frame::Simple(Bytes::from("a")),
                    Frame::Simple(Bytes::from("b"))
                ]),
                Frame::array(vec![Frame::Integer(42)])
            ])
        );
    }

    #[test]
    fn test_parse_incomplete() {
        let mut buf = BytesMut::from("+OK");
        assert_eq!(parse_frame(&mut buf).unwrap(), None);

        let mut buf = BytesMut::from("$5\r\nhel");
        assert_eq!(parse_frame(&mut buf).unwrap(), None);

        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n");
        assert_eq!(parse_frame(&mut buf).unwrap(), None);
    }

    #[test]
    fn test_parse_invalid() {
        let mut buf = BytesMut::from("invalid\r\n");
        assert!(parse_frame(&mut buf).is_err());
    }

    #[test]
    fn test_parse_negative_lengths() {
        let mut buf = BytesMut::from("*-2\r\n");
        assert!(parse_frame(&mut buf).is_err());

        let mut buf = BytesMut::from("%-2\r\n");
        assert!(parse_frame(&mut buf).is_err());

        let mut buf = BytesMut::from("~-2\r\n");
        assert!(parse_frame(&mut buf).is_err());

        let mut buf = BytesMut::from(">-2\r\n");
        assert!(parse_frame(&mut buf).is_err());

        let mut buf = BytesMut::from("!-2\r\n");
        assert!(parse_frame(&mut buf).is_err());

        let mut buf = BytesMut::from("=-2\r\n");
        assert!(parse_frame(&mut buf).is_err());
    }

    #[test]
    fn test_parse_multiple_frames() {
        let mut buf = BytesMut::from("+OK\r\n:42\r\n");

        let frame1 = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame1, Frame::Simple(Bytes::from("OK")));

        let frame2 = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame2, Frame::Integer(42));

        assert!(buf.is_empty());
    }

    // RESP3 tests
    #[test]
    fn test_parse_resp3_null() {
        let mut buf = BytesMut::from("_\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn test_parse_resp3_boolean() {
        let mut buf = BytesMut::from("#t\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(true));

        let mut buf = BytesMut::from("#f\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(false));
    }

    #[test]
    fn test_parse_resp3_double() {
        let mut buf = BytesMut::from(",3.14\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Double(3.14));

        let mut buf = BytesMut::from(",inf\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Double(f64::INFINITY));

        let mut buf = BytesMut::from(",-inf\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_parse_resp3_big_number() {
        let mut buf = BytesMut::from("(3492890328409238509324850943850943825024385\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::BigNumber("3492890328409238509324850943850943825024385".to_string())
        );
    }

    #[test]
    fn test_parse_resp3_bulk_error() {
        let mut buf = BytesMut::from("!21\r\nSYNTAX invalid syntax\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkError("SYNTAX invalid syntax".to_string()));
    }

    #[test]
    fn test_parse_resp3_verbatim_string() {
        let mut buf = BytesMut::from("=15\r\ntxt:Some string\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        match frame {
            Frame::VerbatimString { encoding, data } => {
                assert_eq!(encoding, "txt");
                assert_eq!(data.as_ref(), b"Some string");
            }
            _ => panic!("Expected VerbatimString"),
        }
    }

    #[test]
    fn test_parse_resp3_map() {
        let mut buf = BytesMut::from("%2\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n:42\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Map(map) => {
                assert_eq!(map.len(), 2);
                assert_eq!(
                    map.get(&Bytes::from_static(b"key1")),
                    Some(&Frame::bulk("value1"))
                );
                assert_eq!(
                    map.get(&Bytes::from_static(b"key2")),
                    Some(&Frame::Integer(42))
                );
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_parse_resp3_set() {
        let mut buf = BytesMut::from("~3\r\n+a\r\n+b\r\n+c\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Set(elements) => {
                assert_eq!(elements.len(), 3);
                assert!(elements.contains(&Frame::Simple(Bytes::from("a"))));
                assert!(elements.contains(&Frame::Simple(Bytes::from("b"))));
                assert!(elements.contains(&Frame::Simple(Bytes::from("c"))));
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_parse_resp3_push() {
        let mut buf = BytesMut::from(">3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Push(elements) => {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Frame::bulk("message"));
                assert_eq!(elements[1], Frame::bulk("hello"));
                assert_eq!(elements[2], Frame::bulk("world"));
            }
            _ => panic!("Expected Push"),
        }
    }

    #[test]
    fn test_resp3_roundtrip() {
        use crate::protocol::encoder::encode_frame;

        // Test various RESP3 types round-trip
        let frames = vec![
            Frame::Null,
            Frame::Boolean(true),
            Frame::Boolean(false),
            Frame::Double(3.14159),
            Frame::Double(f64::INFINITY),
            Frame::Double(f64::NEG_INFINITY),
            Frame::BigNumber("12345678901234567890".to_string()),
            Frame::BulkError("ERR something went wrong".to_string()),
            Frame::VerbatimString {
                encoding: "txt".to_string(),
                data: bytes::Bytes::from("Hello World"),
            },
            Frame::Set(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
            ]),
            Frame::Push(vec![Frame::bulk("pubsub"), Frame::bulk("message")]),
        ];

        for original in frames {
            let mut encoded = BytesMut::new();
            encode_frame(&original, &mut encoded);
            let decoded = parse_frame(&mut encoded).unwrap().unwrap();
            assert_eq!(original, decoded, "Round-trip failed for {:?}", original);
        }
    }
}
