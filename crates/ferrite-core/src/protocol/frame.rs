//! RESP Frame types
//!
//! This module defines the Frame enum representing all RESP2/RESP3 data types.

use std::collections::HashMap;

use bytes::Bytes;

/// RESP protocol frame types
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    // RESP2 types
    /// Simple string: +OK\r\n
    Simple(Bytes),

    /// Error: -ERR message\r\n
    Error(Bytes),

    /// Integer: :1000\r\n
    Integer(i64),

    /// Bulk string: $5\r\nhello\r\n or $-1\r\n for null
    Bulk(Option<Bytes>),

    /// Array: *2\r\n... or *-1\r\n for null
    Array(Option<Vec<Frame>>),

    // RESP3 types
    /// Null: _\r\n (RESP3 explicit null)
    Null,

    /// Boolean: #t\r\n or #f\r\n
    Boolean(bool),

    /// Double: ,3.14\r\n
    Double(f64),

    /// Big number: (12345...\r\n (arbitrary precision integer as string)
    BigNumber(String),

    /// Bulk error: !<len>\r\n<error>\r\n
    BulkError(String),

    /// Verbatim string: =<len>\r\n<encoding>:<data>\r\n
    VerbatimString {
        /// The encoding type (e.g., "txt", "mkd")
        encoding: String,
        /// The verbatim string data
        data: Bytes,
    },

    /// Map: %<count>\r\n<key><value>...
    Map(HashMap<Bytes, Frame>),

    /// Set: ~<count>\r\n<element>...
    Set(Vec<Frame>),

    /// Push: ><count>\r\n<element>... (server-initiated message)
    Push(Vec<Frame>),
}

impl Frame {
    /// Create a simple string frame
    #[inline]
    pub fn simple(s: impl Into<Bytes>) -> Self {
        Frame::Simple(s.into())
    }

    /// Create an error frame
    #[cold]
    #[inline]
    pub fn error(s: impl Into<Bytes>) -> Self {
        Frame::Error(s.into())
    }

    /// Create an integer frame
    #[inline]
    pub fn integer(n: i64) -> Self {
        Frame::Integer(n)
    }

    /// Create a bulk string frame
    #[inline]
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Frame::Bulk(Some(data.into()))
    }

    /// Create a null bulk string frame
    #[inline]
    pub fn null() -> Self {
        Frame::Bulk(None)
    }

    /// Create a null array frame
    #[inline]
    pub fn null_array() -> Self {
        Frame::Array(None)
    }

    /// Create an array frame
    #[inline]
    pub fn array(frames: Vec<Frame>) -> Self {
        Frame::Array(Some(frames))
    }

    /// Check if this frame is null (bulk, array, or RESP3 Null)
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Frame::Bulk(None) | Frame::Array(None) | Frame::Null)
    }

    /// Create a RESP3 null frame
    #[inline]
    pub fn resp3_null() -> Self {
        Frame::Null
    }

    /// Create a boolean frame (RESP3)
    #[inline]
    pub fn boolean(b: bool) -> Self {
        Frame::Boolean(b)
    }

    /// Create a double frame (RESP3)
    #[inline]
    pub fn double(d: f64) -> Self {
        Frame::Double(d)
    }

    /// Create a map frame (RESP3)
    #[inline]
    pub fn map(m: HashMap<Bytes, Frame>) -> Self {
        Frame::Map(m)
    }

    /// Create a set frame (RESP3)
    #[inline]
    pub fn set(elements: Vec<Frame>) -> Self {
        Frame::Set(elements)
    }

    /// Create a push frame (RESP3)
    #[inline]
    pub fn push(elements: Vec<Frame>) -> Self {
        Frame::Push(elements)
    }

    /// Check if this frame is an error
    #[inline]
    pub fn is_error(&self) -> bool {
        matches!(self, Frame::Error(_))
    }

    /// Get the string value if this is a Simple or Bulk frame
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Frame::Simple(b) => std::str::from_utf8(b).ok(),
            Frame::Bulk(Some(b)) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Get the bytes if this is a Bulk frame
    #[inline]
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Frame::Bulk(Some(b)) => Some(b),
            _ => None,
        }
    }

    /// Get the integer value if this is an Integer frame
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Frame::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Get the array if this is an Array frame
    pub fn as_array(&self) -> Option<&Vec<Frame>> {
        match self {
            Frame::Array(Some(arr)) => Some(arr),
            _ => None,
        }
    }

    /// Convert to owned array if this is an Array frame
    pub fn into_array(self) -> Option<Vec<Frame>> {
        match self {
            Frame::Array(Some(arr)) => Some(arr),
            _ => None,
        }
    }

    /// Convert to owned bytes if this is a Bulk frame
    pub fn into_bytes(self) -> Option<Bytes> {
        match self {
            Frame::Bulk(Some(b)) => Some(b),
            _ => None,
        }
    }
}

impl From<String> for Frame {
    fn from(s: String) -> Self {
        Frame::bulk(s)
    }
}

impl From<&str> for Frame {
    fn from(s: &str) -> Self {
        Frame::bulk(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<i64> for Frame {
    fn from(n: i64) -> Self {
        Frame::integer(n)
    }
}

impl From<Bytes> for Frame {
    fn from(b: Bytes) -> Self {
        Frame::bulk(b)
    }
}

impl From<Vec<u8>> for Frame {
    fn from(v: Vec<u8>) -> Self {
        Frame::bulk(Bytes::from(v))
    }
}

impl From<&[u8]> for Frame {
    fn from(b: &[u8]) -> Self {
        Frame::bulk(Bytes::copy_from_slice(b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_constructors() {
        assert_eq!(Frame::simple("OK"), Frame::Simple(Bytes::from("OK")));
        assert_eq!(Frame::error("ERR"), Frame::Error(Bytes::from("ERR")));
        assert_eq!(Frame::integer(42), Frame::Integer(42));
        assert_eq!(
            Frame::bulk("hello"),
            Frame::Bulk(Some(Bytes::from("hello")))
        );
        assert_eq!(Frame::null(), Frame::Bulk(None));
        assert_eq!(Frame::null_array(), Frame::Array(None));
    }

    #[test]
    fn test_frame_is_null() {
        assert!(Frame::null().is_null());
        assert!(Frame::null_array().is_null());
        assert!(!Frame::simple("OK").is_null());
        assert!(!Frame::bulk("hello").is_null());
    }

    #[test]
    fn test_frame_is_error() {
        assert!(Frame::error("ERR").is_error());
        assert!(!Frame::simple("OK").is_error());
    }

    #[test]
    fn test_frame_as_str() {
        assert_eq!(Frame::simple("OK").as_str(), Some("OK"));
        assert_eq!(Frame::bulk("hello").as_str(), Some("hello"));
        assert_eq!(Frame::integer(42).as_str(), None);
        assert_eq!(Frame::null().as_str(), None);
    }

    #[test]
    fn test_frame_as_integer() {
        assert_eq!(Frame::integer(42).as_integer(), Some(42));
        assert_eq!(Frame::simple("OK").as_integer(), None);
    }

    #[test]
    fn test_frame_conversions() {
        let frame: Frame = "hello".into();
        assert_eq!(frame.as_str(), Some("hello"));

        let frame: Frame = 42i64.into();
        assert_eq!(frame.as_integer(), Some(42));

        let frame: Frame = Bytes::from("world").into();
        assert_eq!(frame.as_str(), Some("world"));
    }
}
