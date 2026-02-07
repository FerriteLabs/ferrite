//! RESP protocol encoder
//!
//! This module implements encoding of RESP frames to bytes.

use bytes::{BufMut, BytesMut};

use super::Frame;

/// Encode a frame into the buffer
pub fn encode_frame(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        // RESP2 types
        Frame::Simple(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            buf.put_slice(n.to_string().as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::Bulk(None) => {
            buf.put_slice(b"$-1\r\n");
        }
        Frame::Bulk(Some(data)) => {
            buf.put_u8(b'$');
            buf.put_slice(data.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Array(None) => {
            buf.put_slice(b"*-1\r\n");
        }
        Frame::Array(Some(frames)) => {
            buf.put_u8(b'*');
            buf.put_slice(frames.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for frame in frames {
                encode_frame(frame, buf);
            }
        }
        // RESP3 types
        Frame::Null => {
            buf.put_slice(b"_\r\n");
        }
        Frame::Boolean(b) => {
            buf.put_u8(b'#');
            buf.put_u8(if *b { b't' } else { b'f' });
            buf.put_slice(b"\r\n");
        }
        Frame::Double(d) => {
            buf.put_u8(b',');
            if d.is_infinite() {
                if *d > 0.0 {
                    buf.put_slice(b"inf");
                } else {
                    buf.put_slice(b"-inf");
                }
            } else if d.is_nan() {
                buf.put_slice(b"nan");
            } else {
                buf.put_slice(d.to_string().as_bytes());
            }
            buf.put_slice(b"\r\n");
        }
        Frame::BigNumber(s) => {
            buf.put_u8(b'(');
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::BulkError(s) => {
            buf.put_u8(b'!');
            buf.put_slice(s.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::VerbatimString { encoding, data } => {
            buf.put_u8(b'=');
            // Format: encoding:data
            let total_len = 3 + 1 + data.len(); // encoding (3) + colon (1) + data
            buf.put_slice(total_len.to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(encoding.as_bytes());
            buf.put_u8(b':');
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Map(map) => {
            buf.put_u8(b'%');
            buf.put_slice(map.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for (key, value) in map {
                // Encode key as bulk string
                encode_frame(&Frame::Bulk(Some(key.clone())), buf);
                encode_frame(value, buf);
            }
        }
        Frame::Set(elements) => {
            buf.put_u8(b'~');
            buf.put_slice(elements.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for element in elements {
                encode_frame(element, buf);
            }
        }
        Frame::Push(elements) => {
            buf.put_u8(b'>');
            buf.put_slice(elements.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for element in elements {
                encode_frame(element, buf);
            }
        }
    }
}

/// Convenience function to encode a frame to a new BytesMut
// Convenience helper reserved for protocol encoding API
#[allow(dead_code)]
pub fn encode_to_bytes(frame: &Frame) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_frame(frame, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_encode_simple_string() {
        let frame = Frame::Simple(Bytes::from("OK"));
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let frame = Frame::Error(Bytes::from("ERR unknown command"));
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let frame = Frame::Integer(1000);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b":1000\r\n");

        let frame = Frame::Integer(-500);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b":-500\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let frame = Frame::bulk("hello");
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let frame = Frame::null();
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_empty_bulk_string() {
        let frame = Frame::bulk("");
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"$0\r\n\r\n");
    }

    #[test]
    fn test_encode_array() {
        let frame = Frame::array(vec![Frame::bulk("foo"), Frame::bulk("bar")]);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_encode_null_array() {
        let frame = Frame::null_array();
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"*-1\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        let frame = Frame::array(vec![]);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"*0\r\n");
    }

    #[test]
    fn test_encode_nested_array() {
        let frame = Frame::array(vec![
            Frame::array(vec![
                Frame::Simple(Bytes::from("a")),
                Frame::Simple(Bytes::from("b")),
            ]),
            Frame::array(vec![Frame::Integer(42)]),
        ]);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"*2\r\n*2\r\n+a\r\n+b\r\n*1\r\n:42\r\n");
    }

    #[test]
    fn test_encode_mixed_array() {
        let frame = Frame::array(vec![
            Frame::Simple(Bytes::from("OK")),
            Frame::Integer(42),
            Frame::bulk("hello"),
            Frame::null(),
        ]);
        let encoded = encode_to_bytes(&frame);
        assert_eq!(&encoded[..], b"*4\r\n+OK\r\n:42\r\n$5\r\nhello\r\n$-1\r\n");
    }
}
