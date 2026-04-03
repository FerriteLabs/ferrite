//! Frame assertion and extraction helpers for integration tests.

use bytes::Bytes;
use ferrite::protocol::Frame;

/// Assert that `frame` is a Simple string "OK".
#[allow(dead_code)]
pub fn assert_ok(frame: &Frame) {
    match frame {
        Frame::Simple(s) if s == &Bytes::from("OK") => {}
        other => panic!("expected +OK, got: {other:?}"),
    }
}

/// Assert that `frame` is an Error.
#[allow(dead_code)]
pub fn assert_error(frame: &Frame) {
    assert!(
        matches!(frame, Frame::Error(_)),
        "expected an error frame, got: {frame:?}"
    );
}

/// Assert that `frame` is a Bulk string equal to `expected`.
#[allow(dead_code)]
pub fn assert_bulk_eq(frame: &Frame, expected: &str) {
    match frame {
        Frame::Bulk(Some(data)) => {
            let actual = std::str::from_utf8(data).expect("bulk data should be valid UTF-8");
            assert_eq!(actual, expected, "bulk string mismatch");
        }
        other => panic!("expected Bulk(\"{expected}\"), got: {other:?}"),
    }
}

/// Assert that `frame` is an Integer equal to `expected`.
#[allow(dead_code)]
pub fn assert_integer_eq(frame: &Frame, expected: i64) {
    match frame {
        Frame::Integer(n) => assert_eq!(*n, expected, "integer mismatch"),
        other => panic!("expected Integer({expected}), got: {other:?}"),
    }
}

/// Extract an `i64` from an Integer frame.
#[allow(dead_code)]
pub fn extract_integer(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::Integer(n) => Some(*n),
        _ => None,
    }
}

/// Extract a `String` from a Bulk frame.
#[allow(dead_code)]
pub fn extract_bulk(frame: &Frame) -> Option<String> {
    match frame {
        Frame::Bulk(Some(data)) => Some(String::from_utf8_lossy(data).into_owned()),
        _ => None,
    }
}

/// Assert that `frame` is a non-null Array and return its elements.
#[allow(dead_code)]
pub fn assert_array(frame: &Frame) -> &Vec<Frame> {
    match frame {
        Frame::Array(Some(items)) => items,
        other => panic!("expected Array, got: {other:?}"),
    }
}
