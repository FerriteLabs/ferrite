//! Per-category command parsers.
//!
//! Each sub-module contains the `parse_*` free functions for one command
//! category. The shared frame-extraction utilities (`get_string`, `get_bytes`, …)
//! live here so every sub-module can import them without circular dependencies.

use bytes::Bytes;

use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) mod strings;
pub(crate) mod lists;
pub(crate) mod hashes;
pub(crate) mod sets;
pub(crate) mod sorted_sets;
pub(crate) mod pubsub;
pub(crate) mod keys;
pub(crate) mod server;
pub(crate) mod streams;
pub(crate) mod geo;
pub(crate) mod hyperloglog;
pub(crate) mod scan;
pub(crate) mod bitmap;
pub(crate) mod scripting_parsers;
pub(crate) mod cluster;
pub(crate) mod transactions;
pub(crate) mod connection;
pub(crate) mod advanced;

/// Extract a string/bytes from a frame
pub(crate) fn get_string(frame: &Frame) -> Result<String> {
    match frame {
        Frame::Simple(b) | Frame::Bulk(Some(b)) => String::from_utf8(b.to_vec())
            .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string())),
        _ => Err(FerriteError::Protocol("expected string".to_string())),
    }
}

/// Extract bytes from a frame
pub(crate) fn get_bytes(frame: &Frame) -> Result<Bytes> {
    match frame {
        Frame::Simple(b) | Frame::Bulk(Some(b)) => Ok(b.clone()),
        _ => Err(FerriteError::Protocol("expected bulk string".to_string())),
    }
}

/// Extract an integer from a frame
pub(crate) fn get_int(frame: &Frame) -> Result<i64> {
    match frame {
        Frame::Integer(n) => Ok(*n),
        Frame::Simple(b) | Frame::Bulk(Some(b)) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;
            s.parse().map_err(|_| FerriteError::NotInteger)
        }
        _ => Err(FerriteError::NotInteger),
    }
}

/// Extract a float from a frame
pub(crate) fn get_float(frame: &Frame) -> Result<f64> {
    match frame {
        Frame::Integer(n) => Ok(*n as f64),
        Frame::Simple(b) | Frame::Bulk(Some(b)) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;
            s.parse().map_err(|_| FerriteError::NotFloat)
        }
        _ => Err(FerriteError::NotFloat),
    }
}

