//! RESP protocol implementation
//!
//! This module implements the Redis Serialization Protocol (RESP) versions 2 and 3.

/// HELLO command, CLIENT commands, and client-side caching support.
pub mod client_negotiation;
mod encoder;
mod frame;
mod parser;

pub use encoder::encode_frame;
pub use frame::Frame;
pub use parser::{parse_frame, parse_frame_with_limits, ParseError, ParserLimits};
