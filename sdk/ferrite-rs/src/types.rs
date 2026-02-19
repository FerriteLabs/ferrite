//! Value types for Ferrite client responses.

use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;

use crate::error::{Error, Result};

/// Represents a value returned from the Ferrite server.
///
/// This maps to the RESP protocol types and provides convenient
/// conversion methods for common Rust types.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A string value (from RESP Simple String or Bulk String).
    String(Bytes),

    /// An integer value (from RESP Integer).
    Integer(i64),

    /// An array of values (from RESP Array).
    Array(Vec<Value>),

    /// A null value (from RESP Null Bulk String or Null Array).
    Nil,

    /// A boolean value (RESP3).
    Boolean(bool),

    /// A double/float value (RESP3).
    Double(f64),

    /// A map of key-value pairs (RESP3).
    Map(HashMap<String, Value>),

    /// A status/OK response (from RESP Simple String like "+OK").
    Status(String),
}

impl Value {
    /// Returns the value as a string, if it is one.
    ///
    /// # Examples
    /// ```
    /// # use ferrite_rs::types::Value;
    /// let val = Value::String(bytes::Bytes::from("hello"));
    /// assert_eq!(val.as_str(), Some("hello"));
    /// ```
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(b) => std::str::from_utf8(b).ok(),
            Value::Status(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Returns the value as bytes, if it is a string.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::String(b) => Some(b),
            _ => None,
        }
    }

    /// Returns the value as an integer, if it is one.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the value as a boolean, if it is one.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Integer(n) => Some(*n != 0),
            _ => None,
        }
    }

    /// Returns the value as an array, if it is one.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Returns `true` if the value is nil/null.
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    /// Converts the value into a `String`.
    ///
    /// Returns an error if the value is not a string type.
    pub fn into_string(self) -> Result<String> {
        match self {
            Value::String(b) => String::from_utf8(b.to_vec()).map_err(|e| {
                Error::Protocol(format!("invalid UTF-8 in string value: {}", e))
            }),
            Value::Status(s) => Ok(s),
            other => Err(Error::UnexpectedResponse {
                expected: "string",
                actual: other.type_name().to_string(),
            }),
        }
    }

    /// Converts the value into an `i64`.
    ///
    /// Returns an error if the value is not an integer.
    pub fn into_integer(self) -> Result<i64> {
        match self {
            Value::Integer(n) => Ok(n),
            other => Err(Error::UnexpectedResponse {
                expected: "integer",
                actual: other.type_name().to_string(),
            }),
        }
    }

    /// Converts the value into a `Vec<Value>`.
    ///
    /// Returns an error if the value is not an array.
    pub fn into_array(self) -> Result<Vec<Value>> {
        match self {
            Value::Array(arr) => Ok(arr),
            Value::Nil => Ok(vec![]),
            other => Err(Error::UnexpectedResponse {
                expected: "array",
                actual: other.type_name().to_string(),
            }),
        }
    }

    /// Returns a human-readable type name for this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Integer(_) => "integer",
            Value::Array(_) => "array",
            Value::Nil => "nil",
            Value::Boolean(_) => "boolean",
            Value::Double(_) => "double",
            Value::Map(_) => "map",
            Value::Status(_) => "status",
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "\"{}\"", s),
                Err(_) => write!(f, "<binary {} bytes>", b.len()),
            },
            Value::Integer(n) => write!(f, "(integer) {}", n),
            Value::Array(arr) => {
                for (i, val) in arr.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{}) {}", i + 1, val)?;
                }
                Ok(())
            }
            Value::Nil => write!(f, "(nil)"),
            Value::Boolean(b) => write!(f, "(boolean) {}", b),
            Value::Double(d) => write!(f, "(double) {}", d),
            Value::Map(m) => {
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{}) \"{}\" -> {}", i + 1, k, v)?;
                }
                Ok(())
            }
            Value::Status(s) => write!(f, "{}", s),
        }
    }
}

/// Trait for types that can be converted into command arguments.
pub trait ToArg {
    /// Encode this value as a RESP bulk string argument.
    fn to_arg(&self) -> Bytes;
}

impl ToArg for &str {
    fn to_arg(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl ToArg for String {
    fn to_arg(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl ToArg for &String {
    fn to_arg(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl ToArg for Bytes {
    fn to_arg(&self) -> Bytes {
        self.clone()
    }
}

impl ToArg for &[u8] {
    fn to_arg(&self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl ToArg for Vec<u8> {
    fn to_arg(&self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl ToArg for i64 {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl ToArg for u64 {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl ToArg for i32 {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl ToArg for u32 {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl ToArg for usize {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

impl ToArg for f64 {
    fn to_arg(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}
