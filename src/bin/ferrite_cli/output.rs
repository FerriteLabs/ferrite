//! Output formatting for ferrite-cli

use colored::Colorize;

use crate::client::Frame;
use crate::OutputFormat;

/// Trait for formatting RESP frames
pub trait OutputFormatter {
    fn format(&self, frame: &Frame) -> String;
}

/// Get the appropriate formatter for the output format
pub fn get_formatter(format: OutputFormat) -> Box<dyn OutputFormatter> {
    match format {
        OutputFormat::Raw => Box::new(RawFormatter),
        OutputFormat::Pretty => Box::new(PrettyFormatter),
        OutputFormat::Json => Box::new(JsonFormatter),
    }
}

/// Raw output formatter (default, similar to redis-cli)
pub struct RawFormatter;

impl OutputFormatter for RawFormatter {
    fn format(&self, frame: &Frame) -> String {
        format_frame_raw(frame, 0)
    }
}

fn format_frame_raw(frame: &Frame, _indent: usize) -> String {
    match frame {
        Frame::Simple(s) => s.clone(),
        Frame::Error(e) => format!("(error) {}", e),
        Frame::Integer(n) => format!("(integer) {}", n),
        Frame::Bulk(Some(data)) => {
            match String::from_utf8(data.clone()) {
                Ok(s) => format!("\"{}\"", s),
                Err(_) => format!("{:?}", data), // Show bytes for binary data
            }
        }
        Frame::Bulk(None) | Frame::Null => "(nil)".to_string(),
        Frame::Array(Some(items)) => {
            if items.is_empty() {
                "(empty array)".to_string()
            } else {
                let mut result = String::new();
                for (i, item) in items.iter().enumerate() {
                    result.push_str(&format!("{}) {}\n", i + 1, format_frame_raw(item, 0)));
                }
                result.trim_end().to_string()
            }
        }
        Frame::Array(None) => "(empty array)".to_string(),
    }
}

/// Pretty-printed output with colors
pub struct PrettyFormatter;

impl OutputFormatter for PrettyFormatter {
    fn format(&self, frame: &Frame) -> String {
        format_frame_pretty(frame, 0)
    }
}

fn format_frame_pretty(frame: &Frame, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    match frame {
        Frame::Simple(s) => format!("{}{}", prefix, s.green()),
        Frame::Error(e) => format!("{}{}", prefix, e.red()),
        Frame::Integer(n) => format!("{}{}", prefix, n.to_string().cyan()),
        Frame::Bulk(Some(data)) => match String::from_utf8(data.clone()) {
            Ok(s) => format!("{}{}", prefix, s.yellow()),
            Err(_) => format!("{}{:?}", prefix, data),
        },
        Frame::Bulk(None) | Frame::Null => format!("{}{}", prefix, "nil".dimmed()),
        Frame::Array(Some(items)) => {
            if items.is_empty() {
                format!("{}{}", prefix, "(empty array)".dimmed())
            } else {
                let mut result = String::new();
                for (i, item) in items.iter().enumerate() {
                    let idx = format!("{})", i + 1).blue();
                    result.push_str(&format!(
                        "{}{} {}\n",
                        prefix,
                        idx,
                        format_frame_pretty(item, 0).trim_start()
                    ));
                }
                result.trim_end().to_string()
            }
        }
        Frame::Array(None) => format!("{}{}", prefix, "(empty array)".dimmed()),
    }
}

/// JSON output formatter
pub struct JsonFormatter;

impl OutputFormatter for JsonFormatter {
    fn format(&self, frame: &Frame) -> String {
        serde_json::to_string_pretty(&frame_to_json(frame)).unwrap_or_else(|_| "null".to_string())
    }
}

fn frame_to_json(frame: &Frame) -> serde_json::Value {
    match frame {
        Frame::Simple(s) => serde_json::Value::String(s.clone()),
        Frame::Error(e) => serde_json::json!({ "error": e }),
        Frame::Integer(n) => serde_json::Value::Number((*n).into()),
        Frame::Bulk(Some(data)) => {
            match String::from_utf8(data.clone()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => {
                    // Encode binary data as base64 or array of bytes
                    serde_json::json!({ "binary": data })
                }
            }
        }
        Frame::Bulk(None) | Frame::Null => serde_json::Value::Null,
        Frame::Array(Some(items)) => {
            serde_json::Value::Array(items.iter().map(frame_to_json).collect())
        }
        Frame::Array(None) => serde_json::Value::Array(vec![]),
    }
}
