//! Trigger conditions and pattern matching
//!
//! Defines the event types and patterns that triggers can match against.

use serde::{Deserialize, Serialize};

use super::TriggerEvent;

/// Simple glob-style pattern matching
/// Supports:
/// - `*` matches any sequence of characters
/// - `?` matches any single character
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    fn match_helper(pattern: &[char], text: &[char]) -> bool {
        match (pattern.first(), text.first()) {
            (None, None) => true,
            (Some('*'), _) => {
                // Try matching rest of pattern with current text position
                // or skip one char in text and try again
                match_helper(&pattern[1..], text)
                    || (!text.is_empty() && match_helper(pattern, &text[1..]))
            }
            (Some('?'), Some(_)) => match_helper(&pattern[1..], &text[1..]),
            (Some(p), Some(t)) if *p == *t => match_helper(&pattern[1..], &text[1..]),
            (Some(_), _) => false,
            (None, Some(_)) => false,
        }
    }

    match_helper(&pattern_chars, &text_chars)
}

/// Event types that can trigger actions
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Key was set (SET, MSET, etc.)
    Set,
    /// Key was deleted (DEL, UNLINK, etc.)
    Delete,
    /// Key expired (TTL reached)
    Expire,
    /// Key was renamed (RENAME, RENAMENX)
    Rename,
    /// List operation (LPUSH, RPUSH, LPOP, RPOP, etc.)
    List,
    /// Hash operation (HSET, HDEL, etc.)
    Hash,
    /// Set operation (SADD, SREM, etc.)
    SetOp,
    /// Sorted set operation (ZADD, ZREM, etc.)
    ZSet,
    /// Stream operation (XADD, etc.)
    Stream,
    /// Any write operation
    Write,
    /// Any read operation
    Read,
    /// Any operation
    Any,
}

impl EventType {
    /// Parse event type from string
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SET" => Some(EventType::Set),
            "DEL" | "DELETE" => Some(EventType::Delete),
            "EXPIRE" | "EXPIRED" => Some(EventType::Expire),
            "RENAME" => Some(EventType::Rename),
            "LIST" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" => Some(EventType::List),
            "HASH" | "HSET" | "HDEL" => Some(EventType::Hash),
            "SETOP" | "SADD" | "SREM" => Some(EventType::SetOp),
            "ZSET" | "ZADD" | "ZREM" => Some(EventType::ZSet),
            "STREAM" | "XADD" => Some(EventType::Stream),
            "WRITE" => Some(EventType::Write),
            "READ" => Some(EventType::Read),
            "ANY" | "*" => Some(EventType::Any),
            _ => None,
        }
    }

    /// Check if this event type matches another
    pub fn matches(&self, other: &EventType) -> bool {
        if *self == EventType::Any || *other == EventType::Any {
            return true;
        }

        if *self == EventType::Write {
            return matches!(
                other,
                EventType::Set
                    | EventType::Delete
                    | EventType::Expire
                    | EventType::Rename
                    | EventType::List
                    | EventType::Hash
                    | EventType::SetOp
                    | EventType::ZSet
                    | EventType::Stream
            );
        }

        *self == *other
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Set => write!(f, "SET"),
            EventType::Delete => write!(f, "DELETE"),
            EventType::Expire => write!(f, "EXPIRE"),
            EventType::Rename => write!(f, "RENAME"),
            EventType::List => write!(f, "LIST"),
            EventType::Hash => write!(f, "HASH"),
            EventType::SetOp => write!(f, "SET"),
            EventType::ZSet => write!(f, "ZSET"),
            EventType::Stream => write!(f, "STREAM"),
            EventType::Write => write!(f, "WRITE"),
            EventType::Read => write!(f, "READ"),
            EventType::Any => write!(f, "ANY"),
        }
    }
}

/// Pattern for matching keys
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Pattern {
    /// Exact key match
    Exact(String),
    /// Glob pattern (e.g., "users:*")
    Glob(String),
    /// Regex pattern
    Regex(String),
    /// Prefix match
    Prefix(String),
    /// Suffix match
    Suffix(String),
    /// Any key
    Any,
}

impl Pattern {
    /// Check if a key matches this pattern
    pub fn matches(&self, key: &str) -> bool {
        match self {
            Pattern::Exact(s) => key == s,
            Pattern::Glob(pattern) => glob_match(pattern, key),
            Pattern::Regex(pattern) => {
                // Simple regex-like matching for basic patterns
                // Full regex support would require the regex crate
                glob_match(pattern, key)
            }
            Pattern::Prefix(prefix) => key.starts_with(prefix),
            Pattern::Suffix(suffix) => key.ends_with(suffix),
            Pattern::Any => true,
        }
    }

    /// Parse pattern from string
    pub fn parse_str(s: &str) -> Self {
        if s == "*" {
            Pattern::Any
        } else if s.contains('*') || s.contains('?') || s.contains('[') {
            Pattern::Glob(s.to_string())
        } else if s.starts_with('^') || s.ends_with('$') {
            Pattern::Regex(s.to_string())
        } else if s.ends_with(':') {
            Pattern::Prefix(s.to_string())
        } else {
            Pattern::Exact(s.to_string())
        }
    }
}

impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pattern::Exact(s) => write!(f, "{}", s),
            Pattern::Glob(s) => write!(f, "{}", s),
            Pattern::Regex(s) => write!(f, "/{}/", s),
            Pattern::Prefix(s) => write!(f, "{}*", s),
            Pattern::Suffix(s) => write!(f, "*{}", s),
            Pattern::Any => write!(f, "*"),
        }
    }
}

/// Condition for triggering actions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Condition {
    /// Event type to match
    pub event_type: EventType,
    /// Key pattern to match
    pub pattern: Pattern,
    /// Optional filter expression
    pub filter: Option<Filter>,
}

impl Condition {
    /// Create a new condition
    pub fn new(event_type: EventType, pattern: Pattern) -> Self {
        Self {
            event_type,
            pattern,
            filter: None,
        }
    }

    /// Create a condition with a filter
    pub fn with_filter(event_type: EventType, pattern: Pattern, filter: Filter) -> Self {
        Self {
            event_type,
            pattern,
            filter: Some(filter),
        }
    }

    /// Check if an event matches this condition
    pub fn matches(&self, event: &TriggerEvent) -> bool {
        // Check event type
        if !self.event_type.matches(&event.event_type) {
            return false;
        }

        // Check key pattern
        if !self.pattern.matches(&event.key) {
            return false;
        }

        // Check filter if present
        if let Some(filter) = &self.filter {
            if !filter.matches(event) {
                return false;
            }
        }

        true
    }
}

/// Filter for additional condition matching
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Filter {
    /// Value matches a pattern
    ValueMatches(String),
    /// Value is of a certain type
    ValueType(String),
    /// TTL is set
    HasTtl,
    /// TTL is within range
    TtlRange {
        /// Minimum TTL value (inclusive)
        min: Option<i64>,
        /// Maximum TTL value (inclusive)
        max: Option<i64>,
    },
    /// Combined filters (AND)
    And(Vec<Filter>),
    /// Combined filters (OR)
    Or(Vec<Filter>),
    /// Negation
    Not(Box<Filter>),
}

impl Filter {
    /// Check if an event matches this filter
    pub fn matches(&self, event: &TriggerEvent) -> bool {
        match self {
            Filter::ValueMatches(pattern) => {
                if let Some(value) = &event.value {
                    let value_str = String::from_utf8_lossy(value);
                    glob_match(pattern, &value_str)
                } else {
                    false
                }
            }
            Filter::ValueType(t) => event.value_type.as_deref() == Some(t.as_str()),
            Filter::HasTtl => event.ttl.map(|ttl| ttl > 0).unwrap_or(false),
            Filter::TtlRange { min, max } => {
                if let Some(ttl) = event.ttl {
                    let min_ok = min.map(|m| ttl >= m).unwrap_or(true);
                    let max_ok = max.map(|m| ttl <= m).unwrap_or(true);
                    min_ok && max_ok
                } else {
                    false
                }
            }
            Filter::And(filters) => filters.iter().all(|f| f.matches(event)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(event)),
            Filter::Not(filter) => !filter.matches(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_parsing() {
        assert_eq!(EventType::parse_str("SET"), Some(EventType::Set));
        assert_eq!(EventType::parse_str("delete"), Some(EventType::Delete));
        assert_eq!(EventType::parse_str("*"), Some(EventType::Any));
        assert_eq!(EventType::parse_str("invalid"), None);
    }

    #[test]
    fn test_event_type_matching() {
        assert!(EventType::Any.matches(&EventType::Set));
        assert!(EventType::Set.matches(&EventType::Any));
        assert!(EventType::Write.matches(&EventType::Set));
        assert!(EventType::Write.matches(&EventType::Delete));
        assert!(!EventType::Set.matches(&EventType::Delete));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(Pattern::Any.matches("anything"));
        assert!(Pattern::Exact("foo".to_string()).matches("foo"));
        assert!(!Pattern::Exact("foo".to_string()).matches("bar"));

        assert!(Pattern::Glob("users:*".to_string()).matches("users:123"));
        assert!(!Pattern::Glob("users:*".to_string()).matches("orders:123"));

        assert!(Pattern::Prefix("cache:".to_string()).matches("cache:key"));
        assert!(!Pattern::Prefix("cache:".to_string()).matches("other:key"));

        assert!(Pattern::Suffix(":active".to_string()).matches("user:active"));
        assert!(!Pattern::Suffix(":active".to_string()).matches("user:inactive"));
    }

    #[test]
    fn test_condition_matching() {
        let condition = Condition::new(EventType::Set, Pattern::Glob("orders:*".to_string()));

        let matching_event = TriggerEvent {
            event_type: EventType::Set,
            key: "orders:123".to_string(),
            value: None,
            old_value: None,
            value_type: None,
            ttl: None,
            timestamp: 0,
        };

        let non_matching_key = TriggerEvent {
            event_type: EventType::Set,
            key: "users:123".to_string(),
            value: None,
            old_value: None,
            value_type: None,
            ttl: None,
            timestamp: 0,
        };

        let non_matching_event = TriggerEvent {
            event_type: EventType::Delete,
            key: "orders:123".to_string(),
            value: None,
            old_value: None,
            value_type: None,
            ttl: None,
            timestamp: 0,
        };

        assert!(condition.matches(&matching_event));
        assert!(!condition.matches(&non_matching_key));
        assert!(!condition.matches(&non_matching_event));
    }

    #[test]
    fn test_filter_matching() {
        let event = TriggerEvent {
            event_type: EventType::Set,
            key: "test".to_string(),
            value: Some(b"hello world".to_vec()),
            old_value: None,
            value_type: Some("string".to_string()),
            ttl: Some(3600),
            timestamp: 0,
        };

        assert!(Filter::ValueMatches("hello*".to_string()).matches(&event));
        assert!(!Filter::ValueMatches("goodbye*".to_string()).matches(&event));

        assert!(Filter::ValueType("string".to_string()).matches(&event));
        assert!(!Filter::ValueType("list".to_string()).matches(&event));

        assert!(Filter::HasTtl.matches(&event));
        assert!(Filter::TtlRange {
            min: Some(1000),
            max: Some(5000)
        }
        .matches(&event));

        let combined = Filter::And(vec![
            Filter::ValueType("string".to_string()),
            Filter::HasTtl,
        ]);
        assert!(combined.matches(&event));
    }
}
