//! Sample and Timestamp types for time-series data
//!
//! Provides the fundamental data types for time-series measurements.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A timestamp in nanoseconds since Unix epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Create a timestamp from nanoseconds since Unix epoch
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Create a timestamp from milliseconds since Unix epoch
    pub fn from_millis(millis: i64) -> Self {
        Self(millis * 1_000_000)
    }

    /// Create a timestamp from seconds since Unix epoch
    pub fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000_000)
    }

    /// Get current timestamp
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self::from_nanos(duration.as_nanos() as i64)
    }

    /// Get timestamp as nanoseconds
    pub fn as_nanos(&self) -> i64 {
        self.0
    }

    /// Get timestamp as milliseconds
    pub fn as_millis(&self) -> i64 {
        self.0 / 1_000_000
    }

    /// Get timestamp as seconds
    pub fn as_secs(&self) -> i64 {
        self.0 / 1_000_000_000
    }

    /// Get timestamp as Duration
    pub fn as_duration(&self) -> Duration {
        Duration::from_nanos(self.0 as u64)
    }

    /// Check if timestamp is before another
    pub fn is_before(&self, other: &Timestamp) -> bool {
        self.0 < other.0
    }

    /// Check if timestamp is after another
    pub fn is_after(&self, other: &Timestamp) -> bool {
        self.0 > other.0
    }

    /// Add duration to timestamp
    pub fn add(&self, duration: Duration) -> Self {
        Self(self.0 + duration.as_nanos() as i64)
    }

    /// Subtract duration from timestamp
    pub fn sub(&self, duration: Duration) -> Self {
        Self(self.0 - duration.as_nanos() as i64)
    }

    /// Get difference between timestamps
    pub fn diff(&self, other: &Timestamp) -> Duration {
        let diff = (self.0 - other.0).abs();
        Duration::from_nanos(diff as u64)
    }

    /// Truncate to interval boundary
    pub fn truncate(&self, interval: Duration) -> Self {
        let interval_nanos = interval.as_nanos() as i64;
        Self((self.0 / interval_nanos) * interval_nanos)
    }

    /// Format as ISO 8601 string
    pub fn to_iso8601(&self) -> String {
        let secs = self.as_secs();
        let nanos = (self.0 % 1_000_000_000) as u32;
        let datetime = UNIX_EPOCH + Duration::new(secs as u64, nanos);

        // Simple ISO 8601 formatting
        let duration = datetime.duration_since(UNIX_EPOCH).unwrap_or_default();
        let total_secs = duration.as_secs();
        let days = total_secs / 86400;
        let remaining = total_secs % 86400;
        let hours = remaining / 3600;
        let minutes = (remaining % 3600) / 60;
        let seconds = remaining % 60;

        // Approximate date calculation (not accounting for leap years perfectly)
        let years = 1970 + (days / 365);
        let day_of_year = days % 365;
        let month = (day_of_year / 30) + 1;
        let day = (day_of_year % 30) + 1;

        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
            years, month, day, hours, minutes, seconds, nanos
        )
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

impl From<i64> for Timestamp {
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}

impl From<Timestamp> for i64 {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A numeric value in a time series
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum Value {
    /// 64-bit floating point value
    Float(f64),
    /// 64-bit signed integer value
    Integer(i64),
    /// Unsigned 64-bit integer
    Unsigned(u64),
    /// Boolean value
    Boolean(bool),
    /// Null/missing value
    #[default]
    Null,
}

impl Value {
    /// Create a float value
    pub fn float(v: f64) -> Self {
        Self::Float(v)
    }

    /// Create an integer value
    pub fn integer(v: i64) -> Self {
        Self::Integer(v)
    }

    /// Create an unsigned value
    pub fn unsigned(v: u64) -> Self {
        Self::Unsigned(v)
    }

    /// Create a boolean value
    pub fn boolean(v: bool) -> Self {
        Self::Boolean(v)
    }

    /// Get value as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            Value::Integer(v) => Some(*v as f64),
            Value::Unsigned(v) => Some(*v as f64),
            Value::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::Null => None,
        }
    }

    /// Get value as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Float(v) => Some(*v as i64),
            Value::Integer(v) => Some(*v),
            Value::Unsigned(v) => Some(*v as i64),
            Value::Boolean(v) => Some(if *v { 1 } else { 0 }),
            Value::Null => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if value is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Value::Float(_) | Value::Integer(_) | Value::Unsigned(_)
        )
    }

    /// Add two values
    pub fn add(&self, other: &Value) -> Value {
        match (self.as_f64(), other.as_f64()) {
            (Some(a), Some(b)) => Value::Float(a + b),
            _ => Value::Null,
        }
    }

    /// Subtract two values
    pub fn sub(&self, other: &Value) -> Value {
        match (self.as_f64(), other.as_f64()) {
            (Some(a), Some(b)) => Value::Float(a - b),
            _ => Value::Null,
        }
    }

    /// Multiply two values
    pub fn mul(&self, other: &Value) -> Value {
        match (self.as_f64(), other.as_f64()) {
            (Some(a), Some(b)) => Value::Float(a * b),
            _ => Value::Null,
        }
    }

    /// Divide two values
    pub fn div(&self, other: &Value) -> Value {
        match (self.as_f64(), other.as_f64()) {
            (Some(a), Some(b)) if b != 0.0 => Value::Float(a / b),
            _ => Value::Null,
        }
    }
}



impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Unsigned(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Float(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Unsigned(v) => write!(f, "{}", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "null"),
        }
    }
}

/// A single data point in a time series
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Timestamp
    pub timestamp: Timestamp,
    /// Value
    pub value: Value,
}

impl Sample {
    /// Create a new sample
    pub fn new(timestamp: Timestamp, value: impl Into<Value>) -> Self {
        Self {
            timestamp,
            value: value.into(),
        }
    }

    /// Create a sample with current timestamp
    pub fn now(value: impl Into<Value>) -> Self {
        Self {
            timestamp: Timestamp::now(),
            value: value.into(),
        }
    }

    /// Create a sample from milliseconds timestamp
    pub fn from_millis(millis: i64, value: impl Into<Value>) -> Self {
        Self {
            timestamp: Timestamp::from_millis(millis),
            value: value.into(),
        }
    }

    /// Create a sample from seconds timestamp
    pub fn from_secs(secs: i64, value: impl Into<Value>) -> Self {
        Self {
            timestamp: Timestamp::from_secs(secs),
            value: value.into(),
        }
    }

    /// Get the value as f64
    pub fn as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    /// Check if this sample is before another
    pub fn is_before(&self, other: &Sample) -> bool {
        self.timestamp.is_before(&other.timestamp)
    }

    /// Check if this sample is after another
    pub fn is_after(&self, other: &Sample) -> bool {
        self.timestamp.is_after(&other.timestamp)
    }
}

impl Default for Sample {
    fn default() -> Self {
        Self::now(Value::Null)
    }
}

/// A batch of samples for efficient ingestion
#[derive(Debug, Clone)]
pub struct SampleBatch {
    /// Metric name
    pub metric: String,
    /// Samples
    pub samples: Vec<Sample>,
}

impl SampleBatch {
    /// Create a new sample batch
    pub fn new(metric: impl Into<String>) -> Self {
        Self {
            metric: metric.into(),
            samples: Vec::new(),
        }
    }

    /// Create a batch with capacity
    pub fn with_capacity(metric: impl Into<String>, capacity: usize) -> Self {
        Self {
            metric: metric.into(),
            samples: Vec::with_capacity(capacity),
        }
    }

    /// Add a sample to the batch
    pub fn add(&mut self, sample: Sample) {
        self.samples.push(sample);
    }

    /// Add a value with current timestamp
    pub fn add_now(&mut self, value: impl Into<Value>) {
        self.samples.push(Sample::now(value));
    }

    /// Get the number of samples
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Clear the batch
    pub fn clear(&mut self) {
        self.samples.clear();
    }

    /// Sort samples by timestamp
    pub fn sort(&mut self) {
        self.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::from_secs(1000);
        assert_eq!(ts.as_secs(), 1000);
        assert_eq!(ts.as_millis(), 1_000_000);
        assert_eq!(ts.as_nanos(), 1_000_000_000_000);
    }

    #[test]
    fn test_timestamp_now() {
        let ts1 = Timestamp::now();
        std::thread::sleep(Duration::from_millis(1));
        let ts2 = Timestamp::now();
        assert!(ts2.is_after(&ts1));
    }

    #[test]
    fn test_timestamp_arithmetic() {
        let ts = Timestamp::from_secs(1000);
        let ts2 = ts.add(Duration::from_secs(100));
        assert_eq!(ts2.as_secs(), 1100);

        let ts3 = ts2.sub(Duration::from_secs(50));
        assert_eq!(ts3.as_secs(), 1050);
    }

    #[test]
    fn test_timestamp_truncate() {
        let ts = Timestamp::from_secs(125);
        let truncated = ts.truncate(Duration::from_secs(60));
        assert_eq!(truncated.as_secs(), 120);
    }

    #[test]
    fn test_value_types() {
        let float = Value::Float(3.14);
        let int = Value::Integer(42);
        let uint = Value::Unsigned(100);
        let boolean = Value::Boolean(true);

        assert_eq!(float.as_f64(), Some(3.14));
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(uint.as_f64(), Some(100.0));
        assert_eq!(boolean.as_f64(), Some(1.0));
    }

    #[test]
    fn test_value_arithmetic() {
        let a = Value::Float(10.0);
        let b = Value::Float(3.0);

        assert_eq!(a.add(&b).as_f64(), Some(13.0));
        assert_eq!(a.sub(&b).as_f64(), Some(7.0));
        assert_eq!(a.mul(&b).as_f64(), Some(30.0));
        assert!(matches!(a.div(&b), Value::Float(v) if (v - 3.333).abs() < 0.01));
    }

    #[test]
    fn test_sample_creation() {
        let sample = Sample::now(42.0);
        assert_eq!(sample.as_f64(), Some(42.0));

        let sample2 = Sample::from_secs(1000, 100.0);
        assert_eq!(sample2.timestamp.as_secs(), 1000);
        assert_eq!(sample2.as_f64(), Some(100.0));
    }

    #[test]
    fn test_sample_batch() {
        let mut batch = SampleBatch::new("cpu.usage");

        batch.add_now(50.0);
        batch.add_now(60.0);
        batch.add_now(70.0);

        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_sample_batch_sort() {
        let mut batch = SampleBatch::new("test");

        batch.add(Sample::from_secs(3, 30.0));
        batch.add(Sample::from_secs(1, 10.0));
        batch.add(Sample::from_secs(2, 20.0));

        batch.sort();

        assert_eq!(batch.samples[0].timestamp.as_secs(), 1);
        assert_eq!(batch.samples[1].timestamp.as_secs(), 2);
        assert_eq!(batch.samples[2].timestamp.as_secs(), 3);
    }
}
