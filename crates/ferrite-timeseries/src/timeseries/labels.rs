//! Labels for time series identification and filtering
//!
//! Labels are key-value pairs that identify and describe time series.

use super::TimeSeriesError;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

/// A single label (key-value pair)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Label {
    /// Label name/key
    pub name: String,
    /// Label value
    pub value: String,
}

impl Label {
    /// Create a new label
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Parse a label from "key:value" or "key=value" format
    pub fn parse(s: &str) -> Result<Self, TimeSeriesError> {
        if let Some((name, value)) = s.split_once(':') {
            Ok(Self::new(name.trim(), value.trim()))
        } else if let Some((name, value)) = s.split_once('=') {
            Ok(Self::new(name.trim(), value.trim()))
        } else {
            Err(TimeSeriesError::ConfigError(format!(
                "invalid label format: {}, expected 'key:value' or 'key=value'",
                s
            )))
        }
    }

    /// Check if label matches a pattern (supports wildcards)
    pub fn matches(&self, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return self.value.starts_with(prefix);
        }
        if let Some(suffix) = pattern.strip_prefix('*') {
            return self.value.ends_with(suffix);
        }
        self.value == pattern
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}=\"{}\"", self.name, self.value)
    }
}

/// A collection of labels
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Labels {
    /// Labels stored in sorted order by name
    labels: BTreeMap<String, String>,
}

impl Labels {
    /// Create empty labels
    pub fn empty() -> Self {
        Self {
            labels: BTreeMap::new(),
        }
    }

    /// Create labels from a vector of Label
    pub fn new(labels: Vec<Label>) -> Self {
        let mut map = BTreeMap::new();
        for label in labels {
            map.insert(label.name, label.value);
        }
        Self { labels: map }
    }

    /// Create labels from key-value pairs
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut map = BTreeMap::new();
        for (name, value) in pairs {
            map.insert(name.to_string(), value.to_string());
        }
        Self { labels: map }
    }

    /// Parse labels from string array ["key:value", ...]
    pub fn parse(labels: &[&str]) -> Result<Self, TimeSeriesError> {
        let mut map = BTreeMap::new();
        for s in labels {
            let label = Label::parse(s)?;
            map.insert(label.name, label.value);
        }
        Ok(Self { labels: map })
    }

    /// Parse labels from comma-separated string "key=value,key2=value2"
    pub fn parse_string(s: &str) -> Result<Self, TimeSeriesError> {
        if s.trim().is_empty() {
            return Ok(Self::empty());
        }

        let mut map = BTreeMap::new();
        for part in s.split(',') {
            let label = Label::parse(part.trim())?;
            map.insert(label.name, label.value);
        }
        Ok(Self { labels: map })
    }

    /// Add a label
    pub fn add(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.labels.insert(name.into(), value.into());
    }

    /// Get a label value
    pub fn get(&self, name: &str) -> Option<&str> {
        self.labels.get(name).map(|s| s.as_str())
    }

    /// Check if a label exists
    pub fn contains(&self, name: &str) -> bool {
        self.labels.contains_key(name)
    }

    /// Remove a label
    pub fn remove(&mut self, name: &str) -> Option<String> {
        self.labels.remove(name)
    }

    /// Get all label names
    pub fn names(&self) -> Vec<&str> {
        self.labels.keys().map(|s| s.as_str()).collect()
    }

    /// Get all label values
    pub fn values(&self) -> Vec<&str> {
        self.labels.values().map(|s| s.as_str()).collect()
    }

    /// Get as vector of Label
    pub fn to_vec(&self) -> Vec<Label> {
        self.labels
            .iter()
            .map(|(k, v)| Label::new(k.clone(), v.clone()))
            .collect()
    }

    /// Get label count
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Compute a fingerprint for the labels
    pub fn fingerprint(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        for (k, v) in &self.labels {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Check if labels match a set of matchers
    pub fn matches(&self, matchers: &[LabelMatcher]) -> bool {
        for matcher in matchers {
            if !matcher.matches(self) {
                return false;
            }
        }
        true
    }

    /// Merge with another set of labels (other takes precedence)
    pub fn merge(&self, other: &Labels) -> Labels {
        let mut result = self.labels.clone();
        for (k, v) in &other.labels {
            result.insert(k.clone(), v.clone());
        }
        Labels { labels: result }
    }

    /// Filter labels by name prefix
    pub fn filter_prefix(&self, prefix: &str) -> Labels {
        let labels: BTreeMap<String, String> = self
            .labels
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Labels { labels }
    }

    /// Iterate over labels
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.labels.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

impl Default for Labels {
    fn default() -> Self {
        Self::empty()
    }
}

impl std::fmt::Display for Labels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .labels
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

impl Hash for Labels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (k, v) in &self.labels {
            k.hash(state);
            v.hash(state);
        }
    }
}

/// Matcher for label filtering
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    /// Label name to match
    pub name: String,
    /// Value pattern
    pub value: String,
    /// Match type
    pub match_type: LabelMatcherType,
}

impl LabelMatcher {
    /// Create an exact match matcher
    pub fn exact(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            match_type: LabelMatcherType::Equal,
        }
    }

    /// Create a not-equal matcher
    pub fn not_equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            match_type: LabelMatcherType::NotEqual,
        }
    }

    /// Create a regex matcher
    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: pattern.into(),
            match_type: LabelMatcherType::Regex,
        }
    }

    /// Create a not-regex matcher
    pub fn not_regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: pattern.into(),
            match_type: LabelMatcherType::NotRegex,
        }
    }

    /// Create an exists matcher
    pub fn exists(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: String::new(),
            match_type: LabelMatcherType::Exists,
        }
    }

    /// Create a not-exists matcher
    pub fn not_exists(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: String::new(),
            match_type: LabelMatcherType::NotExists,
        }
    }

    /// Check if labels match
    pub fn matches(&self, labels: &Labels) -> bool {
        match self.match_type {
            LabelMatcherType::Equal => labels.get(&self.name).is_some_and(|v| v == self.value),
            LabelMatcherType::NotEqual => labels.get(&self.name).map_or(true, |v| v != self.value),
            LabelMatcherType::Regex => {
                if let Some(v) = labels.get(&self.name) {
                    regex::Regex::new(&self.value)
                        .map(|re| re.is_match(v))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            LabelMatcherType::NotRegex => {
                if let Some(v) = labels.get(&self.name) {
                    regex::Regex::new(&self.value)
                        .map(|re| !re.is_match(v))
                        .unwrap_or(true)
                } else {
                    true
                }
            }
            LabelMatcherType::Exists => labels.contains(&self.name),
            LabelMatcherType::NotExists => !labels.contains(&self.name),
        }
    }

    /// Parse from string format "name=value", "name!=value", "name=~regex", "name!~regex"
    pub fn parse(s: &str) -> Result<Self, TimeSeriesError> {
        // Check for regex matchers first (=~ or !~)
        if let Some(idx) = s.find("=~") {
            return Ok(Self::regex(&s[..idx], &s[idx + 2..]));
        }
        if let Some(idx) = s.find("!~") {
            return Ok(Self::not_regex(&s[..idx], &s[idx + 2..]));
        }
        if let Some(idx) = s.find("!=") {
            return Ok(Self::not_equal(&s[..idx], &s[idx + 2..]));
        }
        if let Some(idx) = s.find('=') {
            return Ok(Self::exact(&s[..idx], &s[idx + 1..]));
        }

        Err(TimeSeriesError::ConfigError(format!(
            "invalid matcher format: {}",
            s
        )))
    }
}

/// Types of label matching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelMatcherType {
    /// Exact equality (=)
    Equal,
    /// Not equal (!=)
    NotEqual,
    /// Regex match (=~)
    Regex,
    /// Not regex match (!~)
    NotRegex,
    /// Label exists
    Exists,
    /// Label does not exist
    NotExists,
}

impl std::fmt::Display for LabelMatcherType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LabelMatcherType::Equal => write!(f, "="),
            LabelMatcherType::NotEqual => write!(f, "!="),
            LabelMatcherType::Regex => write!(f, "=~"),
            LabelMatcherType::NotRegex => write!(f, "!~"),
            LabelMatcherType::Exists => write!(f, "exists"),
            LabelMatcherType::NotExists => write!(f, "!exists"),
        }
    }
}

/// Builder for label matchers
#[derive(Debug, Default)]
pub struct LabelMatcherBuilder {
    matchers: Vec<LabelMatcher>,
}

impl LabelMatcherBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an exact match
    pub fn exact(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.matchers.push(LabelMatcher::exact(name, value));
        self
    }

    /// Add a not-equal match
    pub fn not_equal(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.matchers.push(LabelMatcher::not_equal(name, value));
        self
    }

    /// Add a regex match
    pub fn regex(mut self, name: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.matchers.push(LabelMatcher::regex(name, pattern));
        self
    }

    /// Build the matchers
    pub fn build(self) -> Vec<LabelMatcher> {
        self.matchers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_parse() {
        let label = Label::parse("host:server1").unwrap();
        assert_eq!(label.name, "host");
        assert_eq!(label.value, "server1");

        let label2 = Label::parse("env=prod").unwrap();
        assert_eq!(label2.name, "env");
        assert_eq!(label2.value, "prod");
    }

    #[test]
    fn test_label_matches() {
        let label = Label::new("host", "server1");

        assert!(label.matches("server1"));
        assert!(label.matches("server*"));
        assert!(label.matches("*1"));
        assert!(label.matches("*"));
        assert!(!label.matches("server2"));
    }

    #[test]
    fn test_labels_parse() {
        let labels = Labels::parse(&["host:server1", "env:prod"]).unwrap();

        assert_eq!(labels.get("host"), Some("server1"));
        assert_eq!(labels.get("env"), Some("prod"));
        assert_eq!(labels.len(), 2);
    }

    #[test]
    fn test_labels_parse_string() {
        let labels = Labels::parse_string("host=server1,env=prod").unwrap();

        assert_eq!(labels.get("host"), Some("server1"));
        assert_eq!(labels.get("env"), Some("prod"));
    }

    #[test]
    fn test_labels_fingerprint() {
        let labels1 = Labels::parse(&["host:server1", "env:prod"]).unwrap();
        let labels2 = Labels::parse(&["env:prod", "host:server1"]).unwrap();
        let labels3 = Labels::parse(&["host:server2", "env:prod"]).unwrap();

        // Same labels should have same fingerprint regardless of order
        assert_eq!(labels1.fingerprint(), labels2.fingerprint());
        // Different labels should have different fingerprint
        assert_ne!(labels1.fingerprint(), labels3.fingerprint());
    }

    #[test]
    fn test_labels_merge() {
        let labels1 = Labels::parse(&["host:server1"]).unwrap();
        let labels2 = Labels::parse(&["env:prod"]).unwrap();

        let merged = labels1.merge(&labels2);
        assert_eq!(merged.get("host"), Some("server1"));
        assert_eq!(merged.get("env"), Some("prod"));
    }

    #[test]
    fn test_label_matcher_exact() {
        let labels = Labels::parse(&["host:server1"]).unwrap();

        let matcher = LabelMatcher::exact("host", "server1");
        assert!(matcher.matches(&labels));

        let matcher2 = LabelMatcher::exact("host", "server2");
        assert!(!matcher2.matches(&labels));
    }

    #[test]
    fn test_label_matcher_not_equal() {
        let labels = Labels::parse(&["host:server1"]).unwrap();

        let matcher = LabelMatcher::not_equal("host", "server2");
        assert!(matcher.matches(&labels));

        let matcher2 = LabelMatcher::not_equal("host", "server1");
        assert!(!matcher2.matches(&labels));
    }

    #[test]
    fn test_label_matcher_regex() {
        let labels = Labels::parse(&["host:server1"]).unwrap();

        let matcher = LabelMatcher::regex("host", "server\\d+");
        assert!(matcher.matches(&labels));

        let matcher2 = LabelMatcher::regex("host", "client\\d+");
        assert!(!matcher2.matches(&labels));
    }

    #[test]
    fn test_label_matcher_exists() {
        let labels = Labels::parse(&["host:server1"]).unwrap();

        let matcher = LabelMatcher::exists("host");
        assert!(matcher.matches(&labels));

        let matcher2 = LabelMatcher::exists("env");
        assert!(!matcher2.matches(&labels));
    }

    #[test]
    fn test_label_matcher_parse() {
        let exact = LabelMatcher::parse("host=server1").unwrap();
        assert!(matches!(exact.match_type, LabelMatcherType::Equal));

        let not_equal = LabelMatcher::parse("host!=server1").unwrap();
        assert!(matches!(not_equal.match_type, LabelMatcherType::NotEqual));

        let regex = LabelMatcher::parse("host=~server\\d+").unwrap();
        assert!(matches!(regex.match_type, LabelMatcherType::Regex));
    }

    #[test]
    fn test_labels_matches_multiple() {
        let labels = Labels::parse(&["host:server1", "env:prod"]).unwrap();

        let matchers = vec![
            LabelMatcher::exact("host", "server1"),
            LabelMatcher::exact("env", "prod"),
        ];

        assert!(labels.matches(&matchers));

        let matchers2 = vec![
            LabelMatcher::exact("host", "server1"),
            LabelMatcher::exact("env", "dev"),
        ];

        assert!(!labels.matches(&matchers2));
    }

    #[test]
    fn test_label_matcher_builder() {
        let matchers = LabelMatcherBuilder::new()
            .exact("host", "server1")
            .not_equal("env", "dev")
            .regex("region", "us-.*")
            .build();

        assert_eq!(matchers.len(), 3);
    }
}

/// Index of labels for fast series lookup by label values.
///
/// Maintains an inverted index: label_name -> label_value -> set of series IDs.
/// Used for efficient MRANGE-style queries like `FILTER region=us-east`.
#[derive(Debug, Default)]
pub struct LabelIndex {
    /// Inverted index: label_name -> label_value -> set of series keys
    index: std::collections::HashMap<String, std::collections::HashMap<String, Vec<String>>>,
}

impl LabelIndex {
    /// Create a new empty label index
    pub fn new() -> Self {
        Self {
            index: std::collections::HashMap::new(),
        }
    }

    /// Add a series to the index
    pub fn add(&mut self, series_key: &str, labels: &Labels) {
        for (name, value) in labels.iter() {
            self.index
                .entry(name.to_string())
                .or_default()
                .entry(value.to_string())
                .or_default()
                .push(series_key.to_string());
        }
    }

    /// Remove a series from the index
    pub fn remove(&mut self, series_key: &str) {
        for values_map in self.index.values_mut() {
            for keys in values_map.values_mut() {
                keys.retain(|k| k != series_key);
            }
        }
    }

    /// Find series matching a single label matcher
    pub fn find(&self, matcher: &LabelMatcher) -> Vec<String> {
        match matcher.match_type {
            LabelMatcherType::Equal => self
                .index
                .get(&matcher.name)
                .and_then(|values| values.get(&matcher.value))
                .cloned()
                .unwrap_or_default(),
            LabelMatcherType::NotEqual => {
                let mut result = Vec::new();
                if let Some(values) = self.index.get(&matcher.name) {
                    for (value, keys) in values {
                        if value != &matcher.value {
                            result.extend(keys.iter().cloned());
                        }
                    }
                }
                result
            }
            LabelMatcherType::Regex => {
                let mut result = Vec::new();
                if let Ok(re) = regex::Regex::new(&matcher.value) {
                    if let Some(values) = self.index.get(&matcher.name) {
                        for (value, keys) in values {
                            if re.is_match(value) {
                                result.extend(keys.iter().cloned());
                            }
                        }
                    }
                }
                result
            }
            LabelMatcherType::Exists => {
                let mut result = Vec::new();
                if let Some(values) = self.index.get(&matcher.name) {
                    for keys in values.values() {
                        result.extend(keys.iter().cloned());
                    }
                }
                result
            }
            _ => Vec::new(),
        }
    }

    /// Find series matching all given matchers (AND semantics)
    pub fn find_all(&self, matchers: &[LabelMatcher]) -> Vec<String> {
        if matchers.is_empty() {
            // Return all indexed series
            let mut all = Vec::new();
            for values in self.index.values() {
                for keys in values.values() {
                    for key in keys {
                        if !all.contains(key) {
                            all.push(key.clone());
                        }
                    }
                }
            }
            return all;
        }

        let mut result: Option<Vec<String>> = None;

        for matcher in matchers {
            let matches = self.find(matcher);
            result = Some(match result {
                Some(prev) => prev.into_iter().filter(|k| matches.contains(k)).collect(),
                None => matches,
            });
        }

        result.unwrap_or_default()
    }

    /// Get all unique values for a label name
    pub fn values_for(&self, label_name: &str) -> Vec<String> {
        self.index
            .get(label_name)
            .map(|values| values.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all indexed label names
    pub fn label_names(&self) -> Vec<String> {
        self.index.keys().cloned().collect()
    }

    /// Get the number of indexed series
    pub fn series_count(&self) -> usize {
        let mut unique = std::collections::HashSet::new();
        for values in self.index.values() {
            for keys in values.values() {
                for key in keys {
                    unique.insert(key.clone());
                }
            }
        }
        unique.len()
    }
}

#[cfg(test)]
mod label_index_tests {
    use super::*;

    #[test]
    fn test_label_index_add_and_find() {
        let mut index = LabelIndex::new();

        let labels1 = Labels::parse(&["host:server1", "region:us-east"]).unwrap();
        let labels2 = Labels::parse(&["host:server2", "region:us-east"]).unwrap();
        let labels3 = Labels::parse(&["host:server3", "region:eu-west"]).unwrap();

        index.add("series1", &labels1);
        index.add("series2", &labels2);
        index.add("series3", &labels3);

        let result = index.find(&LabelMatcher::exact("region", "us-east"));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"series1".to_string()));
        assert!(result.contains(&"series2".to_string()));
    }

    #[test]
    fn test_label_index_find_all() {
        let mut index = LabelIndex::new();

        let labels1 = Labels::parse(&["host:server1", "region:us-east", "env:prod"]).unwrap();
        let labels2 = Labels::parse(&["host:server2", "region:us-east", "env:dev"]).unwrap();
        let labels3 = Labels::parse(&["host:server3", "region:eu-west", "env:prod"]).unwrap();

        index.add("s1", &labels1);
        index.add("s2", &labels2);
        index.add("s3", &labels3);

        let result = index.find_all(&[
            LabelMatcher::exact("region", "us-east"),
            LabelMatcher::exact("env", "prod"),
        ]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "s1");
    }

    #[test]
    fn test_label_index_regex() {
        let mut index = LabelIndex::new();

        let labels1 = Labels::parse(&["host:server1"]).unwrap();
        let labels2 = Labels::parse(&["host:server2"]).unwrap();
        let labels3 = Labels::parse(&["host:client1"]).unwrap();

        index.add("s1", &labels1);
        index.add("s2", &labels2);
        index.add("s3", &labels3);

        let result = index.find(&LabelMatcher::regex("host", "server\\d+"));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_label_index_remove() {
        let mut index = LabelIndex::new();

        let labels = Labels::parse(&["host:server1"]).unwrap();
        index.add("s1", &labels);
        index.add("s2", &labels);

        index.remove("s1");

        let result = index.find(&LabelMatcher::exact("host", "server1"));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "s2");
    }

    #[test]
    fn test_label_index_values_for() {
        let mut index = LabelIndex::new();

        index.add("s1", &Labels::parse(&["region:us-east"]).unwrap());
        index.add("s2", &Labels::parse(&["region:us-west"]).unwrap());
        index.add("s3", &Labels::parse(&["region:eu-west"]).unwrap());

        let values = index.values_for("region");
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_label_index_series_count() {
        let mut index = LabelIndex::new();

        index.add("s1", &Labels::parse(&["host:a"]).unwrap());
        index.add("s2", &Labels::parse(&["host:b"]).unwrap());
        index.add("s3", &Labels::parse(&["host:a", "env:prod"]).unwrap());

        assert_eq!(index.series_count(), 3);
    }
}
