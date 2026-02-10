//! Pub/Sub subscription management
//!
//! This module implements the subscription manager for Redis pub/sub functionality.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::broadcast;

/// Default channel capacity for broadcast channels
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// A message published to a channel
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    /// The channel the message was published to
    pub channel: Bytes,
    /// The message content
    pub message: Bytes,
    /// The pattern that matched (for pattern subscriptions)
    pub pattern: Option<Bytes>,
}

/// Manages pub/sub subscriptions and message delivery
#[derive(Debug)]
pub struct SubscriptionManager {
    /// Channel subscriptions: channel -> broadcast sender
    channels: RwLock<HashMap<Bytes, broadcast::Sender<PubSubMessage>>>,
    /// Pattern subscriptions: pattern -> broadcast sender
    patterns: RwLock<HashMap<Bytes, broadcast::Sender<PubSubMessage>>>,
    /// Channel capacity for new channels
    channel_capacity: usize,
}

impl SubscriptionManager {
    /// Create a new subscription manager
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    /// Create a new subscription manager with custom channel capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
            channel_capacity: capacity,
        }
    }

    /// Subscribe to a channel, returns a receiver for messages
    pub fn subscribe(&self, channel: &Bytes) -> broadcast::Receiver<PubSubMessage> {
        let mut channels = self.channels.write();

        if let Some(sender) = channels.get(channel) {
            sender.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(self.channel_capacity);
            channels.insert(channel.clone(), tx);
            rx
        }
    }

    /// Subscribe to a pattern, returns a receiver for messages
    pub fn psubscribe(&self, pattern: &Bytes) -> broadcast::Receiver<PubSubMessage> {
        let mut patterns = self.patterns.write();

        if let Some(sender) = patterns.get(pattern) {
            sender.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(self.channel_capacity);
            patterns.insert(pattern.clone(), tx);
            rx
        }
    }

    /// Publish a message to a channel, returns number of receivers
    pub fn publish(&self, channel: &Bytes, message: &Bytes) -> usize {
        let mut total_receivers = 0;

        // Send to channel subscribers
        {
            let channels = self.channels.read();
            if let Some(sender) = channels.get(channel) {
                let msg = PubSubMessage {
                    channel: channel.clone(),
                    message: message.clone(),
                    pattern: None,
                };
                // receiver_count returns number of active receivers
                let sent = sender.send(msg).unwrap_or(0);
                total_receivers += sent;
            }
        }

        // Send to pattern subscribers
        {
            let patterns = self.patterns.read();
            for (pattern, sender) in patterns.iter() {
                if pattern_matches(pattern, channel) {
                    let msg = PubSubMessage {
                        channel: channel.clone(),
                        message: message.clone(),
                        pattern: Some(pattern.clone()),
                    };
                    let sent = sender.send(msg).unwrap_or(0);
                    total_receivers += sent;
                }
            }
        }

        total_receivers
    }

    /// Get the number of subscribers to a channel
    pub fn numsub(&self, channel: &Bytes) -> usize {
        let channels = self.channels.read();
        channels
            .get(channel)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// Get all active channels (those with at least one subscriber)
    pub fn channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let channels = self.channels.read();
        channels
            .iter()
            .filter(|(ch, sender)| {
                sender.receiver_count() > 0
                    && pattern.map(|p| pattern_matches(p, ch)).unwrap_or(true)
            })
            .map(|(ch, _)| ch.clone())
            .collect()
    }

    /// Get the number of pattern subscriptions
    pub fn numpat(&self) -> usize {
        let patterns = self.patterns.read();
        patterns.values().map(|s| s.receiver_count()).sum()
    }

    /// Clean up channels with no subscribers
    pub fn cleanup(&self) {
        {
            let mut channels = self.channels.write();
            channels.retain(|_, sender| sender.receiver_count() > 0);
        }
        {
            let mut patterns = self.patterns.write();
            patterns.retain(|_, sender| sender.receiver_count() > 0);
        }
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a pattern matches a channel name
/// Supports * (any sequence of characters) and ? (any single character)
fn pattern_matches(pattern: &Bytes, channel: &Bytes) -> bool {
    let pattern = std::str::from_utf8(pattern).unwrap_or("");
    let channel = std::str::from_utf8(channel).unwrap_or("");

    glob_match(pattern, channel)
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();

    glob_match_recursive(&pattern, &text, 0, 0)
}

fn glob_match_recursive(pattern: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    // Both exhausted - match
    if pi >= pattern.len() && ti >= text.len() {
        return true;
    }

    // Pattern exhausted but text remaining - no match
    if pi >= pattern.len() {
        return false;
    }

    let pc = pattern[pi];

    match pc {
        '*' => {
            // Try matching zero or more characters
            // Skip consecutive *s
            let mut next_pi = pi;
            while next_pi < pattern.len() && pattern[next_pi] == '*' {
                next_pi += 1;
            }

            // Try matching from current position onwards
            for i in ti..=text.len() {
                if glob_match_recursive(pattern, text, next_pi, i) {
                    return true;
                }
            }
            false
        }
        '?' => {
            // Match any single character
            if ti < text.len() {
                glob_match_recursive(pattern, text, pi + 1, ti + 1)
            } else {
                false
            }
        }
        '[' => {
            // Character class
            if ti >= text.len() {
                return false;
            }

            let tc = text[ti];
            let mut matched = false;
            let mut negated = false;
            let mut i = pi + 1;

            if i < pattern.len() && pattern[i] == '^' {
                negated = true;
                i += 1;
            }

            while i < pattern.len() && pattern[i] != ']' {
                if i + 2 < pattern.len() && pattern[i + 1] == '-' && pattern[i + 2] != ']' {
                    // Range
                    if tc >= pattern[i] && tc <= pattern[i + 2] {
                        matched = true;
                    }
                    i += 3;
                } else {
                    if tc == pattern[i] {
                        matched = true;
                    }
                    i += 1;
                }
            }

            if negated {
                matched = !matched;
            }

            if matched && i < pattern.len() {
                glob_match_recursive(pattern, text, i + 1, ti + 1)
            } else {
                false
            }
        }
        _ => {
            // Literal character
            if ti < text.len() && pc == text[ti] {
                glob_match_recursive(pattern, text, pi + 1, ti + 1)
            } else {
                false
            }
        }
    }
}

/// Connection subscription state
#[derive(Debug, Default)]
pub struct ConnectionSubscriptions {
    /// Subscribed channels
    pub channels: HashSet<Bytes>,
    /// Subscribed patterns
    pub patterns: HashSet<Bytes>,
}

impl ConnectionSubscriptions {
    /// Create new empty subscription state
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if connection has any subscriptions
    pub fn is_subscribed(&self) -> bool {
        !self.channels.is_empty() || !self.patterns.is_empty()
    }

    /// Get total subscription count
    pub fn count(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

/// Thread-safe subscription manager handle
pub type SharedSubscriptionManager = Arc<SubscriptionManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        // Exact match
        assert!(pattern_matches(&Bytes::from("foo"), &Bytes::from("foo")));
        assert!(!pattern_matches(&Bytes::from("foo"), &Bytes::from("bar")));

        // Wildcard *
        assert!(pattern_matches(
            &Bytes::from("foo*"),
            &Bytes::from("foobar")
        ));
        assert!(pattern_matches(
            &Bytes::from("*bar"),
            &Bytes::from("foobar")
        ));
        assert!(pattern_matches(&Bytes::from("*"), &Bytes::from("anything")));
        assert!(pattern_matches(
            &Bytes::from("foo*bar"),
            &Bytes::from("fooxyzbar")
        ));

        // Single char ?
        assert!(pattern_matches(&Bytes::from("foo?"), &Bytes::from("foob")));
        assert!(!pattern_matches(&Bytes::from("foo?"), &Bytes::from("foo")));
        assert!(pattern_matches(&Bytes::from("f?o"), &Bytes::from("foo")));

        // Character class
        assert!(pattern_matches(
            &Bytes::from("foo[abc]"),
            &Bytes::from("fooa")
        ));
        assert!(!pattern_matches(
            &Bytes::from("foo[abc]"),
            &Bytes::from("food")
        ));

        // Complex patterns
        assert!(pattern_matches(
            &Bytes::from("news.*"),
            &Bytes::from("news.sports")
        ));
        assert!(pattern_matches(
            &Bytes::from("news.*.updates"),
            &Bytes::from("news.sports.updates")
        ));
    }

    #[test]
    fn test_subscribe_publish() {
        let manager = SubscriptionManager::new();
        let channel = Bytes::from("test-channel");
        let message = Bytes::from("hello");

        // Subscribe
        let mut rx = manager.subscribe(&channel);

        // Publish
        let receivers = manager.publish(&channel, &message);
        assert_eq!(receivers, 1);

        // Receive
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.channel, channel);
        assert_eq!(msg.message, message);
        assert!(msg.pattern.is_none());
    }

    #[test]
    fn test_psubscribe() {
        let manager = SubscriptionManager::new();
        let pattern = Bytes::from("news.*");
        let channel = Bytes::from("news.sports");
        let message = Bytes::from("goal!");

        // Pattern subscribe
        let mut rx = manager.psubscribe(&pattern);

        // Publish to matching channel
        let receivers = manager.publish(&channel, &message);
        assert_eq!(receivers, 1);

        // Receive
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.channel, channel);
        assert_eq!(msg.message, message);
        assert_eq!(msg.pattern, Some(pattern));
    }

    #[test]
    fn test_multiple_subscribers() {
        let manager = SubscriptionManager::new();
        let channel = Bytes::from("chat");
        let message = Bytes::from("hi");

        // Multiple subscribers
        let mut rx1 = manager.subscribe(&channel);
        let mut rx2 = manager.subscribe(&channel);

        // Publish
        let receivers = manager.publish(&channel, &message);
        assert_eq!(receivers, 2);

        // Both receive
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_numsub() {
        let manager = SubscriptionManager::new();
        let channel = Bytes::from("test");

        assert_eq!(manager.numsub(&channel), 0);

        let _rx1 = manager.subscribe(&channel);
        assert_eq!(manager.numsub(&channel), 1);

        let _rx2 = manager.subscribe(&channel);
        assert_eq!(manager.numsub(&channel), 2);
    }

    #[test]
    fn test_connection_subscriptions() {
        let mut subs = ConnectionSubscriptions::new();
        assert!(!subs.is_subscribed());
        assert_eq!(subs.count(), 0);

        subs.channels.insert(Bytes::from("ch1"));
        assert!(subs.is_subscribed());
        assert_eq!(subs.count(), 1);

        subs.patterns.insert(Bytes::from("pat*"));
        assert_eq!(subs.count(), 2);
    }
}
