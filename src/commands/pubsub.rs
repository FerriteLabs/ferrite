//! Pub/Sub command implementations
//!
//! This module implements Redis pub/sub commands.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::runtime::SharedSubscriptionManager;

/// PUBLISH channel message
/// Publish a message to a channel, returns number of subscribers that received it
pub fn publish(manager: &SharedSubscriptionManager, channel: &Bytes, message: &Bytes) -> Frame {
    let receivers = manager.publish(channel, message);
    Frame::Integer(receivers as i64)
}

/// PUBSUB CHANNELS [pattern]
/// List active channels optionally matching pattern
pub fn pubsub_channels(manager: &SharedSubscriptionManager, pattern: Option<&Bytes>) -> Frame {
    let channels = manager.channels(pattern);
    let frames: Vec<Frame> = channels.into_iter().map(Frame::bulk).collect();
    Frame::array(frames)
}

/// PUBSUB NUMSUB [channel ...]
/// Return number of subscribers for specified channels
pub fn pubsub_numsub(manager: &SharedSubscriptionManager, channels: &[Bytes]) -> Frame {
    let mut result = Vec::with_capacity(channels.len() * 2);
    for channel in channels {
        result.push(Frame::bulk(channel.clone()));
        result.push(Frame::Integer(manager.numsub(channel) as i64));
    }
    Frame::array(result)
}

/// PUBSUB NUMPAT
/// Return number of pattern subscriptions
pub fn pubsub_numpat(manager: &SharedSubscriptionManager) -> Frame {
    Frame::Integer(manager.numpat() as i64)
}

// --- Sharded Pub/Sub (Redis 7.0+) ---

/// SPUBLISH shardchannel message
/// Publish a message to a shard channel (slot-based routing)
/// For now, this behaves the same as PUBLISH but is conceptually distinct
/// In a cluster setup, SPUBLISH routes messages based on slot
pub fn spublish(manager: &SharedSubscriptionManager, channel: &Bytes, message: &Bytes) -> Frame {
    // In sharded pub/sub, messages are routed based on the channel's slot
    // For single-node operation, this behaves like regular PUBLISH
    let receivers = manager.publish(channel, message);
    Frame::Integer(receivers as i64)
}

/// PUBSUB SHARDCHANNELS [pattern]
/// List active shard channels optionally matching pattern
pub fn pubsub_shardchannels(manager: &SharedSubscriptionManager, pattern: Option<&Bytes>) -> Frame {
    // In a full implementation, this would return only sharded channel subscriptions
    // For now, return the same as regular channels
    let channels = manager.channels(pattern);
    let frames: Vec<Frame> = channels.into_iter().map(Frame::bulk).collect();
    Frame::array(frames)
}

/// PUBSUB SHARDNUMSUB [shardchannel ...]
/// Return number of subscribers for specified shard channels
pub fn pubsub_shardnumsub(manager: &SharedSubscriptionManager, channels: &[Bytes]) -> Frame {
    // In a full implementation, this would count only sharded channel subscriptions
    // For now, behave like regular NUMSUB
    let mut result = Vec::with_capacity(channels.len() * 2);
    for channel in channels {
        result.push(Frame::bulk(channel.clone()));
        result.push(Frame::Integer(manager.numsub(channel) as i64));
    }
    Frame::array(result)
}

/// Create a ssubscribe response message
pub fn ssubscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("ssubscribe"),
        Frame::bulk(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create a sunsubscribe response message
pub fn sunsubscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("sunsubscribe"),
        Frame::bulk(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create a smessage notification for shard channel subscription
pub fn smessage_notification(channel: &Bytes, message: &Bytes) -> Frame {
    Frame::array(vec![
        Frame::bulk("smessage"),
        Frame::bulk(channel.clone()),
        Frame::bulk(message.clone()),
    ])
}

/// Create a subscribe response message
pub fn subscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("subscribe"),
        Frame::bulk(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create an unsubscribe response message
pub fn unsubscribe_response(channel: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("unsubscribe"),
        Frame::bulk(channel.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create a psubscribe response message
pub fn psubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("psubscribe"),
        Frame::bulk(pattern.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create a punsubscribe response message
pub fn punsubscribe_response(pattern: &Bytes, count: usize) -> Frame {
    Frame::array(vec![
        Frame::bulk("punsubscribe"),
        Frame::bulk(pattern.clone()),
        Frame::Integer(count as i64),
    ])
}

/// Create a message notification for channel subscription
pub fn message_notification(channel: &Bytes, message: &Bytes) -> Frame {
    Frame::array(vec![
        Frame::bulk("message"),
        Frame::bulk(channel.clone()),
        Frame::bulk(message.clone()),
    ])
}

/// Create a pmessage notification for pattern subscription
pub fn pmessage_notification(pattern: &Bytes, channel: &Bytes, message: &Bytes) -> Frame {
    Frame::array(vec![
        Frame::bulk("pmessage"),
        Frame::bulk(pattern.clone()),
        Frame::bulk(channel.clone()),
        Frame::bulk(message.clone()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SubscriptionManager;
    use std::sync::Arc;

    fn create_manager() -> SharedSubscriptionManager {
        Arc::new(SubscriptionManager::new())
    }

    #[test]
    fn test_publish_no_subscribers() {
        let manager = create_manager();
        let result = publish(&manager, &Bytes::from("channel"), &Bytes::from("msg"));
        assert!(matches!(result, Frame::Integer(0)));
    }

    #[test]
    fn test_publish_with_subscribers() {
        let manager = create_manager();
        let channel = Bytes::from("test");

        // Subscribe
        let _rx = manager.subscribe(&channel);

        // Publish
        let result = publish(&manager, &channel, &Bytes::from("hello"));
        assert!(matches!(result, Frame::Integer(1)));
    }

    #[test]
    fn test_pubsub_channels() {
        let manager = create_manager();
        let ch1 = Bytes::from("news.sports");
        let ch2 = Bytes::from("news.tech");
        let ch3 = Bytes::from("weather");

        // Subscribe to channels
        let _rx1 = manager.subscribe(&ch1);
        let _rx2 = manager.subscribe(&ch2);
        let _rx3 = manager.subscribe(&ch3);

        // Get all channels
        let result = pubsub_channels(&manager, None);
        match result {
            Frame::Array(Some(frames)) => assert_eq!(frames.len(), 3),
            _ => panic!("Expected array"),
        }

        // Get channels matching pattern
        let result = pubsub_channels(&manager, Some(&Bytes::from("news.*")));
        match result {
            Frame::Array(Some(frames)) => assert_eq!(frames.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_numsub() {
        let manager = create_manager();
        let ch1 = Bytes::from("ch1");
        let ch2 = Bytes::from("ch2");

        let _rx1 = manager.subscribe(&ch1);
        let _rx2 = manager.subscribe(&ch1);
        let _rx3 = manager.subscribe(&ch2);

        let result = pubsub_numsub(&manager, &[ch1.clone(), ch2.clone()]);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 4);
                // ch1 has 2 subscribers
                assert!(matches!(&frames[1], Frame::Integer(2)));
                // ch2 has 1 subscriber
                assert!(matches!(&frames[3], Frame::Integer(1)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_numpat() {
        let manager = create_manager();

        let _rx1 = manager.psubscribe(&Bytes::from("news.*"));
        let _rx2 = manager.psubscribe(&Bytes::from("sports.*"));

        let result = pubsub_numpat(&manager);
        assert!(matches!(result, Frame::Integer(2)));
    }

    #[test]
    fn test_subscribe_response() {
        let result = subscribe_response(&Bytes::from("test"), 1);
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_message_notification() {
        let result = message_notification(&Bytes::from("ch"), &Bytes::from("msg"));
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pmessage_notification() {
        let result = pmessage_notification(
            &Bytes::from("pat*"),
            &Bytes::from("pattern"),
            &Bytes::from("msg"),
        );
        match result {
            Frame::Array(Some(frames)) => {
                assert_eq!(frames.len(), 4);
            }
            _ => panic!("Expected array"),
        }
    }
}
