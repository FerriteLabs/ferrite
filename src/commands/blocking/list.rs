//! Blocking list operations (BLPOP, BRPOP, BLMOVE, BLMPOP).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::commands::parser::ListDirection;
use crate::protocol::Frame;
use crate::storage::{Store, Value};

pub struct BlockingListManager {
    /// Channel senders for each key being watched
    /// When a push happens, we notify all waiters for that key
    pub(super) waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
}

impl Default for BlockingListManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingListManager {
    /// Create a new blocking list manager
    pub fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// Notify waiters that a key has new elements
    pub fn notify(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(sender) = self.waiters.get(&key_tuple) {
            // Ignore send errors - means no one is waiting
            let _ = sender.send(());
        }
    }

    /// Get or create a receiver for a key
    pub(crate) fn get_receiver(&self, db: u8, key: &Bytes) -> broadcast::Receiver<()> {
        let key_tuple = (db, key.clone());

        // Get existing or create new channel
        let sender = self
            .waiters
            .entry(key_tuple)
            .or_insert_with(|| broadcast::channel(16).0);

        sender.subscribe()
    }

    /// Clean up channel if no more subscribers
    pub(super) fn cleanup(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(entry) = self.waiters.get(&key_tuple) {
            if entry.receiver_count() == 0 {
                drop(entry);
                self.waiters.remove(&key_tuple);
            }
        }
    }
}

/// Shared blocking list manager type
pub type SharedBlockingListManager = Arc<BlockingListManager>;

/// Try to pop from any of the given keys, returning the first non-empty result
pub(crate) fn try_pop_from_keys(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    from_left: bool,
) -> Option<(Bytes, Bytes)> {
    for key in keys {
        if let Some(Value::List(mut list)) = store.get(db, key) {
            let value = if from_left {
                list.pop_front()
            } else {
                list.pop_back()
            };

            if let Some(v) = value {
                // Update or delete the list
                if list.is_empty() {
                    store.del(db, std::slice::from_ref(key));
                } else {
                    store.set(db, key.clone(), Value::List(list));
                }
                return Some((key.clone(), v));
            }
        }
    }
    None
}

/// BLPOP key [key ...] timeout
/// Blocking left pop - waits for an element to be available
pub async fn blpop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
) -> Frame {
    blocking_pop(store, blocking_manager, db, keys, timeout_secs, true).await
}

/// BRPOP key [key ...] timeout
/// Blocking right pop - waits for an element to be available
pub async fn brpop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
) -> Frame {
    blocking_pop(store, blocking_manager, db, keys, timeout_secs, false).await
}

/// Common implementation for BLPOP and BRPOP
async fn blocking_pop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
    from_left: bool,
) -> Frame {
    // First, try immediate pop
    if let Some((key, value)) = try_pop_from_keys(store, db, keys, from_left) {
        return Frame::Array(Some(vec![Frame::Bulk(Some(key)), Frame::Bulk(Some(value))]));
    }

    // If timeout is 0, block indefinitely; otherwise use the specified timeout
    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    // Subscribe to all keys
    let mut receivers: Vec<_> = keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    // Wait loop
    loop {
        // Try to pop again (in case something was pushed while we were setting up)
        if let Some((key, value)) = try_pop_from_keys(store, db, keys, from_left) {
            // Cleanup subscriptions
            for key in keys {
                blocking_manager.cleanup(db, key);
            }
            return Frame::Array(Some(vec![Frame::Bulk(Some(key)), Frame::Bulk(Some(value))]));
        }

        // Wait for notification on any key
        let wait_future = async {
            // Create futures for all receivers
            let mut any_notified = false;

            // Use select to wait on any receiver
            // We'll use a simple polling approach with a short sleep
            loop {
                for receiver in &mut receivers {
                    if let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) =
                        receiver.try_recv()
                    {
                        any_notified = true;
                        break;
                    }
                }

                if any_notified {
                    break;
                }

                // Short sleep to avoid busy loop
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                match timeout(duration, wait_future).await {
                    Ok(_) => {
                        // Got notification, try pop again
                        continue;
                    }
                    Err(_) => {
                        // Timeout expired
                        for key in keys {
                            blocking_manager.cleanup(db, key);
                        }
                        return Frame::Null;
                    }
                }
            }
            None => {
                // Block indefinitely
                wait_future.await;
                // Got notification, loop back to try pop
            }
        }
    }
}

/// Notify the blocking manager when elements are pushed to a list
/// This should be called after LPUSH, RPUSH, etc.
pub fn notify_list_push(blocking_manager: &SharedBlockingListManager, db: u8, key: &Bytes) {
    blocking_manager.notify(db, key);
}

/// Try to pop from source and push to destination atomically
pub(crate) fn try_rpop_lpush(
    store: &Arc<Store>,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
) -> Option<Bytes> {
    use std::collections::VecDeque;

    // Try to pop from source (right side)
    if let Some(Value::List(mut source_list)) = store.get(db, source) {
        if let Some(value) = source_list.pop_back() {
            // Update or delete the source list
            if source_list.is_empty() {
                store.del(db, std::slice::from_ref(source));
            } else {
                store.set(db, source.clone(), Value::List(source_list));
            }

            // Push to destination (left side)
            let mut dest_list = if let Some(Value::List(list)) = store.get(db, destination) {
                list
            } else {
                VecDeque::new()
            };
            dest_list.push_front(value.clone());
            store.set(db, destination.clone(), Value::List(dest_list));

            return Some(value);
        }
    }
    None
}

/// BRPOPLPUSH source destination timeout
/// Blocking pop from source (right) and push to destination (left)
pub async fn brpoplpush(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
    timeout_secs: f64,
) -> Frame {
    // First, try immediate pop-push
    if let Some(value) = try_rpop_lpush(store, db, source, destination) {
        // Notify destination that it has new elements
        blocking_manager.notify(db, destination);
        return Frame::Bulk(Some(value));
    }

    // If timeout is 0, block indefinitely; otherwise use the specified timeout
    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    // Subscribe to source key only
    let mut receiver = blocking_manager.get_receiver(db, source);

    // Wait loop
    loop {
        // Try pop-push again
        if let Some(value) = try_rpop_lpush(store, db, source, destination) {
            blocking_manager.cleanup(db, source);
            // Notify destination that it has new elements
            blocking_manager.notify(db, destination);
            return Frame::Bulk(Some(value));
        }

        // Wait for notification on source key
        let wait_future = async {
            loop {
                match receiver.try_recv() {
                    Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        break;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => {}
                    Err(broadcast::error::TryRecvError::Closed) => {}
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                match timeout(duration, wait_future).await {
                    Ok(_) => {
                        // Got notification, try pop-push again
                        continue;
                    }
                    Err(_) => {
                        // Timeout expired
                        blocking_manager.cleanup(db, source);
                        return Frame::Null;
                    }
                }
            }
            None => {
                // Block indefinitely
                wait_future.await;
                // Got notification, loop back to try pop-push
            }
        }
    }
}

/// Try to pop from source and push to destination with configurable directions
pub(crate) fn try_move(
    store: &Arc<Store>,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
    wherefrom: ListDirection,
    whereto: ListDirection,
) -> Option<Bytes> {
    use std::collections::VecDeque;

    // Try to pop from source based on direction
    if let Some(Value::List(mut source_list)) = store.get(db, source) {
        let value = match wherefrom {
            ListDirection::Left => source_list.pop_front(),
            ListDirection::Right => source_list.pop_back(),
        };

        if let Some(value) = value {
            // Update or delete the source list
            if source_list.is_empty() {
                store.del(db, std::slice::from_ref(source));
            } else {
                store.set(db, source.clone(), Value::List(source_list));
            }

            // Push to destination based on direction
            let mut dest_list = if let Some(Value::List(list)) = store.get(db, destination) {
                list
            } else {
                VecDeque::new()
            };

            match whereto {
                ListDirection::Left => dest_list.push_front(value.clone()),
                ListDirection::Right => dest_list.push_back(value.clone()),
            }
            store.set(db, destination.clone(), Value::List(dest_list));

            return Some(value);
        }
    }
    None
}

/// Try to pop multiple elements from any of the given keys, returning the first non-empty result
pub(crate) fn try_mpop_from_keys(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    direction: ListDirection,
    count: usize,
) -> Option<(Bytes, Vec<Bytes>)> {
    for key in keys {
        if let Some(Value::List(mut list)) = store.get(db, key) {
            if list.is_empty() {
                continue;
            }

            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let value = match direction {
                    ListDirection::Left => list.pop_front(),
                    ListDirection::Right => list.pop_back(),
                };
                match value {
                    Some(v) => values.push(v),
                    None => break,
                }
            }

            if !values.is_empty() {
                // Update or delete the list
                if list.is_empty() {
                    store.del(db, std::slice::from_ref(key));
                } else {
                    store.set(db, key.clone(), Value::List(list));
                }
                return Some((key.clone(), values));
            }
        }
    }
    None
}

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
/// Blocking pop of multiple elements from any of the given keys
pub async fn blmpop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    keys: &[Bytes],
    direction: ListDirection,
    count: usize,
    timeout_secs: f64,
) -> Frame {
    // First, try immediate pop
    if let Some((key, values)) = try_mpop_from_keys(store, db, keys, direction, count) {
        let value_frames: Vec<Frame> = values.into_iter().map(|v| Frame::Bulk(Some(v))).collect();
        return Frame::Array(Some(vec![
            Frame::Bulk(Some(key)),
            Frame::Array(Some(value_frames)),
        ]));
    }

    // If timeout is 0, block indefinitely; otherwise use the specified timeout
    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    // Subscribe to all keys
    let mut receivers: Vec<_> = keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    // Wait loop
    loop {
        // Try to pop again (in case something was pushed while we were setting up)
        if let Some((key, values)) = try_mpop_from_keys(store, db, keys, direction, count) {
            // Cleanup subscriptions
            for key in keys {
                blocking_manager.cleanup(db, key);
            }
            let value_frames: Vec<Frame> =
                values.into_iter().map(|v| Frame::Bulk(Some(v))).collect();
            return Frame::Array(Some(vec![
                Frame::Bulk(Some(key)),
                Frame::Array(Some(value_frames)),
            ]));
        }

        // Wait for notification on any key
        let wait_future = async {
            let mut any_notified = false;

            loop {
                for receiver in &mut receivers {
                    if let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) =
                        receiver.try_recv()
                    {
                        any_notified = true;
                        break;
                    }
                }

                if any_notified {
                    break;
                }

                // Small sleep to prevent busy waiting
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                match timeout(duration, wait_future).await {
                    Ok(_) => {
                        // Got notification, try pop again
                        continue;
                    }
                    Err(_) => {
                        // Timeout expired
                        for key in keys {
                            blocking_manager.cleanup(db, key);
                        }
                        return Frame::Null;
                    }
                }
            }
            None => {
                // Block indefinitely
                wait_future.await;
                // Got notification, loop back to try pop
            }
        }
    }
}

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
/// Blocking pop from source and push to destination with configurable directions
#[allow(clippy::too_many_arguments)]
pub async fn blmove(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingListManager,
    db: u8,
    source: &Bytes,
    destination: &Bytes,
    wherefrom: ListDirection,
    whereto: ListDirection,
    timeout_secs: f64,
) -> Frame {
    // First, try immediate move
    if let Some(value) = try_move(store, db, source, destination, wherefrom, whereto) {
        // Notify destination that it has new elements
        blocking_manager.notify(db, destination);
        return Frame::Bulk(Some(value));
    }

    // If timeout is 0, block indefinitely; otherwise use the specified timeout
    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    // Subscribe to source key only
    let mut receiver = blocking_manager.get_receiver(db, source);

    // Wait loop
    loop {
        // Try move again
        if let Some(value) = try_move(store, db, source, destination, wherefrom, whereto) {
            blocking_manager.cleanup(db, source);
            // Notify destination that it has new elements
            blocking_manager.notify(db, destination);
            return Frame::Bulk(Some(value));
        }

        // Wait for notification on source key
        let wait_future = async {
            loop {
                match receiver.try_recv() {
                    Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        break;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => {}
                    Err(broadcast::error::TryRecvError::Closed) => {}
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                match timeout(duration, wait_future).await {
                    Ok(_) => {
                        // Got notification, try move again
                        continue;
                    }
                    Err(_) => {
                        // Timeout expired
                        blocking_manager.cleanup(db, source);
                        return Frame::Null;
                    }
                }
            }
            None => {
                // Block indefinitely
                wait_future.await;
                // Got notification, loop back to try move
            }
        }
    }
}
