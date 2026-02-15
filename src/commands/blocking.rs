//! Blocking commands implementation
//!
//! Implements blocking versions of list and stream commands that wait for
//! elements to be available.
//!
//! - BLPOP, BRPOP, BRPOPLPUSH, BLMOVE - blocking list commands
//! - XREAD BLOCK, XREADGROUP BLOCK - blocking stream commands
//! - BZPOPMIN, BZPOPMAX, BZMPOP - blocking sorted set commands

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

use super::parser::ListDirection;
use super::streams;

/// Check if a result frame is empty (null or empty array)
fn is_empty_result(result: &Frame) -> bool {
    match result {
        Frame::Null => true,
        Frame::Array(None) => true,
        Frame::Array(Some(v)) if v.is_empty() => true,
        _ => false,
    }
}

/// Manager for blocking list operations
/// Handles notifications when elements are pushed to lists
pub struct BlockingListManager {
    /// Channel senders for each key being watched
    /// When a push happens, we notify all waiters for that key
    waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
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
    fn get_receiver(&self, db: u8, key: &Bytes) -> broadcast::Receiver<()> {
        let key_tuple = (db, key.clone());

        // Get existing or create new channel
        let sender = self
            .waiters
            .entry(key_tuple)
            .or_insert_with(|| broadcast::channel(16).0);

        sender.subscribe()
    }

    /// Clean up channel if no more subscribers
    fn cleanup(&self, db: u8, key: &Bytes) {
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
fn try_pop_from_keys(
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
fn try_rpop_lpush(
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
fn try_move(
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
fn try_mpop_from_keys(
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

// ============================================================================
// Blocking Stream Manager
// ============================================================================

/// Manager for blocking stream operations
/// Handles notifications when entries are added to streams
pub struct BlockingStreamManager {
    /// Channel senders for each stream key being watched
    waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
}

impl Default for BlockingStreamManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingStreamManager {
    /// Create a new blocking stream manager
    pub fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// Notify waiters that a stream has new entries
    pub fn notify(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(sender) = self.waiters.get(&key_tuple) {
            let _ = sender.send(());
        }
    }

    /// Get or create a receiver for a stream key
    fn get_receiver(&self, db: u8, key: &Bytes) -> broadcast::Receiver<()> {
        let key_tuple = (db, key.clone());
        let sender = self
            .waiters
            .entry(key_tuple)
            .or_insert_with(|| broadcast::channel(16).0);
        sender.subscribe()
    }

    /// Clean up channel if no more subscribers
    fn cleanup(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(entry) = self.waiters.get(&key_tuple) {
            if entry.receiver_count() == 0 {
                drop(entry);
                self.waiters.remove(&key_tuple);
            }
        }
    }
}

/// Shared blocking stream manager type
pub type SharedBlockingStreamManager = Arc<BlockingStreamManager>;

/// Notify the blocking manager when entries are added to a stream
pub fn notify_stream_add(manager: &SharedBlockingStreamManager, db: u8, key: &Bytes) {
    manager.notify(db, key);
}

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
/// Blocking read from streams
pub async fn xread_blocking(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingStreamManager,
    db: u8,
    streams_arg: &[(Bytes, String)],
    count: Option<usize>,
    block_ms: u64,
) -> Frame {
    // First, try immediate read
    {
        let result = streams::xread(store, db, streams_arg, count);
        if !is_empty_result(&result) {
            return result;
        }
    }

    // If block is 0, block indefinitely; otherwise use the specified timeout
    let wait_duration = if block_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(block_ms))
    };

    // Subscribe to all stream keys
    let stream_keys: Vec<Bytes> = streams_arg.iter().map(|(k, _)| k.clone()).collect();
    let mut receivers: Vec<_> = stream_keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    // Wait loop
    loop {
        // Try to read again
        {
            let result = streams::xread(store, db, streams_arg, count);
            if !is_empty_result(&result) {
                for key in &stream_keys {
                    blocking_manager.cleanup(db, key);
                }
                return result;
            }
        }

        // Wait for notification on any stream
        let wait_future = async {
            loop {
                let mut any_notified = false;
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                if timeout(duration, wait_future).await.is_err() {
                    for key in &stream_keys {
                        blocking_manager.cleanup(db, key);
                    }
                    return Frame::Null;
                }
            }
            None => {
                wait_future.await;
            }
        }
    }
}

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
/// Blocking read from streams as part of a consumer group
#[allow(clippy::too_many_arguments)]
pub async fn xreadgroup_blocking(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingStreamManager,
    db: u8,
    group: &Bytes,
    consumer: &Bytes,
    count: Option<usize>,
    block_ms: u64,
    noack: bool,
    streams_arg: &[(Bytes, String)],
) -> Frame {
    // First, try immediate read
    {
        let result = streams::xreadgroup(store, db, group, consumer, count, noack, streams_arg);
        if !is_empty_result(&result) {
            return result;
        }
    }

    let wait_duration = if block_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(block_ms))
    };

    let stream_keys: Vec<Bytes> = streams_arg.iter().map(|(k, _)| k.clone()).collect();
    let mut receivers: Vec<_> = stream_keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    loop {
        {
            let result = streams::xreadgroup(store, db, group, consumer, count, noack, streams_arg);
            if !is_empty_result(&result) {
                for key in &stream_keys {
                    blocking_manager.cleanup(db, key);
                }
                return result;
            }
        }

        let wait_future = async {
            loop {
                let mut any_notified = false;
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                if timeout(duration, wait_future).await.is_err() {
                    for key in &stream_keys {
                        blocking_manager.cleanup(db, key);
                    }
                    return Frame::Null;
                }
            }
            None => {
                wait_future.await;
            }
        }
    }
}

// ============================================================================
// Blocking Sorted Set Manager
// ============================================================================

/// Manager for blocking sorted set operations
/// Handles notifications when elements are added to sorted sets
pub struct BlockingSortedSetManager {
    /// Channel senders for each sorted set key being watched
    waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
}

impl Default for BlockingSortedSetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingSortedSetManager {
    /// Create a new blocking sorted set manager
    pub fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// Notify waiters that a sorted set has new elements
    pub fn notify(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(sender) = self.waiters.get(&key_tuple) {
            let _ = sender.send(());
        }
    }

    /// Get or create a receiver for a sorted set key
    fn get_receiver(&self, db: u8, key: &Bytes) -> broadcast::Receiver<()> {
        let key_tuple = (db, key.clone());
        let sender = self
            .waiters
            .entry(key_tuple)
            .or_insert_with(|| broadcast::channel(16).0);
        sender.subscribe()
    }

    /// Clean up channel if no more subscribers
    fn cleanup(&self, db: u8, key: &Bytes) {
        let key_tuple = (db, key.clone());
        if let Some(entry) = self.waiters.get(&key_tuple) {
            if entry.receiver_count() == 0 {
                drop(entry);
                self.waiters.remove(&key_tuple);
            }
        }
    }
}

/// Shared blocking sorted set manager type
pub type SharedBlockingSortedSetManager = Arc<BlockingSortedSetManager>;

/// Notify the blocking manager when elements are added to a sorted set
pub fn notify_sorted_set_add(manager: &SharedBlockingSortedSetManager, db: u8, key: &Bytes) {
    manager.notify(db, key);
}

/// Try to pop min/max from sorted set
fn try_zpop(store: &Arc<Store>, db: u8, key: &Bytes, pop_min: bool) -> Option<(Bytes, f64)> {
    if let Some(Value::SortedSet {
        by_score,
        by_member,
    }) = store.get(db, key)
    {
        let mut by_score = by_score;
        let mut by_member = by_member;

        let entry = if pop_min {
            by_score
                .iter()
                .next()
                .map(|((score, member), _)| (member.clone(), *score))
        } else {
            by_score
                .iter()
                .next_back()
                .map(|((score, member), _)| (member.clone(), *score))
        };

        if let Some((member, score)) = entry {
            by_score.remove(&(score, member.clone()));
            by_member.remove(&member);

            if by_score.is_empty() {
                store.del(db, std::slice::from_ref(key));
            } else {
                store.set(
                    db,
                    key.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
            }
            return Some((member, score.into_inner()));
        }
    }
    None
}

/// BZPOPMIN key [key ...] timeout
/// Blocking pop of minimum score element from sorted sets
pub async fn bzpopmin(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingSortedSetManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
) -> Frame {
    bzpop(store, blocking_manager, db, keys, timeout_secs, true).await
}

/// BZPOPMAX key [key ...] timeout
/// Blocking pop of maximum score element from sorted sets
pub async fn bzpopmax(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingSortedSetManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
) -> Frame {
    bzpop(store, blocking_manager, db, keys, timeout_secs, false).await
}

/// Common implementation for BZPOPMIN and BZPOPMAX
async fn bzpop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingSortedSetManager,
    db: u8,
    keys: &[Bytes],
    timeout_secs: f64,
    pop_min: bool,
) -> Frame {
    // First, try immediate pop
    for key in keys {
        if let Some((member, score)) = try_zpop(store, db, key, pop_min) {
            return Frame::Array(Some(vec![
                Frame::Bulk(Some(key.clone())),
                Frame::Bulk(Some(member)),
                Frame::bulk(score.to_string()),
            ]));
        }
    }

    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    let mut receivers: Vec<_> = keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    loop {
        // Try to pop again
        for key in keys {
            if let Some((member, score)) = try_zpop(store, db, key, pop_min) {
                for k in keys {
                    blocking_manager.cleanup(db, k);
                }
                return Frame::Array(Some(vec![
                    Frame::Bulk(Some(key.clone())),
                    Frame::Bulk(Some(member)),
                    Frame::bulk(score.to_string()),
                ]));
            }
        }

        let wait_future = async {
            loop {
                let mut any_notified = false;
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                if timeout(duration, wait_future).await.is_err() {
                    for key in keys {
                        blocking_manager.cleanup(db, key);
                    }
                    return Frame::Null;
                }
            }
            None => {
                wait_future.await;
            }
        }
    }
}

/// Try to pop multiple elements from sorted set
fn try_zmpop(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    pop_min: bool,
    count: usize,
) -> Option<(Bytes, Vec<(Bytes, f64)>)> {
    for key in keys {
        if let Some(Value::SortedSet {
            by_score,
            by_member,
        }) = store.get(db, key)
        {
            let mut by_score = by_score;
            let mut by_member = by_member;
            let mut results = Vec::with_capacity(count);

            for _ in 0..count {
                let entry = if pop_min {
                    by_score
                        .iter()
                        .next()
                        .map(|((score, member), _)| (member.clone(), *score))
                } else {
                    by_score
                        .iter()
                        .next_back()
                        .map(|((score, member), _)| (member.clone(), *score))
                };

                if let Some((member, score)) = entry {
                    by_score.remove(&(score, member.clone()));
                    by_member.remove(&member);
                    results.push((member, score.into_inner()));
                } else {
                    break;
                }
            }

            if !results.is_empty() {
                if by_score.is_empty() {
                    store.del(db, std::slice::from_ref(key));
                } else {
                    store.set(
                        db,
                        key.clone(),
                        Value::SortedSet {
                            by_score,
                            by_member,
                        },
                    );
                }
                return Some((key.clone(), results));
            }
        }
    }
    None
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
/// Blocking pop multiple elements from sorted sets
pub async fn bzmpop(
    store: &Arc<Store>,
    blocking_manager: &SharedBlockingSortedSetManager,
    db: u8,
    keys: &[Bytes],
    pop_min: bool,
    count: usize,
    timeout_secs: f64,
) -> Frame {
    // First, try immediate pop
    if let Some((key, results)) = try_zmpop(store, db, keys, pop_min, count) {
        let elements: Vec<Frame> = results
            .into_iter()
            .map(|(member, score)| {
                Frame::Array(Some(vec![
                    Frame::Bulk(Some(member)),
                    Frame::bulk(score.to_string()),
                ]))
            })
            .collect();
        return Frame::Array(Some(vec![
            Frame::Bulk(Some(key)),
            Frame::Array(Some(elements)),
        ]));
    }

    let wait_duration = if timeout_secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    let mut receivers: Vec<_> = keys
        .iter()
        .map(|k| blocking_manager.get_receiver(db, k))
        .collect();

    loop {
        if let Some((key, results)) = try_zmpop(store, db, keys, pop_min, count) {
            for k in keys {
                blocking_manager.cleanup(db, k);
            }
            let elements: Vec<Frame> = results
                .into_iter()
                .map(|(member, score)| {
                    Frame::Array(Some(vec![
                        Frame::Bulk(Some(member)),
                        Frame::bulk(score.to_string()),
                    ]))
                })
                .collect();
            return Frame::Array(Some(vec![
                Frame::Bulk(Some(key)),
                Frame::Array(Some(elements)),
            ]));
        }

        let wait_future = async {
            loop {
                let mut any_notified = false;
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        match wait_duration {
            Some(duration) => {
                if timeout(duration, wait_future).await.is_err() {
                    for key in keys {
                        blocking_manager.cleanup(db, key);
                    }
                    return Frame::Null;
                }
            }
            None => {
                wait_future.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::collections::VecDeque;
    use tokio::time::sleep;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[tokio::test]
    async fn test_blpop_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push some data first
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("value1"));
        list.push_back(Bytes::from("value2"));
        store.set(0, key.clone(), Value::List(list));

        // BLPOP should return immediately
        let result = blpop(&store, &manager, 0, &[key.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"mylist");
                }
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"value1");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_blpop_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("empty_list");

        // BLPOP on empty key should timeout
        let result = blpop(&store, &manager, 0, &[key], 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blpop_with_push() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will push after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("pushed_value"));
            store_clone.set(0, key_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &key_clone);
        });

        // BLPOP should wait and then get the value
        let result = blpop(&store, &manager, 0, &[key], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"pushed_value");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_brpop_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push some data first
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("last"));
        store.set(0, key.clone(), Value::List(list));

        // BRPOP should return the last element
        let result = brpop(&store, &manager, 0, &[key.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"last");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_blpop_multiple_keys() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key1 = Bytes::from("list1");
        let key2 = Bytes::from("list2");

        // Only populate second key
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("from_list2"));
        store.set(0, key2.clone(), Value::List(list));

        // BLPOP should return from the first non-empty key
        let result = blpop(&store, &manager, 0, &[key1, key2.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"list2");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_brpoplpush_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        // Push some data to source
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("last"));
        store.set(0, source.clone(), Value::List(list));

        // BRPOPLPUSH should return immediately with "last"
        let result = brpoplpush(&store, &manager, 0, &source, &dest, 1.0).await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"last");
            }
            _ => panic!("Expected bulk string result, got {:?}", result),
        }

        // Verify destination has the value
        if let Some(Value::List(dest_list)) = store.get(0, &dest) {
            assert_eq!(dest_list.len(), 1);
            assert_eq!(dest_list[0].as_ref(), b"last");
        } else {
            panic!("Destination list not found");
        }

        // Verify source still has "first"
        if let Some(Value::List(source_list)) = store.get(0, &source) {
            assert_eq!(source_list.len(), 1);
            assert_eq!(source_list[0].as_ref(), b"first");
        } else {
            panic!("Source list not found");
        }
    }

    #[tokio::test]
    async fn test_brpoplpush_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("empty_source");
        let dest = Bytes::from("dest");

        // BRPOPLPUSH on empty source should timeout
        let result = brpoplpush(&store, &manager, 0, &source, &dest, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_brpoplpush_same_key() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("circular");

        // Push some data
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(0, key.clone(), Value::List(list));

        // BRPOPLPUSH with same source and dest (rotate)
        let result = brpoplpush(&store, &manager, 0, &key, &key, 1.0).await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"c");
            }
            _ => panic!("Expected bulk string result"),
        }

        // Verify the list is rotated: c, a, b
        if let Some(Value::List(list)) = store.get(0, &key) {
            assert_eq!(list.len(), 3);
            assert_eq!(list[0].as_ref(), b"c");
            assert_eq!(list[1].as_ref(), b"a");
            assert_eq!(list[2].as_ref(), b"b");
        } else {
            panic!("List not found");
        }
    }

    #[tokio::test]
    async fn test_blmove_right_left() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        // Push some data to source: [first, last]
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("last"));
        store.set(0, source.clone(), Value::List(list));

        // BLMOVE source dest RIGHT LEFT (same as BRPOPLPUSH)
        let result = blmove(
            &store,
            &manager,
            0,
            &source,
            &dest,
            ListDirection::Right,
            ListDirection::Left,
            1.0,
        )
        .await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"last");
            }
            _ => panic!("Expected bulk string result, got {:?}", result),
        }

        // Verify destination has the value at the front
        if let Some(Value::List(dest_list)) = store.get(0, &dest) {
            assert_eq!(dest_list.len(), 1);
            assert_eq!(dest_list[0].as_ref(), b"last");
        } else {
            panic!("Destination list not found");
        }
    }

    #[tokio::test]
    async fn test_blmove_left_right() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        // Push some data to source: [first, middle, last]
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("middle"));
        list.push_back(Bytes::from("last"));
        store.set(0, source.clone(), Value::List(list));

        // BLMOVE source dest LEFT RIGHT (pop from left, push to right)
        let result = blmove(
            &store,
            &manager,
            0,
            &source,
            &dest,
            ListDirection::Left,
            ListDirection::Right,
            1.0,
        )
        .await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"first");
            }
            _ => panic!("Expected bulk string result, got {:?}", result),
        }

        // Verify destination has the value at the back
        if let Some(Value::List(dest_list)) = store.get(0, &dest) {
            assert_eq!(dest_list.len(), 1);
            assert_eq!(dest_list[0].as_ref(), b"first");
        } else {
            panic!("Destination list not found");
        }

        // Verify source still has [middle, last]
        if let Some(Value::List(source_list)) = store.get(0, &source) {
            assert_eq!(source_list.len(), 2);
            assert_eq!(source_list[0].as_ref(), b"middle");
            assert_eq!(source_list[1].as_ref(), b"last");
        } else {
            panic!("Source list not found");
        }
    }

    #[tokio::test]
    async fn test_blmove_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("empty_source");
        let dest = Bytes::from("dest");

        // BLMOVE on empty source should timeout
        let result = blmove(
            &store,
            &manager,
            0,
            &source,
            &dest,
            ListDirection::Right,
            ListDirection::Left,
            0.1,
        )
        .await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blmove_same_key_rotate() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("circular");

        // Push some data: [a, b, c]
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(0, key.clone(), Value::List(list));

        // BLMOVE with same source and dest LEFT RIGHT (move first to last)
        let result = blmove(
            &store,
            &manager,
            0,
            &key,
            &key,
            ListDirection::Left,
            ListDirection::Right,
            1.0,
        )
        .await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"a");
            }
            _ => panic!("Expected bulk string result"),
        }

        // Verify the list is rotated: [b, c, a]
        if let Some(Value::List(list)) = store.get(0, &key) {
            assert_eq!(list.len(), 3);
            assert_eq!(list[0].as_ref(), b"b");
            assert_eq!(list[1].as_ref(), b"c");
            assert_eq!(list[2].as_ref(), b"a");
        } else {
            panic!("List not found");
        }
    }

    // ============================================================================
    // BLMPOP Tests
    // ============================================================================

    #[tokio::test]
    async fn test_blmpop_immediate_left() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push some data first
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(0, key.clone(), Value::List(list));

        // BLMPOP should return immediately with 2 elements from left
        let result = blmpop(
            &store,
            &manager,
            0,
            &[key.clone()],
            ListDirection::Left,
            2,
            1.0,
        )
        .await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                // First element is the key
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"mylist");
                }
                // Second element is array of values
                if let Frame::Array(Some(values)) = &arr[1] {
                    assert_eq!(values.len(), 2);
                    if let Frame::Bulk(Some(v)) = &values[0] {
                        assert_eq!(v.as_ref(), b"a");
                    }
                    if let Frame::Bulk(Some(v)) = &values[1] {
                        assert_eq!(v.as_ref(), b"b");
                    }
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }

        // Verify only "c" remains
        if let Some(Value::List(list)) = store.get(0, &key) {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].as_ref(), b"c");
        } else {
            panic!("List not found");
        }
    }

    #[tokio::test]
    async fn test_blmpop_immediate_right() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push some data first
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(0, key.clone(), Value::List(list));

        // BLMPOP should return immediately with 2 elements from right
        let result = blmpop(
            &store,
            &manager,
            0,
            &[key.clone()],
            ListDirection::Right,
            2,
            1.0,
        )
        .await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Array(Some(values)) = &arr[1] {
                    assert_eq!(values.len(), 2);
                    if let Frame::Bulk(Some(v)) = &values[0] {
                        assert_eq!(v.as_ref(), b"c");
                    }
                    if let Frame::Bulk(Some(v)) = &values[1] {
                        assert_eq!(v.as_ref(), b"b");
                    }
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_blmpop_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("empty_list");

        // BLMPOP on empty key should timeout
        let result = blmpop(&store, &manager, 0, &[key], ListDirection::Left, 1, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blmpop_multiple_keys() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key1 = Bytes::from("empty");
        let key2 = Bytes::from("has_data");

        // Only populate second key
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("value1"));
        list.push_back(Bytes::from("value2"));
        store.set(0, key2.clone(), Value::List(list));

        // BLMPOP should return from first non-empty key
        let result = blmpop(
            &store,
            &manager,
            0,
            &[key1, key2.clone()],
            ListDirection::Left,
            1,
            1.0,
        )
        .await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"has_data");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_blmpop_count_exceeds_list_size() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("short_list");

        // Push only 2 elements
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        store.set(0, key.clone(), Value::List(list));

        // Request 5 elements, should only get 2
        let result = blmpop(
            &store,
            &manager,
            0,
            &[key.clone()],
            ListDirection::Left,
            5,
            1.0,
        )
        .await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Array(Some(values)) = &arr[1] {
                    assert_eq!(values.len(), 2);
                }
            }
            _ => panic!("Expected array result"),
        }

        // Verify list is now empty (deleted)
        assert!(store.get(0, &key).is_none());
    }

    #[tokio::test]
    async fn test_blmpop_with_push_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will push after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("pushed1"));
            list.push_back(Bytes::from("pushed2"));
            store_clone.set(0, key_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &key_clone);
        });

        // BLMPOP should wait and then get the values
        let result = blmpop(&store, &manager, 0, &[key], ListDirection::Left, 2, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Array(Some(values)) = &arr[1] {
                    assert_eq!(values.len(), 2);
                    if let Frame::Bulk(Some(v)) = &values[0] {
                        assert_eq!(v.as_ref(), b"pushed1");
                    }
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    // ============================================================================
    // BZPOPMIN / BZPOPMAX Tests
    // ============================================================================

    fn create_sorted_set(store: &Arc<Store>, key: &Bytes, members: &[(f64, &str)]) {
        use ordered_float::OrderedFloat;
        use std::collections::BTreeMap;
        use std::collections::HashMap;

        let mut by_score: BTreeMap<(OrderedFloat<f64>, Bytes), ()> = BTreeMap::new();
        let mut by_member: HashMap<Bytes, f64> = HashMap::new();

        for (score, member) in members {
            let member_bytes = Bytes::copy_from_slice(member.as_bytes());
            let ordered_score = OrderedFloat(*score);
            by_score.insert((ordered_score, member_bytes.clone()), ());
            by_member.insert(member_bytes, *score);
        }

        store.set(
            0,
            key.clone(),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );
    }

    #[tokio::test]
    async fn test_bzpopmin_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        // Create sorted set with scores
        create_sorted_set(&store, &key, &[(1.0, "one"), (2.0, "two"), (3.0, "three")]);

        // BZPOPMIN should return minimum immediately
        let result = bzpopmin(&store, &manager, 0, &[key.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"myzset");
                }
                if let Frame::Bulk(Some(m)) = &arr[1] {
                    assert_eq!(m.as_ref(), b"one");
                }
                if let Frame::Bulk(Some(s)) = &arr[2] {
                    assert_eq!(s.as_ref(), b"1");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_bzpopmax_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        // Create sorted set with scores
        create_sorted_set(&store, &key, &[(1.0, "one"), (2.0, "two"), (3.0, "three")]);

        // BZPOPMAX should return maximum immediately
        let result = bzpopmax(&store, &manager, 0, &[key.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"myzset");
                }
                if let Frame::Bulk(Some(m)) = &arr[1] {
                    assert_eq!(m.as_ref(), b"three");
                }
                if let Frame::Bulk(Some(s)) = &arr[2] {
                    assert_eq!(s.as_ref(), b"3");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_bzpopmin_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("empty_zset");

        // BZPOPMIN on empty key should timeout
        let result = bzpopmin(&store, &manager, 0, &[key], 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_bzpopmax_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("empty_zset");

        // BZPOPMAX on empty key should timeout
        let result = bzpopmax(&store, &manager, 0, &[key], 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_bzpopmin_multiple_keys() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key1 = Bytes::from("empty");
        let key2 = Bytes::from("has_data");

        // Only populate second key
        create_sorted_set(&store, &key2, &[(5.0, "five"), (10.0, "ten")]);

        // BZPOPMIN should return from first non-empty key
        let result = bzpopmin(&store, &manager, 0, &[key1, key2.clone()], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"has_data");
                }
                if let Frame::Bulk(Some(m)) = &arr[1] {
                    assert_eq!(m.as_ref(), b"five");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_bzpopmin_removes_from_set() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        // Create sorted set with 2 members
        create_sorted_set(&store, &key, &[(1.0, "one"), (2.0, "two")]);

        // Pop minimum
        let _result = bzpopmin(&store, &manager, 0, &[key.clone()], 1.0).await;

        // Verify only "two" remains
        if let Some(Value::SortedSet { by_member, .. }) = store.get(0, &key) {
            assert_eq!(by_member.len(), 1);
            assert!(by_member.contains_key(&Bytes::from("two")));
        } else {
            panic!("Sorted set not found");
        }
    }

    #[tokio::test]
    async fn test_bzpopmax_deletes_empty_set() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("single");

        // Create sorted set with single member
        create_sorted_set(&store, &key, &[(1.0, "only")]);

        // Pop maximum (only element)
        let _result = bzpopmax(&store, &manager, 0, &[key.clone()], 1.0).await;

        // Verify set is deleted
        assert!(store.get(0, &key).is_none());
    }

    #[tokio::test]
    async fn test_bzpopmin_with_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will add to sorted set after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            // Create sorted set
            create_sorted_set(&store_clone, &key_clone, &[(42.0, "answer")]);
            notify_sorted_set_add(&manager_clone, 0, &key_clone);
        });

        // BZPOPMIN should wait and then get the value
        let result = bzpopmin(&store, &manager, 0, &[key], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(m)) = &arr[1] {
                    assert_eq!(m.as_ref(), b"answer");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    // ============================================================================
    // BZMPOP Tests
    // ============================================================================

    #[tokio::test]
    async fn test_bzmpop_min_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        // Create sorted set with scores
        create_sorted_set(
            &store,
            &key,
            &[(1.0, "one"), (2.0, "two"), (3.0, "three"), (4.0, "four")],
        );

        // BZMPOP MIN with count 2
        let result = bzmpop(&store, &manager, 0, &[key.clone()], true, 2, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"myzset");
                }
                if let Frame::Array(Some(elements)) = &arr[1] {
                    assert_eq!(elements.len(), 2);
                    // First element should be "one" with score 1
                    if let Frame::Array(Some(pair)) = &elements[0] {
                        if let Frame::Bulk(Some(m)) = &pair[0] {
                            assert_eq!(m.as_ref(), b"one");
                        }
                    }
                    // Second element should be "two" with score 2
                    if let Frame::Array(Some(pair)) = &elements[1] {
                        if let Frame::Bulk(Some(m)) = &pair[0] {
                            assert_eq!(m.as_ref(), b"two");
                        }
                    }
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_bzmpop_max_immediate() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        // Create sorted set with scores
        create_sorted_set(
            &store,
            &key,
            &[(1.0, "one"), (2.0, "two"), (3.0, "three"), (4.0, "four")],
        );

        // BZMPOP MAX with count 2
        let result = bzmpop(&store, &manager, 0, &[key.clone()], false, 2, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Array(Some(elements)) = &arr[1] {
                    assert_eq!(elements.len(), 2);
                    // First element should be "four" with score 4
                    if let Frame::Array(Some(pair)) = &elements[0] {
                        if let Frame::Bulk(Some(m)) = &pair[0] {
                            assert_eq!(m.as_ref(), b"four");
                        }
                    }
                    // Second element should be "three" with score 3
                    if let Frame::Array(Some(pair)) = &elements[1] {
                        if let Frame::Bulk(Some(m)) = &pair[0] {
                            assert_eq!(m.as_ref(), b"three");
                        }
                    }
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_bzmpop_timeout() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("empty_zset");

        // BZMPOP on empty key should timeout
        let result = bzmpop(&store, &manager, 0, &[key], true, 1, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_bzmpop_multiple_keys() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key1 = Bytes::from("empty");
        let key2 = Bytes::from("has_data");

        // Only populate second key
        create_sorted_set(&store, &key2, &[(5.0, "five"), (10.0, "ten")]);

        // BZMPOP should return from first non-empty key
        let result = bzmpop(&store, &manager, 0, &[key1, key2.clone()], true, 1, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(k)) = &arr[0] {
                    assert_eq!(k.as_ref(), b"has_data");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_bzmpop_count_exceeds_set_size() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("small_set");

        // Create sorted set with only 2 members
        create_sorted_set(&store, &key, &[(1.0, "one"), (2.0, "two")]);

        // Request 5 elements, should only get 2
        let result = bzmpop(&store, &manager, 0, &[key.clone()], true, 5, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Array(Some(elements)) = &arr[1] {
                    assert_eq!(elements.len(), 2);
                }
            }
            _ => panic!("Expected array result"),
        }

        // Verify set is now empty (deleted)
        assert!(store.get(0, &key).is_none());
    }

    #[tokio::test]
    async fn test_bzmpop_with_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingSortedSetManager::new());
        let key = Bytes::from("myzset");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will add to sorted set after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            create_sorted_set(
                &store_clone,
                &key_clone,
                &[(1.0, "first"), (2.0, "second"), (3.0, "third")],
            );
            notify_sorted_set_add(&manager_clone, 0, &key_clone);
        });

        // BZMPOP should wait and then get the values
        let result = bzmpop(&store, &manager, 0, &[key], true, 2, 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Array(Some(elements)) = &arr[1] {
                    assert_eq!(elements.len(), 2);
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    // ============================================================================
    // BlockingListManager Tests
    // ============================================================================

    #[test]
    fn test_blocking_list_manager_notify_no_waiters() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("test_key");

        // Should not panic when notifying with no waiters
        manager.notify(0, &key);
    }

    #[test]
    fn test_blocking_list_manager_get_receiver() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("test_key");

        // Get receiver should create entry
        let _receiver = manager.get_receiver(0, &key);
        assert!(manager.waiters.contains_key(&(0, key.clone())));
    }

    #[test]
    fn test_blocking_list_manager_cleanup() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("test_key");

        // Get receiver and then drop it
        let receiver = manager.get_receiver(0, &key);
        drop(receiver);

        // Cleanup should remove the entry
        manager.cleanup(0, &key);
        assert!(!manager.waiters.contains_key(&(0, key)));
    }

    #[test]
    fn test_blocking_list_manager_multiple_databases() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("same_key");

        // Same key in different databases should be separate
        let _receiver1 = manager.get_receiver(0, &key);
        let _receiver2 = manager.get_receiver(1, &key);

        assert!(manager.waiters.contains_key(&(0, key.clone())));
        assert!(manager.waiters.contains_key(&(1, key.clone())));
    }

    // ============================================================================
    // BlockingSortedSetManager Tests
    // ============================================================================

    #[test]
    fn test_blocking_sorted_set_manager_notify_no_waiters() {
        let manager = BlockingSortedSetManager::new();
        let key = Bytes::from("test_key");

        // Should not panic when notifying with no waiters
        manager.notify(0, &key);
    }

    #[test]
    fn test_blocking_sorted_set_manager_get_receiver() {
        let manager = BlockingSortedSetManager::new();
        let key = Bytes::from("test_key");

        // Get receiver should create entry
        let _receiver = manager.get_receiver(0, &key);
        assert!(manager.waiters.contains_key(&(0, key.clone())));
    }

    // ============================================================================
    // BlockingStreamManager Tests
    // ============================================================================

    #[test]
    fn test_blocking_stream_manager_notify_no_waiters() {
        let manager = BlockingStreamManager::new();
        let key = Bytes::from("test_key");

        // Should not panic when notifying with no waiters
        manager.notify(0, &key);
    }

    #[test]
    fn test_blocking_stream_manager_get_receiver() {
        let manager = BlockingStreamManager::new();
        let key = Bytes::from("test_key");

        // Get receiver should create entry
        let _receiver = manager.get_receiver(0, &key);
        assert!(manager.waiters.contains_key(&(0, key.clone())));
    }

    #[test]
    fn test_blocking_stream_manager_cleanup() {
        let manager = BlockingStreamManager::new();
        let key = Bytes::from("test_key");

        // Get receiver and then drop it
        let receiver = manager.get_receiver(0, &key);
        drop(receiver);

        // Cleanup should remove the entry
        manager.cleanup(0, &key);
        assert!(!manager.waiters.contains_key(&(0, key)));
    }

    // ============================================================================
    // Helper Function Tests
    // ============================================================================

    #[test]
    fn test_is_empty_result() {
        assert!(is_empty_result(&Frame::Null));
        assert!(is_empty_result(&Frame::Array(None)));
        assert!(is_empty_result(&Frame::Array(Some(vec![]))));
        assert!(!is_empty_result(&Frame::Array(Some(vec![Frame::Integer(
            1
        )]))));
        assert!(!is_empty_result(&Frame::Simple(Bytes::from("OK"))));
    }

    #[tokio::test]
    async fn test_try_pop_from_keys_empty() {
        let store = create_store();
        let key = Bytes::from("empty");

        // Should return None for non-existent key
        let result = try_pop_from_keys(&store, 0, &[key], true);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_pop_from_keys_left() {
        let store = create_store();
        let key = Bytes::from("mylist");

        // Setup list
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("last"));
        store.set(0, key.clone(), Value::List(list));

        // Pop from left
        let result = try_pop_from_keys(&store, 0, &[key.clone()], true);
        assert!(result.is_some());
        let (k, v) = result.unwrap();
        assert_eq!(k.as_ref(), b"mylist");
        assert_eq!(v.as_ref(), b"first");
    }

    #[tokio::test]
    async fn test_try_pop_from_keys_right() {
        let store = create_store();
        let key = Bytes::from("mylist");

        // Setup list
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("last"));
        store.set(0, key.clone(), Value::List(list));

        // Pop from right
        let result = try_pop_from_keys(&store, 0, &[key.clone()], false);
        assert!(result.is_some());
        let (k, v) = result.unwrap();
        assert_eq!(k.as_ref(), b"mylist");
        assert_eq!(v.as_ref(), b"last");
    }

    #[tokio::test]
    async fn test_try_pop_deletes_empty_list() {
        let store = create_store();
        let key = Bytes::from("single");

        // Setup list with single element
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("only"));
        store.set(0, key.clone(), Value::List(list));

        // Pop the only element
        let result = try_pop_from_keys(&store, 0, &[key.clone()], true);
        assert!(result.is_some());

        // Verify list is deleted
        assert!(store.get(0, &key).is_none());
    }

    // ============================================================================
    // Additional Timeout and Edge Case Tests
    // ============================================================================

    #[tokio::test]
    async fn test_blpop_zero_timeout_blocks_indefinitely() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will push after a short delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("delayed_value"));
            store_clone.set(0, key_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &key_clone);
        });

        // BLPOP with timeout 0 should wait indefinitely and get the value
        let result = blpop(&store, &manager, 0, &[key], 0.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"delayed_value");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_brpop_negative_timeout_blocks_indefinitely() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn a task that will push after a short delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("delayed_value"));
            store_clone.set(0, key_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &key_clone);
        });

        // BRPOP with negative timeout should wait indefinitely
        let result = brpop(&store, &manager, 0, &[key], -1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"delayed_value");
                }
            }
            _ => panic!("Expected array result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_blpop_wrong_type() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("not_a_list");

        // Set a non-list value
        store.set(0, key.clone(), Value::String(Bytes::from("string_value")));

        // BLPOP should timeout since it's not a list
        let result = blpop(&store, &manager, 0, &[key], 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_brpop_empty_list_value() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("empty_list");

        // Create an empty list
        let list = VecDeque::new();
        store.set(0, key.clone(), Value::List(list));

        // BRPOP should timeout on empty list
        let result = brpop(&store, &manager, 0, &[key], 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blpop_multiple_waiters() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Spawn first waiter
        let store_clone1 = store.clone();
        let manager_clone1 = manager.clone();
        let key_clone1 = key.clone();
        let waiter1 = tokio::spawn(async move {
            blpop(&store_clone1, &manager_clone1, 0, &[key_clone1], 2.0).await
        });

        // Spawn second waiter
        let store_clone2 = store.clone();
        let manager_clone2 = manager.clone();
        let key_clone2 = key.clone();
        let waiter2 = tokio::spawn(async move {
            blpop(&store_clone2, &manager_clone2, 0, &[key_clone2], 2.0).await
        });

        // Wait a bit to ensure both are waiting
        sleep(Duration::from_millis(50)).await;

        // Push two values
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("first"));
        list.push_back(Bytes::from("second"));
        store.set(0, key.clone(), Value::List(list));

        // Notify both waiters
        notify_list_push(&manager, 0, &key);

        // Both waiters should get values
        let result1 = waiter1.await.unwrap();
        let result2 = waiter2.await.unwrap();

        // At least one should succeed
        let mut success_count = 0;
        if !matches!(result1, Frame::Null) {
            success_count += 1;
        }
        if !matches!(result2, Frame::Null) {
            success_count += 1;
        }
        assert!(success_count >= 1, "At least one waiter should get a value");
    }

    #[tokio::test]
    async fn test_brpoplpush_with_push_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let source_clone = source.clone();

        // Spawn a task that will push after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("pushed_value"));
            store_clone.set(0, source_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &source_clone);
        });

        // BRPOPLPUSH should wait and then get the value
        let result = brpoplpush(&store, &manager, 0, &source, &dest, 1.0).await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"pushed_value");
            }
            _ => panic!("Expected bulk string result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_blmove_with_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let source_clone = source.clone();

        // Spawn a task that will push after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item1"));
            list.push_back(Bytes::from("item2"));
            store_clone.set(0, source_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &source_clone);
        });

        // BLMOVE should wait and then get the value
        let result = blmove(
            &store,
            &manager,
            0,
            &source,
            &dest,
            ListDirection::Left,
            ListDirection::Right,
            1.0,
        )
        .await;

        match result {
            Frame::Bulk(Some(v)) => {
                assert_eq!(v.as_ref(), b"item1");
            }
            _ => panic!("Expected bulk string result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_blmove_all_directions() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());

        // Test LEFT -> LEFT
        {
            let source = Bytes::from("src_ll");
            let dest = Bytes::from("dst_ll");

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            store.set(0, source.clone(), Value::List(list));

            let result = blmove(
                &store,
                &manager,
                0,
                &source,
                &dest,
                ListDirection::Left,
                ListDirection::Left,
                1.0,
            )
            .await;

            if let Frame::Bulk(Some(v)) = result {
                assert_eq!(v.as_ref(), b"a");
            }
        }

        // Test RIGHT -> RIGHT
        {
            let source = Bytes::from("src_rr");
            let dest = Bytes::from("dst_rr");

            let mut list = VecDeque::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            store.set(0, source.clone(), Value::List(list));

            let result = blmove(
                &store,
                &manager,
                0,
                &source,
                &dest,
                ListDirection::Right,
                ListDirection::Right,
                1.0,
            )
            .await;

            if let Frame::Bulk(Some(v)) = result {
                assert_eq!(v.as_ref(), b"b");
            }
        }
    }

    #[tokio::test]
    async fn test_blmpop_empty_list() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("empty");

        // Create empty list
        let list = VecDeque::new();
        store.set(0, key.clone(), Value::List(list));

        // BLMPOP should timeout on empty list
        let result = blmpop(&store, &manager, 0, &[key], ListDirection::Left, 1, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blmpop_wrong_type() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("not_a_list");

        // Set a non-list value
        store.set(0, key.clone(), Value::String(Bytes::from("string_value")));

        // BLMPOP should timeout since it's not a list
        let result = blmpop(&store, &manager, 0, &[key], ListDirection::Left, 1, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    #[tokio::test]
    async fn test_blmpop_zero_count() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Setup list
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        store.set(0, key.clone(), Value::List(list));

        // BLMPOP with count 0 should timeout (no elements to pop)
        let result = blmpop(&store, &manager, 0, &[key], ListDirection::Left, 0, 0.1).await;
        assert!(matches!(result, Frame::Null));
    }

    // ============================================================================
    // Manager Coordination Tests
    // ============================================================================

    #[test]
    fn test_blocking_list_manager_multiple_receivers() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("test_key");

        // Create multiple receivers
        let _receiver1 = manager.get_receiver(0, &key);
        let _receiver2 = manager.get_receiver(0, &key);
        let _receiver3 = manager.get_receiver(0, &key);

        // Should still have the entry
        assert!(manager.waiters.contains_key(&(0, key.clone())));
    }

    #[test]
    fn test_blocking_list_manager_cleanup_with_active_receivers() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("test_key");

        let _receiver = manager.get_receiver(0, &key);

        // Cleanup should not remove entry while receiver exists
        manager.cleanup(0, &key);
        assert!(manager.waiters.contains_key(&(0, key)));
    }

    #[tokio::test]
    async fn test_notification_before_subscribe() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push data first
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("existing"));
        store.set(0, key.clone(), Value::List(list));

        // Notify before anyone is waiting (should be ignored)
        notify_list_push(&manager, 0, &key);

        // BLPOP should still get the value immediately
        let result = blpop(&store, &manager, 0, &[key], 1.0).await;

        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"existing");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    // ============================================================================
    // Cross-Database Tests
    // ============================================================================

    #[tokio::test]
    async fn test_blpop_different_databases() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Push to database 0
        {
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("db0_value"));
            store.set(0, key.clone(), Value::List(list));
        }

        // Push to database 1
        {
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("db1_value"));
            store.set(1, key.clone(), Value::List(list));
        }

        // BLPOP from db 0
        let result0 = blpop(&store, &manager, 0, &[key.clone()], 1.0).await;
        if let Frame::Array(Some(arr)) = result0 {
            if let Frame::Bulk(Some(v)) = &arr[1] {
                assert_eq!(v.as_ref(), b"db0_value");
            }
        }

        // BLPOP from db 1
        let result1 = blpop(&store, &manager, 1, &[key.clone()], 1.0).await;
        if let Frame::Array(Some(arr)) = result1 {
            if let Frame::Bulk(Some(v)) = &arr[1] {
                assert_eq!(v.as_ref(), b"db1_value");
            }
        }
    }

    #[tokio::test]
    async fn test_notification_isolation_between_databases() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        // Waiter on db 0
        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();
        let waiter =
            tokio::spawn(
                async move { blpop(&store_clone, &manager_clone, 0, &[key_clone], 0.2).await },
            );

        sleep(Duration::from_millis(50)).await;

        // Push to db 1 (different database)
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("db1_value"));
        store.set(1, key.clone(), Value::List(list));

        // Notify db 1 (should not wake up db 0 waiter)
        notify_list_push(&manager, 1, &key);

        // Waiter should timeout since notification was for different db
        let result = waiter.await.unwrap();
        assert!(matches!(result, Frame::Null));
    }

    // ============================================================================
    // Try Helper Function Tests
    // ============================================================================

    #[tokio::test]
    async fn test_try_rpop_lpush_source_not_exists() {
        let store = create_store();
        let source = Bytes::from("nonexistent");
        let dest = Bytes::from("dest");

        let result = try_rpop_lpush(&store, 0, &source, &dest);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_rpop_lpush_creates_destination() {
        let store = create_store();
        let source = Bytes::from("source");
        let dest = Bytes::from("new_dest");

        // Setup source
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("value"));
        store.set(0, source.clone(), Value::List(list));

        // Try rpop lpush
        let result = try_rpop_lpush(&store, 0, &source, &dest);
        assert!(result.is_some());

        // Verify destination was created
        assert!(store.get(0, &dest).is_some());
    }

    #[tokio::test]
    async fn test_try_move_all_combinations() {
        let store = create_store();

        // Test all 4 combinations of directions
        let combinations = [
            (ListDirection::Left, ListDirection::Left, "a"),
            (ListDirection::Left, ListDirection::Right, "a"),
            (ListDirection::Right, ListDirection::Left, "c"),
            (ListDirection::Right, ListDirection::Right, "c"),
        ];

        for (idx, (from_dir, to_dir, expected)) in combinations.iter().enumerate() {
            let source = Bytes::from(format!("src_{}", idx));
            let dest = Bytes::from(format!("dst_{}", idx));

            // Setup source with [a, b, c]
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            list.push_back(Bytes::from("c"));
            store.set(0, source.clone(), Value::List(list));

            // Try move
            let result = try_move(&store, 0, &source, &dest, *from_dir, *to_dir);
            assert!(result.is_some());
            assert_eq!(result.unwrap().as_ref(), expected.as_bytes());
        }
    }

    #[tokio::test]
    async fn test_try_mpop_from_keys_honors_count() {
        let store = create_store();
        let key = Bytes::from("mylist");

        // Setup list with 5 elements
        let mut list = VecDeque::new();
        for i in 1..=5 {
            list.push_back(Bytes::from(format!("item{}", i)));
        }
        store.set(0, key.clone(), Value::List(list));

        // Pop 3 elements
        let result = try_mpop_from_keys(&store, 0, &[key.clone()], ListDirection::Left, 3);
        assert!(result.is_some());
        let (_, values) = result.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_ref(), b"item1");
        assert_eq!(values[1].as_ref(), b"item2");
        assert_eq!(values[2].as_ref(), b"item3");

        // Verify 2 elements remain
        if let Some(Value::List(list)) = store.get(0, &key) {
            assert_eq!(list.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_try_zpop_nonexistent_key() {
        let store = create_store();
        let key = Bytes::from("nonexistent");

        let result = try_zpop(&store, 0, &key, true);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_zpop_wrong_type() {
        let store = create_store();
        let key = Bytes::from("not_a_zset");

        // Set a non-sorted-set value
        store.set(0, key.clone(), Value::String(Bytes::from("string_value")));

        let result = try_zpop(&store, 0, &key, true);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_zmpop_first_non_empty() {
        let store = create_store();
        let key1 = Bytes::from("empty1");
        let key2 = Bytes::from("empty2");
        let key3 = Bytes::from("has_data");

        // Only populate third key
        create_sorted_set(&store, &key3, &[(1.0, "one"), (2.0, "two")]);

        // Should skip empty keys and return from third
        let result = try_zmpop(&store, 0, &[key1, key2, key3.clone()], true, 1);
        assert!(result.is_some());
        let (k, _) = result.unwrap();
        assert_eq!(k.as_ref(), b"has_data");
    }

    // ============================================================================
    // Cleanup and Resource Management Tests
    // ============================================================================

    #[test]
    fn test_manager_cleanup_nonexistent_key() {
        let manager = BlockingListManager::new();
        let key = Bytes::from("never_existed");

        // Cleanup on non-existent key should not panic
        manager.cleanup(0, &key);
    }

    #[tokio::test]
    async fn test_multiple_notifications_single_waiter() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let key = Bytes::from("mylist");

        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let key_clone = key.clone();

        // Spawn notifier that sends multiple notifications
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;

            // Send multiple notifications (only last one matters)
            notify_list_push(&manager_clone, 0, &key_clone);
            notify_list_push(&manager_clone, 0, &key_clone);
            notify_list_push(&manager_clone, 0, &key_clone);

            // Now push actual data
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("value"));
            store_clone.set(0, key_clone.clone(), Value::List(list));

            notify_list_push(&manager_clone, 0, &key_clone);
        });

        // Should eventually get the value
        let result = blpop(&store, &manager, 0, &[key], 1.0).await;
        match result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"value");
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[tokio::test]
    async fn test_brpoplpush_destination_notification() {
        let store = create_store();
        let manager = Arc::new(BlockingListManager::new());
        let source = Bytes::from("source");
        let dest = Bytes::from("dest");

        // Setup source
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("value"));
        store.set(0, source.clone(), Value::List(list));

        // Spawn a waiter for destination
        let store_clone = store.clone();
        let manager_clone = manager.clone();
        let dest_clone = dest.clone();
        let dest_waiter = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            blpop(&store_clone, &manager_clone, 0, &[dest_clone], 1.0).await
        });

        // Do BRPOPLPUSH which should notify destination
        let _result = brpoplpush(&store, &manager, 0, &source, &dest, 1.0).await;

        // Destination waiter should get the value
        let dest_result = dest_waiter.await.unwrap();
        match dest_result {
            Frame::Array(Some(arr)) => {
                if let Frame::Bulk(Some(v)) = &arr[1] {
                    assert_eq!(v.as_ref(), b"value");
                }
            }
            _ => panic!("Expected array result for destination waiter"),
        }
    }
}
