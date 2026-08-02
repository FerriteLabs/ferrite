//! Blocking stream operations (XREAD BLOCK, XREADGROUP BLOCK).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::time::timeout;

use super::is_empty_result;
use crate::commands::streams;
use crate::protocol::Frame;
use crate::storage::Store;

pub struct BlockingStreamManager {
    /// Channel senders for each stream key being watched
    pub(super) waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
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
    pub(crate) fn get_receiver(&self, db: u8, key: &Bytes) -> broadcast::Receiver<()> {
        let key_tuple = (db, key.clone());
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
