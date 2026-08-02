//! Blocking sorted set operations (BZPOPMIN, BZPOPMAX, BZMPOP).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

pub struct BlockingSortedSetManager {
    /// Channel senders for each sorted set key being watched
    pub(super) waiters: DashMap<(u8, Bytes), broadcast::Sender<()>>,
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

/// Shared blocking sorted set manager type
pub type SharedBlockingSortedSetManager = Arc<BlockingSortedSetManager>;

/// Notify the blocking manager when elements are added to a sorted set
pub fn notify_sorted_set_add(manager: &SharedBlockingSortedSetManager, db: u8, key: &Bytes) {
    manager.notify(db, key);
}

/// Try to pop min/max from sorted set
pub(crate) fn try_zpop(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    pop_min: bool,
) -> Option<(Bytes, f64)> {
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
pub(crate) fn try_zmpop(
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
