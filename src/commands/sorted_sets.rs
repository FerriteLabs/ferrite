//! Sorted Set command implementations
//!
//! This module implements Redis sorted set commands.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
/// Add members to a sorted set, or update scores
pub fn zadd(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    options: &ZAddOptions,
    pairs: &[(f64, Bytes)],
) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => (BTreeMap::new(), HashMap::new()),
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for (score, member) in pairs {
        let score = *score;

        if let Some(&old_score) = by_member.get(member) {
            // Member exists
            if options.nx {
                // NX: Only add new elements, skip existing
                continue;
            }

            // Check GT/LT conditions
            if options.gt && score <= old_score {
                continue;
            }
            if options.lt && score >= old_score {
                continue;
            }

            if (score - old_score).abs() > f64::EPSILON {
                // Score changed - update both structures
                by_score.remove(&(OrderedFloat(old_score), member.clone()));
                by_score.insert((OrderedFloat(score), member.clone()), ());
                by_member.insert(member.clone(), score);
                changed += 1;
            }
        } else {
            // Member doesn't exist
            if options.xx {
                // XX: Only update existing elements, skip new
                continue;
            }

            by_score.insert((OrderedFloat(score), member.clone()), ());
            by_member.insert(member.clone(), score);
            added += 1;
            changed += 1;
        }
    }

    store.set(
        db,
        key.clone(),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );

    // CH flag returns number of changed elements, otherwise just added
    if options.ch {
        Frame::Integer(changed)
    } else {
        Frame::Integer(added)
    }
}

/// Options for ZADD command
#[derive(Debug, Default, Clone)]
pub struct ZAddOptions {
    /// NX: Only add new elements
    pub nx: bool,
    /// XX: Only update existing elements
    pub xx: bool,
    /// GT: Only update when new score > current score
    pub gt: bool,
    /// LT: Only update when new score < current score
    pub lt: bool,
    /// CH: Return number of changed elements (added + updated)
    pub ch: bool,
}

/// ZREM key member [member ...]
/// Remove members from a sorted set
pub fn zrem(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    let mut removed = 0i64;

    for member in members {
        if let Some(score) = by_member.remove(member) {
            by_score.remove(&(OrderedFloat(score), member.clone()));
            removed += 1;
        }
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::Integer(removed)
}

/// ZSCORE key member
/// Get the score of a member
pub fn zscore(store: &Arc<Store>, db: u8, key: &Bytes, member: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => match by_member.get(member) {
            Some(&score) => Frame::bulk(format!("{}", score)),
            None => Frame::null(),
        },
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// ZCARD key
/// Get the number of members in a sorted set
pub fn zcard(store: &Arc<Store>, db: u8, key: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => Frame::Integer(by_member.len() as i64),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// ZCOUNT key min max
/// Count members with scores in the given range
pub fn zcount(store: &Arc<Store>, db: u8, key: &Bytes, min: f64, max: f64) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            let count = by_member
                .values()
                .filter(|&&score| score >= min && score <= max)
                .count();
            Frame::Integer(count as i64)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// ZRANK key member
/// Get the rank of a member (0-based, by ascending score)
pub fn zrank(store: &Arc<Store>, db: u8, key: &Bytes, member: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => {
            if let Some(&score) = by_member.get(member) {
                let rank = by_score
                    .range(..(OrderedFloat(score), member.clone()))
                    .count();
                Frame::Integer(rank as i64)
            } else {
                Frame::null()
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// ZREVRANK key member
/// Get the rank of a member (0-based, by descending score)
pub fn zrevrank(store: &Arc<Store>, db: u8, key: &Bytes, member: &Bytes) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => {
            if let Some(&score) = by_member.get(member) {
                // Count elements greater than this one
                let elements_after = by_score
                    .range((OrderedFloat(score), member.clone())..)
                    .count();
                // Subtract 1 because the range includes the element itself
                Frame::Integer((elements_after - 1) as i64)
            } else {
                Frame::null()
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// ZRANGE key start stop [WITHSCORES]
/// Get members by rank range (0-based, inclusive)
pub fn zrange(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    start: i64,
    stop: i64,
    with_scores: bool,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            let len = by_score.len() as i64;

            // Convert negative indices
            let start = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };

            let stop = if stop < 0 {
                (len + stop).max(0) as usize
            } else {
                stop.min(len - 1) as usize
            };

            if start > stop || start >= len as usize {
                return Frame::array(vec![]);
            }

            let items: Vec<Frame> = by_score
                .iter()
                .skip(start)
                .take(stop - start + 1)
                .flat_map(|((score, member), _)| {
                    if with_scores {
                        vec![
                            Frame::bulk(member.clone()),
                            Frame::bulk(format!("{}", score.0)),
                        ]
                    } else {
                        vec![Frame::bulk(member.clone())]
                    }
                })
                .collect();

            Frame::array(items)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZREVRANGE key start stop [WITHSCORES]
/// Get members by rank range in reverse order (highest to lowest score)
pub fn zrevrange(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    start: i64,
    stop: i64,
    with_scores: bool,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            let len = by_score.len() as i64;

            // Convert negative indices
            let start = if start < 0 {
                (len + start).max(0) as usize
            } else {
                start.min(len) as usize
            };

            let stop = if stop < 0 {
                (len + stop).max(0) as usize
            } else {
                stop.min(len - 1) as usize
            };

            if start > stop || start >= len as usize {
                return Frame::array(vec![]);
            }

            let items: Vec<Frame> = by_score
                .iter()
                .rev()
                .skip(start)
                .take(stop - start + 1)
                .flat_map(|((score, member), _)| {
                    if with_scores {
                        vec![
                            Frame::bulk(member.clone()),
                            Frame::bulk(format!("{}", score.0)),
                        ]
                    } else {
                        vec![Frame::bulk(member.clone())]
                    }
                })
                .collect();

            Frame::array(items)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
/// Get members with scores in the given range
#[allow(clippy::too_many_arguments)]
pub fn zrangebyscore(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    min: f64,
    max: f64,
    with_scores: bool,
    offset: Option<usize>,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            let offset = offset.unwrap_or(0);

            let iter = by_score
                .iter()
                .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                .skip(offset);

            let items: Vec<Frame> = if let Some(count) = count {
                iter.take(count)
                    .flat_map(|((score, member), _)| {
                        if with_scores {
                            vec![
                                Frame::bulk(member.clone()),
                                Frame::bulk(format!("{}", score.0)),
                            ]
                        } else {
                            vec![Frame::bulk(member.clone())]
                        }
                    })
                    .collect()
            } else {
                iter.flat_map(|((score, member), _)| {
                    if with_scores {
                        vec![
                            Frame::bulk(member.clone()),
                            Frame::bulk(format!("{}", score.0)),
                        ]
                    } else {
                        vec![Frame::bulk(member.clone())]
                    }
                })
                .collect()
            };

            Frame::array(items)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
/// Get members with scores in the given range (reverse order)
#[allow(clippy::too_many_arguments)]
pub fn zrevrangebyscore(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    max: f64,
    min: f64,
    with_scores: bool,
    offset: Option<usize>,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            let offset = offset.unwrap_or(0);

            let iter = by_score
                .iter()
                .rev()
                .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                .skip(offset);

            let items: Vec<Frame> = if let Some(count) = count {
                iter.take(count)
                    .flat_map(|((score, member), _)| {
                        if with_scores {
                            vec![
                                Frame::bulk(member.clone()),
                                Frame::bulk(format!("{}", score.0)),
                            ]
                        } else {
                            vec![Frame::bulk(member.clone())]
                        }
                    })
                    .collect()
            } else {
                iter.flat_map(|((score, member), _)| {
                    if with_scores {
                        vec![
                            Frame::bulk(member.clone()),
                            Frame::bulk(format!("{}", score.0)),
                        ]
                    } else {
                        vec![Frame::bulk(member.clone())]
                    }
                })
                .collect()
            };

            Frame::array(items)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZINCRBY key increment member
/// Increment the score of a member
pub fn zincrby(store: &Arc<Store>, db: u8, key: &Bytes, increment: f64, member: &Bytes) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => (BTreeMap::new(), HashMap::new()),
    };

    let new_score = if let Some(&old_score) = by_member.get(member) {
        // Remove old entry
        by_score.remove(&(OrderedFloat(old_score), member.clone()));
        old_score + increment
    } else {
        increment
    };

    // Add new entry
    by_score.insert((OrderedFloat(new_score), member.clone()), ());
    by_member.insert(member.clone(), new_score);

    store.set(
        db,
        key.clone(),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );

    Frame::bulk(format!("{}", new_score))
}

/// ZPOPMIN key [count]
/// Remove and return members with lowest scores
pub fn zpopmin(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<usize>) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::array(vec![]),
    };

    let count = count.unwrap_or(1);
    let mut result = Vec::new();

    for _ in 0..count {
        if let Some(((score, member), _)) = by_score.pop_first() {
            by_member.remove(&member);
            result.push(Frame::bulk(member));
            result.push(Frame::bulk(format!("{}", score.0)));
        } else {
            break;
        }
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::array(result)
}

/// ZPOPMAX key [count]
/// Remove and return members with highest scores
pub fn zpopmax(store: &Arc<Store>, db: u8, key: &Bytes, count: Option<usize>) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::array(vec![]),
    };

    let count = count.unwrap_or(1);
    let mut result = Vec::new();

    for _ in 0..count {
        if let Some(((score, member), _)) = by_score.pop_last() {
            by_member.remove(&member);
            result.push(Frame::bulk(member));
            result.push(Frame::bulk(format!("{}", score.0)));
        } else {
            break;
        }
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::array(result)
}

/// ZMSCORE key member [member ...]
/// Get scores for multiple members
pub fn zmscore(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            let scores: Vec<Frame> = members
                .iter()
                .map(|member| match by_member.get(member) {
                    Some(&score) => Frame::bulk(format!("{}", score)),
                    None => Frame::null(),
                })
                .collect();
            Frame::array(scores)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(members.iter().map(|_| Frame::null()).collect()),
    }
}

/// Aggregate function for combining scores
#[derive(Debug, Clone, Copy, Default)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
}

/// ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
/// Return the union of multiple sorted sets
pub fn zunion(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
    withscores: bool,
) -> Frame {
    let result = compute_union(store, db, keys, weights, aggregate);
    sorted_set_to_frame(&result, withscores)
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
/// Store the union of multiple sorted sets
pub fn zunionstore(
    store: &Arc<Store>,
    db: u8,
    destination: &Bytes,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
) -> Frame {
    let result = compute_union(store, db, keys, weights, aggregate);
    let len = result.len() as i64;

    if result.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in result {
            by_score.insert((OrderedFloat(score), member.clone()), ());
            by_member.insert(member, score);
        }
        store.set(
            db,
            destination.clone(),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );
    }

    Frame::Integer(len)
}

/// ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
/// Return the intersection of multiple sorted sets
pub fn zinter(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
    withscores: bool,
) -> Frame {
    let result = compute_intersection(store, db, keys, weights, aggregate);
    sorted_set_to_frame(&result, withscores)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
/// Store the intersection of multiple sorted sets
pub fn zinterstore(
    store: &Arc<Store>,
    db: u8,
    destination: &Bytes,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
) -> Frame {
    let result = compute_intersection(store, db, keys, weights, aggregate);
    let len = result.len() as i64;

    if result.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in result {
            by_score.insert((OrderedFloat(score), member.clone()), ());
            by_member.insert(member, score);
        }
        store.set(
            db,
            destination.clone(),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );
    }

    Frame::Integer(len)
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
/// Return the difference between the first sorted set and all subsequent sets
pub fn zdiff(store: &Arc<Store>, db: u8, keys: &[Bytes], withscores: bool) -> Frame {
    let result = compute_difference(store, db, keys);
    sorted_set_to_frame(&result, withscores)
}

/// ZDIFFSTORE destination numkeys key [key ...]
/// Store the difference between the first sorted set and all subsequent sets
pub fn zdiffstore(store: &Arc<Store>, db: u8, destination: &Bytes, keys: &[Bytes]) -> Frame {
    let result = compute_difference(store, db, keys);
    let len = result.len() as i64;

    if result.is_empty() {
        store.del(db, &[destination.clone()]);
    } else {
        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        for (member, score) in result {
            by_score.insert((OrderedFloat(score), member.clone()), ());
            by_member.insert(member, score);
        }
        store.set(
            db,
            destination.clone(),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );
    }

    Frame::Integer(len)
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
/// Return the cardinality of the intersection of multiple sorted sets
pub fn zintercard(store: &Arc<Store>, db: u8, keys: &[Bytes], limit: Option<usize>) -> Frame {
    if keys.is_empty() {
        return Frame::Integer(0);
    }

    // Get the first sorted set
    let first = match store.get(db, &keys[0]) {
        Some(Value::SortedSet { by_member, .. }) => by_member,
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    // Build list of all sets' members
    let mut sets: Vec<HashMap<Bytes, f64>> = vec![first];
    for key in &keys[1..] {
        match store.get(db, key) {
            Some(Value::SortedSet { by_member, .. }) => sets.push(by_member),
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => return Frame::Integer(0), // Empty intersection
        }
    }

    // Find smallest set to iterate over
    let Some((smallest_idx, _)) = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
    else {
        return Frame::Integer(0);
    };

    let smallest = &sets[smallest_idx];
    let mut count = 0usize;
    let limit = limit.unwrap_or(0);

    for member in smallest.keys() {
        let in_all = sets
            .iter()
            .enumerate()
            .all(|(i, s)| i == smallest_idx || s.contains_key(member));
        if in_all {
            count += 1;
            if limit > 0 && count >= limit {
                return Frame::Integer(limit as i64);
            }
        }
    }

    Frame::Integer(count as i64)
}

// Helper functions for sorted set operations

fn compute_union(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
) -> Vec<(Bytes, f64)> {
    let mut result: HashMap<Bytes, f64> = HashMap::new();

    for (i, key) in keys.iter().enumerate() {
        let weight = weights.and_then(|w| w.get(i).copied()).unwrap_or(1.0);

        if let Some(Value::SortedSet { by_member, .. }) = store.get(db, key) {
            for (member, score) in by_member.iter() {
                let weighted_score = score * weight;
                result
                    .entry(member.clone())
                    .and_modify(|existing| {
                        *existing = apply_aggregate(*existing, weighted_score, aggregate);
                    })
                    .or_insert(weighted_score);
            }
        }
    }

    let mut vec: Vec<_> = result.into_iter().collect();
    vec.sort_by(|a, b| {
        OrderedFloat(a.1)
            .cmp(&OrderedFloat(b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    vec
}

fn compute_intersection(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    weights: Option<&[f64]>,
    aggregate: Aggregate,
) -> Vec<(Bytes, f64)> {
    if keys.is_empty() {
        return vec![];
    }

    // Get first set
    let first_weight = weights.and_then(|w| w.first().copied()).unwrap_or(1.0);
    let mut result: HashMap<Bytes, f64> = match store.get(db, &keys[0]) {
        Some(Value::SortedSet { by_member, .. }) => by_member
            .iter()
            .map(|(m, s)| (m.clone(), s * first_weight))
            .collect(),
        _ => return vec![],
    };

    // Intersect with remaining sets
    for (i, key) in keys.iter().enumerate().skip(1) {
        let weight = weights.and_then(|w| w.get(i).copied()).unwrap_or(1.0);

        match store.get(db, key) {
            Some(Value::SortedSet { by_member, .. }) => {
                result.retain(|member, existing_score| {
                    if let Some(&score) = by_member.get(member) {
                        let weighted_score = score * weight;
                        *existing_score =
                            apply_aggregate(*existing_score, weighted_score, aggregate);
                        true
                    } else {
                        false
                    }
                });
            }
            _ => return vec![],
        }

        if result.is_empty() {
            return vec![];
        }
    }

    let mut vec: Vec<_> = result.into_iter().collect();
    vec.sort_by(|a, b| {
        OrderedFloat(a.1)
            .cmp(&OrderedFloat(b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    vec
}

fn compute_difference(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Vec<(Bytes, f64)> {
    if keys.is_empty() {
        return vec![];
    }

    // Get first set
    let mut result: HashMap<Bytes, f64> = match store.get(db, &keys[0]) {
        Some(Value::SortedSet { by_member, .. }) => {
            by_member.iter().map(|(m, s)| (m.clone(), *s)).collect()
        }
        _ => return vec![],
    };

    // Remove members from subsequent sets
    for key in &keys[1..] {
        if let Some(Value::SortedSet { by_member, .. }) = store.get(db, key) {
            for member in by_member.keys() {
                result.remove(member);
            }
        }
    }

    let mut vec: Vec<_> = result.into_iter().collect();
    vec.sort_by(|a, b| {
        OrderedFloat(a.1)
            .cmp(&OrderedFloat(b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    vec
}

fn apply_aggregate(a: f64, b: f64, aggregate: Aggregate) -> f64 {
    match aggregate {
        Aggregate::Sum => a + b,
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    }
}

fn sorted_set_to_frame(data: &[(Bytes, f64)], withscores: bool) -> Frame {
    if withscores {
        let items: Vec<Frame> = data
            .iter()
            .flat_map(|(member, score)| {
                vec![
                    Frame::bulk(member.clone()),
                    Frame::bulk(format!("{}", score)),
                ]
            })
            .collect();
        Frame::array(items)
    } else {
        let items: Vec<Frame> = data
            .iter()
            .map(|(member, _)| Frame::bulk(member.clone()))
            .collect();
        Frame::array(items)
    }
}

/// Lexicographic range bound for ZRANGEBYLEX commands
#[derive(Debug, Clone)]
pub enum LexBound {
    /// Unbounded (- or +)
    Unbounded,
    /// Inclusive bound [value
    Inclusive(Bytes),
    /// Exclusive bound (value
    Exclusive(Bytes),
}

impl LexBound {
    /// Parse a lex bound from a string like "[a", "(b", "-", "+"
    pub fn parse(s: &str) -> Option<Self> {
        if s == "-" || s == "+" {
            Some(LexBound::Unbounded)
        } else if let Some(rest) = s.strip_prefix('[') {
            Some(LexBound::Inclusive(Bytes::from(rest.to_string())))
        } else { s.strip_prefix('(').map(|rest| LexBound::Exclusive(Bytes::from(rest.to_string()))) }
    }

    fn includes(&self, member: &[u8], is_min: bool) -> bool {
        match self {
            LexBound::Unbounded => true,
            LexBound::Inclusive(bound) => {
                if is_min {
                    member >= bound.as_ref()
                } else {
                    member <= bound.as_ref()
                }
            }
            LexBound::Exclusive(bound) => {
                if is_min {
                    member > bound.as_ref()
                } else {
                    member < bound.as_ref()
                }
            }
        }
    }
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
/// Return members in a sorted set within a lexicographic range
pub fn zrangebylex(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    min: &LexBound,
    max: &LexBound,
    offset: Option<usize>,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            // Get all members sorted by score, then lex
            let mut members: Vec<Bytes> = by_score
                .keys()
                .filter(|(_, member)| min.includes(member, true) && max.includes(member, false))
                .map(|(_, member)| member.clone())
                .collect();

            // Sort lexicographically
            members.sort();

            // Apply offset and count
            let offset = offset.unwrap_or(0);
            let members: Vec<Frame> = members
                .into_iter()
                .skip(offset)
                .take(count.unwrap_or(usize::MAX))
                .map(Frame::bulk)
                .collect();

            Frame::array(members)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
/// Return members in a sorted set within a lexicographic range (reverse order)
pub fn zrevrangebylex(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    max: &LexBound,
    min: &LexBound,
    offset: Option<usize>,
    count: Option<usize>,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            // Get all members sorted by score, then lex
            let mut members: Vec<Bytes> = by_score
                .keys()
                .filter(|(_, member)| min.includes(member, true) && max.includes(member, false))
                .map(|(_, member)| member.clone())
                .collect();

            // Sort lexicographically descending
            members.sort();
            members.reverse();

            // Apply offset and count
            let offset = offset.unwrap_or(0);
            let members: Vec<Frame> = members
                .into_iter()
                .skip(offset)
                .take(count.unwrap_or(usize::MAX))
                .map(Frame::bulk)
                .collect();

            Frame::array(members)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// ZLEXCOUNT key min max
/// Count members in a sorted set within a lexicographic range
pub fn zlexcount(store: &Arc<Store>, db: u8, key: &Bytes, min: &LexBound, max: &LexBound) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_score, .. }) => {
            let count = by_score
                .keys()
                .filter(|(_, member)| min.includes(member, true) && max.includes(member, false))
                .count();

            Frame::Integer(count as i64)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::Integer(0),
    }
}

/// ZREMRANGEBYRANK key start stop
/// Remove members in a sorted set by rank range
pub fn zremrangebyrank(store: &Arc<Store>, db: u8, key: &Bytes, start: i64, stop: i64) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    let len = by_score.len() as i64;

    // Normalize indices
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start.min(len)
    } as usize;
    let stop = if stop < 0 {
        (len + stop).max(0)
    } else {
        stop.min(len - 1)
    } as usize;

    if start > stop || start >= by_score.len() {
        return Frame::Integer(0);
    }

    // Collect members to remove
    let to_remove: Vec<_> = by_score
        .iter()
        .enumerate()
        .filter(|(i, _)| *i >= start && *i <= stop)
        .map(|(_, ((score, member), _))| (*score, member.clone()))
        .collect();

    let removed = to_remove.len();

    // Remove them
    for (score, member) in to_remove {
        by_score.remove(&(score, member.clone()));
        by_member.remove(&member);
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::Integer(removed as i64)
}

/// ZREMRANGEBYSCORE key min max
/// Remove members in a sorted set by score range
pub fn zremrangebyscore(store: &Arc<Store>, db: u8, key: &Bytes, min: f64, max: f64) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    // Collect members to remove
    let to_remove: Vec<_> = by_score
        .iter()
        .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
        .map(|((score, member), _)| (*score, member.clone()))
        .collect();

    let removed = to_remove.len();

    // Remove them
    for (score, member) in to_remove {
        by_score.remove(&(score, member.clone()));
        by_member.remove(&member);
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::Integer(removed as i64)
}

/// ZREMRANGEBYLEX key min max
/// Remove members in a sorted set by lexicographic range
pub fn zremrangebylex(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    min: &LexBound,
    max: &LexBound,
) -> Frame {
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => return Frame::Integer(0),
    };

    // Collect members to remove
    let to_remove: Vec<_> = by_score
        .iter()
        .filter(|((_, member), _)| min.includes(member, true) && max.includes(member, false))
        .map(|((score, member), _)| (*score, member.clone()))
        .collect();

    let removed = to_remove.len();

    // Remove them
    for (score, member) in to_remove {
        by_score.remove(&(score, member.clone()));
        by_member.remove(&member);
    }

    if by_member.is_empty() {
        store.del(db, &[key.clone()]);
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

    Frame::Integer(removed as i64)
}

/// ZRANDMEMBER key [count [WITHSCORES]]
/// Get one or more random members from a sorted set
pub fn zrandmember(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    count: Option<i64>,
    withscores: bool,
) -> Frame {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            if by_member.is_empty() {
                return if count.is_some() {
                    Frame::array(vec![])
                } else {
                    Frame::null()
                };
            }

            let members: Vec<_> = by_member.iter().collect();
            let mut rng = thread_rng();

            match count {
                None => {
                    // Return single random member
                    if let Some(&(member, _)) = members.choose(&mut rng) {
                        Frame::bulk(member.clone())
                    } else {
                        Frame::null()
                    }
                }
                Some(0) => Frame::array(vec![]),
                Some(n) if n > 0 => {
                    // Return up to n unique members
                    let n = n as usize;
                    let selected: Vec<_> = members
                        .choose_multiple(&mut rng, n.min(members.len()))
                        .collect();

                    let items: Vec<Frame> = if withscores {
                        selected
                            .iter()
                            .flat_map(|(member, score)| {
                                vec![
                                    Frame::bulk((*member).clone()),
                                    Frame::bulk(format!("{}", score)),
                                ]
                            })
                            .collect()
                    } else {
                        selected
                            .iter()
                            .map(|(member, _)| Frame::bulk((*member).clone()))
                            .collect()
                    };
                    Frame::array(items)
                }
                Some(n) => {
                    // Negative count: return n members (with possible duplicates)
                    let n = (-n) as usize;
                    let mut selected = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(&(member, score)) = members.choose(&mut rng) {
                            selected.push((member.clone(), *score));
                        }
                    }

                    let items: Vec<Frame> = if withscores {
                        selected
                            .iter()
                            .flat_map(|(member, score)| {
                                vec![
                                    Frame::bulk(member.clone()),
                                    Frame::bulk(format!("{}", score)),
                                ]
                            })
                            .collect()
                    } else {
                        selected
                            .iter()
                            .map(|(member, _)| Frame::bulk(member.clone()))
                            .collect()
                    };
                    Frame::array(items)
                }
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            if count.is_some() {
                Frame::array(vec![])
            } else {
                Frame::null()
            }
        }
    }
}

/// Direction for ZMPOP command
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ZPopDirection {
    /// Pop minimum elements
    Min,
    /// Pop maximum elements
    Max,
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
/// Pop multiple elements from the first non-empty sorted set
pub fn zmpop(
    store: &Arc<Store>,
    db: u8,
    keys: &[Bytes],
    direction: ZPopDirection,
    count: Option<usize>,
) -> Frame {
    let count = count.unwrap_or(1);

    for key in keys {
        let (mut by_score, mut by_member) = match store.get(db, key) {
            Some(Value::SortedSet {
                by_score,
                by_member,
            }) => {
                if by_member.is_empty() {
                    continue;
                }
                (by_score, by_member)
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => continue,
        };

        let mut result = Vec::new();

        for _ in 0..count {
            let item = match direction {
                ZPopDirection::Min => by_score.pop_first(),
                ZPopDirection::Max => by_score.pop_last(),
            };

            if let Some(((score, member), _)) = item {
                by_member.remove(&member);
                result.push(Frame::bulk(member));
                result.push(Frame::bulk(format!("{}", score.0)));
            } else {
                break;
            }
        }

        if by_member.is_empty() {
            store.del(db, &[key.clone()]);
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

        // Return [key, [[member, score], ...]]
        return Frame::array(vec![Frame::bulk(key.clone()), Frame::array(result)]);
    }

    // No non-empty sorted set found
    Frame::null()
}

/// BZPOPMIN key [key ...] timeout
/// Blocking pop minimum from sorted sets
/// Note: This is a non-blocking implementation that checks once and returns
pub fn bzpopmin(store: &Arc<Store>, db: u8, keys: &[Bytes], _timeout: f64) -> Frame {
    for key in keys {
        let (mut by_score, mut by_member) = match store.get(db, key) {
            Some(Value::SortedSet {
                by_score,
                by_member,
            }) => {
                if by_member.is_empty() {
                    continue;
                }
                (by_score, by_member)
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => continue,
        };

        if let Some(((score, member), _)) = by_score.pop_first() {
            by_member.remove(&member);

            if by_member.is_empty() {
                store.del(db, &[key.clone()]);
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

            return Frame::array(vec![
                Frame::bulk(key.clone()),
                Frame::bulk(member),
                Frame::bulk(format!("{}", score.0)),
            ]);
        }
    }

    Frame::null()
}

/// BZPOPMAX key [key ...] timeout
/// Blocking pop maximum from sorted sets
/// Note: This is a non-blocking implementation that checks once and returns
pub fn bzpopmax(store: &Arc<Store>, db: u8, keys: &[Bytes], _timeout: f64) -> Frame {
    for key in keys {
        let (mut by_score, mut by_member) = match store.get(db, key) {
            Some(Value::SortedSet {
                by_score,
                by_member,
            }) => {
                if by_member.is_empty() {
                    continue;
                }
                (by_score, by_member)
            }
            Some(_) => {
                return Frame::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )
            }
            None => continue,
        };

        if let Some(((score, member), _)) = by_score.pop_last() {
            by_member.remove(&member);

            if by_member.is_empty() {
                store.del(db, &[key.clone()]);
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

            return Frame::array(vec![
                Frame::bulk(key.clone()),
                Frame::bulk(member),
                Frame::bulk(format!("{}", score.0)),
            ]);
        }
    }

    Frame::null()
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
/// Blocking version of ZMPOP
/// Note: This is a non-blocking implementation that checks once and returns
pub fn bzmpop(
    store: &Arc<Store>,
    db: u8,
    _timeout: f64,
    keys: &[Bytes],
    direction: ZPopDirection,
    count: Option<usize>,
) -> Frame {
    // For now, just call zmpop since we don't have proper blocking support
    zmpop(store, db, keys, direction, count)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_zadd_basic() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        let result = zadd(
            &store,
            0,
            &key,
            &options,
            &[(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
        );
        assert!(matches!(result, Frame::Integer(2)));

        // Add more, including duplicate
        let result = zadd(
            &store,
            0,
            &key,
            &options,
            &[(3.0, Bytes::from("c")), (1.5, Bytes::from("a"))],
        );
        assert!(matches!(result, Frame::Integer(1))); // Only c is new

        // Check cardinality
        let result = zcard(&store, 0, &key);
        assert!(matches!(result, Frame::Integer(3)));
    }

    #[test]
    fn test_zadd_nx() {
        let store = create_store();
        let key = Bytes::from("zset");
        let default_opts = ZAddOptions::default();
        let nx_opts = ZAddOptions {
            nx: true,
            ..Default::default()
        };

        // Add initial member
        zadd(&store, 0, &key, &default_opts, &[(1.0, Bytes::from("a"))]);

        // Try to update with NX - should not update
        let result = zadd(
            &store,
            0,
            &key,
            &nx_opts,
            &[(5.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
        );
        assert!(matches!(result, Frame::Integer(1))); // Only b added

        // Score of a should still be 1.0
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("1")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_zadd_xx() {
        let store = create_store();
        let key = Bytes::from("zset");
        let default_opts = ZAddOptions::default();
        let xx_opts = ZAddOptions {
            xx: true,
            ..Default::default()
        };

        // Add initial member
        zadd(&store, 0, &key, &default_opts, &[(1.0, Bytes::from("a"))]);

        // Try with XX - should only update a, not add b
        let result = zadd(
            &store,
            0,
            &key,
            &xx_opts,
            &[(5.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
        );
        assert!(matches!(result, Frame::Integer(0))); // No new members

        // Score of a should be 5.0
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("5")),
            _ => panic!("Expected bulk string"),
        }

        // b should not exist
        let result = zscore(&store, 0, &key, &Bytes::from("b"));
        assert!(matches!(result, Frame::Bulk(None)));
    }

    #[test]
    fn test_zadd_gt_lt() {
        let store = create_store();
        let key = Bytes::from("zset");
        let default_opts = ZAddOptions::default();
        let gt_opts = ZAddOptions {
            gt: true,
            ..Default::default()
        };
        let lt_opts = ZAddOptions {
            lt: true,
            ..Default::default()
        };

        // Add initial member with score 5
        zadd(&store, 0, &key, &default_opts, &[(5.0, Bytes::from("a"))]);

        // Try GT with lower score - should not update
        zadd(&store, 0, &key, &gt_opts, &[(3.0, Bytes::from("a"))]);
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("5")),
            _ => panic!("Expected bulk string"),
        }

        // Try GT with higher score - should update
        zadd(&store, 0, &key, &gt_opts, &[(10.0, Bytes::from("a"))]);
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("10")),
            _ => panic!("Expected bulk string"),
        }

        // Try LT with higher score - should not update
        zadd(&store, 0, &key, &lt_opts, &[(15.0, Bytes::from("a"))]);
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("10")),
            _ => panic!("Expected bulk string"),
        }

        // Try LT with lower score - should update
        zadd(&store, 0, &key, &lt_opts, &[(2.0, Bytes::from("a"))]);
        let result = zscore(&store, 0, &key, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("2")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_zrem() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Remove members
        let result = zrem(
            &store,
            0,
            &key,
            &[
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("nonexistent"),
            ],
        );
        assert!(matches!(result, Frame::Integer(2)));

        // Check remaining
        let result = zcard(&store, 0, &key);
        assert!(matches!(result, Frame::Integer(1)));
    }

    #[test]
    fn test_zscore() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add member
        #[allow(clippy::approx_constant)]
        zadd(&store, 0, &key, &options, &[(3.14, Bytes::from("pi"))]);

        // Get score
        let result = zscore(&store, 0, &key, &Bytes::from("pi"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("3.14")),
            _ => panic!("Expected bulk string"),
        }

        // Non-existent member
        let result = zscore(&store, 0, &key, &Bytes::from("nonexistent"));
        assert!(matches!(result, Frame::Bulk(None)));

        // Non-existent key
        let result = zscore(
            &store,
            0,
            &Bytes::from("nonexistent"),
            &Bytes::from("member"),
        );
        assert!(matches!(result, Frame::Bulk(None)));
    }

    #[test]
    fn test_zcount() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
                (4.0, Bytes::from("d")),
                (5.0, Bytes::from("e")),
            ],
        );

        // Count all
        let result = zcount(&store, 0, &key, f64::NEG_INFINITY, f64::INFINITY);
        assert!(matches!(result, Frame::Integer(5)));

        // Count range
        let result = zcount(&store, 0, &key, 2.0, 4.0);
        assert!(matches!(result, Frame::Integer(3)));

        // Count empty range
        let result = zcount(&store, 0, &key, 10.0, 20.0);
        assert!(matches!(result, Frame::Integer(0)));
    }

    #[test]
    fn test_zrank() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Get ranks
        let result = zrank(&store, 0, &key, &Bytes::from("a"));
        assert!(matches!(result, Frame::Integer(0)));

        let result = zrank(&store, 0, &key, &Bytes::from("b"));
        assert!(matches!(result, Frame::Integer(1)));

        let result = zrank(&store, 0, &key, &Bytes::from("c"));
        assert!(matches!(result, Frame::Integer(2)));

        // Non-existent member
        let result = zrank(&store, 0, &key, &Bytes::from("nonexistent"));
        assert!(matches!(result, Frame::Bulk(None)));
    }

    #[test]
    fn test_zrevrank() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Get reverse ranks
        let result = zrevrank(&store, 0, &key, &Bytes::from("c"));
        assert!(matches!(result, Frame::Integer(0)));

        let result = zrevrank(&store, 0, &key, &Bytes::from("b"));
        assert!(matches!(result, Frame::Integer(1)));

        let result = zrevrank(&store, 0, &key, &Bytes::from("a"));
        assert!(matches!(result, Frame::Integer(2)));
    }

    #[test]
    fn test_zrange() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Get range without scores
        let result = zrange(&store, 0, &key, 0, -1, false);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected array"),
        }

        // Get range with scores
        let result = zrange(&store, 0, &key, 0, 1, true);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 4); // 2 members * 2 (member + score)
            }
            _ => panic!("Expected array"),
        }

        // Negative indices
        let result = zrange(&store, 0, &key, -2, -1, false);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrevrange() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Get reverse range
        let result = zrevrange(&store, 0, &key, 0, -1, false);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                // First item should be "c" (highest score)
                match &items[0] {
                    Frame::Bulk(Some(data)) => assert_eq!(data, &Bytes::from("c")),
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrangebyscore() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
                (4.0, Bytes::from("d")),
                (5.0, Bytes::from("e")),
            ],
        );

        // Get by score range
        let result = zrangebyscore(&store, 0, &key, 2.0, 4.0, false, None, None);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected array"),
        }

        // With LIMIT
        let result = zrangebyscore(&store, 0, &key, 1.0, 5.0, false, Some(1), Some(2));
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zincrby() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add member
        zadd(&store, 0, &key, &options, &[(5.0, Bytes::from("a"))]);

        // Increment
        let result = zincrby(&store, 0, &key, 2.5, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("7.5")),
            _ => panic!("Expected bulk string"),
        }

        // Decrement
        let result = zincrby(&store, 0, &key, -10.0, &Bytes::from("a"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("-2.5")),
            _ => panic!("Expected bulk string"),
        }

        // New member
        let result = zincrby(&store, 0, &key, 1.0, &Bytes::from("b"));
        match result {
            Frame::Bulk(Some(data)) => assert_eq!(data, Bytes::from("1")),
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_zpopmin() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Pop minimum
        let result = zpopmin(&store, 0, &key, Some(2));
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 4); // 2 members * 2 (member + score)
            }
            _ => panic!("Expected array"),
        }

        // Check remaining
        let result = zcard(&store, 0, &key);
        assert!(matches!(result, Frame::Integer(1)));
    }

    #[test]
    fn test_zpopmax() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
        );

        // Pop maximum
        let result = zpopmax(&store, 0, &key, Some(1));
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                // First item should be "c"
                match &items[0] {
                    Frame::Bulk(Some(data)) => assert_eq!(data, &Bytes::from("c")),
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zmscore() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members
        zadd(
            &store,
            0,
            &key,
            &options,
            &[(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
        );

        // Get multiple scores
        let result = zmscore(
            &store,
            0,
            &key,
            &[
                Bytes::from("a"),
                Bytes::from("nonexistent"),
                Bytes::from("b"),
            ],
        );
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                // Second should be null
                assert!(matches!(&items[1], Frame::Bulk(None)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_wrongtype_error() {
        let store = create_store();
        let key = Bytes::from("string_key");
        let options = ZAddOptions::default();

        // Set a string value
        store.set(0, key.clone(), Value::String(Bytes::from("value")));

        // Try sorted set operations on string key
        let result = zadd(&store, 0, &key, &options, &[(1.0, Bytes::from("a"))]);
        match result {
            Frame::Error(msg) => {
                let err_str = String::from_utf8_lossy(&msg);
                assert!(err_str.contains("WRONGTYPE"));
            }
            _ => panic!("Expected error"),
        }

        let result = zcard(&store, 0, &key);
        match result {
            Frame::Error(msg) => {
                let err_str = String::from_utf8_lossy(&msg);
                assert!(err_str.contains("WRONGTYPE"));
            }
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_sorted_ordering() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members in random order
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (3.0, Bytes::from("c")),
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
            ],
        );

        // Verify ordering
        let result = zrange(&store, 0, &key, 0, -1, false);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                match (&items[0], &items[1], &items[2]) {
                    (Frame::Bulk(Some(a)), Frame::Bulk(Some(b)), Frame::Bulk(Some(c))) => {
                        assert_eq!(a, &Bytes::from("a"));
                        assert_eq!(b, &Bytes::from("b"));
                        assert_eq!(c, &Bytes::from("c"));
                    }
                    _ => panic!("Expected bulk strings"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_same_score_ordering() {
        let store = create_store();
        let key = Bytes::from("zset");
        let options = ZAddOptions::default();

        // Add members with same score - should be ordered lexicographically
        zadd(
            &store,
            0,
            &key,
            &options,
            &[
                (1.0, Bytes::from("c")),
                (1.0, Bytes::from("a")),
                (1.0, Bytes::from("b")),
            ],
        );

        // Verify lexicographic ordering for same scores
        let result = zrange(&store, 0, &key, 0, -1, false);
        match result {
            Frame::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                match (&items[0], &items[1], &items[2]) {
                    (Frame::Bulk(Some(a)), Frame::Bulk(Some(b)), Frame::Bulk(Some(c))) => {
                        assert_eq!(a, &Bytes::from("a"));
                        assert_eq!(b, &Bytes::from("b"));
                        assert_eq!(c, &Bytes::from("c"));
                    }
                    _ => panic!("Expected bulk strings"),
                }
            }
            _ => panic!("Expected array"),
        }
    }
}
