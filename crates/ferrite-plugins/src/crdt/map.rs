//! CRDT Map implementations

use super::clock::HybridTimestamp;
use super::counter::PNCounter;
use super::types::{SiteId, UniqueTag};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Observed-Remove Map (OR-Map)
///
/// A map CRDT that supports adding and removing key-value pairs with
/// add-wins semantics. Values can be various CRDT types.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{OrMap, SiteId, HybridTimestamp};
/// use bytes::Bytes;
///
/// let site = SiteId::new(1);
///
/// let mut map = OrMap::<Bytes>::new();
/// map.set(
///     Bytes::from("name"),
///     Bytes::from("Alice"),
///     HybridTimestamp::new(1, 0, site),
/// );
///
/// assert_eq!(map.get(&Bytes::from("name")), Some(&Bytes::from("Alice")));
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrMap<V>
where
    V: Clone,
{
    /// Field -> (value, timestamp, tags)
    entries: HashMap<Bytes, MapEntry<V>>,
}

/// Entry in an OR-Map
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MapEntry<V> {
    /// The value
    value: V,
    /// Timestamp of the last update
    timestamp: HybridTimestamp,
    /// Tags for this entry (for add-wins semantics)
    tags: HashSet<UniqueTag>,
    /// Whether this entry has been removed
    removed: bool,
    /// Remove timestamp (if removed)
    remove_time: Option<HybridTimestamp>,
}

impl<V: Clone> OrMap<V> {
    /// Create a new empty OR-Map
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Set a field value
    pub fn set(&mut self, field: Bytes, value: V, timestamp: HybridTimestamp) {
        let tag = UniqueTag::new(timestamp.site, timestamp.physical);
        let entry = self.entries.entry(field).or_insert(MapEntry {
            value: value.clone(),
            timestamp,
            tags: HashSet::new(),
            removed: false,
            remove_time: None,
        });

        if timestamp >= entry.timestamp {
            entry.value = value;
            entry.timestamp = timestamp;
            entry.removed = false;
        }
        entry.tags.insert(tag);
    }

    /// Set with a specific tag
    pub fn set_with_tag(
        &mut self,
        field: Bytes,
        value: V,
        timestamp: HybridTimestamp,
        tag: UniqueTag,
    ) {
        let entry = self.entries.entry(field).or_insert(MapEntry {
            value: value.clone(),
            timestamp,
            tags: HashSet::new(),
            removed: false,
            remove_time: None,
        });

        if timestamp >= entry.timestamp {
            entry.value = value;
            entry.timestamp = timestamp;
            entry.removed = false;
        }
        entry.tags.insert(tag);
    }

    /// Get a field value
    pub fn get(&self, field: &Bytes) -> Option<&V> {
        self.entries.get(field).and_then(|e| {
            if !e.removed || e.remove_time.map_or(true, |rt| e.timestamp > rt) {
                Some(&e.value)
            } else {
                None
            }
        })
    }

    /// Delete a field
    pub fn delete(&mut self, field: &Bytes, timestamp: HybridTimestamp) {
        if let Some(entry) = self.entries.get_mut(field) {
            if entry.remove_time.map_or(true, |rt| timestamp > rt) {
                entry.removed = true;
                entry.remove_time = Some(timestamp);
            }
        }
    }

    /// Check if a field exists
    pub fn contains(&self, field: &Bytes) -> bool {
        self.get(field).is_some()
    }

    /// Get all fields
    pub fn fields(&self) -> impl Iterator<Item = &Bytes> {
        self.entries.iter().filter_map(|(k, e)| {
            if !e.removed || e.remove_time.map_or(true, |rt| e.timestamp > rt) {
                Some(k)
            } else {
                None
            }
        })
    }

    /// Get all values
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().filter_map(|(_, e)| {
            if !e.removed || e.remove_time.map_or(true, |rt| e.timestamp > rt) {
                Some(&e.value)
            } else {
                None
            }
        })
    }

    /// Get all entries as (field, value) pairs
    pub fn entries(&self) -> impl Iterator<Item = (&Bytes, &V)> {
        self.entries.iter().filter_map(|(k, e)| {
            if !e.removed || e.remove_time.map_or(true, |rt| e.timestamp > rt) {
                Some((k, &e.value))
            } else {
                None
            }
        })
    }

    /// Get the number of fields
    pub fn len(&self) -> usize {
        self.entries
            .values()
            .filter(|e| !e.removed || e.remove_time.map_or(true, |rt| e.timestamp > rt))
            .count()
    }

    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merge another map into this one
    pub fn merge(&mut self, other: &OrMap<V>) {
        for (field, other_entry) in &other.entries {
            let entry = self
                .entries
                .entry(field.clone())
                .or_insert_with(|| other_entry.clone());

            // Merge tags
            entry.tags.extend(other_entry.tags.iter().copied());

            // Take the later value
            if other_entry.timestamp > entry.timestamp {
                entry.value = other_entry.value.clone();
                entry.timestamp = other_entry.timestamp;
            }

            // Take the later remove
            if let Some(other_rt) = other_entry.remove_time {
                if entry.remove_time.map_or(true, |rt| other_rt > rt) {
                    entry.remove_time = Some(other_rt);
                }
            }

            // Determine removed status based on timestamps
            if let Some(rt) = entry.remove_time {
                entry.removed = rt > entry.timestamp;
            }
        }
    }
}

impl<V: Clone + PartialEq> PartialEq for OrMap<V> {
    fn eq(&self, other: &Self) -> bool {
        // Compare active entries
        let self_entries: HashMap<_, _> = self
            .entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let other_entries: HashMap<_, _> = other
            .entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self_entries == other_entries
    }
}

impl<V: Clone + Eq> Eq for OrMap<V> {}

/// Counter Map
///
/// A map where values are PN-Counters, useful for per-field counting.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CounterMap {
    /// Field -> counter
    counters: HashMap<Bytes, PNCounter>,
}

impl CounterMap {
    /// Create a new counter map
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Increment a field's counter
    pub fn increment(&mut self, field: Bytes, site: SiteId, delta: u64) {
        self.counters
            .entry(field)
            .or_default()
            .increment(site, delta);
    }

    /// Decrement a field's counter
    pub fn decrement(&mut self, field: Bytes, site: SiteId, delta: u64) {
        self.counters
            .entry(field)
            .or_default()
            .decrement(site, delta);
    }

    /// Get a field's counter value
    pub fn get(&self, field: &Bytes) -> i64 {
        self.counters.get(field).map_or(0, |c| c.value())
    }

    /// Get the counter for a field
    pub fn counter(&self, field: &Bytes) -> Option<&PNCounter> {
        self.counters.get(field)
    }

    /// Get all fields
    pub fn fields(&self) -> impl Iterator<Item = &Bytes> {
        self.counters.keys()
    }

    /// Get all entries
    pub fn entries(&self) -> impl Iterator<Item = (&Bytes, i64)> {
        self.counters.iter().map(|(k, c)| (k, c.value()))
    }

    /// Merge another counter map
    pub fn merge(&mut self, other: &CounterMap) {
        for (field, counter) in &other.counters {
            self.counters
                .entry(field.clone())
                .or_default()
                .merge(counter);
        }
    }

    /// Get the number of fields
    pub fn len(&self) -> usize {
        self.counters.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_or_map_basic() {
        let site = SiteId::new(1);
        let mut map = OrMap::<Bytes>::new();

        map.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            HybridTimestamp::new(1, 0, site),
        );

        assert_eq!(map.get(&Bytes::from("name")), Some(&Bytes::from("Alice")));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_or_map_update() {
        let site = SiteId::new(1);
        let mut map = OrMap::<Bytes>::new();

        map.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            HybridTimestamp::new(1, 0, site),
        );
        map.set(
            Bytes::from("name"),
            Bytes::from("Bob"),
            HybridTimestamp::new(2, 0, site),
        );

        assert_eq!(map.get(&Bytes::from("name")), Some(&Bytes::from("Bob")));
    }

    #[test]
    fn test_or_map_delete() {
        let site = SiteId::new(1);
        let mut map = OrMap::<Bytes>::new();

        map.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            HybridTimestamp::new(1, 0, site),
        );
        map.delete(&Bytes::from("name"), HybridTimestamp::new(2, 0, site));

        assert_eq!(map.get(&Bytes::from("name")), None);
    }

    #[test]
    fn test_or_map_add_after_delete() {
        let site = SiteId::new(1);
        let mut map = OrMap::<Bytes>::new();

        map.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            HybridTimestamp::new(1, 0, site),
        );
        map.delete(&Bytes::from("name"), HybridTimestamp::new(2, 0, site));
        map.set(
            Bytes::from("name"),
            Bytes::from("Bob"),
            HybridTimestamp::new(3, 0, site),
        );

        assert_eq!(map.get(&Bytes::from("name")), Some(&Bytes::from("Bob")));
    }

    #[test]
    fn test_or_map_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut map1 = OrMap::<Bytes>::new();
        map1.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            HybridTimestamp::new(1, 0, site1),
        );

        let mut map2 = OrMap::<Bytes>::new();
        map2.set(
            Bytes::from("name"),
            Bytes::from("Bob"),
            HybridTimestamp::new(2, 0, site2),
        );

        map1.merge(&map2);
        assert_eq!(map1.get(&Bytes::from("name")), Some(&Bytes::from("Bob")));
    }

    #[test]
    fn test_counter_map_basic() {
        let site = SiteId::new(1);
        let mut map = CounterMap::new();

        map.increment(Bytes::from("views"), site, 5);
        assert_eq!(map.get(&Bytes::from("views")), 5);

        map.decrement(Bytes::from("views"), site, 2);
        assert_eq!(map.get(&Bytes::from("views")), 3);
    }

    #[test]
    fn test_counter_map_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut map1 = CounterMap::new();
        map1.increment(Bytes::from("views"), site1, 5);

        let mut map2 = CounterMap::new();
        map2.increment(Bytes::from("views"), site2, 3);

        map1.merge(&map2);
        assert_eq!(map1.get(&Bytes::from("views")), 8);
    }
}
