//! CRDT Set implementations

use super::clock::HybridTimestamp;
use super::types::{SiteId, UniqueTag};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// Observed-Remove Set (OR-Set)
///
/// A set CRDT with add-wins semantics on concurrent add/remove operations.
/// Each element is tagged with unique identifiers that track which additions
/// have been observed.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{OrSet, SiteId};
/// use bytes::Bytes;
///
/// let site = SiteId::new(1);
///
/// let mut set = OrSet::<Bytes>::new();
/// set.add(Bytes::from("hello"), site, 1);
/// set.add(Bytes::from("world"), site, 2);
///
/// assert!(set.contains(&Bytes::from("hello")));
/// assert_eq!(set.len(), 2);
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrSet<T>
where
    T: Hash + Eq + Clone,
{
    /// Element -> set of unique tags
    elements: HashMap<T, HashSet<UniqueTag>>,
}

impl<T: Hash + Eq + Clone> OrSet<T> {
    /// Create a new empty OR-Set
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
        }
    }

    /// Add an element to the set
    pub fn add(&mut self, element: T, site: SiteId, counter: u64) {
        let tag = UniqueTag::new(site, counter);
        self.elements.entry(element).or_default().insert(tag);
    }

    /// Add with an existing tag
    pub fn add_with_tag(&mut self, element: T, tag: UniqueTag) {
        self.elements.entry(element).or_default().insert(tag);
    }

    /// Remove an element from the set
    /// Only removes currently observed tags
    pub fn remove(&mut self, element: &T) {
        self.elements.remove(element);
    }

    /// Remove with a specific tag set
    /// Only removes the specified tags, leaving any new ones
    pub fn remove_tags(&mut self, element: &T, tags_to_remove: &HashSet<UniqueTag>) {
        if let Some(tags) = self.elements.get_mut(element) {
            for tag in tags_to_remove {
                tags.remove(tag);
            }
            if tags.is_empty() {
                self.elements.remove(element);
            }
        }
    }

    /// Check if an element is in the set
    pub fn contains(&self, element: &T) -> bool {
        self.elements
            .get(element)
            .is_some_and(|tags| !tags.is_empty())
    }

    /// Get all members
    pub fn members(&self) -> impl Iterator<Item = &T> {
        self.elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(elem, _)| elem)
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements
            .values()
            .filter(|tags| !tags.is_empty())
            .count()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merge another set into this one
    pub fn merge(&mut self, other: &OrSet<T>) {
        for (elem, tags) in &other.elements {
            self.elements
                .entry(elem.clone())
                .or_default()
                .extend(tags.iter().copied());
        }
    }

    /// Get the tags for an element
    pub fn tags(&self, element: &T) -> Option<&HashSet<UniqueTag>> {
        self.elements.get(element)
    }

    /// Get all elements with their tags
    pub fn elements_with_tags(&self) -> &HashMap<T, HashSet<UniqueTag>> {
        &self.elements
    }
}

impl<T: Hash + Eq + Clone + PartialEq> PartialEq for OrSet<T> {
    fn eq(&self, other: &Self) -> bool {
        self.elements == other.elements
    }
}

impl<T: Hash + Eq + Clone> Eq for OrSet<T> {}

/// LWW-Element Set
///
/// A sorted set CRDT with Last-Writer-Wins semantics per element.
/// Each element has a score, add timestamp, and optional remove timestamp.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{LwwElementSet, HybridTimestamp, SiteId};
/// use bytes::Bytes;
///
/// let site = SiteId::new(1);
///
/// let mut set = LwwElementSet::new();
/// set.add(Bytes::from("alice"), 100.0, HybridTimestamp::new(1, 0, site));
/// set.add(Bytes::from("bob"), 95.0, HybridTimestamp::new(2, 0, site));
///
/// assert_eq!(set.score(&Bytes::from("alice")), Some(100.0));
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LwwElementSet {
    /// Element -> entry with score and timestamps
    elements: HashMap<Bytes, LwwElementEntry>,
}

/// Entry in an LWW element set
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LwwElementEntry {
    /// Element score
    score: f64,
    /// Timestamp when element was added
    add_time: HybridTimestamp,
    /// Timestamp when element was removed (if any)
    remove_time: Option<HybridTimestamp>,
}

impl LwwElementSet {
    /// Create a new empty LWW element set
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
        }
    }

    /// Add an element with a score
    pub fn add(&mut self, element: Bytes, score: f64, timestamp: HybridTimestamp) {
        let entry = self.elements.entry(element).or_insert(LwwElementEntry {
            score,
            add_time: timestamp,
            remove_time: None,
        });

        if timestamp > entry.add_time {
            entry.score = score;
            entry.add_time = timestamp;
        }
    }

    /// Remove an element
    pub fn remove(&mut self, element: &Bytes, timestamp: HybridTimestamp) {
        if let Some(entry) = self.elements.get_mut(element) {
            if entry.remove_time.map_or(true, |t| timestamp > t) {
                entry.remove_time = Some(timestamp);
            }
        }
    }

    /// Get the score of an element
    pub fn score(&self, element: &Bytes) -> Option<f64> {
        self.elements.get(element).and_then(|e| {
            // Element exists if add_time > remove_time
            if e.remove_time.map_or(true, |rt| e.add_time > rt) {
                Some(e.score)
            } else {
                None
            }
        })
    }

    /// Check if an element exists
    pub fn contains(&self, element: &Bytes) -> bool {
        self.score(element).is_some()
    }

    /// Get all members with their scores
    pub fn members_with_scores(&self) -> Vec<(&Bytes, f64)> {
        self.elements
            .iter()
            .filter_map(|(elem, entry)| {
                if entry.remove_time.map_or(true, |rt| entry.add_time > rt) {
                    Some((elem, entry.score))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get members in score order (ascending)
    pub fn members_by_score(&self) -> Vec<(&Bytes, f64)> {
        let mut members = self.members_with_scores();
        members.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        members
    }

    /// Get members in score order (descending)
    pub fn members_by_score_desc(&self) -> Vec<(&Bytes, f64)> {
        let mut members = self.members_with_scores();
        members.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        members
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements
            .values()
            .filter(|e| e.remove_time.map_or(true, |rt| e.add_time > rt))
            .count()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Increment the score of an element
    pub fn incr(&mut self, element: Bytes, delta: f64, timestamp: HybridTimestamp) {
        let entry = self.elements.entry(element).or_insert(LwwElementEntry {
            score: 0.0,
            add_time: timestamp,
            remove_time: None,
        });

        if timestamp >= entry.add_time {
            entry.score += delta;
            entry.add_time = timestamp;
        }
    }

    /// Merge another set into this one
    pub fn merge(&mut self, other: &LwwElementSet) {
        for (elem, other_entry) in &other.elements {
            let entry = self
                .elements
                .entry(elem.clone())
                .or_insert_with(|| other_entry.clone());

            // Take the later add
            if other_entry.add_time > entry.add_time {
                entry.score = other_entry.score;
                entry.add_time = other_entry.add_time;
            }

            // Take the later remove
            if let Some(other_rt) = other_entry.remove_time {
                if entry.remove_time.map_or(true, |rt| other_rt > rt) {
                    entry.remove_time = Some(other_rt);
                }
            }
        }
    }

    /// Get range by score
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(&Bytes, f64)> {
        self.members_with_scores()
            .into_iter()
            .filter(|(_, score)| *score >= min && *score <= max)
            .collect()
    }

    /// Get range by rank (index)
    pub fn range_by_rank(&self, start: usize, stop: usize) -> Vec<(&Bytes, f64)> {
        let members = self.members_by_score();
        let end = (stop + 1).min(members.len());
        if start >= members.len() {
            return Vec::new();
        }
        members[start..end].to_vec()
    }
}

impl PartialEq for LwwElementSet {
    fn eq(&self, other: &Self) -> bool {
        self.elements.len() == other.elements.len()
            && self.elements.iter().all(|(k, v)| {
                other.elements.get(k).is_some_and(|ov| {
                    v.score == ov.score
                        && v.add_time == ov.add_time
                        && v.remove_time == ov.remove_time
                })
            })
    }
}

impl Eq for LwwElementSet {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_or_set_basic() {
        let site = SiteId::new(1);
        let mut set = OrSet::<Bytes>::new();

        set.add(Bytes::from("hello"), site, 1);
        assert!(set.contains(&Bytes::from("hello")));
        assert_eq!(set.len(), 1);

        set.add(Bytes::from("world"), site, 2);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_or_set_remove() {
        let site = SiteId::new(1);
        let mut set = OrSet::<Bytes>::new();

        set.add(Bytes::from("hello"), site, 1);
        assert!(set.contains(&Bytes::from("hello")));

        set.remove(&Bytes::from("hello"));
        assert!(!set.contains(&Bytes::from("hello")));
    }

    #[test]
    fn test_or_set_add_wins() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        // Site 1 adds element
        let mut set1 = OrSet::<Bytes>::new();
        set1.add(Bytes::from("x"), site1, 1);

        // Site 2 removes element (saw it from site1)
        let mut set2 = set1.clone();
        set2.remove(&Bytes::from("x"));

        // Site 1 adds again concurrently
        set1.add(Bytes::from("x"), site1, 2);

        // Merge
        set1.merge(&set2);
        set2.merge(&set1);

        // Add should win
        assert!(set1.contains(&Bytes::from("x")));
        assert!(set2.contains(&Bytes::from("x")));
    }

    #[test]
    fn test_or_set_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut set1 = OrSet::<Bytes>::new();
        set1.add(Bytes::from("a"), site1, 1);

        let mut set2 = OrSet::<Bytes>::new();
        set2.add(Bytes::from("b"), site2, 1);

        set1.merge(&set2);
        assert!(set1.contains(&Bytes::from("a")));
        assert!(set1.contains(&Bytes::from("b")));
    }

    #[test]
    fn test_lww_element_set_basic() {
        let site = SiteId::new(1);
        let mut set = LwwElementSet::new();

        set.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site),
        );
        assert_eq!(set.score(&Bytes::from("alice")), Some(100.0));

        set.add(Bytes::from("bob"), 95.0, HybridTimestamp::new(2, 0, site));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_lww_element_set_remove() {
        let site = SiteId::new(1);
        let mut set = LwwElementSet::new();

        set.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site),
        );
        set.remove(&Bytes::from("alice"), HybridTimestamp::new(2, 0, site));

        assert_eq!(set.score(&Bytes::from("alice")), None);
    }

    #[test]
    fn test_lww_element_set_add_after_remove() {
        let site = SiteId::new(1);
        let mut set = LwwElementSet::new();

        set.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site),
        );
        set.remove(&Bytes::from("alice"), HybridTimestamp::new(2, 0, site));
        set.add(
            Bytes::from("alice"),
            200.0,
            HybridTimestamp::new(3, 0, site),
        );

        assert_eq!(set.score(&Bytes::from("alice")), Some(200.0));
    }

    #[test]
    fn test_lww_element_set_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut set1 = LwwElementSet::new();
        set1.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site1),
        );

        let mut set2 = LwwElementSet::new();
        set2.add(
            Bytes::from("alice"),
            200.0,
            HybridTimestamp::new(2, 0, site2),
        );

        set1.merge(&set2);
        assert_eq!(set1.score(&Bytes::from("alice")), Some(200.0));
    }

    #[test]
    fn test_lww_element_set_range() {
        let site = SiteId::new(1);
        let mut set = LwwElementSet::new();

        set.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site),
        );
        set.add(Bytes::from("bob"), 95.0, HybridTimestamp::new(2, 0, site));
        set.add(
            Bytes::from("carol"),
            105.0,
            HybridTimestamp::new(3, 0, site),
        );

        let range = set.range_by_score(96.0, 101.0);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].1, 100.0);
    }

    #[test]
    fn test_lww_element_set_rank() {
        let site = SiteId::new(1);
        let mut set = LwwElementSet::new();

        set.add(
            Bytes::from("alice"),
            100.0,
            HybridTimestamp::new(1, 0, site),
        );
        set.add(Bytes::from("bob"), 95.0, HybridTimestamp::new(2, 0, site));
        set.add(
            Bytes::from("carol"),
            105.0,
            HybridTimestamp::new(3, 0, site),
        );

        let range = set.range_by_rank(0, 1);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].1, 95.0); // bob
        assert_eq!(range[1].1, 100.0); // alice
    }
}
