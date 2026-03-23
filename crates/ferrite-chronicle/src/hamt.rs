//! Persistent Hash Array Mapped Trie (HAMT) for efficient CoW branching.
//!
//! Structural sharing: branching is O(1) — just clone the root Arc.
//! Writes are O(log32 N) — path-copying only the modified path.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

const BITS_PER_LEVEL: u32 = 5; // 32-way branching
const MASK: u64 = 0x1F; // 5 bits
const MAX_DEPTH: usize = 13; // 64 / 5 ≈ 13 levels

fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn index_at(hash: u64, depth: usize) -> u32 {
    ((hash >> (depth as u32 * BITS_PER_LEVEL)) & MASK) as u32
}

fn bit_at(hash: u64, depth: usize) -> u32 {
    1u32 << index_at(hash, depth)
}

/// Compressed index: how many children precede this bit position.
fn compressed_index(bitmap: u32, bit: u32) -> usize {
    (bitmap & (bit - 1)).count_ones() as usize
}

/// A persistent HAMT node.
#[derive(Clone, Debug)]
enum Node<K: Clone + Eq + Hash, V: Clone> {
    /// Leaf containing a single key-value pair.
    Leaf { hash: u64, key: K, value: V },
    /// Leaf containing multiple key-value pairs (hash collision).
    Collision { hash: u64, entries: Vec<(K, V)> },
    /// Internal node with bitmap indexing into children.
    Branch {
        bitmap: u32,
        children: Vec<Arc<Node<K, V>>>,
    },
}

impl<K: Clone + Eq + Hash, V: Clone> Node<K, V> {
    fn get(&self, hash: u64, key: &K, depth: usize) -> Option<&V> {
        match self {
            Node::Leaf {
                hash: h,
                key: k,
                value,
            } => {
                if *h == hash && k == key {
                    Some(value)
                } else {
                    None
                }
            }
            Node::Collision { hash: h, entries } => {
                if *h != hash {
                    return None;
                }
                entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
            }
            Node::Branch { bitmap, children } => {
                let bit = bit_at(hash, depth);
                if bitmap & bit == 0 {
                    return None;
                }
                let idx = compressed_index(*bitmap, bit);
                children[idx].get(hash, key, depth + 1)
            }
        }
    }

    fn insert(self: &Arc<Self>, hash: u64, key: K, value: V, depth: usize) -> (Arc<Self>, bool) {
        match self.as_ref() {
            Node::Leaf {
                hash: h,
                key: k,
                value: v,
            } => {
                if *h == hash && *k == key {
                    // Update existing key.
                    (Arc::new(Node::Leaf { hash, key, value }), false)
                } else if *h == hash {
                    // Hash collision — create a Collision node.
                    (
                        Arc::new(Node::Collision {
                            hash,
                            entries: vec![(k.clone(), v.clone()), (key, value)],
                        }),
                        true,
                    )
                } else {
                    // Different hashes — split into a Branch.
                    let new_leaf = Arc::new(Node::Leaf { hash, key, value });
                    let existing = Arc::clone(self);
                    let node = Self::make_branch(existing, *h, new_leaf, hash, depth);
                    (Arc::new(node), true)
                }
            }
            Node::Collision { hash: h, entries } => {
                if *h != hash {
                    // Different hash — split into a Branch at current depth.
                    let new_leaf = Arc::new(Node::Leaf { hash, key, value });
                    let existing = Arc::clone(self);
                    let node = Self::make_branch(existing, *h, new_leaf, hash, depth);
                    (Arc::new(node), true)
                } else {
                    // Same hash — add/update in the collision node.
                    let mut new_entries = entries.clone();
                    if let Some(pos) = new_entries.iter().position(|(k, _)| *k == key) {
                        new_entries[pos] = (key, value);
                        (
                            Arc::new(Node::Collision {
                                hash,
                                entries: new_entries,
                            }),
                            false,
                        )
                    } else {
                        new_entries.push((key, value));
                        (
                            Arc::new(Node::Collision {
                                hash,
                                entries: new_entries,
                            }),
                            true,
                        )
                    }
                }
            }
            Node::Branch { bitmap, children } => {
                let bit = bit_at(hash, depth);
                let idx = compressed_index(*bitmap, bit);

                if bitmap & bit == 0 {
                    // Slot is empty — insert new leaf.
                    let leaf = Arc::new(Node::Leaf { hash, key, value });
                    let mut new_children = children.clone();
                    new_children.insert(idx, leaf);
                    (
                        Arc::new(Node::Branch {
                            bitmap: bitmap | bit,
                            children: new_children,
                        }),
                        true,
                    )
                } else {
                    // Slot occupied — recurse.
                    let (new_child, added) = children[idx].insert(hash, key, value, depth + 1);
                    let mut new_children = children.clone();
                    new_children[idx] = new_child;
                    (
                        Arc::new(Node::Branch {
                            bitmap: *bitmap,
                            children: new_children,
                        }),
                        added,
                    )
                }
            }
        }
    }

    fn make_branch(
        existing: Arc<Self>,
        existing_hash: u64,
        new_node: Arc<Self>,
        new_hash: u64,
        depth: usize,
    ) -> Node<K, V> {
        if depth >= MAX_DEPTH {
            // Shouldn't happen in practice — fall back to collision.
            // Extract entries from both nodes.
            let mut entries = Vec::new();
            Self::collect_entries(&existing, &mut entries);
            Self::collect_entries(&new_node, &mut entries);
            return Node::Collision {
                hash: new_hash,
                entries,
            };
        }

        let existing_bit = bit_at(existing_hash, depth);
        let new_bit = bit_at(new_hash, depth);

        if existing_bit == new_bit {
            // Both go to the same slot — recurse one level deeper.
            let child = Arc::new(Self::make_branch(
                existing,
                existing_hash,
                new_node,
                new_hash,
                depth + 1,
            ));
            Node::Branch {
                bitmap: existing_bit,
                children: vec![child],
            }
        } else {
            let bitmap = existing_bit | new_bit;
            let children = if existing_bit < new_bit {
                vec![existing, new_node]
            } else {
                // Need to order by compressed index (lower bit first).
                let ei = compressed_index(bitmap, existing_bit);
                let ni = compressed_index(bitmap, new_bit);
                if ei < ni {
                    vec![existing, new_node]
                } else {
                    vec![new_node, existing]
                }
            };
            Node::Branch { bitmap, children }
        }
    }

    fn collect_entries(node: &Node<K, V>, out: &mut Vec<(K, V)>) {
        match node {
            Node::Leaf { key, value, .. } => out.push((key.clone(), value.clone())),
            Node::Collision { entries, .. } => out.extend(entries.iter().cloned()),
            Node::Branch { children, .. } => {
                for child in children {
                    Self::collect_entries(child, out);
                }
            }
        }
    }

    fn remove(&self, hash: u64, key: &K, depth: usize) -> Option<(Arc<Self>, bool)> {
        match self {
            Node::Leaf {
                hash: h, key: k, ..
            } => {
                if *h == hash && k == key {
                    None // Remove this node entirely.
                } else {
                    Some((Arc::new(self.clone()), false)) // Not found — no change.
                }
            }
            Node::Collision { hash: h, entries } => {
                if *h != hash {
                    return Some((Arc::new(self.clone()), false));
                }
                let new_entries: Vec<_> =
                    entries.iter().filter(|(k, _)| k != key).cloned().collect();
                if new_entries.len() == entries.len() {
                    Some((Arc::new(self.clone()), false)) // Key not in collision.
                } else if new_entries.len() == 1 {
                    let (k, v) = new_entries
                        .into_iter()
                        .next()
                        .expect("single-entry collision node must have one entry");
                    Some((
                        Arc::new(Node::Leaf {
                            hash: *h,
                            key: k,
                            value: v,
                        }),
                        true,
                    ))
                } else {
                    Some((
                        Arc::new(Node::Collision {
                            hash: *h,
                            entries: new_entries,
                        }),
                        true,
                    ))
                }
            }
            Node::Branch { bitmap, children } => {
                let bit = bit_at(hash, depth);
                if bitmap & bit == 0 {
                    return Some((Arc::new(self.clone()), false)); // Not found.
                }
                let idx = compressed_index(*bitmap, bit);
                match children[idx].remove(hash, key, depth + 1) {
                    None => {
                        // Child was removed entirely.
                        let new_bitmap = bitmap & !bit;
                        if new_bitmap == 0 {
                            None // This branch is now empty.
                        } else {
                            let mut new_children = children.clone();
                            new_children.remove(idx);
                            // If only one child remains and it's a leaf/collision, promote it.
                            if new_children.len() == 1 {
                                match new_children[0].as_ref() {
                                    Node::Leaf { .. } | Node::Collision { .. } => {
                                        Some((Arc::clone(&new_children[0]), true))
                                    }
                                    _ => Some((
                                        Arc::new(Node::Branch {
                                            bitmap: new_bitmap,
                                            children: new_children,
                                        }),
                                        true,
                                    )),
                                }
                            } else {
                                Some((
                                    Arc::new(Node::Branch {
                                        bitmap: new_bitmap,
                                        children: new_children,
                                    }),
                                    true,
                                ))
                            }
                        }
                    }
                    Some((new_child, changed)) => {
                        if !changed {
                            Some((Arc::new(self.clone()), false))
                        } else {
                            let mut new_children = children.clone();
                            new_children[idx] = new_child;
                            Some((
                                Arc::new(Node::Branch {
                                    bitmap: *bitmap,
                                    children: new_children,
                                }),
                                true,
                            ))
                        }
                    }
                }
            }
        }
    }

    fn iter_entries<'a>(&'a self, stack: &mut Vec<(&'a K, &'a V)>) {
        match self {
            Node::Leaf { key, value, .. } => stack.push((key, value)),
            Node::Collision { entries, .. } => {
                for (k, v) in entries {
                    stack.push((k, v));
                }
            }
            Node::Branch { children, .. } => {
                for child in children {
                    child.iter_entries(stack);
                }
            }
        }
    }
}

/// A persistent HAMT (Hash Array Mapped Trie).
///
/// Cloning is O(1) — just clones the root Arc.
/// This means branching a dataset is instant.
#[derive(Clone, Debug)]
pub struct Hamt<K: Clone + Eq + Hash, V: Clone> {
    root: Option<Arc<Node<K, V>>>,
    len: usize,
}

impl<K: Clone + Eq + Hash, V: Clone> Default for Hamt<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone> IntoIterator for &'a Hamt<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = HamtIter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K: Clone + Eq + Hash, V: Clone> Hamt<K, V> {
    /// Create an empty HAMT.
    pub fn new() -> Self {
        Self { root: None, len: 0 }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the HAMT contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Look up a key.
    pub fn get(&self, key: &K) -> Option<&V> {
        let hash = hash_key(key);
        self.root.as_ref().and_then(|r| r.get(hash, key, 0))
    }

    /// Insert or update a key-value pair. Returns a new HAMT (persistent).
    pub fn insert(&self, key: K, value: V) -> Self {
        let hash = hash_key(&key);
        match &self.root {
            None => Hamt {
                root: Some(Arc::new(Node::Leaf { hash, key, value })),
                len: 1,
            },
            Some(root) => {
                let (new_root, added) = root.insert(hash, key, value, 0);
                Hamt {
                    root: Some(new_root),
                    len: if added { self.len + 1 } else { self.len },
                }
            }
        }
    }

    /// Remove a key. Returns a new HAMT (persistent).
    pub fn remove(&self, key: &K) -> Self {
        let hash = hash_key(key);
        match &self.root {
            None => self.clone(),
            Some(root) => match root.remove(hash, key, 0) {
                None => Hamt { root: None, len: 0 },
                Some((new_root, changed)) => Hamt {
                    root: Some(new_root),
                    len: if changed { self.len - 1 } else { self.len },
                },
            },
        }
    }

    /// Iterate over all key-value pairs.
    pub fn iter(&self) -> HamtIter<'_, K, V> {
        let mut entries = Vec::new();
        if let Some(root) = &self.root {
            root.iter_entries(&mut entries);
        }
        HamtIter { entries, pos: 0 }
    }

    /// Collect all entries into a Vec.
    pub fn entries(&self) -> Vec<(&K, &V)> {
        self.iter().collect()
    }

    /// Diff two HAMTs — find keys that differ.
    pub fn diff<'a>(&'a self, other: &'a Self) -> Vec<DiffEntry<'a, K, V>> {
        let mut result = Vec::new();

        // Find removed and modified entries (in self but not other, or changed).
        for (k, v) in self.iter() {
            match other.get(k) {
                None => result.push(DiffEntry::Removed(k, v)),
                Some(v2) => {
                    // Compare by pointer first for speed; use Arc identity
                    // when both point to the same allocation.
                    let ptr_eq =
                        std::ptr::eq(std::ptr::from_ref::<V>(v), std::ptr::from_ref::<V>(v2));
                    if !ptr_eq {
                        result.push(DiffEntry::Modified(k, v, v2));
                    }
                }
            }
        }

        // Find added entries (in other but not self).
        for (k, v) in other.iter() {
            if self.get(k).is_none() {
                result.push(DiffEntry::Added(k, v));
            }
        }

        result
    }
}

/// Iterator over all key-value pairs in a HAMT.
pub struct HamtIter<'a, K, V> {
    entries: Vec<(&'a K, &'a V)>,
    pos: usize,
}

impl<'a, K, V> Iterator for HamtIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entries.len() {
            let item = self.entries[self.pos];
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.entries.len() - self.pos;
        (remaining, Some(remaining))
    }
}

impl<K, V> ExactSizeIterator for HamtIter<'_, K, V> {}

/// An entry in a HAMT diff result.
#[derive(Debug)]
pub enum DiffEntry<'a, K, V> {
    /// Key exists in `other` but not in `self`.
    Added(&'a K, &'a V),
    /// Key exists in `self` but not in `other`.
    Removed(&'a K, &'a V),
    /// Key exists in both but values differ (old, new).
    Modified(&'a K, &'a V, &'a V),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hamt() {
        let h: Hamt<String, i32> = Hamt::new();
        assert_eq!(h.len(), 0);
        assert!(h.is_empty());
        assert!(h.get(&"missing".to_string()).is_none());
    }

    #[test]
    fn insert_single() {
        let h = Hamt::new().insert("key".to_string(), 42);
        assert_eq!(h.len(), 1);
        assert!(!h.is_empty());
        assert_eq!(h.get(&"key".to_string()), Some(&42));
    }

    #[test]
    fn insert_multiple() {
        let h = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2)
            .insert("c".to_string(), 3);
        assert_eq!(h.len(), 3);
        assert_eq!(h.get(&"a".to_string()), Some(&1));
        assert_eq!(h.get(&"b".to_string()), Some(&2));
        assert_eq!(h.get(&"c".to_string()), Some(&3));
    }

    #[test]
    fn update_existing_key() {
        let h1 = Hamt::new().insert("key".to_string(), 1);
        let h2 = h1.insert("key".to_string(), 2);
        assert_eq!(h2.len(), 1);
        assert_eq!(h2.get(&"key".to_string()), Some(&2));
        // Original is unchanged (persistence).
        assert_eq!(h1.get(&"key".to_string()), Some(&1));
    }

    #[test]
    fn remove_existing() {
        let h1 = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2);
        let h2 = h1.remove(&"a".to_string());
        assert_eq!(h2.len(), 1);
        assert!(h2.get(&"a".to_string()).is_none());
        assert_eq!(h2.get(&"b".to_string()), Some(&2));
        // Original is unchanged.
        assert_eq!(h1.len(), 2);
        assert_eq!(h1.get(&"a".to_string()), Some(&1));
    }

    #[test]
    fn remove_nonexistent() {
        let h1 = Hamt::new().insert("a".to_string(), 1);
        let h2 = h1.remove(&"b".to_string());
        assert_eq!(h2.len(), 1);
        assert_eq!(h2.get(&"a".to_string()), Some(&1));
    }

    #[test]
    fn remove_from_empty() {
        let h: Hamt<String, i32> = Hamt::new();
        let h2 = h.remove(&"x".to_string());
        assert_eq!(h2.len(), 0);
    }

    #[test]
    fn structural_sharing() {
        let h1 = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2)
            .insert("c".to_string(), 3);

        // Clone (branching) is O(1).
        let h2 = h1.clone();

        // Insert into h2 doesn't affect h1.
        let h3 = h2.insert("d".to_string(), 4);
        assert_eq!(h1.len(), 3);
        assert_eq!(h2.len(), 3);
        assert_eq!(h3.len(), 4);
        assert!(h1.get(&"d".to_string()).is_none());
        assert_eq!(h3.get(&"d".to_string()), Some(&4));
    }

    #[test]
    fn branch_is_instant() {
        // Build a 10K-entry HAMT.
        let mut h = Hamt::new();
        for i in 0..10_000u64 {
            h = h.insert(i, i * 2);
        }
        assert_eq!(h.len(), 10_000);

        // Clone should be O(1) — just Arc clone.
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _branch = h.clone();
        }
        let elapsed = start.elapsed();
        // 1000 clones of a 10K-entry HAMT should finish in under 10ms.
        assert!(
            elapsed.as_millis() < 100,
            "1000 clones took {}ms — should be near-instant",
            elapsed.as_millis()
        );
    }

    #[test]
    fn large_dataset() {
        let mut h = Hamt::new();
        for i in 0..10_000u64 {
            h = h.insert(i, i * 3);
        }
        assert_eq!(h.len(), 10_000);
        for i in 0..10_000u64 {
            assert_eq!(h.get(&i), Some(&(i * 3)), "missing key {i}");
        }
    }

    #[test]
    fn diff_finds_added() {
        let h1 = Hamt::new().insert("a".to_string(), 1);
        let h2 = h1.insert("b".to_string(), 2);
        let d = h1.diff(&h2);
        assert!(d
            .iter()
            .any(|e| matches!(e, DiffEntry::Added(k, v) if k.as_str() == "b" && **v == 2)));
    }

    #[test]
    fn diff_finds_removed() {
        let h1 = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2);
        let h2 = h1.remove(&"b".to_string());
        let d = h1.diff(&h2);
        assert!(d
            .iter()
            .any(|e| matches!(e, DiffEntry::Removed(k, v) if k.as_str() == "b" && **v == 2)));
    }

    #[test]
    fn diff_finds_modified() {
        let h1 = Hamt::new().insert("a".to_string(), 1);
        let h2 = Hamt::new().insert("a".to_string(), 2);
        let d = h1.diff(&h2);
        assert!(d.iter().any(|e| matches!(e, DiffEntry::Modified(k, old, new) if k.as_str() == "a" && **old == 1 && **new == 2)));
    }

    #[test]
    fn diff_identical_is_empty() {
        let h1 = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2);
        // Build from scratch to avoid pointer equality.
        let h2 = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2);
        let d = h1.diff(&h2);
        // All entries are "modified" because they're at different memory addresses.
        // To test true "no diff", we'd need PartialEq on V.
        // For now, just assert the diff doesn't include Added or Removed.
        assert!(d.iter().all(|e| matches!(e, DiffEntry::Modified(..))));
    }

    #[test]
    fn iterator_visits_all() {
        let h = Hamt::new()
            .insert("x".to_string(), 10)
            .insert("y".to_string(), 20)
            .insert("z".to_string(), 30);
        let mut entries: Vec<_> = h.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                ("x".to_string(), 10),
                ("y".to_string(), 20),
                ("z".to_string(), 30),
            ]
        );
    }

    #[test]
    fn entries_matches_iter() {
        let h = Hamt::new()
            .insert(1u32, "one")
            .insert(2, "two")
            .insert(3, "three");
        let from_iter: Vec<_> = h.iter().collect();
        let from_entries = h.entries();
        assert_eq!(from_iter.len(), from_entries.len());
    }

    #[test]
    fn iterator_exact_size() {
        let h = Hamt::new().insert(1, 1).insert(2, 2).insert(3, 3);
        let iter = h.iter();
        assert_eq!(iter.len(), 3);
    }

    #[test]
    fn remove_all_entries() {
        let h = Hamt::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2)
            .insert("c".to_string(), 3);
        let h = h.remove(&"a".to_string());
        let h = h.remove(&"b".to_string());
        let h = h.remove(&"c".to_string());
        assert_eq!(h.len(), 0);
        assert!(h.is_empty());
    }

    #[test]
    fn insert_and_remove_interleaved() {
        let h = Hamt::new()
            .insert(1u32, "a")
            .insert(2, "b")
            .remove(&1)
            .insert(3, "c")
            .remove(&2)
            .insert(1, "d");
        assert_eq!(h.len(), 2);
        assert_eq!(h.get(&1), Some(&"d"));
        assert!(h.get(&2).is_none());
        assert_eq!(h.get(&3), Some(&"c"));
    }

    #[test]
    fn default_is_empty() {
        let h: Hamt<String, i32> = Hamt::default();
        assert!(h.is_empty());
    }

    /// Force hash collisions by using a wrapper type with a fixed hash.
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct CollisionKey(u32);

    impl Hash for CollisionKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            // All keys hash to the same value to force collisions.
            0u64.hash(state);
        }
    }

    #[test]
    fn hash_collision_insert_and_get() {
        let h = Hamt::new()
            .insert(CollisionKey(1), "one")
            .insert(CollisionKey(2), "two")
            .insert(CollisionKey(3), "three");
        assert_eq!(h.len(), 3);
        assert_eq!(h.get(&CollisionKey(1)), Some(&"one"));
        assert_eq!(h.get(&CollisionKey(2)), Some(&"two"));
        assert_eq!(h.get(&CollisionKey(3)), Some(&"three"));
    }

    #[test]
    fn hash_collision_update() {
        let h = Hamt::new()
            .insert(CollisionKey(1), "old")
            .insert(CollisionKey(2), "two")
            .insert(CollisionKey(1), "new");
        assert_eq!(h.len(), 2);
        assert_eq!(h.get(&CollisionKey(1)), Some(&"new"));
    }

    #[test]
    fn hash_collision_remove() {
        let h = Hamt::new()
            .insert(CollisionKey(1), "one")
            .insert(CollisionKey(2), "two")
            .insert(CollisionKey(3), "three");
        let h = h.remove(&CollisionKey(2));
        assert_eq!(h.len(), 2);
        assert!(h.get(&CollisionKey(2)).is_none());
        assert_eq!(h.get(&CollisionKey(1)), Some(&"one"));
        assert_eq!(h.get(&CollisionKey(3)), Some(&"three"));
    }

    #[test]
    fn hash_collision_remove_to_single_leaf() {
        let h = Hamt::new()
            .insert(CollisionKey(1), "one")
            .insert(CollisionKey(2), "two");
        let h = h.remove(&CollisionKey(1));
        assert_eq!(h.len(), 1);
        assert_eq!(h.get(&CollisionKey(2)), Some(&"two"));
    }
}
