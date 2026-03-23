//! Merkle anti-entropy — detect divergence between replicas.
//!
//! Uses a Merkle hash tree over CRDT key hashes to efficiently find
//! keys that need synchronization.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// A Merkle hash tree for a set of CRDT keys.
///
/// The tree is built over a sorted `BTreeMap` of key→value-hash pairs.
/// The root hash summarises the entire key-space so two replicas can
/// detect divergence with a single comparison.
pub struct AntiEntropyTree {
    entries: BTreeMap<String, [u8; 32]>,
}

impl Default for AntiEntropyTree {
    fn default() -> Self {
        Self::new()
    }
}

impl AntiEntropyTree {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Insert or update a key's hash.
    pub fn update(&mut self, key: &str, value_hash: [u8; 32]) {
        self.entries.insert(key.to_owned(), value_hash);
    }

    /// Remove a key from the tree.
    pub fn remove(&mut self, key: &str) {
        self.entries.remove(key);
    }

    /// Compute the root hash of the tree.
    ///
    /// An empty tree returns the all-zeros hash.  Otherwise the root is
    /// `SHA-256(key_1 || hash_1 || key_2 || hash_2 || ...)` over the
    /// sorted entries.
    pub fn root_hash(&self) -> [u8; 32] {
        if self.entries.is_empty() {
            return [0u8; 32];
        }
        let mut hasher = Sha256::new();
        for (key, hash) in &self.entries {
            hasher.update(key.as_bytes());
            hasher.update(hash);
        }
        hasher.finalize().into()
    }

    /// Compare with another tree, returning keys that differ.
    ///
    /// A key "differs" if it exists only in one tree, or exists in both
    /// but with different value hashes.
    pub fn diff(&self, other: &AntiEntropyTree) -> Vec<String> {
        let mut divergent = Vec::new();

        for (key, hash) in &self.entries {
            match other.entries.get(key) {
                Some(other_hash) if other_hash == hash => {}
                _ => divergent.push(key.clone()),
            }
        }

        for key in other.entries.keys() {
            if !self.entries.contains_key(key) {
                divergent.push(key.clone());
            }
        }

        divergent
    }

    /// Number of entries in the tree.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_of(data: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(data);
        h.finalize().into()
    }

    #[test]
    fn empty_tree_has_zero_hash() {
        let tree = AntiEntropyTree::new();
        assert_eq!(tree.root_hash(), [0u8; 32]);
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn insert_changes_root_hash() {
        let mut tree = AntiEntropyTree::new();
        let h0 = tree.root_hash();
        tree.update("key1", hash_of(b"value1"));
        let h1 = tree.root_hash();
        assert_ne!(h0, h1);
    }

    #[test]
    fn identical_trees_same_root() {
        let mut a = AntiEntropyTree::new();
        let mut b = AntiEntropyTree::new();

        let h1 = hash_of(b"v1");
        let h2 = hash_of(b"v2");

        a.update("k1", h1);
        a.update("k2", h2);
        b.update("k1", h1);
        b.update("k2", h2);

        assert_eq!(a.root_hash(), b.root_hash());
        assert!(a.diff(&b).is_empty());
    }

    #[test]
    fn diff_finds_divergent_keys() {
        let mut a = AntiEntropyTree::new();
        let mut b = AntiEntropyTree::new();

        let h1 = hash_of(b"v1");
        let h2 = hash_of(b"v2");
        let h3 = hash_of(b"v3");

        // Shared key with same hash — should NOT appear in diff.
        a.update("shared", h1);
        b.update("shared", h1);

        // Same key, different hash — divergent.
        a.update("differ", h2);
        b.update("differ", h3);

        // Key only in a.
        a.update("only_a", h1);

        // Key only in b.
        b.update("only_b", h2);

        let mut d = a.diff(&b);
        d.sort();
        assert_eq!(d, vec!["differ", "only_a", "only_b"]);
    }

    #[test]
    fn update_existing_key_changes_hash() {
        let mut tree = AntiEntropyTree::new();
        tree.update("k", hash_of(b"old"));
        let h_old = tree.root_hash();

        tree.update("k", hash_of(b"new"));
        let h_new = tree.root_hash();

        assert_ne!(h_old, h_new);
    }

    #[test]
    fn remove_key() {
        let mut tree = AntiEntropyTree::new();
        tree.update("k", hash_of(b"v"));
        assert_eq!(tree.len(), 1);

        tree.remove("k");
        assert!(tree.is_empty());
        assert_eq!(tree.root_hash(), [0u8; 32]);
    }
}
