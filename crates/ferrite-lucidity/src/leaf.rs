//! Audit leaf: stores `value_hash`, never plaintext, so audit replay
//! is independent of value size.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Set,
    Del,
    Forget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leaf {
    pub op: Op,
    pub key_hash: [u8; 32],
    pub value_hash: [u8; 32],
    pub ts_ms: u64,
}

impl Leaf {
    pub fn for_set(key: &[u8], value: &[u8], ts_ms: u64) -> Self {
        Self {
            op: Op::Set,
            key_hash: hash(key),
            value_hash: hash(value),
            ts_ms,
        }
    }

    pub fn for_del(key: &[u8], ts_ms: u64) -> Self {
        Self {
            op: Op::Del,
            key_hash: hash(key),
            value_hash: [0u8; 32],
            ts_ms,
        }
    }

    pub fn for_forget(key: &[u8], ts_ms: u64) -> Self {
        Self {
            op: Op::Forget,
            key_hash: hash(key),
            value_hash: [0u8; 32],
            ts_ms,
        }
    }

    /// Canonical leaf hash used in the Merkle tree.  Domain-separated with
    /// the 0x00 prefix per RFC 9162 §2.1.
    pub fn merkle_hash(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update([0x00u8]); // leaf domain separator
        h.update([self.op as u8]);
        h.update(self.key_hash);
        h.update(self.value_hash);
        h.update(self.ts_ms.to_be_bytes());
        h.finalize().into()
    }
}

pub fn hash(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_hash_is_deterministic() {
        let a = Leaf::for_set(b"k", b"v", 1);
        let b = Leaf::for_set(b"k", b"v", 1);
        assert_eq!(a.merkle_hash(), b.merkle_hash());
    }

    #[test]
    fn different_ops_give_different_hashes() {
        let a = Leaf::for_set(b"k", b"v", 1);
        let b = Leaf::for_del(b"k", 1);
        assert_ne!(a.merkle_hash(), b.merkle_hash());
    }

    #[test]
    fn timestamp_changes_hash() {
        let a = Leaf::for_set(b"k", b"v", 1);
        let b = Leaf::for_set(b"k", b"v", 2);
        assert_ne!(a.merkle_hash(), b.merkle_hash());
    }
}
