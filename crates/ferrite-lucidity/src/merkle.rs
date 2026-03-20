//! Binary Merkle tree (RFC 9162 §2.1).
//!
//! This is the simplest correct implementation: store every leaf, recompute
//! the root on demand.  P1 swaps in an incremental accumulator.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];

/// Internal-node hash, with the 0x01 domain separator.
fn parent(left: &Hash, right: &Hash) -> Hash {
    let mut h = Sha256::new();
    h.update([0x01u8]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// Compute the root of the tree built from `leaves`.  Empty tree returns
/// the all-zero hash.
pub fn root(leaves: &[Hash]) -> Hash {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    if leaves.len() == 1 {
        return leaves[0];
    }
    // Largest power of two ≤ N.
    let split = largest_pow2_lt(leaves.len());
    let l = root(&leaves[..split]);
    let r = root(&leaves[split..]);
    parent(&l, &r)
}

fn largest_pow2_lt(n: usize) -> usize {
    debug_assert!(n > 1);
    1 << (usize::BITS - 1 - (n - 1).leading_zeros())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InclusionProof {
    pub leaf_index: usize,
    pub leaf_hash: Hash,
    pub tree_size: usize,
    pub audit_path: Vec<Hash>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsistencyProof {
    pub old_size: usize,
    pub new_size: usize,
    pub path: Vec<Hash>,
}

/// RFC 9162 §2.1.1 — inclusion proof for `index` in a tree of `leaves.len()`.
pub fn inclusion_proof(leaves: &[Hash], index: usize) -> Option<InclusionProof> {
    if index >= leaves.len() {
        return None;
    }
    let mut path = Vec::new();
    fn walk(leaves: &[Hash], index: usize, path: &mut Vec<Hash>) {
        let n = leaves.len();
        if n <= 1 {
            return;
        }
        let split = largest_pow2_lt(n);
        if index < split {
            path.push(root(&leaves[split..]));
            walk(&leaves[..split], index, path);
        } else {
            path.push(root(&leaves[..split]));
            walk(&leaves[split..], index - split, path);
        }
    }
    walk(leaves, index, &mut path);
    Some(InclusionProof {
        leaf_index: index,
        leaf_hash: leaves[index],
        tree_size: leaves.len(),
        audit_path: path,
    })
}

/// Verify an inclusion proof against a known root.
///
/// Mirrors the top-down recursion used to generate the proof: at each
/// level we know the subtree size (`n`), split it at the largest power
/// of 2, and combine the leaf hash with the sibling on the correct side.
pub fn verify_inclusion(proof: &InclusionProof, root_hash: &Hash) -> bool {
    if proof.tree_size == 0 || proof.leaf_index >= proof.tree_size {
        return false;
    }
    let computed = recompute(
        proof.leaf_index,
        proof.tree_size,
        proof.leaf_hash,
        &proof.audit_path,
    );
    computed.is_some_and(|h| h == *root_hash)
}

fn recompute(index: usize, n: usize, leaf: Hash, path: &[Hash]) -> Option<Hash> {
    if n <= 1 {
        return if path.is_empty() { Some(leaf) } else { None };
    }
    let split = largest_pow2_lt(n);
    let (sibling, rest) = path.split_first()?;
    if index < split {
        let left = recompute(index, split, leaf, rest)?;
        Some(parent(&left, sibling))
    } else {
        let right = recompute(index - split, n - split, leaf, rest)?;
        Some(parent(sibling, &right))
    }
}

/// RFC 9162 §2.1.2 — consistency between an older tree of size `old` and the
/// current tree.  P0 spike: simple "recompute the old root inside the new
/// tree" check rather than the full optimised path.
pub fn consistency_proof(leaves: &[Hash], old_size: usize) -> Option<ConsistencyProof> {
    if old_size == 0 || old_size > leaves.len() {
        return None;
    }
    Some(ConsistencyProof {
        old_size,
        new_size: leaves.len(),
        path: vec![root(&leaves[..old_size]), root(leaves)],
    })
}

/// Verify a consistency proof produced by [`consistency_proof`].
pub fn verify_consistency(proof: &ConsistencyProof, old_root: &Hash, new_root: &Hash) -> bool {
    proof.path.len() == 2
        && proof.path[0] == *old_root
        && proof.path[1] == *new_root
        && proof.old_size <= proof.new_size
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash {
        let mut a = [0u8; 32];
        a[0] = b;
        a
    }

    #[test]
    fn empty_tree_root_is_zero() {
        assert_eq!(root(&[]), [0u8; 32]);
    }

    #[test]
    fn single_leaf_root_is_the_leaf() {
        let leaves = vec![h(1)];
        assert_eq!(root(&leaves), h(1));
    }

    #[test]
    fn pow2_pair() {
        let leaves = vec![h(1), h(2)];
        let r = root(&leaves);
        let expect = parent(&h(1), &h(2));
        assert_eq!(r, expect);
    }

    #[test]
    fn three_leaves_split_2_1() {
        let leaves = vec![h(1), h(2), h(3)];
        let r = root(&leaves);
        let left = parent(&h(1), &h(2));
        let expect = parent(&left, &h(3));
        assert_eq!(r, expect);
    }

    #[test]
    fn inclusion_roundtrip_for_every_index() {
        let leaves: Vec<Hash> = (0..7).map(|i| h(i as u8)).collect();
        let r = root(&leaves);
        for i in 0..leaves.len() {
            let p = inclusion_proof(&leaves, i).unwrap();
            assert!(verify_inclusion(&p, &r), "inclusion failed for index {i}");
        }
    }

    #[test]
    fn inclusion_rejects_wrong_root() {
        let leaves: Vec<Hash> = (0..4).map(|i| h(i as u8)).collect();
        let p = inclusion_proof(&leaves, 0).unwrap();
        let bad = h(99);
        assert!(!verify_inclusion(&p, &bad));
    }

    #[test]
    fn inclusion_out_of_range_is_none() {
        let leaves: Vec<Hash> = (0..4).map(|i| h(i as u8)).collect();
        assert!(inclusion_proof(&leaves, 4).is_none());
    }

    #[test]
    fn consistency_links_old_and_new() {
        let leaves: Vec<Hash> = (0..6).map(|i| h(i as u8)).collect();
        let old_root = root(&leaves[..3]);
        let new_root = root(&leaves);
        let p = consistency_proof(&leaves, 3).unwrap();
        assert!(verify_consistency(&p, &old_root, &new_root));
    }

    #[test]
    fn consistency_rejects_mismatched_old_root() {
        let leaves: Vec<Hash> = (0..6).map(|i| h(i as u8)).collect();
        let new_root = root(&leaves);
        let p = consistency_proof(&leaves, 3).unwrap();
        assert!(!verify_consistency(&p, &h(99), &new_root));
    }
}
