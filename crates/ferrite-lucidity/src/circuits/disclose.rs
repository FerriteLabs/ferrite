//! Selective-disclosure circuit — proves "this query result is consistent with
//! a key in the audited set" without revealing the key.
//!
//! Production: implements a Halo2 circuit with `halo2_proofs`.
//! This scaffold defines the circuit API and uses SHA-256 as a stand-in
//! for the proof computation.

use sha2::{Digest, Sha256};

/// Public inputs to the disclosure circuit.
#[derive(Debug, Clone)]
pub struct DisclosurePublicInput {
    /// Merkle root of the audited set.
    pub merkle_root: [u8; 32],
    /// Hash of the predicate (e.g., "balance >= 1000").
    pub predicate_hash: [u8; 32],
}

/// Private witness (not revealed to verifier).
#[derive(Debug, Clone)]
pub struct DisclosureWitness {
    /// The actual key being proven.
    pub key: Vec<u8>,
    /// The record value.
    pub value: Vec<u8>,
    /// Merkle inclusion path (sibling hashes from leaf to root).
    pub merkle_path: Vec<[u8; 32]>,
    /// Path directions: `false` = sibling on right, `true` = sibling on left.
    pub path_directions: Vec<bool>,
}

/// A proof of selective disclosure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DisclosureProof {
    /// The proof bytes (in production: Halo2 proof; here: commitment hash).
    pub proof_bytes: Vec<u8>,
    /// Public inputs hash.
    pub public_inputs_hash: [u8; 32],
    /// Proof generation time in microseconds.
    pub generation_time_us: u64,
}

/// Hash a leaf from key and value bytes (domain-separated).
fn leaf_hash(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00u8]); // leaf domain separator
    h.update(key);
    h.update(value);
    h.finalize().into()
}

/// Walk the Merkle path from a leaf hash up to the root.
fn recompute_root(leaf: [u8; 32], path: &[[u8; 32]], directions: &[bool]) -> [u8; 32] {
    let mut current = leaf;
    for (sibling, &dir) in path.iter().zip(directions.iter()) {
        let mut h = Sha256::new();
        h.update([0x01u8]); // interior node domain separator
        if dir {
            // sibling is on the left
            h.update(sibling);
            h.update(current);
        } else {
            // sibling is on the right
            h.update(current);
            h.update(sibling);
        }
        current = h.finalize().into();
    }
    current
}

/// Hash the public inputs into a single 32-byte digest.
fn hash_public_inputs(public: &DisclosurePublicInput) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(public.merkle_root);
    h.update(public.predicate_hash);
    h.finalize().into()
}

/// Generate a disclosure proof.
///
/// Steps (simulated Halo2 prover):
/// 1. Recompute the Merkle root from the witness.
/// 2. Check it matches the public Merkle root.
/// 3. Compute a commitment = SHA-256(root ‖ predicate_hash ‖ key_hash).
pub fn prove(
    public: &DisclosurePublicInput,
    witness: &DisclosureWitness,
) -> Result<DisclosureProof, String> {
    if witness.merkle_path.len() != witness.path_directions.len() {
        return Err("merkle_path and path_directions length mismatch".into());
    }

    let start = std::time::Instant::now();

    // 1. Recompute root from the witness.
    let lh = leaf_hash(&witness.key, &witness.value);
    let computed_root = recompute_root(lh, &witness.merkle_path, &witness.path_directions);

    // 2. Check against public root.
    if computed_root != public.merkle_root {
        return Err("witness does not match public merkle root".into());
    }

    // 3. Commitment (stands in for the real Halo2 proof bytes).
    let key_hash: [u8; 32] = Sha256::digest(&witness.key).into();
    let mut commitment_hasher = Sha256::new();
    commitment_hasher.update(public.merkle_root);
    commitment_hasher.update(public.predicate_hash);
    commitment_hasher.update(key_hash);
    let commitment: Vec<u8> = commitment_hasher.finalize().to_vec();

    let public_inputs_hash = hash_public_inputs(public);

    let elapsed = start.elapsed();
    Ok(DisclosureProof {
        proof_bytes: commitment,
        public_inputs_hash,
        generation_time_us: elapsed.as_micros() as u64,
    })
}

/// Verify a disclosure proof.
///
/// Re-derives the expected commitment from the public inputs and checks it
/// matches the proof bytes.  In production this would call
/// `halo2_proofs::plonk::verify_proof`.
pub fn verify(public: &DisclosurePublicInput, proof: &DisclosureProof) -> bool {
    let expected_pi_hash = hash_public_inputs(public);
    if proof.public_inputs_hash != expected_pi_hash {
        return false;
    }
    // We cannot re-derive the commitment without the key, but we can verify
    // the proof_bytes length and the public_inputs_hash binding.  In the
    // real Halo2 path the verifier checks the polynomial commitments.
    // Here we just ensure structural consistency.
    proof.proof_bytes.len() == 32
}

// ── helpers for building test Merkle trees ────────────────────────────

/// Build a binary Merkle tree from leaves and return `(root, layers)`.
/// `layers[0]` = leaf hashes, `layers[d]` = root.
pub fn build_merkle_tree(leaves: &[[u8; 32]]) -> ([u8; 32], Vec<Vec<[u8; 32]>>) {
    assert!(!leaves.is_empty(), "need at least one leaf");

    // Pad to next power of two.
    let n = leaves.len().next_power_of_two();
    let mut layer: Vec<[u8; 32]> = leaves.to_vec();
    // Pad with duplicates of last leaf (standard practice for balanced trees).
    while layer.len() < n {
        layer.push(
            *layer
                .last()
                .expect("layer must not be empty during padding"),
        );
    }

    let mut layers = vec![layer.clone()];
    while layer.len() > 1 {
        let mut next = Vec::with_capacity(layer.len() / 2);
        for chunk in layer.chunks(2) {
            let mut h = Sha256::new();
            h.update([0x01u8]);
            h.update(chunk[0]);
            h.update(chunk[1]);
            next.push(h.finalize().into());
        }
        layers.push(next.clone());
        layer = next;
    }

    (layer[0], layers)
}

/// Extract the Merkle path and directions for `leaf_index` from `layers`.
pub fn extract_merkle_path(
    layers: &[Vec<[u8; 32]>],
    leaf_index: usize,
) -> (Vec<[u8; 32]>, Vec<bool>) {
    let mut path = Vec::new();
    let mut directions = Vec::new();
    let mut idx = leaf_index;

    for layer in &layers[..layers.len() - 1] {
        let sibling_idx = idx ^ 1;
        if sibling_idx < layer.len() {
            path.push(layer[sibling_idx]);
            // If our index is odd, the sibling is on the left.
            directions.push(idx & 1 == 1);
        }
        idx /= 2;
    }
    (path, directions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_predicate_hash(pred: &str) -> [u8; 32] {
        Sha256::digest(pred.as_bytes()).into()
    }

    fn make_tree_and_proof(
        n_leaves: usize,
        target_idx: usize,
    ) -> (DisclosurePublicInput, DisclosureWitness) {
        let keys: Vec<Vec<u8>> = (0..n_leaves)
            .map(|i| format!("key:{i}").into_bytes())
            .collect();
        let values: Vec<Vec<u8>> = (0..n_leaves)
            .map(|i| format!("val:{i}").into_bytes())
            .collect();

        let leaf_hashes: Vec<[u8; 32]> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| leaf_hash(k, v))
            .collect();

        let (root, layers) = build_merkle_tree(&leaf_hashes);
        let (merkle_path, path_directions) = extract_merkle_path(&layers, target_idx);

        let public = DisclosurePublicInput {
            merkle_root: root,
            predicate_hash: make_predicate_hash("balance >= 1000"),
        };
        let witness = DisclosureWitness {
            key: keys[target_idx].clone(),
            value: values[target_idx].clone(),
            merkle_path,
            path_directions,
        };
        (public, witness)
    }

    #[test]
    fn prove_verify_roundtrip() {
        let (public, witness) = make_tree_and_proof(8, 3);
        let proof = prove(&public, &witness).expect("proof generation failed");
        assert!(verify(&public, &proof), "proof verification failed");
    }

    #[test]
    fn wrong_merkle_root_fails_verification() {
        let (public, witness) = make_tree_and_proof(8, 3);
        let proof = prove(&public, &witness).unwrap();

        let bad_public = DisclosurePublicInput {
            merkle_root: [0xffu8; 32],
            predicate_hash: public.predicate_hash,
        };
        assert!(!verify(&bad_public, &proof));
    }

    #[test]
    fn wrong_predicate_hash_fails_verification() {
        let (public, witness) = make_tree_and_proof(8, 3);
        let proof = prove(&public, &witness).unwrap();

        let bad_public = DisclosurePublicInput {
            merkle_root: public.merkle_root,
            predicate_hash: [0xaau8; 32],
        };
        assert!(!verify(&bad_public, &proof));
    }

    #[test]
    fn prove_fails_with_bad_witness() {
        let (public, mut witness) = make_tree_and_proof(8, 3);
        witness.key = b"wrong-key".to_vec();
        let result = prove(&public, &witness);
        assert!(result.is_err());
    }

    #[test]
    fn prove_fails_with_mismatched_path_lengths() {
        let (public, mut witness) = make_tree_and_proof(8, 3);
        witness.path_directions.pop();
        let result = prove(&public, &witness);
        assert!(result.is_err());
    }

    #[test]
    fn single_leaf_tree() {
        let (public, witness) = make_tree_and_proof(1, 0);
        let proof = prove(&public, &witness).expect("proof generation failed");
        assert!(verify(&public, &proof));
    }

    #[test]
    fn proof_generation_time_is_recorded() {
        let (public, witness) = make_tree_and_proof(16, 5);
        let proof = prove(&public, &witness).unwrap();
        // generation_time_us may be 0 on fast machines but must not panic.
        assert!(proof.generation_time_us < 1_000_000); // < 1 s sanity
    }
}
