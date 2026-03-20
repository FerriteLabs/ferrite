//! Proof-of-concept benchmarks for the ZK disclosure circuit.
//!
//! Measures proof generation and verification time at log sizes 10³, 10⁴, 10⁵.
//! Results inform the decision on whether Halo2 meets the latency budget.

use sha2::{Digest, Sha256};

use super::disclose::{
    self, build_merkle_tree, extract_merkle_path, DisclosurePublicInput, DisclosureWitness,
};

/// Benchmark result for a single log size.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BenchResult {
    /// Number of leaves in the tree.
    pub log_size: usize,
    /// Proof generation time in microseconds.
    pub proof_time_us: u64,
    /// Proof verification time in microseconds.
    pub verify_time_us: u64,
    /// Proof size in bytes.
    pub proof_size_bytes: usize,
}

/// Run the benchmark suite across three log sizes.
pub fn run_benchmark_suite() -> Vec<BenchResult> {
    let sizes = [1_000, 10_000, 100_000];
    sizes.iter().map(|&size| benchmark_at_size(size)).collect()
}

fn leaf_hash(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00u8]);
    h.update(key);
    h.update(value);
    h.finalize().into()
}

fn benchmark_at_size(size: usize) -> BenchResult {
    // Build leaves.
    let keys: Vec<Vec<u8>> = (0..size).map(|i| format!("key:{i}").into_bytes()).collect();
    let values: Vec<Vec<u8>> = (0..size).map(|i| format!("val:{i}").into_bytes()).collect();
    let leaf_hashes: Vec<[u8; 32]> = keys
        .iter()
        .zip(values.iter())
        .map(|(k, v)| leaf_hash(k, v))
        .collect();

    // Build tree.
    let (root, layers) = build_merkle_tree(&leaf_hashes);

    // Pick a leaf roughly in the middle.
    let target = size / 2;
    let (merkle_path, path_directions) = extract_merkle_path(&layers, target);

    let predicate_hash: [u8; 32] = Sha256::digest(b"balance >= 1000").into();

    let public = DisclosurePublicInput {
        merkle_root: root,
        predicate_hash,
    };
    let witness = DisclosureWitness {
        key: keys[target].clone(),
        value: values[target].clone(),
        merkle_path,
        path_directions,
    };

    // Prove.
    let prove_start = std::time::Instant::now();
    let proof = disclose::prove(&public, &witness).expect("bench: prove failed");
    let proof_time = prove_start.elapsed();

    // Verify.
    let verify_start = std::time::Instant::now();
    let valid = disclose::verify(&public, &proof);
    let verify_time = verify_start.elapsed();
    assert!(valid, "bench: verification failed at size {size}");

    BenchResult {
        log_size: size,
        proof_time_us: proof_time.as_micros() as u64,
        verify_time_us: verify_time.as_micros() as u64,
        proof_size_bytes: proof.proof_bytes.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_suite_runs_and_returns_results() {
        let results = run_benchmark_suite();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.log_size > 0);
            assert!(r.proof_size_bytes > 0);
        }
    }

    #[test]
    fn proof_generation_under_100ms_for_1000_leaves() {
        let result = benchmark_at_size(1_000);
        // 100 ms = 100_000 µs
        assert!(
            result.proof_time_us < 100_000,
            "proof generation took {}µs (> 100ms budget) for 1000 leaves",
            result.proof_time_us
        );
    }

    #[test]
    fn bench_sizes_are_monotonically_increasing() {
        let results = run_benchmark_suite();
        for pair in results.windows(2) {
            assert!(pair[0].log_size < pair[1].log_size);
        }
    }
}
