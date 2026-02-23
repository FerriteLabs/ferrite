//! HyperLogLog commands implementation
//!
//! HyperLogLog is a probabilistic data structure for cardinality estimation.
//! It uses very little memory (about 12KB) to estimate cardinalities with
//! a standard error of 0.81%.

use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::protocol::Frame;
use crate::storage::{Store, Value};
use std::sync::Arc;

/// Number of registers (2^14 = 16384)
const NUM_REGISTERS: usize = 16384;

/// Bits used for register index (14 bits)
const INDEX_BITS: u32 = 14;

/// Alpha constant for bias correction (for m = 16384)
const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / 16384.0);

/// HyperLogLog data structure
#[derive(Clone, Debug)]
pub struct HyperLogLog {
    /// Registers storing the maximum number of leading zeros + 1
    registers: Vec<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// Create a new HyperLogLog with default registers
    pub fn new() -> Self {
        Self {
            registers: vec![0; NUM_REGISTERS],
        }
    }

    /// Create a HyperLogLog from existing register data
    pub fn from_registers(registers: Vec<u8>) -> Self {
        let mut hll = Self::new();
        if registers.len() == NUM_REGISTERS {
            hll.registers = registers;
        }
        hll
    }

    /// Get the raw registers
    #[allow(dead_code)] // Planned for v0.2 — reserved for HyperLogLog introspection API
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Hash a value and return the 64-bit hash
    fn hash_value(value: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Add a value to the HyperLogLog
    /// Returns true if the cardinality estimate changed
    pub fn add(&mut self, value: &[u8]) -> bool {
        let hash = Self::hash_value(value);

        // Use first INDEX_BITS bits for register index
        let index = (hash & ((1 << INDEX_BITS) - 1)) as usize;

        // Use remaining bits to count leading zeros
        let remaining = hash >> INDEX_BITS;

        // Count leading zeros + 1 (minimum value is 1)
        let leading_zeros = if remaining == 0 {
            (64 - INDEX_BITS) as u8
        } else {
            remaining.leading_zeros().saturating_sub(INDEX_BITS) as u8 + 1
        };

        let rho = leading_zeros.min(51); // Cap at 51 to prevent overflow

        if rho > self.registers[index] {
            self.registers[index] = rho;
            true
        } else {
            false
        }
    }

    /// Estimate the cardinality
    pub fn count(&self) -> u64 {
        let mut sum: f64 = 0.0;
        let mut zeros = 0;

        for &reg in &self.registers {
            sum += 2.0_f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        let m = NUM_REGISTERS as f64;
        let mut estimate = ALPHA * m * m / sum;

        // Small range correction (linear counting)
        if estimate <= 2.5 * m && zeros > 0 {
            estimate = m * (m / zeros as f64).ln();
        }

        // Large range correction (for 64-bit hash)
        // Not typically needed for cardinalities < 2^32

        estimate.round() as u64
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (i, &other_reg) in other.registers.iter().enumerate() {
            if other_reg > self.registers[i] {
                self.registers[i] = other_reg;
            }
        }
    }
}

/// PFADD - Add elements to a HyperLogLog
pub fn pfadd(store: &Arc<Store>, db: u8, key: &Bytes, elements: &[Bytes]) -> Frame {
    if elements.is_empty() {
        // If key exists, return 0; if not, create empty HLL and return 1
        if store.exists(db, &[key.clone()]) == 1 {
            return Frame::Integer(0);
        }
        // Create empty HLL
        let hll = HyperLogLog::new();
        let value = Value::HyperLogLog(hll.registers);
        store.set(db, key.clone(), value);
        return Frame::Integer(1);
    }

    // Get or create HyperLogLog
    let mut hll = match store.get(db, key) {
        Some(Value::HyperLogLog(registers)) => HyperLogLog::from_registers(registers),
        Some(_) => {
            return Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )
        }
        None => HyperLogLog::new(),
    };

    // Add all elements
    let mut changed = false;
    for element in elements {
        if hll.add(element) {
            changed = true;
        }
    }

    // Store updated HLL
    let value = Value::HyperLogLog(hll.registers);
    store.set(db, key.clone(), value);

    Frame::Integer(if changed { 1 } else { 0 })
}

/// PFCOUNT - Get the cardinality estimate
pub fn pfcount(store: &Arc<Store>, db: u8, keys: &[Bytes]) -> Frame {
    if keys.is_empty() {
        return Frame::Error("ERR wrong number of arguments for 'pfcount' command".into());
    }

    if keys.len() == 1 {
        // Single key - return count directly
        match store.get(db, &keys[0]) {
            Some(Value::HyperLogLog(registers)) => {
                let hll = HyperLogLog::from_registers(registers);
                Frame::Integer(hll.count() as i64)
            }
            Some(_) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            None => Frame::Integer(0),
        }
    } else {
        // Multiple keys - merge and return count
        let mut merged = HyperLogLog::new();

        for key in keys {
            match store.get(db, key) {
                Some(Value::HyperLogLog(registers)) => {
                    let hll = HyperLogLog::from_registers(registers);
                    merged.merge(&hll);
                }
                Some(_) => {
                    return Frame::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
                None => {} // Non-existent keys are treated as empty HLLs (no-op in merge)
            }
        }

        Frame::Integer(merged.count() as i64)
    }
}

/// PFMERGE - Merge multiple HyperLogLogs into one
pub fn pfmerge(store: &Arc<Store>, db: u8, destkey: &Bytes, sourcekeys: &[Bytes]) -> Frame {
    let mut merged = HyperLogLog::new();

    // Include destination key if it exists (Redis behavior)
    match store.get(db, destkey) {
        Some(Value::HyperLogLog(registers)) => {
            merged = HyperLogLog::from_registers(registers);
        }
        Some(_) => {
            return Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )
        }
        None => {} // New destination, start with empty HLL
    }

    // Merge all source keys
    for key in sourcekeys {
        match store.get(db, key) {
            Some(Value::HyperLogLog(registers)) => {
                let hll = HyperLogLog::from_registers(registers);
                merged.merge(&hll);
            }
            Some(_) => {
                return Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
            None => {} // Non-existent keys are treated as empty HLLs
        }
    }

    // Store merged result
    let value = Value::HyperLogLog(merged.registers);
    store.set(db, destkey.clone(), value);

    Frame::simple("OK")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_hyperloglog_basic() {
        let mut hll = HyperLogLog::new();

        // Add some elements
        assert!(hll.add(b"element1"));
        assert!(hll.add(b"element2"));
        assert!(hll.add(b"element3"));

        // Adding same element should not change estimate significantly
        let count_before = hll.count();
        hll.add(b"element1");
        let count_after = hll.count();
        assert_eq!(count_before, count_after);
    }

    #[test]
    fn test_hyperloglog_cardinality() {
        let mut hll = HyperLogLog::new();

        // Add 1000 unique elements
        for i in 0..1000 {
            hll.add(format!("element{}", i).as_bytes());
        }

        let estimate = hll.count();
        // HyperLogLog has ~0.81% error rate, so estimate should be within ~3% of 1000
        assert!(estimate >= 950, "Estimate {} too low", estimate);
        assert!(estimate <= 1050, "Estimate {} too high", estimate);
    }

    #[test]
    fn test_hyperloglog_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();

        // Add different elements to each
        for i in 0..500 {
            hll1.add(format!("a{}", i).as_bytes());
        }
        for i in 0..500 {
            hll2.add(format!("b{}", i).as_bytes());
        }

        // Merge
        hll1.merge(&hll2);

        let estimate = hll1.count();
        // Should be close to 1000
        assert!(estimate >= 950, "Merged estimate {} too low", estimate);
        assert!(estimate <= 1050, "Merged estimate {} too high", estimate);
    }

    #[test]
    fn test_hyperloglog_empty() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_pfadd_and_pfcount() {
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));
        let db = 0;

        let key = Bytes::from("hll");

        // Add elements
        let result = pfadd(
            &store,
            db,
            &key,
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        assert!(matches!(result, Frame::Integer(1)));

        // Count
        let result = pfcount(&store, db, &[key.clone()]);
        if let Frame::Integer(count) = result {
            assert!(
                (2..=4).contains(&count),
                "Count {} out of expected range",
                count
            );
        } else {
            panic!("Expected Integer frame");
        }

        // Add same elements again
        let result = pfadd(&store, db, &key, &[Bytes::from("a"), Bytes::from("b")]);
        // Should return 0 since no change in cardinality
        assert!(matches!(result, Frame::Integer(0)));
    }

    #[test]
    fn test_pfmerge() {
        use crate::storage::Store;

        let store = Arc::new(Store::new(16));
        let db = 0;

        let key1 = Bytes::from("hll1");
        let key2 = Bytes::from("hll2");
        let dest = Bytes::from("hll_merged");

        // Add elements to first HLL
        pfadd(&store, db, &key1, &[Bytes::from("a"), Bytes::from("b")]);

        // Add elements to second HLL
        pfadd(&store, db, &key2, &[Bytes::from("c"), Bytes::from("d")]);

        // Merge
        let result = pfmerge(&store, db, &dest, &[key1, key2]);
        assert!(matches!(result, Frame::Simple(_)));

        // Count merged
        let result = pfcount(&store, db, &[dest]);
        if let Frame::Integer(count) = result {
            assert!(
                (3..=5).contains(&count),
                "Count {} out of expected range",
                count
            );
        } else {
            panic!("Expected Integer frame");
        }
    }
}
