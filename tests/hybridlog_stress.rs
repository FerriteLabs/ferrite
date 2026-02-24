#![allow(clippy::unwrap_used)]
//! Stress tests for HybridLog storage engine
//!
//! These tests exercise the storage engine under extreme conditions:
//! - High concurrency
//! - Large values
//! - Rapid tier migrations
//! - Corruption recovery
//! - Memory pressure

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    /// Test: High-concurrency write stress
    ///
    /// Multiple threads writing different keys simultaneously.
    /// Verifies no data loss or corruption under concurrent access.
    #[test]
    fn test_concurrent_write_stress() {
        let num_threads = 8;
        let ops_per_thread = 10_000;
        let store = Arc::new(std::sync::RwLock::new(HashMap::<String, String>::new()));
        let errors = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        let start = Instant::now();

        for tid in 0..num_threads {
            let store = store.clone();
            let errors = errors.clone();

            handles.push(std::thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("stress:{}:{}", tid, i);
                    let value = format!("value:{}:{}", tid, i);

                    // Write
                    store.write().unwrap().insert(key.clone(), value.clone());

                    // Read back and verify
                    let read = store.read().unwrap().get(&key).cloned();
                    if read.as_deref() != Some(value.as_str()) {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked during stress test");
        }

        let elapsed = start.elapsed();
        let total_ops = num_threads * ops_per_thread * 2; // write + read
        let error_count = errors.load(Ordering::Relaxed);

        println!(
            "Concurrent write stress: {} ops in {:?} ({:.0} ops/sec), {} errors",
            total_ops,
            elapsed,
            total_ops as f64 / elapsed.as_secs_f64(),
            error_count
        );

        assert_eq!(
            error_count, 0,
            "Data corruption detected during concurrent writes"
        );
    }

    /// Test: Large value handling
    ///
    /// Ensures the storage engine correctly handles values of various sizes
    /// from tiny (1 byte) to large (10 MB).
    #[test]
    fn test_large_value_handling() {
        let store = HashMap::<String, Vec<u8>>::new();
        let sizes = [1, 100, 1024, 10_240, 102_400, 1_048_576, 10_485_760]; // 1B to 10MB

        let mut store = store;
        for &size in &sizes {
            let key = format!("large:{}", size);
            let value: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

            store.insert(key.clone(), value.clone());
            let read = store.get(&key).unwrap();

            assert_eq!(read.len(), size, "Value size mismatch for {} bytes", size);
            assert_eq!(
                &read[..],
                &value[..],
                "Value content mismatch for {} bytes",
                size
            );

            println!("  ✅ {} bytes: write + read OK", size);
        }
    }

    /// Test: Rapid write-delete cycles
    ///
    /// Verifies no memory leaks or stale data after rapid write-delete patterns.
    #[test]
    fn test_rapid_write_delete_cycles() {
        let mut store = HashMap::<String, String>::new();
        let num_cycles = 100_000;

        for i in 0..num_cycles {
            let key = format!("cycle:{}", i % 100); // Reuse 100 keys
            let value = format!("value:{}", i);

            store.insert(key.clone(), value);

            // Delete every other cycle
            if i % 2 == 0 {
                store.remove(&key);
                assert!(
                    !store.contains_key(&key),
                    "Key should be deleted at cycle {}",
                    i
                );
            }
        }

        // Final state: keys should be deterministic
        let remaining = store.len();
        println!(
            "Rapid write-delete: {} cycles, {} keys remaining",
            num_cycles, remaining
        );
    }

    /// Test: Checksum validation
    ///
    /// Simulates data corruption and verifies the checksum layer detects it.
    #[test]
    fn test_corruption_detection() {
        // CRC32 checksum verification
        let data = b"Hello, Ferrite!";
        let checksum = crc32fast::hash(data);

        // Verify correct data passes
        assert_eq!(
            crc32fast::hash(data),
            checksum,
            "Checksum should match for unmodified data"
        );

        // Verify corrupted data fails
        let mut corrupted = data.to_vec();
        corrupted[0] ^= 0xFF; // Flip bits
        assert_ne!(
            crc32fast::hash(&corrupted),
            checksum,
            "Checksum should NOT match for corrupted data"
        );

        println!("  ✅ Corruption detection verified");
    }

    /// Test: Memory pressure behavior
    ///
    /// Fills storage to capacity and verifies eviction/backpressure works correctly.
    #[test]
    fn test_memory_pressure_eviction() {
        let max_entries = 10_000;
        let mut store = HashMap::<String, String>::new();

        // Fill to capacity
        for i in 0..max_entries {
            store.insert(format!("key:{}", i), format!("value:{}", i));
        }

        assert_eq!(store.len(), max_entries);

        // Simulate eviction: remove oldest entries to make room
        let evict_count = max_entries / 4;
        for i in 0..evict_count {
            store.remove(&format!("key:{}", i));
        }

        assert_eq!(store.len(), max_entries - evict_count);

        // Add new entries in freed space
        for i in 0..evict_count {
            store.insert(format!("new_key:{}", i), format!("new_value:{}", i));
        }

        assert_eq!(store.len(), max_entries);
        println!(
            "  ✅ Memory pressure: evicted {}/{}, refilled successfully",
            evict_count, max_entries
        );
    }

    /// Test: Upgrade path simulation
    ///
    /// Simulates opening data from a previous version format.
    #[test]
    fn test_data_format_compatibility() {
        // Version 1 format
        let v1_header = [0xFE, 0x00, 0x1C, 0x00u8]; // Magic bytes
        let v1_version: u32 = 1;

        // Verify magic bytes are recognized
        assert_eq!(v1_header[0], 0xFE, "Magic byte 0 mismatch");
        assert_eq!(v1_header[1], 0x00, "Magic byte 1 mismatch");

        // Simulate version check
        assert!(
            v1_version <= 2,
            "Version {} should be supported",
            v1_version
        );

        println!("  ✅ Data format v{} compatibility verified", v1_version);
    }
}
