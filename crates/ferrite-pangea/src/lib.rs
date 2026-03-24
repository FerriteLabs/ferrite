//! Pangea — CXL Tier-0 memory.
//!
//! See ADR-023 (`docs/adrs/adr-023-pangea-cxl-tier0.md`).
//!
//! P0 spike: a `CxlAllocator` trait with an in-memory implementation
//! backed by a `Vec<u8>` arena, plus a per-keyspace `Tier0Cache` LRU
//! that exercises the alloc/free API.  Real CXL hardware integration
//! lands in P1.
//!
//! # Quick start
//!
//! ```
//! use ferrite_pangea::{CxlAllocator, InMemoryCxlAllocator, Tier0Cache, EvictionPolicy};
//! let alloc = std::sync::Arc::new(InMemoryCxlAllocator::new(64 * 1024, 4096));
//! let mut cache: Tier0Cache<InMemoryCxlAllocator> = Tier0Cache::new(alloc, 4, EvictionPolicy::Lru);
//! cache.insert("key1".into(), b"hello".to_vec()).unwrap();
//! assert_eq!(cache.get("key1"), Some(b"hello".to_vec()));
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — docs land in P1.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod allocator;
pub mod benchmark;
pub mod cache;
pub mod feature;
pub mod policy;
pub mod sizing;
pub mod topology;
pub mod working_set;

pub use allocator::{AllocError, CxlAllocator, InMemoryCxlAllocator, PageId};
pub use cache::{EvictionPolicy, Tier0Cache};
pub use policy::{AccessStats, MigrationPlan, PolicyEngine, Tier, TierPolicy};
pub use sizing::{InstancePricing, SizingRecommendation};
pub use topology::{Locator, NodeId, NumaTopology, RoutingPolicy};
pub use working_set::{WorkingSet, WorkingSetStats};
