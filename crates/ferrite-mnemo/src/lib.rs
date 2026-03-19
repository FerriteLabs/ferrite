//! Mnemo — Agent Memory OS facade.
//!
//! See ADR-018 (`docs/adrs/adr-018-mnemo-agent-memory-os.md`).
//!
//! This crate defines the **schema** and **key layout** used by Mnemo command
//! handlers in the top-level `ferrite` crate.  It deliberately depends only on
//! `serde` + small primitives so it can be reused by SDKs, eval harnesses, and
//! adapters without dragging in the storage engine.
//!
//! # Quick start
//!
//! ```
//! use ferrite_mnemo::{
//!     InMemoryMnemoStore, MemoryKind, MemoryRecordBuilder, RecallFilter, Scope,
//! };
//!
//! let store = InMemoryMnemoStore::new();
//! let scope = Scope::new("acme", "agent-1");
//!
//! let record = MemoryRecordBuilder::new()
//!     .id("rec-1")
//!     .tenant("acme").agent("agent-1")
//!     .kind(MemoryKind::Semantic)
//!     .content("user prefers dark mode")
//!     .importance(0.8)
//!     .created_at(1)
//!     .build()
//!     .unwrap();
//! store.put(&scope, record).unwrap();
//!
//! let result = store.recall(
//!     &scope,
//!     /* now_ms = */ 1000,
//!     &RecallFilter { limit: 10, ..Default::default() },
//! );
//! assert_eq!(result.records.len(), 1);
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — public docs land in P1 alongside command handlers.
#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::float_cmp, clippy::expect_used)
)]

pub mod keys;
pub mod quota;
pub mod schema;
pub mod scorer;
pub mod store;
pub mod summarizer;
pub mod telemetry;

pub use keys::{key_for_record, key_for_session, key_prefix_for_agent, KeyParts};
pub use quota::{QuotaEnforcer, QuotaError, TenantQuota};
pub use schema::{BuildError, MemoryKind, MemoryRecord, MemoryRecordBuilder, RecordId, SessionId};
pub use scorer::{
    cosine_similarity, dot_product, euclidean_distance, euclidean_similarity, hybrid_score,
    score_records, HybridScorer, Scored, ScorerWeights, SimilarityFn,
};
pub use store::{InMemoryMnemoStore, RecallFilter, RecallResult, Scope, StoreError};
pub use summarizer::{
    create_summary, find_candidates, summarize, SummarizeResult, SummarizeStrategy,
};
pub use telemetry::{
    record_recall, record_request, MnemoTelemetrySnapshot, MNEMO_RECALL_COUNT,
    MNEMO_RECORDS_RESIDENT, MNEMO_REQUESTS_TOTAL,
};
