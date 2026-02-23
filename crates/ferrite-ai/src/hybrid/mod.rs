#![forbid(unsafe_code)]
//! Hybrid retrieval combining dense vector and sparse text search.

pub mod fusion;
pub mod multi_vector;
pub mod reranker;

pub use fusion::{FusedResult, FusionStrategy, LinearCombination, ReciprocalRankFusion, ScoredResult};
pub use multi_vector::{MultiVectorDoc, MultiVectorIndex, MultiVectorStrategy};
pub use reranker::{RankedDoc, Reranker, SimpleReranker};
