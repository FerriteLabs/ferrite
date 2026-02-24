#![forbid(unsafe_code)]
//! BM25 sparse retrieval for hybrid search.

mod index;
mod scorer;

pub use index::{Bm25Index, ScoredDoc};
pub use scorer::Bm25Scorer;
