//! MnemoForgeBridge: lets a Forge runtime read from a Mnemo store.
//!
//! Production wiring will call `link_host_api` with closures that
//! delegate to a [`MnemoForgeBridge`] instance.  For the spike we
//! keep the surface narrow: a `recall` method that takes a tenant +
//! agent + textual cue and returns the top hit's content.
//!
//! The `score_record_for_forge` / `parse_forge_result` pair defines a
//! simple wire format that a WASM Forge function would consume and produce.

use ferrite_mnemo::{InMemoryMnemoStore, MemoryRecord, RecallFilter, Scope, StoreError};

/// Serialize a [`MemoryRecord`] into a compact byte buffer suitable
/// for passing to a Forge WASM function.
///
/// Wire format (all little-endian):
/// ```text
/// [importance: f32 (4B)] [created_at: u64 (8B)] [access_count: u64 (8B)]
/// [content_len: u32 (4B)] [content: utf-8 bytes]
/// ```
pub fn score_record_for_forge(record: &MemoryRecord) -> Vec<u8> {
    let content = record.content.as_bytes();
    let mut buf = Vec::with_capacity(4 + 8 + 8 + 4 + content.len());
    buf.extend_from_slice(&record.importance.to_le_bytes());
    buf.extend_from_slice(&record.created_at.to_le_bytes());
    buf.extend_from_slice(&record.access_count.to_le_bytes());
    buf.extend_from_slice(&(content.len() as u32).to_le_bytes());
    buf.extend_from_slice(content);
    buf
}

/// Parse a score from a Forge function result.
///
/// The expected output is exactly 4 bytes encoding an `f32` in
/// little-endian order. Returns `None` on malformed output.
pub fn parse_forge_result(output: &[u8]) -> Option<f32> {
    if output.len() != 4 {
        return None;
    }
    Some(f32::from_le_bytes(output.try_into().ok()?))
}

pub struct MnemoForgeBridge {
    store: InMemoryMnemoStore,
}

impl MnemoForgeBridge {
    pub fn new() -> Self {
        Self {
            store: InMemoryMnemoStore::new(),
        }
    }

    /// Insert a memory (helper for tests / setup).
    pub fn remember(&self, scope: &Scope, record: MemoryRecord) -> Result<(), StoreError> {
        self.store.put(scope, record)
    }

    /// Recall the top-K most relevant memories.  The `_cue` parameter is
    /// reserved for the production wiring that ranks via cosine similarity
    /// against an embedding; the spike just returns the most-recent K.
    pub fn recall_text(&self, scope: &Scope, _cue: &str, k: usize, now_ms: u64) -> Vec<String> {
        let filter = RecallFilter {
            limit: k,
            ..Default::default()
        };
        self.store
            .recall(scope, now_ms, &filter)
            .records
            .into_iter()
            .map(|r| r.content)
            .collect()
    }

    pub fn store(&self) -> &InMemoryMnemoStore {
        &self.store
    }
}

impl Default for MnemoForgeBridge {
    fn default() -> Self {
        Self::new()
    }
}
