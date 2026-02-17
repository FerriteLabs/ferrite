//! PSYNC2-compatible streaming replication protocol
//!
//! This module implements the PSYNC2 protocol for Redis-compatible replication,
//! supporting both full resynchronization and partial resynchronization with
//! dual replication ID tracking for seamless failover.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::stream::ReplicationCommand;
use super::{ReplicationId, ReplicationOffset};

/// Default replication backlog size (1 MB)
pub const DEFAULT_BACKLOG_SIZE: u64 = 1_048_576;

/// Sentinel value indicating no valid secondary offset
const INVALID_OFFSET: u64 = u64::MAX;

/// Errors specific to the PSYNC2 protocol
#[derive(Debug, thiserror::Error)]
pub enum Psync2Error {
    /// The requested offset has been evicted from the backlog
    #[error("offset {requested} is behind backlog start {backlog_start}")]
    OffsetBehindBacklog {
        /// Offset the replica asked for
        requested: u64,
        /// Earliest offset still available
        backlog_start: u64,
    },

    /// The requested offset is ahead of the current primary offset
    #[error("offset {requested} is ahead of primary offset {primary}")]
    OffsetAheadOfPrimary {
        /// Offset the replica asked for
        requested: u64,
        /// Current primary offset
        primary: u64,
    },

    /// Neither replication ID matches the primary
    #[error("replication ID mismatch: replica sent {replica_id}")]
    ReplicationIdMismatch {
        /// The ID the replica provided
        replica_id: String,
    },

    /// An I/O error occurred during replication
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result of the PSYNC2 handshake negotiation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Psync2Response {
    /// Full resynchronization is required.
    ///
    /// The primary will send its current replication ID and offset,
    /// followed by an RDB snapshot and then the live stream.
    FullResync {
        /// The primary's current replication ID
        replid: ReplicationId,
        /// The primary's current replication offset
        offset: ReplicationOffset,
    },

    /// Partial resynchronization is possible.
    ///
    /// The primary will stream only the commands the replica has missed
    /// starting from the negotiated offset.
    PartialResync {
        /// The replication ID to continue with
        replid: ReplicationId,
        /// The offset from which the stream resumes
        offset: ReplicationOffset,
    },
}

/// Configuration for the PSYNC2 backlog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Psync2BacklogConfig {
    /// Maximum size of the backlog in bytes
    pub max_size: u64,

    /// Whether the backlog is active (disabled until the first replica connects)
    pub active: bool,
}

impl Default for Psync2BacklogConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_BACKLOG_SIZE,
            active: true,
        }
    }
}

/// An entry stored in the PSYNC2 replication backlog
#[derive(Debug, Clone)]
struct Psync2BacklogEntry {
    /// Raw RESP-encoded command bytes
    data: Bytes,
    /// Byte offset where this entry starts in the replication stream
    offset: ReplicationOffset,
}

/// Circular backlog buffer for partial resynchronization.
///
/// Stores recently replicated commands so that replicas that briefly
/// disconnect can catch up without a full RDB transfer.
#[derive(Debug)]
struct Psync2Backlog {
    /// Ring of backlog entries, ordered by ascending offset
    entries: VecDeque<Psync2BacklogEntry>,
    /// Maximum allowed size in bytes
    max_size: u64,
    /// Current total size of stored entries in bytes
    current_size: u64,
    /// The offset of the first byte still present in the backlog
    first_offset: ReplicationOffset,
}

impl Psync2Backlog {
    fn new(max_size: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            max_size,
            current_size: 0,
            first_offset: 0,
        }
    }

    /// Append a RESP-encoded command at the given stream offset.
    fn push(&mut self, data: Bytes, offset: ReplicationOffset) {
        let size = data.len() as u64;
        self.entries.push_back(Psync2BacklogEntry { data, offset });
        self.current_size += size;
        self.trim();
    }

    /// Evict oldest entries until the backlog fits within `max_size`.
    fn trim(&mut self) {
        while self.current_size > self.max_size {
            if let Some(old) = self.entries.pop_front() {
                self.current_size -= old.data.len() as u64;
                self.first_offset = self
                    .entries
                    .front()
                    .map(|e| e.offset)
                    .unwrap_or(self.first_offset);
            } else {
                break;
            }
        }
    }

    /// Return `true` if the backlog contains data from `offset` onward.
    fn contains_offset(&self, offset: ReplicationOffset) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        offset >= self.first_offset && offset <= self.end_offset()
    }

    /// The byte offset just past the last entry (i.e. the next write position).
    fn end_offset(&self) -> ReplicationOffset {
        self.entries
            .back()
            .map(|e| e.offset + e.data.len() as u64)
            .unwrap_or(self.first_offset)
    }

    /// Collect all entry data from `offset` onward into a single `Bytes`.
    fn read_from(&self, offset: ReplicationOffset) -> Vec<Bytes> {
        self.entries
            .iter()
            .filter(|e| e.offset >= offset)
            .map(|e| e.data.clone())
            .collect()
    }

    /// Reset the backlog, discarding all buffered data.
    fn clear(&mut self) {
        self.entries.clear();
        self.current_size = 0;
    }
}

/// Handler for the PSYNC2 replication protocol.
///
/// `Psync2Handler` maintains dual replication IDs (`replid` / `replid2`)
/// and a circular backlog buffer so that replicas can perform partial
/// resynchronization after brief disconnections or after a failover event.
///
/// # PSYNC2 overview
///
/// 1. A replica sends `PSYNC <replid> <offset>`.
/// 2. The primary checks whether `replid` matches its current or secondary ID
///    and whether the requested offset is still available in the backlog.
/// 3. If both conditions hold, the primary responds with `+CONTINUE` and
///    streams only the missed commands (**partial resync**).
/// 4. Otherwise the primary responds with `+FULLRESYNC` and sends an RDB
///    snapshot followed by the live command stream (**full resync**).
pub struct Psync2Handler {
    /// Primary replication ID for the current replication stream
    replid: RwLock<ReplicationId>,
    /// Secondary replication ID used during failover transitions
    replid2: RwLock<ReplicationId>,
    /// Current replication offset of the primary
    offset: RwLock<ReplicationOffset>,
    /// The offset at which `replid2` became invalid (set on failover)
    second_repl_offset: RwLock<ReplicationOffset>,
    /// Circular backlog of recently replicated commands
    backlog: RwLock<Psync2Backlog>,
    /// Backlog configuration
    config: Psync2BacklogConfig,
}

impl Psync2Handler {
    /// Create a new `Psync2Handler` with the given backlog configuration.
    pub fn new(config: Psync2BacklogConfig) -> Self {
        let max_size = config.max_size;
        Self {
            replid: RwLock::new(ReplicationId::new()),
            replid2: RwLock::new(ReplicationId::from_string("")),
            offset: RwLock::new(0),
            second_repl_offset: RwLock::new(INVALID_OFFSET),
            backlog: RwLock::new(Psync2Backlog::new(max_size)),
            config,
        }
    }

    /// Create a handler with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(Psync2BacklogConfig::default())
    }

    // -- Replication ID accessors --

    /// Return the primary replication ID.
    pub async fn replid(&self) -> ReplicationId {
        self.replid.read().await.clone()
    }

    /// Return the secondary replication ID.
    pub async fn replid2(&self) -> ReplicationId {
        self.replid2.read().await.clone()
    }

    /// Return the current replication offset.
    pub async fn current_offset(&self) -> ReplicationOffset {
        *self.offset.read().await
    }

    /// Return the offset at which the secondary ID became invalid.
    pub async fn second_repl_offset(&self) -> ReplicationOffset {
        *self.second_repl_offset.read().await
    }

    // -- Core protocol operations --

    /// Handle an incoming `PSYNC <replid> <offset>` request.
    ///
    /// Returns a [`Psync2Response`] indicating whether the replica should
    /// perform a full or partial resynchronization.
    pub async fn handle_psync(
        &self,
        replica_replid: &str,
        replica_offset: i64,
    ) -> Result<Psync2Response, Psync2Error> {
        // A replica sending "?" and -1 always requests a full resync
        if replica_replid == "?" || replica_offset < 0 {
            return Ok(self.full_resync_response().await);
        }

        let requested_offset = replica_offset as u64;
        let current_offset = *self.offset.read().await;

        if requested_offset > current_offset {
            return Err(Psync2Error::OffsetAheadOfPrimary {
                requested: requested_offset,
                primary: current_offset,
            });
        }

        // Try matching against the primary replication ID
        let primary_id = self.replid.read().await;
        if primary_id.as_str() == replica_replid {
            drop(primary_id);
            return self.try_partial_resync(requested_offset).await;
        }
        drop(primary_id);

        // Try matching against the secondary replication ID (failover case)
        let secondary_id = self.replid2.read().await;
        if secondary_id.as_str() == replica_replid {
            let second_offset = *self.second_repl_offset.read().await;
            drop(secondary_id);
            if requested_offset <= second_offset {
                return self.try_partial_resync(requested_offset).await;
            }
            // Offset is past the point where the secondary ID was valid
            return Ok(self.full_resync_response().await);
        }
        drop(secondary_id);

        // No ID match at all
        Err(Psync2Error::ReplicationIdMismatch {
            replica_id: replica_replid.to_string(),
        })
    }

    /// Attempt a partial resync from the given offset.
    async fn try_partial_resync(
        &self,
        requested_offset: ReplicationOffset,
    ) -> Result<Psync2Response, Psync2Error> {
        let backlog = self.backlog.read().await;
        if backlog.contains_offset(requested_offset) {
            let replid = self.replid.read().await.clone();
            Ok(Psync2Response::PartialResync {
                replid,
                offset: requested_offset,
            })
        } else {
            let backlog_start = backlog.first_offset;
            drop(backlog);
            // If the replica is exactly at our current offset, partial resync is trivially OK
            let current = *self.offset.read().await;
            if requested_offset == current {
                let replid = self.replid.read().await.clone();
                return Ok(Psync2Response::PartialResync {
                    replid,
                    offset: requested_offset,
                });
            }
            Err(Psync2Error::OffsetBehindBacklog {
                requested: requested_offset,
                backlog_start,
            })
        }
    }

    /// Build a full-resync response with the current replid and offset.
    async fn full_resync_response(&self) -> Psync2Response {
        Psync2Response::FullResync {
            replid: self.replid.read().await.clone(),
            offset: *self.offset.read().await,
        }
    }

    /// Retrieve backlog data starting from the given offset for partial resync.
    ///
    /// The caller should stream these chunks to the replica in order.
    pub async fn get_partial_resync_data(
        &self,
        from_offset: ReplicationOffset,
    ) -> Result<Vec<Bytes>, Psync2Error> {
        let backlog = self.backlog.read().await;
        if !backlog.contains_offset(from_offset) {
            return Err(Psync2Error::OffsetBehindBacklog {
                requested: from_offset,
                backlog_start: backlog.first_offset,
            });
        }
        Ok(backlog.read_from(from_offset))
    }

    // -- Backlog management --

    /// Append a replication command to the backlog and advance the offset.
    ///
    /// This should be called for every write command the primary processes.
    pub async fn feed_command(&self, command: &ReplicationCommand) {
        let encoded = Bytes::from(command.to_bytes());
        let size = encoded.len() as u64;

        let mut offset = self.offset.write().await;
        let current = *offset;

        let mut backlog = self.backlog.write().await;
        backlog.push(encoded, current);
        drop(backlog);

        *offset = current + size;
    }

    /// Append raw RESP-encoded bytes at a specific offset.
    ///
    /// Useful when replaying from an AOF or receiving forwarded data.
    pub async fn feed_raw(&self, data: Bytes, at_offset: ReplicationOffset) {
        let size = data.len() as u64;
        let mut backlog = self.backlog.write().await;
        backlog.push(data, at_offset);
        drop(backlog);

        let mut offset = self.offset.write().await;
        let new_offset = at_offset + size;
        if new_offset > *offset {
            *offset = new_offset;
        }
    }

    /// Return the current backlog size in bytes.
    pub async fn backlog_size(&self) -> u64 {
        self.backlog.read().await.current_size
    }

    /// Return the configured maximum backlog size.
    pub fn backlog_max_size(&self) -> u64 {
        self.config.max_size
    }

    /// Return the first offset still present in the backlog.
    pub async fn backlog_first_offset(&self) -> ReplicationOffset {
        self.backlog.read().await.first_offset
    }

    /// Clear the entire backlog (e.g. after a full resync or configuration change).
    pub async fn clear_backlog(&self) {
        self.backlog.write().await.clear();
    }

    // -- Failover support --

    /// Perform a failover rotation of the replication IDs.
    ///
    /// This is called when a replica is promoted to primary. The current
    /// primary ID becomes the secondary ID, and a fresh primary ID is
    /// generated. The `second_repl_offset` records the offset at which
    /// the old stream ended so that replicas of the old primary can still
    /// attempt partial resync against the secondary ID.
    pub async fn rotate_replication_ids(&self) {
        let old_primary = self.replid.read().await.clone();
        let current_offset = *self.offset.read().await;

        *self.replid2.write().await = old_primary;
        *self.second_repl_offset.write().await = current_offset;
        *self.replid.write().await = ReplicationId::new();
    }

    /// Explicitly set both replication IDs (e.g. when synchronizing with a primary).
    pub async fn set_replication_ids(
        &self,
        replid: ReplicationId,
        replid2: ReplicationId,
        second_offset: ReplicationOffset,
    ) {
        *self.replid.write().await = replid;
        *self.replid2.write().await = replid2;
        *self.second_repl_offset.write().await = second_offset;
    }

    /// Set the current replication offset (e.g. after loading an RDB).
    pub async fn set_offset(&self, offset: ReplicationOffset) {
        *self.offset.write().await = offset;
    }

    // -- Diagnostics --

    /// Generate an INFO-style string for the replication section.
    pub async fn info_string(&self) -> String {
        let replid = self.replid.read().await;
        let replid2 = self.replid2.read().await;
        let offset = *self.offset.read().await;
        let second_offset = *self.second_repl_offset.read().await;
        let backlog = self.backlog.read().await;

        format!(
            "master_replid:{}\r\n\
             master_replid2:{}\r\n\
             master_repl_offset:{}\r\n\
             second_repl_offset:{}\r\n\
             repl_backlog_active:{}\r\n\
             repl_backlog_size:{}\r\n\
             repl_backlog_first_byte_offset:{}\r\n\
             repl_backlog_histlen:{}",
            replid.as_str(),
            replid2.as_str(),
            offset,
            if second_offset == INVALID_OFFSET {
                -1_i64
            } else {
                second_offset as i64
            },
            u8::from(self.config.active),
            self.config.max_size,
            backlog.first_offset,
            backlog.current_size,
        )
    }
}

/// Shared reference to a [`Psync2Handler`]
pub type SharedPsync2Handler = Arc<Psync2Handler>;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn make_handler() -> Psync2Handler {
        Psync2Handler::new(Psync2BacklogConfig {
            max_size: 1024,
            active: true,
        })
    }

    fn set_command(key: &str, value: &str) -> ReplicationCommand {
        ReplicationCommand::Set {
            key: Bytes::from(key.to_owned()),
            value: Bytes::from(value.to_owned()),
        }
    }

    // -- Full resync tests --

    #[tokio::test]
    async fn test_initial_psync_triggers_full_resync() {
        let handler = make_handler();
        let resp = handler.handle_psync("?", -1).await.unwrap();
        assert!(matches!(resp, Psync2Response::FullResync { .. }));
    }

    #[tokio::test]
    async fn test_unknown_replid_returns_error() {
        let handler = make_handler();
        let result = handler.handle_psync("unknown-id-abc", 0).await;
        assert!(matches!(
            result,
            Err(Psync2Error::ReplicationIdMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn test_full_resync_contains_current_state() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;
        let replid = handler.replid().await;
        let offset = handler.current_offset().await;

        let resp = handler.handle_psync("?", -1).await.unwrap();
        match resp {
            Psync2Response::FullResync {
                replid: r,
                offset: o,
            } => {
                assert_eq!(r, replid);
                assert_eq!(o, offset);
            }
            _ => panic!("expected FullResync"),
        }
    }

    // -- Partial resync tests --

    #[tokio::test]
    async fn test_partial_resync_with_matching_replid() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;

        let replid = handler.replid().await;
        let resp = handler.handle_psync(replid.as_str(), 0).await.unwrap();
        assert!(matches!(resp, Psync2Response::PartialResync { .. }));
    }

    #[tokio::test]
    async fn test_partial_resync_at_current_offset() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;

        let replid = handler.replid().await;
        let offset = handler.current_offset().await;
        let resp = handler
            .handle_psync(replid.as_str(), offset as i64)
            .await
            .unwrap();
        match resp {
            Psync2Response::PartialResync { offset: o, .. } => {
                assert_eq!(o, offset);
            }
            _ => panic!("expected PartialResync"),
        }
    }

    #[tokio::test]
    async fn test_offset_ahead_of_primary_returns_error() {
        let handler = make_handler();
        let replid = handler.replid().await;
        let result = handler.handle_psync(replid.as_str(), 9999).await;
        assert!(matches!(
            result,
            Err(Psync2Error::OffsetAheadOfPrimary { .. })
        ));
    }

    // -- Backlog tests --

    #[tokio::test]
    async fn test_backlog_feeds_and_reads() {
        let handler = make_handler();
        let cmd = set_command("x", "y");
        handler.feed_command(&cmd).await;

        let data = handler.get_partial_resync_data(0).await.unwrap();
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn test_backlog_eviction_triggers_full_resync() {
        // Tiny backlog that will overflow quickly
        let handler = Psync2Handler::new(Psync2BacklogConfig {
            max_size: 64,
            active: true,
        });
        let replid = handler.replid().await;

        // Feed enough data to overflow the 64-byte backlog
        for i in 0..20 {
            handler
                .feed_command(&set_command(&format!("key{i}"), "value"))
                .await;
        }

        // Offset 0 should have been evicted
        let result = handler.handle_psync(replid.as_str(), 0).await;
        assert!(matches!(
            result,
            Err(Psync2Error::OffsetBehindBacklog { .. })
        ));
    }

    #[tokio::test]
    async fn test_backlog_size_tracking() {
        let handler = make_handler();
        assert_eq!(handler.backlog_size().await, 0);

        handler.feed_command(&set_command("k", "v")).await;
        assert!(handler.backlog_size().await > 0);
    }

    #[tokio::test]
    async fn test_clear_backlog() {
        let handler = make_handler();
        handler.feed_command(&set_command("k", "v")).await;
        assert!(handler.backlog_size().await > 0);

        handler.clear_backlog().await;
        assert_eq!(handler.backlog_size().await, 0);
    }

    #[tokio::test]
    async fn test_feed_raw_advances_offset() {
        let handler = make_handler();
        let data = Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        let size = data.len() as u64;

        handler.feed_raw(data, 0).await;
        assert_eq!(handler.current_offset().await, size);
    }

    // -- Failover / ID rotation tests --

    #[tokio::test]
    async fn test_rotate_replication_ids() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;

        let old_replid = handler.replid().await;
        let offset_before = handler.current_offset().await;

        handler.rotate_replication_ids().await;

        let new_replid = handler.replid().await;
        let replid2 = handler.replid2().await;
        let second_offset = handler.second_repl_offset().await;

        assert_ne!(new_replid, old_replid);
        assert_eq!(replid2, old_replid);
        assert_eq!(second_offset, offset_before);
    }

    #[tokio::test]
    async fn test_partial_resync_via_secondary_id_after_failover() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;

        let old_replid = handler.replid().await;

        handler.rotate_replication_ids().await;

        // Feed more data under the new ID so the primary offset advances
        handler.feed_command(&set_command("b", "2")).await;

        // A replica that knew the old ID should still be able to partial-resync
        // at offset 0, which is before the failover point
        let resp = handler.handle_psync(old_replid.as_str(), 0).await.unwrap();
        assert!(matches!(resp, Psync2Response::PartialResync { .. }));

        // A replica requesting an offset beyond the failover point with the old
        // ID should get a full resync
        let current = handler.current_offset().await;
        let resp = handler
            .handle_psync(old_replid.as_str(), current as i64)
            .await
            .unwrap();
        assert!(matches!(resp, Psync2Response::FullResync { .. }));
    }

    #[tokio::test]
    async fn test_set_replication_ids() {
        let handler = make_handler();
        let id1 = ReplicationId::from_string("id-one");
        let id2 = ReplicationId::from_string("id-two");

        handler
            .set_replication_ids(id1.clone(), id2.clone(), 500)
            .await;

        assert_eq!(handler.replid().await, id1);
        assert_eq!(handler.replid2().await, id2);
        assert_eq!(handler.second_repl_offset().await, 500);
    }

    #[tokio::test]
    async fn test_set_offset() {
        let handler = make_handler();
        handler.set_offset(42).await;
        assert_eq!(handler.current_offset().await, 42);
    }

    // -- Info / diagnostics tests --

    #[tokio::test]
    async fn test_info_string_contains_fields() {
        let handler = make_handler();
        handler.feed_command(&set_command("a", "1")).await;

        let info = handler.info_string().await;
        assert!(info.contains("master_replid:"));
        assert!(info.contains("master_replid2:"));
        assert!(info.contains("master_repl_offset:"));
        assert!(info.contains("second_repl_offset:"));
        assert!(info.contains("repl_backlog_active:1"));
        assert!(info.contains("repl_backlog_size:1024"));
    }

    // -- Config tests --

    #[test]
    fn test_default_config() {
        let config = Psync2BacklogConfig::default();
        assert_eq!(config.max_size, DEFAULT_BACKLOG_SIZE);
        assert!(config.active);
    }

    #[test]
    fn test_backlog_max_size_accessor() {
        let handler = Psync2Handler::new(Psync2BacklogConfig {
            max_size: 2048,
            active: true,
        });
        assert_eq!(handler.backlog_max_size(), 2048);
    }

    // -- Edge case tests --

    #[tokio::test]
    async fn test_negative_offset_triggers_full_resync() {
        let handler = make_handler();
        let replid = handler.replid().await;
        let resp = handler.handle_psync(replid.as_str(), -1).await.unwrap();
        assert!(matches!(resp, Psync2Response::FullResync { .. }));
    }

    #[tokio::test]
    async fn test_empty_replid_triggers_full_resync() {
        let handler = make_handler();
        let resp = handler.handle_psync("?", 0).await.unwrap();
        assert!(matches!(resp, Psync2Response::FullResync { .. }));
    }

    #[tokio::test]
    async fn test_multiple_feed_commands_accumulate_offset() {
        let handler = make_handler();
        let cmd = set_command("k", "v");
        let single_size = Bytes::from(cmd.to_bytes()).len() as u64;

        handler.feed_command(&cmd).await;
        handler.feed_command(&cmd).await;
        handler.feed_command(&cmd).await;

        assert_eq!(handler.current_offset().await, single_size * 3);
    }

    #[tokio::test]
    async fn test_partial_resync_data_not_available() {
        let handler = make_handler();
        let result = handler.get_partial_resync_data(999).await;
        assert!(matches!(
            result,
            Err(Psync2Error::OffsetBehindBacklog { .. })
        ));
    }
}
