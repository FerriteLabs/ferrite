//! Change log for CDC
//!
//! Persistent, ordered stream of changes with replay capability.

use super::event::ChangeEvent;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Segment identifier
pub type SegmentId = u64;

/// Configuration for the change log
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeLogConfig {
    /// Directory for change log segments
    pub directory: PathBuf,
    /// Maximum segment size before rotation
    pub max_segment_size: u64,
    /// Maximum age of segments to retain
    pub retention_time: Duration,
    /// Maximum total size of all segments
    pub max_total_size: u64,
    /// Compression codec
    pub compression: Compression,
    /// Sync mode
    pub sync_mode: SyncMode,
}

impl Default for ChangeLogConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./data/cdc"),
            max_segment_size: 128 * 1024 * 1024, // 128MB
            retention_time: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_total_size: 10 * 1024 * 1024 * 1024, // 10GB
            compression: Compression::Lz4,
            sync_mode: SyncMode::Async,
        }
    }
}

/// Compression codec for segments
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    /// No compression
    None,
    /// LZ4 compression (fast)
    #[default]
    Lz4,
    /// Zstandard compression (better ratio)
    Zstd,
}

impl Compression {
    /// Get compression name
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::None => data.to_vec(),
            Self::Lz4 => {
                // Simple LZ4 compression
                lz4_flex::compress_prepend_size(data)
            }
            Self::Zstd => {
                // Would use zstd crate in production
                // For now, fallback to LZ4
                lz4_flex::compress_prepend_size(data)
            }
        }
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Lz4 | Self::Zstd => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }
}

/// Sync mode for writes
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncMode {
    /// Async writes (fastest, data may be lost on crash)
    #[default]
    Async,
    /// Sync after each write
    Sync,
    /// Fsync after each write (durability guarantee)
    Fsync,
}

impl SyncMode {
    /// Get sync mode name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Async => "async",
            Self::Sync => "sync",
            Self::Fsync => "fsync",
        }
    }
}

/// A segment of the change log
#[derive(Debug)]
pub struct Segment {
    /// Segment ID
    pub id: SegmentId,
    /// Start event ID
    pub start_id: u64,
    /// End event ID (exclusive)
    pub end_id: u64,
    /// Current size in bytes
    pub size: u64,
    /// Creation time
    pub created_at: SystemTime,
    /// Events in memory (for current segment)
    events: Vec<ChangeEvent>,
    /// File path (for persisted segments)
    path: Option<PathBuf>,
    /// Whether this segment is read-only
    is_readonly: bool,
}

impl Segment {
    /// Create a new segment
    pub fn new(id: SegmentId, start_id: u64) -> Self {
        Self {
            id,
            start_id,
            end_id: start_id,
            size: 0,
            created_at: SystemTime::now(),
            events: Vec::new(),
            path: None,
            is_readonly: false,
        }
    }

    /// Create a segment from a file
    pub fn from_file(id: SegmentId, path: PathBuf) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(&path)?;

        // Read segment header
        let mut file = std::fs::File::open(&path)?;
        let mut header = [0u8; 24];
        file.read_exact(&mut header)?;

        let start_id = u64::from_le_bytes(header[0..8].try_into().unwrap_or_default());
        let end_id = u64::from_le_bytes(header[8..16].try_into().unwrap_or_default());

        Ok(Self {
            id,
            start_id,
            end_id,
            size: metadata.len(),
            created_at: metadata.created().unwrap_or(SystemTime::now()),
            events: Vec::new(),
            path: Some(path),
            is_readonly: true,
        })
    }

    /// Append an event to the segment
    pub fn append(&mut self, event: ChangeEvent) -> std::io::Result<u64> {
        if self.is_readonly {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Segment is read-only",
            ));
        }

        let offset = self.events.len() as u64;
        let event_size = self.estimate_event_size(&event);

        self.events.push(event);
        self.end_id += 1;
        self.size += event_size;

        Ok(offset)
    }

    /// Read an event at offset
    pub fn read_at(&self, offset: u64) -> std::io::Result<ChangeEvent> {
        if !self.events.is_empty() {
            // Read from memory
            self.events
                .get(offset as usize)
                .cloned()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Event not found"))
        } else if let Some(ref path) = self.path {
            // Read from file
            self.read_event_from_file(path, offset)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Segment has no data",
            ))
        }
    }

    /// Read all events in range
    pub fn read_range(&self, from: u64, to: u64) -> std::io::Result<Vec<ChangeEvent>> {
        let mut events = Vec::new();
        for offset in from..to {
            if let Ok(event) = self.read_at(offset) {
                events.push(event);
            } else {
                break;
            }
        }
        Ok(events)
    }

    /// Persist segment to disk
    pub fn persist(
        &mut self,
        dir: &std::path::Path,
        compression: Compression,
    ) -> std::io::Result<()> {
        if self.events.is_empty() {
            return Ok(());
        }

        let path = dir.join(format!("segment-{:016x}.cdc", self.id));

        let mut file = std::fs::File::create(&path)?;

        // Write header
        file.write_all(&self.start_id.to_le_bytes())?;
        file.write_all(&self.end_id.to_le_bytes())?;
        file.write_all(&(self.events.len() as u64).to_le_bytes())?;

        // Serialize and compress events
        let events_json = serde_json::to_vec(&self.events)?;
        let compressed = compression.compress(&events_json);

        // Write compression type
        file.write_all(&[compression as u8])?;
        // Write compressed data length
        file.write_all(&(compressed.len() as u64).to_le_bytes())?;
        // Write compressed data
        file.write_all(&compressed)?;

        file.sync_all()?;

        self.path = Some(path);
        self.events.clear();
        self.is_readonly = true;

        Ok(())
    }

    fn estimate_event_size(&self, event: &ChangeEvent) -> u64 {
        // Rough estimate: key + value + metadata overhead
        let key_size = event.key.len() as u64;
        let value_size = event.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64;
        let old_value_size = event.old_value.as_ref().map(|v| v.len()).unwrap_or(0) as u64;
        key_size + value_size + old_value_size + 128 // 128 bytes overhead
    }

    fn read_event_from_file(
        &self,
        path: &std::path::Path,
        offset: u64,
    ) -> std::io::Result<ChangeEvent> {
        let mut file = std::fs::File::open(path)?;

        // Skip header (24 bytes)
        let mut header = [0u8; 24];
        file.read_exact(&mut header)?;

        // Read compression type
        let mut compression_byte = [0u8; 1];
        file.read_exact(&mut compression_byte)?;
        let compression = match compression_byte[0] {
            0 => Compression::None,
            1 => Compression::Lz4,
            2 => Compression::Zstd,
            _ => Compression::None,
        };

        // Read data length
        let mut len_bytes = [0u8; 8];
        file.read_exact(&mut len_bytes)?;
        let data_len = u64::from_le_bytes(len_bytes) as usize;

        // Read compressed data
        let mut compressed = vec![0u8; data_len];
        file.read_exact(&mut compressed)?;

        // Decompress
        let decompressed = compression.decompress(&compressed)?;

        // Deserialize events
        let events: Vec<ChangeEvent> = serde_json::from_slice(&decompressed)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        events
            .get(offset as usize)
            .cloned()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Event not found"))
    }

    /// Get number of events in segment
    pub fn len(&self) -> usize {
        (self.end_id - self.start_id) as usize
    }

    /// Check if segment is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get age of segment
    pub fn age(&self) -> Duration {
        self.created_at.elapsed().unwrap_or(Duration::ZERO)
    }
}

/// Persistent change log
pub struct ChangeLog {
    /// Current segment (writable)
    current_segment: RwLock<Segment>,
    /// Historical segments (read-only)
    segments: RwLock<Vec<Segment>>,
    /// Index: event_id -> (segment_id, offset)
    index: RwLock<BTreeMap<u64, (SegmentId, u64)>>,
    /// Next event ID
    next_id: AtomicU64,
    /// Next segment ID
    next_segment_id: AtomicU64,
    /// Configuration
    config: ChangeLogConfig,
}

impl ChangeLog {
    /// Create a new change log
    pub fn new(config: ChangeLogConfig) -> std::io::Result<Self> {
        // Create directory if needed
        std::fs::create_dir_all(&config.directory)?;

        // Load existing segments
        let (segments, max_id, max_segment_id) = Self::load_segments(&config.directory)?;

        let next_id = max_id + 1;
        let next_segment_id = max_segment_id + 1;

        Ok(Self {
            current_segment: RwLock::new(Segment::new(next_segment_id, next_id)),
            segments: RwLock::new(segments),
            index: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(next_id),
            next_segment_id: AtomicU64::new(next_segment_id + 1),
            config,
        })
    }

    /// Append a change event
    pub async fn append(&self, mut event: ChangeEvent) -> std::io::Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        event.id = id;

        let mut segment = self.current_segment.write().await;
        let offset = segment.append(event)?;

        // Update index
        {
            let mut index = self.index.write().await;
            index.insert(id, (segment.id, offset));
        }

        // Rotate if needed
        if segment.size >= self.config.max_segment_size {
            drop(segment);
            self.rotate_segment().await?;
        }

        Ok(id)
    }

    /// Read events from a position
    pub async fn read_from(&self, from_id: u64, limit: usize) -> std::io::Result<Vec<ChangeEvent>> {
        let mut events = Vec::with_capacity(limit);

        // First check current segment
        let current = self.current_segment.read().await;
        if from_id >= current.start_id && from_id < current.end_id {
            let offset = from_id - current.start_id;
            for i in 0..limit {
                if let Ok(event) = current.read_at(offset + i as u64) {
                    events.push(event);
                } else {
                    break;
                }
            }
            return Ok(events);
        }
        drop(current);

        // Check historical segments
        let segments = self.segments.read().await;
        for segment in segments.iter() {
            if from_id >= segment.start_id && from_id < segment.end_id {
                let offset = from_id - segment.start_id;
                let remaining = limit - events.len();
                let to_read = std::cmp::min(remaining, (segment.end_id - from_id) as usize);
                if let Ok(mut seg_events) = segment.read_range(offset, offset + to_read as u64) {
                    events.append(&mut seg_events);
                }

                if events.len() >= limit {
                    break;
                }
            }
        }

        Ok(events)
    }

    /// Read events in a time range
    pub async fn read_time_range(
        &self,
        from: SystemTime,
        to: SystemTime,
        limit: usize,
    ) -> std::io::Result<Vec<ChangeEvent>> {
        let mut events = Vec::new();

        // Read from all segments and filter by time
        let current = self.current_segment.read().await;
        for i in 0..current.len() {
            if events.len() >= limit {
                break;
            }
            if let Ok(event) = current.read_at(i as u64) {
                if event.timestamp >= from && event.timestamp <= to {
                    events.push(event);
                }
            }
        }
        drop(current);

        let segments = self.segments.read().await;
        for segment in segments.iter() {
            if events.len() >= limit {
                break;
            }
            for i in 0..segment.len() {
                if events.len() >= limit {
                    break;
                }
                if let Ok(event) = segment.read_at(i as u64) {
                    if event.timestamp >= from && event.timestamp <= to {
                        events.push(event);
                    }
                }
            }
        }

        Ok(events)
    }

    /// Get current position (next event ID)
    pub fn current_position(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    /// Get oldest event ID
    pub async fn oldest_position(&self) -> u64 {
        let segments = self.segments.read().await;
        if let Some(oldest) = segments.first() {
            oldest.start_id
        } else {
            let current = self.current_segment.read().await;
            current.start_id
        }
    }

    /// Rotate to a new segment
    async fn rotate_segment(&self) -> std::io::Result<()> {
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let next_id = self.next_id.load(Ordering::SeqCst);

        let mut current = self.current_segment.write().await;

        // Persist current segment
        current.persist(&self.config.directory, self.config.compression)?;

        // Move to historical segments
        let old_segment = std::mem::replace(&mut *current, Segment::new(segment_id, next_id));

        let mut segments = self.segments.write().await;
        segments.push(old_segment);

        // Apply retention policy
        drop(segments);
        self.apply_retention().await?;

        Ok(())
    }

    /// Apply retention policy to remove old segments
    async fn apply_retention(&self) -> std::io::Result<()> {
        let mut segments = self.segments.write().await;
        let mut total_size: u64 = segments.iter().map(|s| s.size).sum();

        // Remove segments that exceed retention time or total size
        segments.retain(|segment| {
            let too_old = segment.age() > self.config.retention_time;
            let too_large = total_size > self.config.max_total_size;

            if too_old || too_large {
                total_size = total_size.saturating_sub(segment.size);
                // Delete file if it exists
                if let Some(ref path) = segment.path {
                    let _ = std::fs::remove_file(path);
                }
                false
            } else {
                true
            }
        });

        Ok(())
    }

    /// Load existing segments from directory
    fn load_segments(dir: &std::path::Path) -> std::io::Result<(Vec<Segment>, u64, SegmentId)> {
        let mut segments = Vec::new();
        let mut max_id = 0u64;
        let mut max_segment_id = 0u64;

        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.extension().map(|e| e == "cdc").unwrap_or(false) {
                    // Parse segment ID from filename
                    if let Some(filename) = path.file_stem() {
                        if let Some(id_str) =
                            filename.to_str().and_then(|s| s.strip_prefix("segment-"))
                        {
                            if let Ok(segment_id) = u64::from_str_radix(id_str, 16) {
                                if let Ok(segment) = Segment::from_file(segment_id, path) {
                                    max_id = max_id.max(segment.end_id);
                                    max_segment_id = max_segment_id.max(segment.id);
                                    segments.push(segment);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by start_id
        segments.sort_by_key(|s| s.start_id);

        Ok((segments, max_id, max_segment_id))
    }

    /// Get log statistics
    pub async fn stats(&self) -> ChangeLogStats {
        let segments = self.segments.read().await;
        let current = self.current_segment.read().await;

        let total_segments = segments.len() + 1;
        let total_events = segments.iter().map(|s| s.len()).sum::<usize>() + current.len();
        let total_size = segments.iter().map(|s| s.size).sum::<u64>() + current.size;

        let oldest_event = segments
            .first()
            .map(|s| s.start_id)
            .unwrap_or(current.start_id);
        let newest_event = current.end_id.saturating_sub(1);

        ChangeLogStats {
            total_segments,
            total_events,
            total_size,
            oldest_event,
            newest_event,
            compression: self.config.compression,
            retention_time: self.config.retention_time,
        }
    }

    /// Compact the log (remove gaps, merge small segments)
    pub async fn compact(&self) -> std::io::Result<CompactionResult> {
        // For now, just apply retention
        self.apply_retention().await?;

        let stats = self.stats().await;
        Ok(CompactionResult {
            segments_removed: 0,
            bytes_reclaimed: 0,
            final_segments: stats.total_segments,
            final_size: stats.total_size,
        })
    }
}

impl Default for ChangeLog {
    fn default() -> Self {
        Self::new(ChangeLogConfig::default()).expect("Failed to create default change log")
    }
}

/// Statistics about the change log
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeLogStats {
    /// Total number of segments
    pub total_segments: usize,
    /// Total number of events
    pub total_events: usize,
    /// Total size in bytes
    pub total_size: u64,
    /// Oldest event ID
    pub oldest_event: u64,
    /// Newest event ID
    pub newest_event: u64,
    /// Compression codec
    pub compression: Compression,
    /// Retention time
    #[serde(with = "duration_serde")]
    pub retention_time: Duration,
}

impl ChangeLogStats {
    /// Get total size in human-readable format
    pub fn total_size_human(&self) -> String {
        if self.total_size >= 1024 * 1024 * 1024 {
            format!(
                "{:.2}GB",
                self.total_size as f64 / (1024.0 * 1024.0 * 1024.0)
            )
        } else if self.total_size >= 1024 * 1024 {
            format!("{:.2}MB", self.total_size as f64 / (1024.0 * 1024.0))
        } else if self.total_size >= 1024 {
            format!("{:.2}KB", self.total_size as f64 / 1024.0)
        } else {
            format!("{}B", self.total_size)
        }
    }

    /// Get retention time in human-readable format
    pub fn retention_human(&self) -> String {
        let secs = self.retention_time.as_secs();
        if secs >= 86400 {
            format!("{}d", secs / 86400)
        } else if secs >= 3600 {
            format!("{}h", secs / 3600)
        } else if secs >= 60 {
            format!("{}m", secs / 60)
        } else {
            format!("{}s", secs)
        }
    }
}

/// Result of compaction operation
#[derive(Clone, Debug)]
pub struct CompactionResult {
    /// Number of segments removed
    pub segments_removed: usize,
    /// Bytes reclaimed
    pub bytes_reclaimed: u64,
    /// Final segment count
    pub final_segments: usize,
    /// Final total size
    pub final_size: u64,
}

// Serde helper for Duration
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_compression() {
        let data = b"hello world, this is a test message for compression";

        let compressed = Compression::Lz4.compress(data);
        let decompressed = Compression::Lz4.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_segment_append() {
        let mut segment = Segment::new(1, 100);

        let event = ChangeEvent::new(
            super::super::event::Operation::Set,
            Bytes::from_static(b"key"),
            0,
        );

        let offset = segment.append(event).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(segment.len(), 1);
        assert_eq!(segment.start_id, 100);
        assert_eq!(segment.end_id, 101);
    }

    #[test]
    fn test_segment_read() {
        let mut segment = Segment::new(1, 100);

        let event = ChangeEvent::new(
            super::super::event::Operation::Set,
            Bytes::from_static(b"key1"),
            0,
        );
        segment.append(event).unwrap();

        let event = ChangeEvent::new(
            super::super::event::Operation::Del,
            Bytes::from_static(b"key2"),
            0,
        );
        segment.append(event).unwrap();

        let read_event = segment.read_at(0).unwrap();
        assert_eq!(read_event.key, Bytes::from_static(b"key1"));

        let read_event = segment.read_at(1).unwrap();
        assert_eq!(read_event.key, Bytes::from_static(b"key2"));
    }

    #[tokio::test]
    async fn test_change_log_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = ChangeLogConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };

        let log = ChangeLog::new(config).unwrap();

        // Append some events
        let event1 = ChangeEvent::new(
            super::super::event::Operation::Set,
            Bytes::from_static(b"key1"),
            0,
        );
        let id1 = log.append(event1).await.unwrap();

        let event2 = ChangeEvent::new(
            super::super::event::Operation::Del,
            Bytes::from_static(b"key2"),
            0,
        );
        let id2 = log.append(event2).await.unwrap();

        assert_eq!(id2, id1 + 1);

        // Read events
        let events = log.read_from(id1, 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].key, Bytes::from_static(b"key1"));
        assert_eq!(events[1].key, Bytes::from_static(b"key2"));
    }

    #[tokio::test]
    async fn test_change_log_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config = ChangeLogConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };

        let log = ChangeLog::new(config).unwrap();

        // Append some events
        for i in 0..5 {
            let event = ChangeEvent::new(
                super::super::event::Operation::Set,
                Bytes::from(format!("key{}", i)),
                0,
            );
            log.append(event).await.unwrap();
        }

        let stats = log.stats().await;
        assert_eq!(stats.total_events, 5);
        assert_eq!(stats.total_segments, 1);
    }
}
