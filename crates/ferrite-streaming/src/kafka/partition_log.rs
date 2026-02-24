#![forbid(unsafe_code)]
//! Append-only partition log for Kafka-compatible streaming.

use super::record::ConsumerRecord;

/// An in-memory, append-only log for a single partition.
#[derive(Debug)]
pub struct PartitionLog {
    records: Vec<ConsumerRecord>,
    /// Offset of the next record to be appended.
    pub high_watermark: i64,
    /// Earliest available offset (may advance after truncation).
    pub log_start_offset: i64,
}

impl PartitionLog {
    /// Create a new empty partition log.
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            high_watermark: 0,
            log_start_offset: 0,
        }
    }

    /// Append a record and return its assigned offset.
    pub fn append(&mut self, mut record: ConsumerRecord) -> i64 {
        let offset = self.high_watermark;
        record.offset = offset;
        self.records.push(record);
        self.high_watermark += 1;
        offset
    }

    /// Read up to `max_records` starting from `from_offset`.
    pub fn read(&self, from_offset: i64, max_records: usize) -> Vec<ConsumerRecord> {
        if from_offset < self.log_start_offset || from_offset >= self.high_watermark {
            return Vec::new();
        }
        let start = (from_offset - self.log_start_offset) as usize;
        self.records
            .iter()
            .skip(start)
            .take(max_records)
            .cloned()
            .collect()
    }

    /// Truncate records before the given offset (retention enforcement).
    pub fn truncate_before(&mut self, offset: i64) {
        if offset <= self.log_start_offset {
            return;
        }
        let remove_count = (offset - self.log_start_offset) as usize;
        if remove_count >= self.records.len() {
            self.records.clear();
        } else {
            self.records.drain(..remove_count);
        }
        self.log_start_offset = offset;
    }

    /// Number of records currently held.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl Default for PartitionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(topic: &str, partition: u32) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.into(),
            partition,
            offset: 0,
            key: None,
            value: b"data".to_vec(),
            headers: vec![],
            timestamp: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_append_and_read() {
        let mut log = PartitionLog::new();
        let o1 = log.append(make_record("t", 0));
        let o2 = log.append(make_record("t", 0));
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
        assert_eq!(log.high_watermark, 2);

        let records = log.read(0, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
    }

    #[test]
    fn test_read_from_middle() {
        let mut log = PartitionLog::new();
        for _ in 0..5 {
            log.append(make_record("t", 0));
        }
        let records = log.read(3, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 3);
    }

    #[test]
    fn test_read_out_of_range() {
        let log = PartitionLog::new();
        assert!(log.read(0, 10).is_empty());
        assert!(log.read(5, 10).is_empty());
    }

    #[test]
    fn test_truncate_before() {
        let mut log = PartitionLog::new();
        for _ in 0..5 {
            log.append(make_record("t", 0));
        }
        log.truncate_before(3);
        assert_eq!(log.log_start_offset, 3);
        assert_eq!(log.len(), 2);
        let records = log.read(3, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 3);
    }

    #[test]
    fn test_truncate_all() {
        let mut log = PartitionLog::new();
        for _ in 0..3 {
            log.append(make_record("t", 0));
        }
        log.truncate_before(10);
        assert!(log.is_empty());
        assert_eq!(log.log_start_offset, 10);
    }
}
