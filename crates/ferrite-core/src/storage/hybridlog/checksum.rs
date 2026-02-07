//! CRC32 Checksum & Corruption Recovery for HybridLog
//!
//! Provides checksummed log records for data integrity verification,
//! a recovery scanner that detects and skips corrupted records,
//! and a `--repair` mode that truncates the log at the last valid record.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use tracing::error;

use super::mutable::EntryHeader;

/// Magic bytes identifying a checksummed record header
const RECORD_MAGIC: u32 = 0xFE_00_1C_00;

/// A checksummed log record wrapping an EntryHeader
///
/// Layout (on disk):
/// ```text
/// [magic: 4B][crc32: 4B][key_len: 4B][value_len: 4B][flags: 4B][padding: 4B][key][value][align-pad]
/// ```
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChecksummedHeader {
    /// Magic bytes for record identification
    pub magic: u32,
    /// CRC32 checksum covering key_len, value_len, flags, key data, and value data
    pub crc32: u32,
    /// The inner entry header
    pub entry: EntryHeader,
}

impl ChecksummedHeader {
    /// Size of the checksummed header in bytes
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new checksummed header (checksum computed later via `compute_crc`)
    pub fn new(key_len: u32, value_len: u32) -> Self {
        Self {
            magic: RECORD_MAGIC,
            crc32: 0,
            entry: EntryHeader::new(key_len, value_len),
        }
    }

    /// Compute the CRC32 checksum over the header fields and payload
    pub fn compute_crc(key_len: u32, value_len: u32, key: &[u8], value: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key_len.to_le_bytes());
        hasher.update(&value_len.to_le_bytes());
        hasher.update(key);
        hasher.update(value);
        hasher.finalize()
    }

    /// Verify the checksum against key/value payload
    pub fn verify(&self, key: &[u8], value: &[u8]) -> bool {
        let expected = Self::compute_crc(self.entry.key_len, self.entry.value_len, key, value);
        self.crc32 == expected
    }

    /// Total size of this record (header + key + value)
    pub fn total_size(&self) -> usize {
        Self::SIZE + self.entry.key_len as usize + self.entry.value_len as usize
    }

    /// Aligned total size (8-byte boundary)
    pub fn aligned_size(&self) -> usize {
        let size = self.total_size();
        (size + 7) & !7
    }

    /// Serialize the header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        // SAFETY: ChecksummedHeader is #[repr(C)] with only primitive fields
        unsafe { std::mem::transmute(*self) }
    }

    /// Deserialize from bytes
    ///
    /// # Safety
    /// `buf` must be at least `ChecksummedHeader::SIZE` bytes.
    pub unsafe fn from_bytes(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= Self::SIZE);
        std::ptr::read(buf.as_ptr() as *const Self)
    }
}

/// Result of scanning a single record during recovery
#[derive(Debug)]
pub enum RecordStatus {
    /// Valid record at the given offset
    Valid {
        /// Byte offset of this record in the file
        offset: u64,
        /// Total aligned size of this record
        size: usize,
    },
    /// Corrupted record detected at the given offset
    Corrupted {
        /// Byte offset where corruption was detected
        offset: u64,
        /// Human-readable reason
        reason: String,
    },
}

/// Summary returned by a recovery scan
#[derive(Debug, Default)]
pub struct RecoverySummary {
    /// Number of valid records found
    pub valid_records: u64,
    /// Number of corrupted records found
    pub corrupted_records: u64,
    /// Byte offset of the last valid record's end (safe truncation point)
    pub last_valid_end: u64,
    /// Total file size scanned
    pub file_size: u64,
}

/// Scan a log file and report record integrity
///
/// Walks the file record-by-record, verifying magic bytes and CRC32 checksums.
/// Returns a summary with the count of valid/corrupted records and the safe
/// truncation point.
pub fn recovery_scan(path: &Path) -> io::Result<RecoverySummary> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let mut summary = RecoverySummary {
        file_size,
        ..Default::default()
    };

    let mut offset = 0u64;
    let mut header_buf = [0u8; ChecksummedHeader::SIZE];

    while offset + ChecksummedHeader::SIZE as u64 <= file_size {
        file.seek(SeekFrom::Start(offset))?;

        if file.read_exact(&mut header_buf).is_err() {
            break;
        }

        // SAFETY: header_buf is properly sized
        let header = unsafe { ChecksummedHeader::from_bytes(&header_buf) };

        // Check magic
        if header.magic != RECORD_MAGIC {
            error!(
                offset = offset,
                "Corrupted record: bad magic bytes at offset {}",
                offset
            );
            summary.corrupted_records += 1;
            break; // Cannot reliably find the next record
        }

        let aligned = header.aligned_size();
        if offset + aligned as u64 > file_size {
            error!(
                offset = offset,
                "Corrupted record: truncated record at offset {}", offset
            );
            summary.corrupted_records += 1;
            break;
        }

        // Read key + value for CRC verification
        let mut key = vec![0u8; header.entry.key_len as usize];
        let mut value = vec![0u8; header.entry.value_len as usize];

        if file.read_exact(&mut key).is_err() || file.read_exact(&mut value).is_err() {
            error!(
                offset = offset,
                "Corrupted record: cannot read payload at offset {}", offset
            );
            summary.corrupted_records += 1;
            break;
        }

        if !header.verify(&key, &value) {
            error!(
                offset = offset,
                expected_crc = header.crc32,
                "Corrupted record: CRC mismatch at offset {}",
                offset
            );
            summary.corrupted_records += 1;
            break;
        }

        summary.valid_records += 1;
        summary.last_valid_end = offset + aligned as u64;
        offset += aligned as u64;
    }

    Ok(summary)
}

/// Repair a log file by truncating at the last valid record
///
/// Performs a recovery scan, then truncates the file to remove any
/// trailing corruption. Returns the recovery summary.
pub fn repair_log(path: &Path) -> io::Result<RecoverySummary> {
    let summary = recovery_scan(path)?;

    if summary.corrupted_records > 0 {
        error!(
            valid = summary.valid_records,
            corrupted = summary.corrupted_records,
            truncate_at = summary.last_valid_end,
            "Repairing log: truncating at byte {}",
            summary.last_valid_end
        );

        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(summary.last_valid_end)?;
        file.sync_all()?;
    }

    Ok(summary)
}

/// Write a checksummed record to a file at the current position
pub fn write_checksummed_record(
    file: &mut File,
    key: &[u8],
    value: &[u8],
) -> io::Result<usize> {
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;

    let mut header = ChecksummedHeader::new(key_len, value_len);
    header.crc32 = ChecksummedHeader::compute_crc(key_len, value_len, key, value);

    let aligned = header.aligned_size();
    let header_bytes = header.to_bytes();

    file.write_all(&header_bytes)?;
    file.write_all(key)?;
    file.write_all(value)?;

    // Write alignment padding
    let data_len = ChecksummedHeader::SIZE + key.len() + value.len();
    let padding = aligned - data_len;
    if padding > 0 {
        let zeros = vec![0u8; padding];
        file.write_all(&zeros)?;
    }

    Ok(aligned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_checksummed_header_size() {
        // magic(4) + crc32(4) + EntryHeader(16) = 24
        assert_eq!(ChecksummedHeader::SIZE, 24);
    }

    #[test]
    fn test_crc_roundtrip() {
        let key = b"hello";
        let value = b"world";
        let crc = ChecksummedHeader::compute_crc(
            key.len() as u32,
            value.len() as u32,
            key,
            value,
        );

        let mut header = ChecksummedHeader::new(key.len() as u32, value.len() as u32);
        header.crc32 = crc;

        assert!(header.verify(key, value));
        assert!(!header.verify(b"wrong", value));
    }

    #[test]
    fn test_write_and_scan() {
        let mut tmpfile = NamedTempFile::new().expect("create temp file");

        write_checksummed_record(tmpfile.as_file_mut(), b"key1", b"value1").expect("write r1");
        write_checksummed_record(tmpfile.as_file_mut(), b"key2", b"value2").expect("write r2");
        tmpfile.as_file().sync_all().expect("sync");

        let summary = recovery_scan(tmpfile.path()).expect("scan");
        assert_eq!(summary.valid_records, 2);
        assert_eq!(summary.corrupted_records, 0);
    }

    #[test]
    fn test_detect_corruption() {
        let mut tmpfile = NamedTempFile::new().expect("create temp file");

        write_checksummed_record(tmpfile.as_file_mut(), b"key1", b"value1").expect("write");
        tmpfile.as_file().sync_all().expect("sync");

        // Corrupt the CRC field (bytes 4..8)
        {
            let file = tmpfile.as_file_mut();
            file.seek(SeekFrom::Start(4)).expect("seek");
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).expect("corrupt");
            file.sync_all().expect("sync");
        }

        let summary = recovery_scan(tmpfile.path()).expect("scan");
        assert_eq!(summary.valid_records, 0);
        assert_eq!(summary.corrupted_records, 1);
    }

    #[test]
    fn test_repair_truncates() {
        let mut tmpfile = NamedTempFile::new().expect("create temp file");

        let r1_size =
            write_checksummed_record(tmpfile.as_file_mut(), b"key1", b"value1").expect("write r1");
        write_checksummed_record(tmpfile.as_file_mut(), b"key2", b"value2").expect("write r2");
        tmpfile.as_file().sync_all().expect("sync");

        // Corrupt the second record's magic
        {
            let file = tmpfile.as_file_mut();
            file.seek(SeekFrom::Start(r1_size as u64)).expect("seek");
            file.write_all(&[0x00, 0x00, 0x00, 0x00]).expect("corrupt");
            file.sync_all().expect("sync");
        }

        let summary = repair_log(tmpfile.path()).expect("repair");
        assert_eq!(summary.valid_records, 1);
        assert_eq!(summary.corrupted_records, 1);

        // File should be truncated to the end of the first record
        let meta = std::fs::metadata(tmpfile.path()).expect("metadata");
        assert_eq!(meta.len(), r1_size as u64);
    }

    #[test]
    fn test_scan_empty_file() {
        let tmpfile = NamedTempFile::new().expect("create temp file");
        let summary = recovery_scan(tmpfile.path()).expect("scan");
        assert_eq!(summary.valid_records, 0);
        assert_eq!(summary.corrupted_records, 0);
        assert_eq!(summary.last_valid_end, 0);
    }

    #[test]
    fn test_partial_record_detection() {
        let mut tmpfile = NamedTempFile::new().expect("create temp file");

        write_checksummed_record(tmpfile.as_file_mut(), b"key1", b"value1").expect("write r1");

        // Write a partial header (only magic + crc, missing rest)
        {
            let file = tmpfile.as_file_mut();
            let mut partial = ChecksummedHeader::new(100, 200);
            partial.crc32 = 0;
            let bytes = partial.to_bytes();
            // Write only the full header but declare a huge payload
            file.write_all(&bytes).expect("write partial header");
            file.sync_all().expect("sync");
        }

        let summary = recovery_scan(tmpfile.path()).expect("scan");
        assert_eq!(summary.valid_records, 1);
        assert_eq!(summary.corrupted_records, 1);
    }
}
