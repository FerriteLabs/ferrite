//! Log Address encoding for HybridLog
//!
//! This module implements the LogAddress type which encodes both
//! the region (mutable/read-only/disk) and the offset within that region.

use std::fmt;

/// Region types in the HybridLog
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Region {
    /// Hot data - in-memory, mutable, lock-free reads
    Mutable = 0,
    /// Warm data - memory-mapped, read-only, zero-copy
    ReadOnly = 1,
    /// Cold data - on disk, async I/O
    Disk = 2,
    /// Special value indicating address is invalid/null
    Invalid = 3,
}

impl From<u8> for Region {
    fn from(value: u8) -> Self {
        match value {
            0 => Region::Mutable,
            1 => Region::ReadOnly,
            2 => Region::Disk,
            _ => Region::Invalid,
        }
    }
}

/// A log address encoding region and offset
///
/// Layout (64 bits):
/// - Bits 62-63: Region (2 bits, 4 possible values)
/// - Bits 0-61: Offset (62 bits, ~4 exabytes addressable)
///
/// Special value: 0xFFFFFFFFFFFFFFFF represents null/invalid
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct LogAddress(u64);

impl LogAddress {
    /// The null/invalid address
    pub const NULL: LogAddress = LogAddress(u64::MAX);

    /// Number of bits used for offset
    const OFFSET_BITS: u32 = 62;

    /// Mask for extracting offset
    const OFFSET_MASK: u64 = (1u64 << Self::OFFSET_BITS) - 1;

    /// Create a new log address from region and offset
    pub fn new(region: Region, offset: u64) -> Self {
        debug_assert!(
            offset <= Self::OFFSET_MASK,
            "offset exceeds maximum addressable range"
        );
        let region_bits = (region as u64) << Self::OFFSET_BITS;
        Self(region_bits | (offset & Self::OFFSET_MASK))
    }

    /// Create a null/invalid address
    pub const fn null() -> Self {
        Self::NULL
    }

    /// Check if this address is null/invalid
    pub fn is_null(&self) -> bool {
        self.0 == u64::MAX
    }

    /// Get the region this address points to
    pub fn region(&self) -> Region {
        if self.is_null() {
            Region::Invalid
        } else {
            Region::from((self.0 >> Self::OFFSET_BITS) as u8)
        }
    }

    /// Get the offset within the region
    pub fn offset(&self) -> u64 {
        if self.is_null() {
            0
        } else {
            self.0 & Self::OFFSET_MASK
        }
    }

    /// Get the raw u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Create from raw u64 value
    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Create a mutable region address
    pub fn mutable(offset: u64) -> Self {
        Self::new(Region::Mutable, offset)
    }

    /// Create a read-only region address
    pub fn read_only(offset: u64) -> Self {
        Self::new(Region::ReadOnly, offset)
    }

    /// Create a disk region address
    pub fn disk(offset: u64) -> Self {
        Self::new(Region::Disk, offset)
    }

    /// Check if address points to mutable region
    pub fn is_mutable(&self) -> bool {
        self.region() == Region::Mutable
    }

    /// Check if address points to read-only region
    pub fn is_read_only(&self) -> bool {
        self.region() == Region::ReadOnly
    }

    /// Check if address points to disk region
    pub fn is_disk(&self) -> bool {
        self.region() == Region::Disk
    }

    /// Advance the offset by a given amount
    pub fn advance(&self, bytes: u64) -> Self {
        Self::new(self.region(), self.offset() + bytes)
    }
}

impl fmt::Debug for LogAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "LogAddress(NULL)")
        } else {
            write!(f, "LogAddress({:?}:0x{:x})", self.region(), self.offset())
        }
    }
}

impl fmt::Display for LogAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "null")
        } else {
            write!(
                f,
                "{}:{:#x}",
                match self.region() {
                    Region::Mutable => "M",
                    Region::ReadOnly => "RO",
                    Region::Disk => "D",
                    Region::Invalid => "?",
                },
                self.offset()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_address() {
        let addr = LogAddress::null();
        assert!(addr.is_null());
        assert_eq!(addr.region(), Region::Invalid);
        assert_eq!(addr.offset(), 0);
    }

    #[test]
    fn test_mutable_address() {
        let addr = LogAddress::mutable(0x1234);
        assert!(!addr.is_null());
        assert!(addr.is_mutable());
        assert_eq!(addr.region(), Region::Mutable);
        assert_eq!(addr.offset(), 0x1234);
    }

    #[test]
    fn test_readonly_address() {
        let addr = LogAddress::read_only(0xABCD);
        assert!(!addr.is_null());
        assert!(addr.is_read_only());
        assert_eq!(addr.region(), Region::ReadOnly);
        assert_eq!(addr.offset(), 0xABCD);
    }

    #[test]
    fn test_disk_address() {
        let addr = LogAddress::disk(0xDEAD);
        assert!(!addr.is_null());
        assert!(addr.is_disk());
        assert_eq!(addr.region(), Region::Disk);
        assert_eq!(addr.offset(), 0xDEAD);
    }

    #[test]
    fn test_large_offset() {
        // Test with a large offset near the 62-bit limit
        let large_offset = (1u64 << 61) - 1;
        let addr = LogAddress::mutable(large_offset);
        assert_eq!(addr.offset(), large_offset);
        assert!(addr.is_mutable());
    }

    #[test]
    fn test_advance() {
        let addr = LogAddress::mutable(100);
        let advanced = addr.advance(50);
        assert_eq!(advanced.offset(), 150);
        assert!(advanced.is_mutable());
    }

    #[test]
    fn test_round_trip() {
        let addr = LogAddress::new(Region::ReadOnly, 0x123456789);
        let raw = addr.as_u64();
        let recovered = LogAddress::from_u64(raw);
        assert_eq!(addr, recovered);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", LogAddress::null()), "null");
        assert_eq!(format!("{}", LogAddress::mutable(0x100)), "M:0x100");
        assert_eq!(format!("{}", LogAddress::read_only(0x200)), "RO:0x200");
        assert_eq!(format!("{}", LogAddress::disk(0x300)), "D:0x300");
    }

    #[test]
    fn test_debug() {
        let addr = LogAddress::mutable(0x42);
        let debug_str = format!("{:?}", addr);
        assert!(debug_str.contains("Mutable"));
        assert!(debug_str.contains("0x42"));
    }
}
