//! Hash slot calculation and management
//!
//! Redis Cluster uses CRC16 to distribute keys across 16384 hash slots.
//! This module implements hash slot calculation including support for hash tags,
//! a consolidated [`SlotMap`] for slot-to-node routing with migration awareness,
//! and [`Redirect`] responses for `-MOVED` / `-ASK` client redirects.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::RangeInclusive;

/// Total number of hash slots in a Redis cluster
pub const CLUSTER_SLOTS: u16 = 16384;

/// CRC16 lookup table (XMODEM polynomial)
const CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Calculate CRC16 using XMODEM polynomial (Redis compatible)
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        let index = ((crc >> 8) ^ (*byte as u16)) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[index & 0xff];
    }
    crc
}

/// Hash slot utilities
pub struct HashSlot;

impl HashSlot {
    /// Calculate the hash slot for a key
    ///
    /// If the key contains a hash tag (e.g., "{user}:1000"), only the content
    /// within the first pair of curly braces is used for hashing. This allows
    /// related keys to be placed on the same node.
    pub fn for_key(key: &[u8]) -> u16 {
        let hash_key = Self::extract_hash_tag(key);
        crc16(hash_key) % CLUSTER_SLOTS
    }

    /// Extract the hash tag from a key
    ///
    /// Returns the portion between the first `{` and following `}` if present,
    /// otherwise returns the entire key.
    fn extract_hash_tag(key: &[u8]) -> &[u8] {
        // Find the first '{'
        if let Some(start) = key.iter().position(|&b| b == b'{') {
            // Find the first '}' after '{'
            if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
                let tag = &key[start + 1..start + 1 + end];
                // Only use tag if it's not empty
                if !tag.is_empty() {
                    return tag;
                }
            }
        }
        key
    }

    /// Get all slots in a range
    pub fn range(start: u16, end: u16) -> RangeInclusive<u16> {
        start..=end.min(CLUSTER_SLOTS - 1)
    }
}

/// A range of hash slots
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotRange {
    /// Start slot (inclusive)
    pub start: u16,
    /// End slot (inclusive)
    pub end: u16,
}

impl SlotRange {
    /// Create a new slot range
    pub fn new(start: u16, end: u16) -> Self {
        Self {
            start: start.min(CLUSTER_SLOTS - 1),
            end: end.min(CLUSTER_SLOTS - 1),
        }
    }

    /// Create a single-slot range
    pub fn single(slot: u16) -> Self {
        Self::new(slot, slot)
    }

    /// Check if a slot is contained in this range
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }

    /// Get the number of slots in this range
    pub fn count(&self) -> usize {
        if self.end >= self.start {
            (self.end - self.start + 1) as usize
        } else {
            0
        }
    }

    /// Check if this range overlaps with another
    pub fn overlaps(&self, other: &SlotRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Merge two adjacent or overlapping ranges
    pub fn merge(&self, other: &SlotRange) -> Option<SlotRange> {
        // Check if ranges are adjacent or overlapping
        if self.end + 1 >= other.start && other.end + 1 >= self.start {
            Some(SlotRange::new(
                self.start.min(other.start),
                self.end.max(other.end),
            ))
        } else {
            None
        }
    }

    /// Split this range at a given slot
    pub fn split(&self, slot: u16) -> (Option<SlotRange>, Option<SlotRange>) {
        if !self.contains(slot) {
            return (Some(self.clone()), None);
        }

        let left = if slot > self.start {
            Some(SlotRange::new(self.start, slot - 1))
        } else {
            None
        };

        let right = if slot < self.end {
            Some(SlotRange::new(slot + 1, self.end))
        } else {
            None
        };

        (left, right)
    }

    /// Iterate over all slots in this range
    pub fn iter(&self) -> impl Iterator<Item = u16> {
        self.start..=self.end
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

/// Slot migration state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotMigrationState {
    /// Slot is stable (not being migrated)
    Stable,
    /// Slot is being imported from another node
    Importing,
    /// Slot is being migrated to another node
    Migrating,
}

/// Slot information with migration state
#[derive(Debug, Clone)]
pub struct SlotInfo {
    /// The slot number
    pub slot: u16,
    /// Node ID that owns this slot
    pub owner: Option<String>,
    /// Migration state
    pub state: SlotMigrationState,
    /// Target node for migration (if migrating)
    pub migration_target: Option<String>,
    /// Source node for import (if importing)
    pub import_source: Option<String>,
}

impl SlotInfo {
    /// Create a new slot info
    pub fn new(slot: u16) -> Self {
        Self {
            slot,
            owner: None,
            state: SlotMigrationState::Stable,
            migration_target: None,
            import_source: None,
        }
    }

    /// Set the owner of this slot
    pub fn set_owner(&mut self, owner: String) {
        self.owner = Some(owner);
    }

    /// Start migrating this slot to another node
    pub fn start_migration(&mut self, target: String) {
        self.state = SlotMigrationState::Migrating;
        self.migration_target = Some(target);
    }

    /// Start importing this slot from another node
    pub fn start_import(&mut self, source: String) {
        self.state = SlotMigrationState::Importing;
        self.import_source = Some(source);
    }

    /// Complete migration/import
    pub fn complete_migration(&mut self) {
        self.state = SlotMigrationState::Stable;
        self.migration_target = None;
        self.import_source = None;
    }

    /// Check if slot is being migrated
    pub fn is_migrating(&self) -> bool {
        self.state == SlotMigrationState::Migrating
    }

    /// Check if slot is being imported
    pub fn is_importing(&self) -> bool {
        self.state == SlotMigrationState::Importing
    }
}

// ── Redirect & SlotMap ──────────────────────────────────────────────────

/// Node identifier (40-character hex string in Redis Cluster).
type NodeId = String;

/// Client redirect response for keys that belong to another node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Redirect {
    /// Permanent redirect – the slot belongs to another node.
    Moved {
        /// The hash slot that was looked up.
        slot: u16,
        /// Address of the node that owns the slot.
        addr: SocketAddr,
    },
    /// Temporary redirect – the slot is being migrated.
    Ask {
        /// The hash slot that was looked up.
        slot: u16,
        /// Address of the node importing the slot.
        addr: SocketAddr,
    },
}

impl Redirect {
    /// Format as the Redis error string sent to clients.
    pub fn to_error_string(&self) -> String {
        match self {
            Self::Moved { slot, addr } => format!("MOVED {} {}", slot, addr),
            Self::Ask { slot, addr } => format!("ASK {} {}", slot, addr),
        }
    }
}

/// Consolidated slot-to-node mapping with migration-aware routing.
///
/// `SlotMap` tracks which node owns each of the 16 384 hash slots and any
/// in-flight migration state, providing the information needed to compute
/// `-MOVED` and `-ASK` client redirects.
pub struct SlotMap {
    /// Slot → NodeId mapping (index = slot number).
    slots: [Option<NodeId>; CLUSTER_SLOTS as usize],
    /// Slots being migrated **out** of this node → target node.
    migrating: HashMap<u16, NodeId>,
    /// Slots being imported **into** this node → source node.
    importing: HashMap<u16, NodeId>,
    /// NodeId → SocketAddr lookup.
    node_addrs: HashMap<NodeId, SocketAddr>,
}

impl SlotMap {
    /// Create an empty slot map.
    pub fn new() -> Self {
        Self {
            slots: [const { None }; CLUSTER_SLOTS as usize],
            migrating: HashMap::new(),
            importing: HashMap::new(),
            node_addrs: HashMap::new(),
        }
    }

    /// Calculate the hash slot for a given key (delegates to [`HashSlot::for_key`]).
    pub fn get_slot_for_key(key: &str) -> u16 {
        HashSlot::for_key(key.as_bytes())
    }

    /// Return the node that owns `slot`, if any.
    pub fn get_node_for_slot(&self, slot: u16) -> Option<&NodeId> {
        self.slots
            .get(slot as usize)
            .and_then(|opt| opt.as_ref())
    }

    /// Determine whether `slot` should be redirected away from `my_id`.
    ///
    /// Returns `Some(Redirect)` when the calling node does not own the slot
    /// (or the slot is being migrated away), or `None` when the command
    /// should be executed locally.
    pub fn should_redirect(&self, slot: u16, my_id: &NodeId) -> Option<Redirect> {
        // If we are migrating this slot, redirect with ASK.
        if let Some(target_id) = self.migrating.get(&slot) {
            if let Some(&addr) = self.node_addrs.get(target_id) {
                return Some(Redirect::Ask { slot, addr });
            }
        }

        match self.get_node_for_slot(slot) {
            Some(owner) if owner == my_id => None, // local
            Some(owner) => {
                if let Some(&addr) = self.node_addrs.get(owner) {
                    Some(Redirect::Moved { slot, addr })
                } else {
                    None // unknown address — treat as local
                }
            }
            None => None, // unassigned — treat as local
        }
    }

    /// Register a node address.
    pub fn set_node_addr(&mut self, node_id: NodeId, addr: SocketAddr) {
        self.node_addrs.insert(node_id, addr);
    }

    /// Assign `slot` to `node_id`.
    pub fn assign_slot(&mut self, slot: u16, node_id: NodeId) {
        if (slot as usize) < CLUSTER_SLOTS as usize {
            self.slots[slot as usize] = Some(node_id);
        }
    }

    /// Assign a contiguous range of slots to `node_id`.
    pub fn assign_range(&mut self, range: &SlotRange, node_id: &NodeId) {
        for slot in range.iter() {
            self.slots[slot as usize] = Some(node_id.clone());
        }
    }

    /// Remove slot ownership (set to unassigned).
    pub fn remove_slot(&mut self, slot: u16) {
        if (slot as usize) < CLUSTER_SLOTS as usize {
            self.slots[slot as usize] = None;
        }
    }

    /// Mark a slot as migrating to `target`.
    pub fn set_migrating(&mut self, slot: u16, target: NodeId) {
        self.migrating.insert(slot, target);
    }

    /// Mark a slot as importing from `source`.
    pub fn set_importing(&mut self, slot: u16, source: NodeId) {
        self.importing.insert(slot, source);
    }

    /// Clear migration/import state for a slot (set it to stable).
    pub fn set_stable(&mut self, slot: u16) {
        self.migrating.remove(&slot);
        self.importing.remove(&slot);
    }

    /// Return `true` if `slot` is migrating out.
    pub fn is_migrating(&self, slot: u16) -> bool {
        self.migrating.contains_key(&slot)
    }

    /// Return `true` if `slot` is being imported.
    pub fn is_importing(&self, slot: u16) -> bool {
        self.importing.contains_key(&slot)
    }

    /// Return the migration target for a slot, if any.
    pub fn migration_target(&self, slot: u16) -> Option<&NodeId> {
        self.migrating.get(&slot)
    }

    /// Return the import source for a slot, if any.
    pub fn import_source(&self, slot: u16) -> Option<&NodeId> {
        self.importing.get(&slot)
    }

    /// Check if every slot is assigned.
    pub fn all_slots_covered(&self) -> bool {
        self.slots.iter().all(|s| s.is_some())
    }

    /// Count assigned slots.
    pub fn covered_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }
}

impl Default for SlotMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        // Test vectors from Redis
        assert_eq!(crc16(b"123456789"), 0x31c3);
        assert_eq!(crc16(b""), 0x0000);
    }

    #[test]
    fn test_hash_slot_calculation() {
        // These should produce consistent results
        let slot1 = HashSlot::for_key(b"foo");
        let slot2 = HashSlot::for_key(b"foo");
        assert_eq!(slot1, slot2);

        // Different keys may produce different slots
        let slot3 = HashSlot::for_key(b"bar");
        // (they might be equal by chance, but usually different)
        let _ = slot3; // just verify it computes

        // Slot should always be < CLUSTER_SLOTS
        for key in &[b"key1".as_slice(), b"key2", b"key3", b"test", b"hello"] {
            let slot = HashSlot::for_key(key);
            assert!(slot < CLUSTER_SLOTS);
        }
    }

    #[test]
    fn test_hash_tags() {
        // Keys with same hash tag should go to same slot
        let slot1 = HashSlot::for_key(b"{user}:1000");
        let slot2 = HashSlot::for_key(b"{user}:2000");
        let slot3 = HashSlot::for_key(b"{user}:profile");
        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);

        // Different hash tags = different slots (usually)
        let slot_user = HashSlot::for_key(b"{user}:1000");
        let slot_order = HashSlot::for_key(b"{order}:1000");
        // These could theoretically be equal, but the important thing
        // is that our hash tag extraction works
        let _ = (slot_user, slot_order);
    }

    #[test]
    fn test_hash_tag_extraction() {
        // Empty tag should use full key
        let slot1 = HashSlot::for_key(b"{}foo");
        let slot2 = HashSlot::for_key(b"{}foo");
        assert_eq!(slot1, slot2);

        // No closing brace - use full key
        let slot3 = HashSlot::for_key(b"{foo");
        let slot4 = HashSlot::for_key(b"{foo");
        assert_eq!(slot3, slot4);

        // Multiple braces - use first pair
        let slot5 = HashSlot::for_key(b"{a}{b}");
        let slot6 = HashSlot::for_key(b"{a}other");
        assert_eq!(slot5, slot6);
    }

    #[test]
    fn test_slot_range() {
        let range = SlotRange::new(0, 100);
        assert!(range.contains(0));
        assert!(range.contains(50));
        assert!(range.contains(100));
        assert!(!range.contains(101));
        assert_eq!(range.count(), 101);
    }

    #[test]
    fn test_slot_range_single() {
        let range = SlotRange::single(42);
        assert!(range.contains(42));
        assert!(!range.contains(41));
        assert!(!range.contains(43));
        assert_eq!(range.count(), 1);
    }

    #[test]
    fn test_slot_range_overlaps() {
        let range1 = SlotRange::new(0, 100);
        let range2 = SlotRange::new(50, 150);
        let range3 = SlotRange::new(200, 300);

        assert!(range1.overlaps(&range2));
        assert!(range2.overlaps(&range1));
        assert!(!range1.overlaps(&range3));
    }

    #[test]
    fn test_slot_range_merge() {
        // Adjacent ranges
        let range1 = SlotRange::new(0, 100);
        let range2 = SlotRange::new(101, 200);
        let merged = range1.merge(&range2).unwrap();
        assert_eq!(merged.start, 0);
        assert_eq!(merged.end, 200);

        // Overlapping ranges
        let range3 = SlotRange::new(50, 150);
        let merged2 = range1.merge(&range3).unwrap();
        assert_eq!(merged2.start, 0);
        assert_eq!(merged2.end, 150);

        // Non-adjacent ranges
        let range4 = SlotRange::new(300, 400);
        assert!(range1.merge(&range4).is_none());
    }

    #[test]
    fn test_slot_range_split() {
        let range = SlotRange::new(0, 100);

        // Split in middle
        let (left, right) = range.split(50);
        assert_eq!(left.unwrap(), SlotRange::new(0, 49));
        assert_eq!(right.unwrap(), SlotRange::new(51, 100));

        // Split at start
        let (left2, right2) = range.split(0);
        assert!(left2.is_none());
        assert_eq!(right2.unwrap(), SlotRange::new(1, 100));

        // Split at end
        let (left3, right3) = range.split(100);
        assert_eq!(left3.unwrap(), SlotRange::new(0, 99));
        assert!(right3.is_none());
    }

    #[test]
    fn test_slot_range_display() {
        let single = SlotRange::single(42);
        assert_eq!(single.to_string(), "42");

        let range = SlotRange::new(0, 100);
        assert_eq!(range.to_string(), "0-100");
    }

    #[test]
    fn test_slot_info() {
        let mut info = SlotInfo::new(42);
        assert!(!info.is_migrating());
        assert!(!info.is_importing());

        info.start_migration("node2".to_string());
        assert!(info.is_migrating());

        info.complete_migration();
        assert!(!info.is_migrating());
    }

    #[test]
    fn test_slot_range_iter() {
        let range = SlotRange::new(10, 15);
        let slots: Vec<u16> = range.iter().collect();
        assert_eq!(slots, vec![10, 11, 12, 13, 14, 15]);
    }

    #[test]
    fn test_known_slot_values() {
        // Test some known slot values to ensure CRC16 implementation is correct
        // These match Redis's implementation
        assert_eq!(HashSlot::for_key(b"foo"), 12182);
        assert_eq!(HashSlot::for_key(b"bar"), 5061);
        assert_eq!(HashSlot::for_key(b"hello"), 866);
    }

    // Property tests using proptest
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// P33: Cluster Routing Consistency
            /// The same key should always hash to the same slot
            #[test]
            fn prop_hash_slot_deterministic(key in prop::collection::vec(any::<u8>(), 0..1000)) {
                let slot1 = HashSlot::for_key(&key);
                let slot2 = HashSlot::for_key(&key);
                prop_assert_eq!(slot1, slot2, "Same key should produce same slot");
            }

            /// Hash slot should always be in valid range [0, CLUSTER_SLOTS)
            #[test]
            fn prop_hash_slot_in_range(key in prop::collection::vec(any::<u8>(), 0..1000)) {
                let slot = HashSlot::for_key(&key);
                prop_assert!(slot < CLUSTER_SLOTS, "Slot {} should be < {}", slot, CLUSTER_SLOTS);
            }

            /// Keys with same hash tag should always hash to same slot
            #[test]
            fn prop_hash_tag_consistency(
                tag in "[a-zA-Z0-9]+",
                suffix1 in "[a-zA-Z0-9]*",
                suffix2 in "[a-zA-Z0-9]*"
            ) {
                let key1 = format!("{{{}}}:{}", tag, suffix1);
                let key2 = format!("{{{}}}:{}", tag, suffix2);
                let slot1 = HashSlot::for_key(key1.as_bytes());
                let slot2 = HashSlot::for_key(key2.as_bytes());
                prop_assert_eq!(slot1, slot2, "Keys with same hash tag should have same slot");
            }

            /// Slot range operations maintain invariants
            #[test]
            fn prop_slot_range_count(start in 0u16..CLUSTER_SLOTS, end in 0u16..CLUSTER_SLOTS) {
                let (s, e) = if start <= end { (start, end) } else { (end, start) };
                let range = SlotRange::new(s, e);
                prop_assert_eq!(range.count(), (e - s + 1) as usize);
            }

            /// Slot range contains is correct
            #[test]
            fn prop_slot_range_contains(
                start in 0u16..CLUSTER_SLOTS,
                end in 0u16..CLUSTER_SLOTS,
                test_slot in 0u16..CLUSTER_SLOTS
            ) {
                let (s, e) = if start <= end { (start, end) } else { (end, start) };
                let range = SlotRange::new(s, e);
                let expected = test_slot >= s && test_slot <= e;
                prop_assert_eq!(range.contains(test_slot), expected);
            }

            /// Split and merge should be inverse operations (when applicable)
            #[test]
            fn prop_split_preserves_count(
                start in 0u16..(CLUSTER_SLOTS - 10),
                len in 3u16..100,
                split_offset in 1u16..100
            ) {
                let end = (start + len).min(CLUSTER_SLOTS - 1);
                let split_at = (start + split_offset.min(len - 1)).min(end);
                let range = SlotRange::new(start, end);
                let original_count = range.count();

                let (left, right) = range.split(split_at);

                // Count should be preserved minus the split slot
                let left_count = left.map(|r| r.count()).unwrap_or(0);
                let right_count = right.map(|r| r.count()).unwrap_or(0);
                prop_assert_eq!(left_count + right_count, original_count - 1,
                    "Split should preserve count minus split slot");
            }

            /// CRC16 should produce uniform-ish distribution
            /// (Not a strict property, but sanity check)
            #[test]
            fn prop_distribution_not_degenerate(
                prefix in "[a-z]{3}",
                suffix in 0u32..10000
            ) {
                let key = format!("{}:{}", prefix, suffix);
                let slot = HashSlot::for_key(key.as_bytes());
                // Just verify it produces a valid slot
                prop_assert!(slot < CLUSTER_SLOTS);
            }
        }
    }

    // ── SlotMap tests ───────────────────────────────────────────────

    #[test]
    fn test_slot_map_get_slot_for_key() {
        // Must agree with HashSlot::for_key
        assert_eq!(SlotMap::get_slot_for_key("foo"), HashSlot::for_key(b"foo"));
        assert_eq!(SlotMap::get_slot_for_key("bar"), HashSlot::for_key(b"bar"));
        assert_eq!(
            SlotMap::get_slot_for_key("{user}:1000"),
            HashSlot::for_key(b"{user}:1000"),
        );
    }

    #[test]
    fn test_slot_map_assign_and_lookup() {
        let mut sm = SlotMap::new();
        sm.assign_slot(100, "node-a".to_string());
        assert_eq!(sm.get_node_for_slot(100), Some(&"node-a".to_string()));
        assert!(sm.get_node_for_slot(101).is_none());
    }

    #[test]
    fn test_slot_map_assign_range() {
        let mut sm = SlotMap::new();
        let range = SlotRange::new(0, 5461);
        sm.assign_range(&range, &"node-a".to_string());
        assert_eq!(sm.get_node_for_slot(0), Some(&"node-a".to_string()));
        assert_eq!(sm.get_node_for_slot(5461), Some(&"node-a".to_string()));
        assert!(sm.get_node_for_slot(5462).is_none());
    }

    #[test]
    fn test_slot_map_remove_slot() {
        let mut sm = SlotMap::new();
        sm.assign_slot(42, "node-a".to_string());
        assert!(sm.get_node_for_slot(42).is_some());
        sm.remove_slot(42);
        assert!(sm.get_node_for_slot(42).is_none());
    }

    #[test]
    fn test_slot_map_redirect_moved() {
        let mut sm = SlotMap::new();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        sm.set_node_addr("node-b".to_string(), addr);
        sm.assign_slot(100, "node-b".to_string());

        let redirect = sm.should_redirect(100, &"node-a".to_string());
        assert_eq!(redirect, Some(Redirect::Moved { slot: 100, addr }));
    }

    #[test]
    fn test_slot_map_redirect_local() {
        let mut sm = SlotMap::new();
        sm.assign_slot(100, "node-a".to_string());

        let redirect = sm.should_redirect(100, &"node-a".to_string());
        assert!(redirect.is_none());
    }

    #[test]
    fn test_slot_map_redirect_ask_during_migration() {
        let mut sm = SlotMap::new();
        let target_addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        sm.set_node_addr("node-b".to_string(), target_addr);
        sm.assign_slot(100, "node-a".to_string());
        sm.set_migrating(100, "node-b".to_string());

        let redirect = sm.should_redirect(100, &"node-a".to_string());
        assert_eq!(
            redirect,
            Some(Redirect::Ask {
                slot: 100,
                addr: target_addr,
            })
        );
    }

    #[test]
    fn test_slot_map_migration_stable() {
        let mut sm = SlotMap::new();
        sm.assign_slot(100, "node-a".to_string());
        sm.set_migrating(100, "node-b".to_string());
        assert!(sm.is_migrating(100));

        sm.set_stable(100);
        assert!(!sm.is_migrating(100));
        assert!(!sm.is_importing(100));
    }

    #[test]
    fn test_slot_map_coverage() {
        let mut sm = SlotMap::new();
        assert_eq!(sm.covered_count(), 0);
        assert!(!sm.all_slots_covered());

        let range = SlotRange::new(0, CLUSTER_SLOTS - 1);
        sm.assign_range(&range, &"node-a".to_string());
        assert_eq!(sm.covered_count(), CLUSTER_SLOTS as usize);
        assert!(sm.all_slots_covered());
    }

    #[test]
    fn test_redirect_error_string() {
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        assert_eq!(
            Redirect::Moved { slot: 100, addr }.to_error_string(),
            "MOVED 100 127.0.0.1:6379"
        );
        assert_eq!(
            Redirect::Ask { slot: 200, addr }.to_error_string(),
            "ASK 200 127.0.0.1:6379"
        );
    }

    #[test]
    fn test_slot_map_redirect_unassigned() {
        let sm = SlotMap::new();
        // Unassigned slot => no redirect (treat as local)
        assert!(sm.should_redirect(500, &"node-a".to_string()).is_none());
    }

    // ── Additional CRC16 Redis compatibility tests ──────────────────

    #[test]
    fn test_redis_crc16_compatibility() {
        // Extended set of known Redis slot values verified against
        // `redis-cli CLUSTER KEYSLOT <key>`.
        assert_eq!(HashSlot::for_key(b"foo"), 12182);
        assert_eq!(HashSlot::for_key(b"bar"), 5061);
        assert_eq!(HashSlot::for_key(b"hello"), 866);
        assert_eq!(HashSlot::for_key(b""), 0);
        assert_eq!(HashSlot::for_key(b"key"), 12539);
        assert_eq!(HashSlot::for_key(b"test"), 6918);

        // Hash tag tests – only the tag content is hashed
        assert_eq!(
            HashSlot::for_key(b"{user}.following"),
            HashSlot::for_key(b"{user}.followers"),
        );
        assert_eq!(
            HashSlot::for_key(b"{user}:1000"),
            HashSlot::for_key(b"user"),
        );
    }
}
