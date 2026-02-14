//! Core CRDT types and traits

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;

/// Unique identifier for a replica/site
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SiteId(pub u64);

impl SiteId {
    /// Create a new site ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner ID
    pub fn id(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for SiteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Site({})", self.0)
    }
}

impl fmt::Display for SiteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for SiteId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Unique tag for elements in observed-remove sets
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UniqueTag {
    /// Site that created this tag
    pub site: SiteId,
    /// Counter at the time of creation
    pub counter: u64,
}

impl UniqueTag {
    /// Create a new unique tag
    pub fn new(site: SiteId, counter: u64) -> Self {
        Self { site, counter }
    }
}

impl fmt::Debug for UniqueTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Tag({}, {})", self.site.0, self.counter)
    }
}

/// Types of CRDT data structures
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrdtType {
    /// Grow-only counter
    GCounter,
    /// Positive-negative counter
    PNCounter,
    /// Last-writer-wins register
    LwwRegister,
    /// Multi-value register
    MvRegister,
    /// Observed-remove set
    OrSet,
    /// LWW element set (sorted set)
    LwwElementSet,
    /// Observed-remove map
    OrMap,
    /// Enable-wins flag
    EnableWinsFlag,
    /// Disable-wins flag
    DisableWinsFlag,
}

impl fmt::Display for CrdtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrdtType::GCounter => write!(f, "g-counter"),
            CrdtType::PNCounter => write!(f, "pn-counter"),
            CrdtType::LwwRegister => write!(f, "lww-register"),
            CrdtType::MvRegister => write!(f, "mv-register"),
            CrdtType::OrSet => write!(f, "or-set"),
            CrdtType::LwwElementSet => write!(f, "lww-element-set"),
            CrdtType::OrMap => write!(f, "or-map"),
            CrdtType::EnableWinsFlag => write!(f, "enable-wins-flag"),
            CrdtType::DisableWinsFlag => write!(f, "disable-wins-flag"),
        }
    }
}

/// Operations that can be applied to CRDTs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtOperation {
    /// Increment counter
    Increment { site: SiteId, delta: i64 },
    /// Decrement counter
    Decrement { site: SiteId, delta: i64 },
    /// Set register value
    SetRegister { value: Bytes, timestamp: u64 },
    /// Add to set
    SetAdd { member: Bytes, tag: UniqueTag },
    /// Remove from set
    SetRemove { member: Bytes },
    /// Add to sorted set
    ZSetAdd {
        member: Bytes,
        score: f64,
        timestamp: u64,
    },
    /// Remove from sorted set
    ZSetRemove { member: Bytes, timestamp: u64 },
    /// Set hash field
    HashSet {
        field: Bytes,
        value: Bytes,
        tag: UniqueTag,
    },
    /// Delete hash field
    HashDelete { field: Bytes },
    /// Enable flag
    Enable { timestamp: u64 },
    /// Disable flag
    Disable { timestamp: u64 },
}

/// Wrapper for CRDT values stored in the database
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtValue {
    /// G-Counter
    GCounter(super::GCounter),
    /// PN-Counter
    PNCounter(super::PNCounter),
    /// LWW Register
    LwwRegister(super::LwwRegister<Bytes>),
    /// MV Register
    MvRegister(super::MvRegister<Bytes>),
    /// OR-Set
    OrSet(super::OrSet<Bytes>),
    /// LWW Element Set
    LwwElementSet(super::LwwElementSet),
    /// OR-Map
    OrMap(super::OrMap<Bytes>),
}

impl CrdtValue {
    /// Get the CRDT type
    pub fn crdt_type(&self) -> CrdtType {
        match self {
            CrdtValue::GCounter(_) => CrdtType::GCounter,
            CrdtValue::PNCounter(_) => CrdtType::PNCounter,
            CrdtValue::LwwRegister(_) => CrdtType::LwwRegister,
            CrdtValue::MvRegister(_) => CrdtType::MvRegister,
            CrdtValue::OrSet(_) => CrdtType::OrSet,
            CrdtValue::LwwElementSet(_) => CrdtType::LwwElementSet,
            CrdtValue::OrMap(_) => CrdtType::OrMap,
        }
    }

    /// Merge with another CRDT value of the same type
    pub fn merge(&mut self, other: &CrdtValue) {
        match (self, other) {
            (CrdtValue::GCounter(a), CrdtValue::GCounter(b)) => a.merge(b),
            (CrdtValue::PNCounter(a), CrdtValue::PNCounter(b)) => a.merge(b),
            (CrdtValue::LwwRegister(a), CrdtValue::LwwRegister(b)) => a.merge(b),
            (CrdtValue::MvRegister(a), CrdtValue::MvRegister(b)) => a.merge(b),
            (CrdtValue::OrSet(a), CrdtValue::OrSet(b)) => a.merge(b),
            (CrdtValue::LwwElementSet(a), CrdtValue::LwwElementSet(b)) => a.merge(b),
            (CrdtValue::OrMap(a), CrdtValue::OrMap(b)) => a.merge(b),
            _ => {} // Type mismatch - ignore
        }
    }
}

/// Full CRDT state for replication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrdtState {
    /// The CRDT value
    pub value: CrdtValue,
    /// Last update timestamp
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_site_id() {
        let site = SiteId::new(42);
        assert_eq!(site.id(), 42);
        assert_eq!(format!("{}", site), "42");
        assert_eq!(format!("{:?}", site), "Site(42)");
    }

    #[test]
    fn test_unique_tag() {
        let tag = UniqueTag::new(SiteId(1), 100);
        assert_eq!(tag.site, SiteId(1));
        assert_eq!(tag.counter, 100);
    }

    #[test]
    fn test_crdt_type_display() {
        assert_eq!(CrdtType::GCounter.to_string(), "g-counter");
        assert_eq!(CrdtType::PNCounter.to_string(), "pn-counter");
        assert_eq!(CrdtType::OrSet.to_string(), "or-set");
    }
}
