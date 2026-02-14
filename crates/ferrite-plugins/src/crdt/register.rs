//! CRDT Register implementations

use super::clock::{HybridTimestamp, VectorClock};
use serde::{Deserialize, Serialize};

/// Last-Writer-Wins Register (LWW-Register)
///
/// A register that uses timestamps to determine which write wins.
/// The write with the highest timestamp is always preserved.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{LwwRegister, HybridTimestamp, SiteId};
///
/// let site = SiteId::new(1);
///
/// let mut reg = LwwRegister::<String>::new();
/// reg.set("hello".to_string(), HybridTimestamp::new(100, 0, site));
/// reg.set("world".to_string(), HybridTimestamp::new(200, 0, site));
///
/// assert_eq!(reg.value(), Some(&"world".to_string()));
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    /// The current value
    value: Option<T>,
    /// Timestamp of the last write
    timestamp: HybridTimestamp,
}

impl<T> Default for LwwRegister<T> {
    fn default() -> Self {
        Self {
            value: None,
            timestamp: HybridTimestamp::default(),
        }
    }
}

impl<T: Clone> LwwRegister<T> {
    /// Create a new empty LWW register
    pub fn new() -> Self {
        Self {
            value: None,
            timestamp: HybridTimestamp::default(),
        }
    }

    /// Create a register with an initial value
    pub fn with_value(value: T, timestamp: HybridTimestamp) -> Self {
        Self {
            value: Some(value),
            timestamp,
        }
    }

    /// Set the register value
    pub fn set(&mut self, value: T, timestamp: HybridTimestamp) {
        if timestamp > self.timestamp {
            self.value = Some(value);
            self.timestamp = timestamp;
        }
    }

    /// Clear the register value
    pub fn clear(&mut self, timestamp: HybridTimestamp) {
        if timestamp > self.timestamp {
            self.value = None;
            self.timestamp = timestamp;
        }
    }

    /// Get the current value
    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Get the current timestamp
    pub fn timestamp(&self) -> HybridTimestamp {
        self.timestamp
    }

    /// Merge another register into this one
    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
        }
    }

    /// Check if the register has a value
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }
}

impl<T: PartialEq> PartialEq for LwwRegister<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.timestamp == other.timestamp
    }
}

impl<T: Eq> Eq for LwwRegister<T> {}

/// Multi-Value Register (MV-Register)
///
/// A register that preserves concurrent writes, allowing the application
/// to see all conflicting values and resolve them manually if needed.
///
/// # Example
///
/// ```
/// use ferrite::crdt::{MvRegister, VectorClock, SiteId};
///
/// let site1 = SiteId::new(1);
/// let site2 = SiteId::new(2);
///
/// let mut reg = MvRegister::<String>::new();
///
/// let mut clock1 = VectorClock::new();
/// clock1.increment(site1);
/// reg.set("value1".to_string(), clock1.clone());
///
/// let mut clock2 = VectorClock::new();
/// clock2.increment(site2);
/// reg.set("value2".to_string(), clock2);
///
/// // Both values are preserved as they are concurrent
/// assert_eq!(reg.values().len(), 2);
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MvRegister<T> {
    /// Values with their vector clocks
    values: Vec<(T, VectorClock)>,
}

impl<T: Clone + PartialEq> MvRegister<T> {
    /// Create a new empty MV register
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Create a register with an initial value
    pub fn with_value(value: T, clock: VectorClock) -> Self {
        Self {
            values: vec![(value, clock)],
        }
    }

    /// Set a new value with the given vector clock
    pub fn set(&mut self, value: T, clock: VectorClock) {
        // Remove values that this write supersedes
        self.values.retain(|(_, vc)| !vc.happened_before(&clock));

        // Add new value if not superseded by existing
        if !self.values.iter().any(|(_, vc)| clock.happened_before(vc)) {
            // Check if this exact value/clock pair already exists
            if !self
                .values
                .iter()
                .any(|(v, vc)| v == &value && vc == &clock)
            {
                self.values.push((value, clock));
            }
        }
    }

    /// Merge another register into this one
    pub fn merge(&mut self, other: &MvRegister<T>) {
        for (val, clock) in &other.values {
            self.set(val.clone(), clock.clone());
        }
    }

    /// Get all current values
    pub fn values(&self) -> Vec<&T> {
        self.values.iter().map(|(v, _)| v).collect()
    }

    /// Get all values with their clocks
    pub fn values_with_clocks(&self) -> &[(T, VectorClock)] {
        &self.values
    }

    /// Get the single value if there's no conflict
    pub fn single_value(&self) -> Option<&T> {
        if self.values.len() == 1 {
            Some(&self.values[0].0)
        } else {
            None
        }
    }

    /// Check if there's a conflict (multiple concurrent values)
    pub fn has_conflict(&self) -> bool {
        self.values.len() > 1
    }

    /// Check if the register is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get the number of concurrent values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Resolve conflict by keeping only the value that matches the predicate
    pub fn resolve<F>(&mut self, predicate: F)
    where
        F: Fn(&T) -> bool,
    {
        if let Some(idx) = self.values.iter().position(|(v, _)| predicate(v)) {
            let kept = self.values.remove(idx);
            self.values.clear();
            self.values.push(kept);
        }
    }
}

impl<T: PartialEq> PartialEq for MvRegister<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        // All values in self should exist in other
        self.values
            .iter()
            .all(|(v1, c1)| other.values.iter().any(|(v2, c2)| v1 == v2 && c1 == c2))
    }
}

impl<T: Eq> Eq for MvRegister<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::SiteId;

    #[test]
    fn test_lww_register_basic() {
        let site = SiteId::new(1);
        let mut reg = LwwRegister::<String>::new();

        assert!(reg.value().is_none());

        reg.set("hello".to_string(), HybridTimestamp::new(100, 0, site));
        assert_eq!(reg.value(), Some(&"hello".to_string()));

        reg.set("world".to_string(), HybridTimestamp::new(200, 0, site));
        assert_eq!(reg.value(), Some(&"world".to_string()));
    }

    #[test]
    fn test_lww_register_older_write_ignored() {
        let site = SiteId::new(1);
        let mut reg = LwwRegister::<String>::new();

        reg.set("newer".to_string(), HybridTimestamp::new(200, 0, site));
        reg.set("older".to_string(), HybridTimestamp::new(100, 0, site));

        assert_eq!(reg.value(), Some(&"newer".to_string()));
    }

    #[test]
    fn test_lww_register_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut reg1 =
            LwwRegister::with_value("value1".to_string(), HybridTimestamp::new(100, 0, site1));
        let reg2 =
            LwwRegister::with_value("value2".to_string(), HybridTimestamp::new(200, 0, site2));

        reg1.merge(&reg2);
        assert_eq!(reg1.value(), Some(&"value2".to_string()));
    }

    #[test]
    fn test_lww_register_clear() {
        let site = SiteId::new(1);
        let mut reg =
            LwwRegister::with_value("value".to_string(), HybridTimestamp::new(100, 0, site));

        reg.clear(HybridTimestamp::new(200, 0, site));
        assert!(reg.value().is_none());
    }

    #[test]
    fn test_mv_register_basic() {
        let site = SiteId::new(1);
        let mut reg = MvRegister::<String>::new();

        let mut clock = VectorClock::new();
        clock.increment(site);

        reg.set("hello".to_string(), clock);
        assert_eq!(reg.values(), vec![&"hello".to_string()]);
    }

    #[test]
    fn test_mv_register_concurrent_writes() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut reg = MvRegister::<String>::new();

        let mut clock1 = VectorClock::new();
        clock1.increment(site1);
        reg.set("value1".to_string(), clock1.clone());

        let mut clock2 = VectorClock::new();
        clock2.increment(site2);
        reg.set("value2".to_string(), clock2);

        // Both values should be preserved
        assert!(reg.has_conflict());
        assert_eq!(reg.values().len(), 2);
    }

    #[test]
    fn test_mv_register_supersede() {
        let site = SiteId::new(1);
        let mut reg = MvRegister::<String>::new();

        let mut clock1 = VectorClock::new();
        clock1.increment(site);
        reg.set("value1".to_string(), clock1.clone());

        let mut clock2 = clock1.clone();
        clock2.increment(site);
        reg.set("value2".to_string(), clock2);

        // value1 should be superseded
        assert!(!reg.has_conflict());
        assert_eq!(reg.single_value(), Some(&"value2".to_string()));
    }

    #[test]
    fn test_mv_register_merge() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut reg1 = MvRegister::<String>::new();
        let mut reg2 = MvRegister::<String>::new();

        let mut clock1 = VectorClock::new();
        clock1.increment(site1);
        reg1.set("value1".to_string(), clock1);

        let mut clock2 = VectorClock::new();
        clock2.increment(site2);
        reg2.set("value2".to_string(), clock2);

        reg1.merge(&reg2);
        assert!(reg1.has_conflict());
        assert_eq!(reg1.len(), 2);
    }

    #[test]
    fn test_mv_register_resolve() {
        let site1 = SiteId::new(1);
        let site2 = SiteId::new(2);

        let mut reg = MvRegister::<String>::new();

        let mut clock1 = VectorClock::new();
        clock1.increment(site1);
        reg.set("value1".to_string(), clock1);

        let mut clock2 = VectorClock::new();
        clock2.increment(site2);
        reg.set("value2".to_string(), clock2);

        reg.resolve(|v| v == "value1");
        assert!(!reg.has_conflict());
        assert_eq!(reg.single_value(), Some(&"value1".to_string()));
    }
}
