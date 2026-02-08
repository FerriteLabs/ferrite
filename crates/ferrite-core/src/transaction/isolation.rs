//! Transaction Isolation Levels
//!
//! Provides different isolation levels for transactions with varying
//! trade-offs between consistency and performance.
//!
//! # Isolation Level Comparison
//!
//! | Level            | Dirty Read | Non-Repeatable | Phantom |
//! |------------------|------------|----------------|---------|
//! | ReadUncommitted  | Yes        | Yes            | Yes     |
//! | ReadCommitted    | No         | Yes            | Yes     |
//! | RepeatableRead   | No         | No             | Yes     |
//! | Serializable     | No         | No             | No      |
//!
//! # Anomaly Definitions
//!
//! - **Dirty Read**: Reading uncommitted data from another transaction
//! - **Non-Repeatable Read**: Same query returns different results within same transaction
//! - **Phantom Read**: New rows appear in repeated queries

use std::fmt;

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Read uncommitted data (fastest, least consistent)
    ///
    /// - May read uncommitted changes from other transactions
    /// - No locks on reads
    /// - Suitable for analytics where dirty reads are acceptable
    ReadUncommitted,

    /// Read only committed data
    ///
    /// - Each query sees a new snapshot of committed data
    /// - No dirty reads
    /// - May see non-repeatable reads between queries
    /// - Default for many databases
    ReadCommitted,

    /// Snapshot at transaction start
    ///
    /// - All queries see the same snapshot
    /// - No dirty reads or non-repeatable reads
    /// - May see phantom rows in repeated range queries
    /// - Good balance of consistency and performance
    #[default]
    RepeatableRead,

    /// Full serializability
    ///
    /// - Transactions execute as if serialized one after another
    /// - No anomalies possible
    /// - Uses 2PL (Two-Phase Locking) + MVCC
    /// - Highest consistency, lowest concurrency
    Serializable,
}

impl IsolationLevel {
    /// Get the strictness level (higher = more strict)
    pub fn strictness(&self) -> u8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
            IsolationLevel::RepeatableRead => 2,
            IsolationLevel::Serializable => 3,
        }
    }

    /// Check if this level prevents dirty reads
    pub fn prevents_dirty_reads(&self) -> bool {
        self.strictness() >= 1
    }

    /// Check if this level prevents non-repeatable reads
    pub fn prevents_non_repeatable_reads(&self) -> bool {
        self.strictness() >= 2
    }

    /// Check if this level prevents phantom reads
    pub fn prevents_phantom_reads(&self) -> bool {
        self.strictness() >= 3
    }

    /// Check if this level uses snapshot isolation
    pub fn uses_snapshot(&self) -> bool {
        matches!(
            self,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable
        )
    }

    /// Check if this level uses read locks
    pub fn uses_read_locks(&self) -> bool {
        matches!(self, IsolationLevel::Serializable)
    }

    /// Check if this level uses write locks
    pub fn uses_write_locks(&self) -> bool {
        true // All levels use write locks
    }

    /// Get the SQL standard name
    pub fn sql_name(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }

    /// Parse from string
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "READ UNCOMMITTED" | "READ_UNCOMMITTED" | "RU" => Some(IsolationLevel::ReadUncommitted),
            "READ COMMITTED" | "READ_COMMITTED" | "RC" => Some(IsolationLevel::ReadCommitted),
            "REPEATABLE READ" | "REPEATABLE_READ" | "RR" => Some(IsolationLevel::RepeatableRead),
            "SERIALIZABLE" | "SR" => Some(IsolationLevel::Serializable),
            _ => None,
        }
    }

    /// Get a list of all isolation levels
    pub fn all() -> Vec<IsolationLevel> {
        vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ]
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sql_name())
    }
}

/// Isolation level properties for documentation
#[derive(Debug)]
pub struct IsolationProperties {
    /// Level name
    pub name: &'static str,
    /// Whether dirty reads are possible
    pub dirty_reads: bool,
    /// Whether non-repeatable reads are possible
    pub non_repeatable_reads: bool,
    /// Whether phantom reads are possible
    pub phantom_reads: bool,
    /// Typical use cases
    pub use_cases: Vec<&'static str>,
}

impl IsolationLevel {
    /// Get detailed properties of this isolation level
    pub fn properties(&self) -> IsolationProperties {
        match self {
            IsolationLevel::ReadUncommitted => IsolationProperties {
                name: "Read Uncommitted",
                dirty_reads: true,
                non_repeatable_reads: true,
                phantom_reads: true,
                use_cases: vec![
                    "Approximate counts",
                    "Analytics queries",
                    "Monitoring dashboards",
                ],
            },
            IsolationLevel::ReadCommitted => IsolationProperties {
                name: "Read Committed",
                dirty_reads: false,
                non_repeatable_reads: true,
                phantom_reads: true,
                use_cases: vec![
                    "Most OLTP workloads",
                    "Web applications",
                    "Default for PostgreSQL",
                ],
            },
            IsolationLevel::RepeatableRead => IsolationProperties {
                name: "Repeatable Read",
                dirty_reads: false,
                non_repeatable_reads: false,
                phantom_reads: true,
                use_cases: vec![
                    "Financial transactions",
                    "Reporting queries",
                    "Default for MySQL InnoDB",
                ],
            },
            IsolationLevel::Serializable => IsolationProperties {
                name: "Serializable",
                dirty_reads: false,
                non_repeatable_reads: false,
                phantom_reads: false,
                use_cases: vec![
                    "Banking systems",
                    "Critical financial operations",
                    "When correctness is paramount",
                ],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strictness_ordering() {
        assert!(
            IsolationLevel::ReadUncommitted.strictness()
                < IsolationLevel::ReadCommitted.strictness()
        );
        assert!(
            IsolationLevel::ReadCommitted.strictness()
                < IsolationLevel::RepeatableRead.strictness()
        );
        assert!(
            IsolationLevel::RepeatableRead.strictness() < IsolationLevel::Serializable.strictness()
        );
    }

    #[test]
    fn test_anomaly_prevention() {
        // ReadUncommitted prevents nothing
        assert!(!IsolationLevel::ReadUncommitted.prevents_dirty_reads());
        assert!(!IsolationLevel::ReadUncommitted.prevents_non_repeatable_reads());
        assert!(!IsolationLevel::ReadUncommitted.prevents_phantom_reads());

        // ReadCommitted prevents dirty reads only
        assert!(IsolationLevel::ReadCommitted.prevents_dirty_reads());
        assert!(!IsolationLevel::ReadCommitted.prevents_non_repeatable_reads());
        assert!(!IsolationLevel::ReadCommitted.prevents_phantom_reads());

        // RepeatableRead prevents dirty and non-repeatable
        assert!(IsolationLevel::RepeatableRead.prevents_dirty_reads());
        assert!(IsolationLevel::RepeatableRead.prevents_non_repeatable_reads());
        assert!(!IsolationLevel::RepeatableRead.prevents_phantom_reads());

        // Serializable prevents all
        assert!(IsolationLevel::Serializable.prevents_dirty_reads());
        assert!(IsolationLevel::Serializable.prevents_non_repeatable_reads());
        assert!(IsolationLevel::Serializable.prevents_phantom_reads());
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            IsolationLevel::parse_str("READ UNCOMMITTED"),
            Some(IsolationLevel::ReadUncommitted)
        );
        assert_eq!(
            IsolationLevel::parse_str("READ_COMMITTED"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::parse_str("RR"),
            Some(IsolationLevel::RepeatableRead)
        );
        assert_eq!(
            IsolationLevel::parse_str("serializable"),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(IsolationLevel::parse_str("invalid"), None);
    }

    #[test]
    fn test_sql_names() {
        assert_eq!(
            IsolationLevel::ReadUncommitted.sql_name(),
            "READ UNCOMMITTED"
        );
        assert_eq!(IsolationLevel::ReadCommitted.sql_name(), "READ COMMITTED");
        assert_eq!(IsolationLevel::RepeatableRead.sql_name(), "REPEATABLE READ");
        assert_eq!(IsolationLevel::Serializable.sql_name(), "SERIALIZABLE");
    }

    #[test]
    fn test_uses_snapshot() {
        assert!(!IsolationLevel::ReadUncommitted.uses_snapshot());
        assert!(!IsolationLevel::ReadCommitted.uses_snapshot());
        assert!(IsolationLevel::RepeatableRead.uses_snapshot());
        assert!(IsolationLevel::Serializable.uses_snapshot());
    }

    #[test]
    fn test_uses_locks() {
        // All use write locks
        for level in IsolationLevel::all() {
            assert!(level.uses_write_locks());
        }

        // Only serializable uses read locks
        assert!(!IsolationLevel::ReadUncommitted.uses_read_locks());
        assert!(!IsolationLevel::ReadCommitted.uses_read_locks());
        assert!(!IsolationLevel::RepeatableRead.uses_read_locks());
        assert!(IsolationLevel::Serializable.uses_read_locks());
    }

    #[test]
    fn test_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::RepeatableRead);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", IsolationLevel::Serializable), "SERIALIZABLE");
    }
}
