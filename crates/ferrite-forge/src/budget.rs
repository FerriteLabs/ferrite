//! Per-call resource budget for Forge functions (ADR-019 §Execution & isolation).

use std::time::Duration;

/// Caps on what a single FN.CALL may consume.
///
/// `fuel` follows wasmtime's metering semantics — one unit per wasm op.  Memory
/// and time are enforced by the host outside the wasm sandbox.
#[derive(Debug, Clone, Copy)]
pub struct ResourceBudget {
    /// Wasmtime fuel units.  `None` = engine default (currently 1_000_000).
    pub fuel: Option<u64>,
    /// Linear-memory cap in bytes.  Defaults to 64 MiB.
    pub memory_bytes: u64,
    /// Wall-clock cap.  Defaults to 50 ms.
    pub wall_time: Duration,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            fuel: Some(1_000_000),
            memory_bytes: 64 * 1024 * 1024,
            wall_time: Duration::from_millis(50),
        }
    }
}

impl ResourceBudget {
    pub fn with_fuel(mut self, fuel: u64) -> Self {
        self.fuel = Some(fuel);
        self
    }
    pub fn with_memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = bytes;
        self
    }
    pub fn with_wall_time(mut self, d: Duration) -> Self {
        self.wall_time = d;
        self
    }

    /// Validate that the budget is internally consistent.
    pub fn validate(&self) -> Result<(), BudgetError> {
        if self.memory_bytes == 0 {
            return Err(BudgetError::InvalidMemory);
        }
        if self.wall_time.is_zero() {
            return Err(BudgetError::InvalidWallTime);
        }
        if self.fuel == Some(0) {
            return Err(BudgetError::InvalidFuel);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BudgetError {
    #[error("memory_bytes must be > 0")]
    InvalidMemory,
    #[error("wall_time must be > 0")]
    InvalidWallTime,
    #[error("fuel must be > 0 when set")]
    InvalidFuel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_is_valid() {
        ResourceBudget::default().validate().unwrap();
    }

    #[test]
    fn zero_memory_is_invalid() {
        let b = ResourceBudget::default().with_memory_bytes(0);
        assert_eq!(b.validate(), Err(BudgetError::InvalidMemory));
    }

    #[test]
    fn zero_wall_time_is_invalid() {
        let b = ResourceBudget::default().with_wall_time(Duration::ZERO);
        assert_eq!(b.validate(), Err(BudgetError::InvalidWallTime));
    }

    #[test]
    fn fuel_zero_is_invalid_but_none_is_ok() {
        let b = ResourceBudget::default().with_fuel(0);
        assert_eq!(b.validate(), Err(BudgetError::InvalidFuel));
        let b2 = ResourceBudget {
            fuel: None,
            ..ResourceBudget::default()
        };
        b2.validate().unwrap();
    }
}
