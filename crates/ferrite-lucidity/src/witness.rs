//! Witness ledger: stores observed STHs and detects forks.
//!
//! A "fork" is two distinct STHs at the same tree size but with
//! different roots — proof the log signer attempted to equivocate.
//! Real witnesses are external services (RFC 9162 §8); the
//! [`InMemoryWitness`] is a deterministic stand-in for tests.

use crate::signer::SignedTreeHead;
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum WitnessError {
    #[error("fork detected at size {size}: existing root differs from submitted root")]
    Fork {
        size: usize,
        existing_root: [u8; 32],
        submitted_root: [u8; 32],
    },
    #[error("regression: submitted size {submitted} is smaller than highest seen {highest}")]
    Regression { submitted: usize, highest: usize },
}

#[derive(Debug, Default)]
pub struct InMemoryWitness {
    /// size → root observed at that size.
    seen: RwLock<HashMap<usize, [u8; 32]>>,
    highest: RwLock<usize>,
}

impl InMemoryWitness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an STH.  Returns `Ok(())` if accepted, or an error
    /// describing the inconsistency.
    pub fn record(&self, sth: &SignedTreeHead) -> Result<(), WitnessError> {
        {
            let highest = *self.highest.read();
            if sth.size < highest {
                return Err(WitnessError::Regression {
                    submitted: sth.size,
                    highest,
                });
            }
        }
        let mut seen = self.seen.write();
        if let Some(existing) = seen.get(&sth.size) {
            if *existing != sth.root {
                return Err(WitnessError::Fork {
                    size: sth.size,
                    existing_root: *existing,
                    submitted_root: sth.root,
                });
            }
            return Ok(());
        }
        seen.insert(sth.size, sth.root);
        let mut h = self.highest.write();
        if sth.size > *h {
            *h = sth.size;
        }
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.seen.read().len()
    }
    pub fn highest(&self) -> usize {
        *self.highest.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signer::{MockSigner, Signer};

    fn sth(size: usize, root_byte: u8) -> SignedTreeHead {
        let signer = MockSigner::new("s");
        let root = [root_byte; 32];
        let sig = signer.sign_sth(size, &root, 0);
        SignedTreeHead {
            size,
            root,
            ts_ms: 0,
            signer_id: "s".into(),
            signature: sig,
        }
    }

    #[test]
    fn first_record_is_accepted() {
        let w = InMemoryWitness::new();
        assert!(w.record(&sth(10, 1)).is_ok());
        assert_eq!(w.count(), 1);
        assert_eq!(w.highest(), 10);
    }

    #[test]
    fn duplicate_record_is_idempotent() {
        let w = InMemoryWitness::new();
        let s = sth(10, 1);
        w.record(&s).unwrap();
        w.record(&s).unwrap();
        assert_eq!(w.count(), 1);
    }

    #[test]
    fn fork_at_same_size_is_detected() {
        let w = InMemoryWitness::new();
        w.record(&sth(10, 1)).unwrap();
        let err = w.record(&sth(10, 2)).unwrap_err();
        assert!(matches!(err, WitnessError::Fork { size: 10, .. }));
    }

    #[test]
    fn regression_in_size_is_rejected() {
        let w = InMemoryWitness::new();
        w.record(&sth(10, 1)).unwrap();
        let err = w.record(&sth(5, 9)).unwrap_err();
        assert!(matches!(
            err,
            WitnessError::Regression {
                submitted: 5,
                highest: 10
            }
        ));
    }

    #[test]
    fn growing_record_advances_highest() {
        let w = InMemoryWitness::new();
        w.record(&sth(10, 1)).unwrap();
        w.record(&sth(15, 2)).unwrap();
        w.record(&sth(20, 3)).unwrap();
        assert_eq!(w.highest(), 20);
        assert_eq!(w.count(), 3);
    }
}
