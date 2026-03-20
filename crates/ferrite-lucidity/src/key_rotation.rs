//! Signer key rotation — rolling window of active verifier keys.

use crate::signer::{SignedTreeHead, Signer};
use std::collections::VecDeque;

/// Manages a rolling window of signers: the newest is used for signing,
/// while all active signers can verify (so old signatures remain valid
/// during a transition period).
pub struct KeyRotation {
    signers: VecDeque<Box<dyn Signer>>,
    max_active: usize,
}

impl KeyRotation {
    pub fn new(max_active: usize) -> Self {
        assert!(max_active >= 1, "max_active must be at least 1");
        Self {
            signers: VecDeque::new(),
            max_active,
        }
    }

    /// Add a new signer, becoming the active signer.
    /// If we exceed `max_active`, the oldest key is retired.
    /// Returns the retired key's ID, if any.
    pub fn rotate(&mut self, signer: Box<dyn Signer>) -> Option<String> {
        self.signers.push_back(signer);
        if self.signers.len() > self.max_active {
            self.signers.pop_front().map(|s| s.id().to_string())
        } else {
            None
        }
    }

    /// Get the current (newest) signer for signing.
    pub fn active_signer(&self) -> Option<&dyn Signer> {
        self.signers.back().map(|s| s.as_ref())
    }

    /// Verify with any active key (for validating old signatures).
    /// Returns the ID of the signer that verified, if any.
    pub fn verify_any(&self, sth: &SignedTreeHead) -> Option<&str> {
        for signer in &self.signers {
            if signer.verify(sth) {
                return Some(signer.id());
            }
        }
        None
    }

    /// Number of active keys.
    pub fn active_count(&self) -> usize {
        self.signers.len()
    }

    /// List active key IDs.
    pub fn active_key_ids(&self) -> Vec<&str> {
        self.signers.iter().map(|s| s.id()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signer::MockSigner;

    #[test]
    fn rotate_adds_new_signer() {
        let mut kr = KeyRotation::new(3);
        assert_eq!(kr.active_count(), 0);
        let retired = kr.rotate(Box::new(MockSigner::new("k1")));
        assert!(retired.is_none());
        assert_eq!(kr.active_count(), 1);
    }

    #[test]
    fn active_signer_is_the_newest() {
        let mut kr = KeyRotation::new(3);
        kr.rotate(Box::new(MockSigner::new("k1")));
        kr.rotate(Box::new(MockSigner::new("k2")));
        kr.rotate(Box::new(MockSigner::new("k3")));
        assert_eq!(kr.active_signer().unwrap().id(), "k3");
    }

    #[test]
    fn verify_any_finds_old_keys() {
        let mut kr = KeyRotation::new(3);
        let s1 = MockSigner::new("k1");
        let root = [5u8; 32];
        let sig = s1.sign_sth(1, &root, 100);
        let sth = SignedTreeHead {
            size: 1,
            root,
            ts_ms: 100,
            signer_id: "k1".into(),
            signature: sig,
        };

        kr.rotate(Box::new(MockSigner::new("k1")));
        kr.rotate(Box::new(MockSigner::new("k2")));

        let result = kr.verify_any(&sth);
        assert_eq!(result, Some("k1"));
    }

    #[test]
    fn oldest_key_retired_when_exceeding_max() {
        let mut kr = KeyRotation::new(2);
        kr.rotate(Box::new(MockSigner::new("k1")));
        kr.rotate(Box::new(MockSigner::new("k2")));
        let retired = kr.rotate(Box::new(MockSigner::new("k3")));
        assert_eq!(retired, Some("k1".to_string()));
        assert_eq!(kr.active_count(), 2);
        assert_eq!(kr.active_key_ids(), vec!["k2", "k3"]);
    }

    #[test]
    fn empty_rotation_returns_none_for_active() {
        let kr = KeyRotation::new(3);
        assert!(kr.active_signer().is_none());
        assert_eq!(kr.active_count(), 0);
        assert!(kr.active_key_ids().is_empty());
    }

    #[test]
    fn verify_any_returns_none_for_unknown_signer() {
        let mut kr = KeyRotation::new(3);
        kr.rotate(Box::new(MockSigner::new("k1")));
        let sth = SignedTreeHead {
            size: 1,
            root: [0u8; 32],
            ts_ms: 0,
            signer_id: "unknown".into(),
            signature: vec![0; 32],
        };
        assert!(kr.verify_any(&sth).is_none());
    }

    #[test]
    fn verify_any_empty_returns_none() {
        let kr = KeyRotation::new(3);
        let sth = SignedTreeHead {
            size: 1,
            root: [0u8; 32],
            ts_ms: 0,
            signer_id: "k1".into(),
            signature: vec![],
        };
        assert!(kr.verify_any(&sth).is_none());
    }
}
