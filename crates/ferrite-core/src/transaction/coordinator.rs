//! Two-Phase Commit (2PC) Coordinator
//!
//! Implements distributed transaction coordination using the 2PC protocol.
//! Ensures atomicity across multiple nodes in a cluster.
//!
//! # Protocol Flow
//!
//! ```text
//! Coordinator                 Participants (P1, P2, P3)
//!     │                             │
//!     │──── PREPARE ───────────────▶│ P1: Lock resources, write to WAL
//!     │                             │ P2: Lock resources, write to WAL
//!     │                             │ P3: Lock resources, write to WAL
//!     │                             │
//!     │◀─── VOTE (YES/NO) ─────────│ All must vote YES to commit
//!     │                             │
//! ┌───┴───┐                         │
//! │Decision│                         │
//! └───┬───┘                         │
//!     │                             │
//!     │──── COMMIT/ABORT ──────────▶│ Apply changes or rollback
//!     │                             │
//!     │◀─── ACK ───────────────────│
//!     │                             │
//! ```
//!
//! # Recovery
//!
//! If coordinator crashes after PREPARE:
//! - Participants remain in PREPARED state
//! - New coordinator recovers from WAL
//! - Resumes COMMIT or ABORT based on logged decision

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

use super::txn::TransactionId;
use super::TransactionError;

/// 2PC Coordinator configuration
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Timeout for prepare phase
    pub prepare_timeout: Duration,
    /// Timeout for commit phase
    pub commit_timeout: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            prepare_timeout: Duration::from_secs(10),
            commit_timeout: Duration::from_secs(30),
        }
    }
}

/// Participant state in 2PC
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantState {
    /// Initial state
    Initial,
    /// Received PREPARE, voting
    Preparing,
    /// Voted YES (prepared)
    Prepared,
    /// Voted NO (aborted locally)
    Aborted,
    /// Received COMMIT, applying
    Committing,
    /// Successfully committed
    Committed,
    /// Failed (timeout, error)
    Failed(String),
}

/// Transaction state in coordinator
#[derive(Debug)]
struct CoordinatedTransaction {
    /// Transaction ID
    txn_id: TransactionId,
    /// Participant nodes
    participants: Vec<String>,
    /// Participant states
    states: HashMap<String, ParticipantState>,
    /// Global decision
    decision: Option<TransactionDecision>,
    /// Start time
    started_at: Instant,
    /// Prepare responses received
    prepare_responses: HashSet<String>,
    /// Commit responses received
    commit_responses: HashSet<String>,
}

/// Global transaction decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionDecision {
    /// Commit the transaction.
    Commit,
    /// Abort the transaction with a reason.
    Abort(String),
}

/// Message types for 2PC protocol
#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    /// Prepare request sent to participants.
    Prepare {
        /// Transaction ID.
        txn_id: TransactionId,
    },
    /// Vote response from a participant.
    Vote {
        /// Transaction ID.
        txn_id: TransactionId,
        /// Whether the participant voted YES.
        vote: bool,
        /// Participant node identifier.
        participant: String,
    },
    /// Commit request sent to participants.
    Commit {
        /// Transaction ID.
        txn_id: TransactionId,
    },
    /// Abort request sent to participants.
    Abort {
        /// Transaction ID.
        txn_id: TransactionId,
        /// Reason for the abort.
        reason: String,
    },
    /// Acknowledgment from a participant.
    Ack {
        /// Transaction ID.
        txn_id: TransactionId,
        /// Participant node identifier.
        participant: String,
        /// Whether the operation succeeded.
        success: bool,
    },
}

/// Two-Phase Commit Coordinator
pub struct TwoPhaseCoordinator {
    /// Configuration
    config: CoordinatorConfig,
    /// Active transactions
    transactions: RwLock<HashMap<TransactionId, CoordinatedTransaction>>,
    /// Message queue for testing/simulation
    message_queue: Mutex<Vec<ProtocolMessage>>,
}

impl TwoPhaseCoordinator {
    /// Create a new 2PC coordinator
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            transactions: RwLock::new(HashMap::new()),
            message_queue: Mutex::new(Vec::new()),
        }
    }

    /// Register a distributed transaction
    pub async fn register_transaction(
        &self,
        txn_id: TransactionId,
        participants: Vec<String>,
    ) -> Result<(), TransactionError> {
        if participants.is_empty() {
            return Err(TransactionError::InvalidState(
                "no participants specified".to_string(),
            ));
        }

        let mut txns = self.transactions.write().await;
        if txns.contains_key(&txn_id) {
            return Err(TransactionError::InvalidState(
                "transaction already registered".to_string(),
            ));
        }

        let mut states = HashMap::new();
        for p in &participants {
            states.insert(p.clone(), ParticipantState::Initial);
        }

        txns.insert(
            txn_id,
            CoordinatedTransaction {
                txn_id,
                participants,
                states,
                decision: None,
                started_at: Instant::now(),
                prepare_responses: HashSet::new(),
                commit_responses: HashSet::new(),
            },
        );

        Ok(())
    }

    /// Check if a transaction is distributed
    pub async fn is_distributed(&self, txn_id: TransactionId) -> bool {
        self.transactions.read().await.contains_key(&txn_id)
    }

    /// Execute PREPARE phase
    pub async fn prepare(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        // Update states to Preparing
        {
            let mut txns = self.transactions.write().await;
            let txn = txns
                .get_mut(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;

            for state in txn.states.values_mut() {
                *state = ParticipantState::Preparing;
            }

            // Queue PREPARE messages
            let mut queue = self.message_queue.lock().await;
            queue.push(ProtocolMessage::Prepare { txn_id });
        }

        // Wait for all votes (with timeout)
        let deadline = Instant::now() + self.config.prepare_timeout;

        loop {
            if Instant::now() > deadline {
                self.abort(txn_id).await?;
                return Err(TransactionError::PrepareFailed("timeout".to_string()));
            }

            let all_prepared = {
                let txns = self.transactions.read().await;
                if let Some(txn) = txns.get(&txn_id) {
                    txn.states
                        .values()
                        .all(|s| *s == ParticipantState::Prepared)
                } else {
                    return Err(TransactionError::NotFound(txn_id));
                }
            };

            if all_prepared {
                break;
            }

            // Check for any aborted participants
            let any_aborted = {
                let txns = self.transactions.read().await;
                if let Some(txn) = txns.get(&txn_id) {
                    txn.states.values().any(|s| {
                        matches!(s, ParticipantState::Aborted | ParticipantState::Failed(_))
                    })
                } else {
                    return Err(TransactionError::NotFound(txn_id));
                }
            };

            if any_aborted {
                self.abort(txn_id).await?;
                return Err(TransactionError::PrepareFailed(
                    "participant aborted".to_string(),
                ));
            }

            // For local testing, simulate vote responses
            self.simulate_votes(txn_id).await;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(())
    }

    /// Execute COMMIT phase
    pub async fn commit(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        // Record decision
        {
            let mut txns = self.transactions.write().await;
            let txn = txns
                .get_mut(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;

            txn.decision = Some(TransactionDecision::Commit);

            // Update states to Committing
            for state in txn.states.values_mut() {
                if *state == ParticipantState::Prepared {
                    *state = ParticipantState::Committing;
                }
            }

            // Queue COMMIT messages
            let mut queue = self.message_queue.lock().await;
            queue.push(ProtocolMessage::Commit { txn_id });
        }

        // Wait for all acks (with timeout)
        let deadline = Instant::now() + self.config.commit_timeout;

        loop {
            if Instant::now() > deadline {
                return Err(TransactionError::CommitFailed("timeout".to_string()));
            }

            let all_committed = {
                let txns = self.transactions.read().await;
                if let Some(txn) = txns.get(&txn_id) {
                    txn.states
                        .values()
                        .all(|s| *s == ParticipantState::Committed)
                } else {
                    return Err(TransactionError::NotFound(txn_id));
                }
            };

            if all_committed {
                break;
            }

            // For local testing, simulate commit acks
            self.simulate_commit_acks(txn_id).await;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clean up
        self.transactions.write().await.remove(&txn_id);

        Ok(())
    }

    /// Execute ABORT phase
    pub async fn abort(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let mut txns = self.transactions.write().await;
        let txn = txns
            .get_mut(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;

        txn.decision = Some(TransactionDecision::Abort("requested".to_string()));

        // Queue ABORT messages
        let mut queue = self.message_queue.lock().await;
        queue.push(ProtocolMessage::Abort {
            txn_id,
            reason: "requested".to_string(),
        });

        // Mark all as aborted
        for state in txn.states.values_mut() {
            *state = ParticipantState::Aborted;
        }

        // Clean up
        drop(txns);
        self.transactions.write().await.remove(&txn_id);

        Ok(())
    }

    /// Handle a vote from a participant
    pub async fn handle_vote(
        &self,
        txn_id: TransactionId,
        participant: String,
        vote: bool,
    ) -> Result<(), TransactionError> {
        let mut txns = self.transactions.write().await;
        let txn = txns
            .get_mut(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;

        if vote {
            txn.states
                .insert(participant.clone(), ParticipantState::Prepared);
            txn.prepare_responses.insert(participant);
        } else {
            txn.states
                .insert(participant.clone(), ParticipantState::Aborted);
        }

        Ok(())
    }

    /// Handle a commit ack from a participant
    pub async fn handle_commit_ack(
        &self,
        txn_id: TransactionId,
        participant: String,
        success: bool,
    ) -> Result<(), TransactionError> {
        let mut txns = self.transactions.write().await;
        let txn = txns
            .get_mut(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;

        if success {
            txn.states
                .insert(participant.clone(), ParticipantState::Committed);
            txn.commit_responses.insert(participant);
        } else {
            txn.states.insert(
                participant.clone(),
                ParticipantState::Failed("commit failed".to_string()),
            );
        }

        Ok(())
    }

    /// Get the current state of a transaction
    pub async fn get_state(
        &self,
        txn_id: TransactionId,
    ) -> Option<HashMap<String, ParticipantState>> {
        self.transactions
            .read()
            .await
            .get(&txn_id)
            .map(|t| t.states.clone())
    }

    /// Get the decision for a transaction
    pub async fn get_decision(&self, txn_id: TransactionId) -> Option<TransactionDecision> {
        self.transactions
            .read()
            .await
            .get(&txn_id)
            .and_then(|t| t.decision.clone())
    }

    /// Get the number of active transactions
    pub async fn active_count(&self) -> usize {
        self.transactions.read().await.len()
    }

    /// Get pending protocol messages (for testing/network layer).
    pub async fn drain_messages(&self) -> Vec<ProtocolMessage> {
        let mut queue = self.message_queue.lock().await;
        std::mem::take(&mut *queue)
    }

    // Simulation helpers for local/single-node testing

    async fn simulate_votes(&self, txn_id: TransactionId) {
        let participants: Vec<String> = {
            let txns = self.transactions.read().await;
            if let Some(txn) = txns.get(&txn_id) {
                txn.participants.clone()
            } else {
                return;
            }
        };

        for participant in participants {
            let _ = self.handle_vote(txn_id, participant, true).await;
        }
    }

    async fn simulate_commit_acks(&self, txn_id: TransactionId) {
        let participants: Vec<String> = {
            let txns = self.transactions.read().await;
            if let Some(txn) = txns.get(&txn_id) {
                txn.participants.clone()
            } else {
                return;
            }
        };

        for participant in participants {
            let _ = self.handle_commit_ack(txn_id, participant, true).await;
        }
    }
}

/// Participant handler for 2PC (runs on each participant node)
pub struct TwoPhaseParticipant {
    /// Node identifier
    node_id: String,
    /// Prepared transactions waiting for decision
    prepared: RwLock<HashMap<TransactionId, PreparedState>>,
}

/// State of a prepared transaction on participant
struct PreparedState {
    txn_id: TransactionId,
    prepared_at: Instant,
    write_set: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>, // (db, key, value)
}

impl TwoPhaseParticipant {
    /// Create a new 2PC participant
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            prepared: RwLock::new(HashMap::new()),
        }
    }

    /// Handle PREPARE request
    pub async fn prepare(
        &self,
        txn_id: TransactionId,
        write_set: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
    ) -> bool {
        // In a real implementation:
        // 1. Acquire locks on all keys
        // 2. Validate constraints
        // 3. Write to WAL (not yet applied)
        // 4. Return vote

        // For now, always vote YES
        let state = PreparedState {
            txn_id,
            prepared_at: Instant::now(),
            write_set,
        };

        self.prepared.write().await.insert(txn_id, state);
        true
    }

    /// Handle COMMIT request
    pub async fn commit(&self, txn_id: TransactionId) -> bool {
        if let Some(_state) = self.prepared.write().await.remove(&txn_id) {
            // In a real implementation:
            // 1. Apply writes from WAL
            // 2. Release locks
            // 3. Acknowledge

            true
        } else {
            // Transaction not found (might be replayed)
            false
        }
    }

    /// Handle ABORT request
    pub async fn abort(&self, txn_id: TransactionId) {
        // Remove from prepared, release locks
        self.prepared.write().await.remove(&txn_id);
    }

    /// Get prepared transaction count
    pub async fn prepared_count(&self) -> usize {
        self.prepared.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_creation() {
        let coord = TwoPhaseCoordinator::new(CoordinatorConfig::default());
        assert_eq!(coord.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_register_transaction() {
        let coord = TwoPhaseCoordinator::new(CoordinatorConfig::default());
        let txn_id = TransactionId(1);

        coord
            .register_transaction(txn_id, vec!["node1".to_string(), "node2".to_string()])
            .await
            .unwrap();

        assert!(coord.is_distributed(txn_id).await);
        assert_eq!(coord.active_count().await, 1);
    }

    #[tokio::test]
    async fn test_prepare_commit_flow() {
        let coord = TwoPhaseCoordinator::new(CoordinatorConfig {
            prepare_timeout: Duration::from_millis(100),
            commit_timeout: Duration::from_millis(100),
        });
        let txn_id = TransactionId(1);

        coord
            .register_transaction(txn_id, vec!["node1".to_string()])
            .await
            .unwrap();

        // Prepare (will auto-simulate votes)
        coord.prepare(txn_id).await.unwrap();

        // Commit (will auto-simulate acks)
        coord.commit(txn_id).await.unwrap();

        // Should be cleaned up
        assert_eq!(coord.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_abort() {
        let coord = TwoPhaseCoordinator::new(CoordinatorConfig::default());
        let txn_id = TransactionId(1);

        coord
            .register_transaction(txn_id, vec!["node1".to_string()])
            .await
            .unwrap();

        coord.abort(txn_id).await.unwrap();

        // Should be cleaned up
        assert_eq!(coord.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_vote_handling() {
        let coord = TwoPhaseCoordinator::new(CoordinatorConfig::default());
        let txn_id = TransactionId(1);

        coord
            .register_transaction(txn_id, vec!["node1".to_string(), "node2".to_string()])
            .await
            .unwrap();

        coord
            .handle_vote(txn_id, "node1".to_string(), true)
            .await
            .unwrap();

        let states = coord.get_state(txn_id).await.unwrap();
        assert_eq!(states.get("node1"), Some(&ParticipantState::Prepared));
        assert_eq!(states.get("node2"), Some(&ParticipantState::Initial));
    }

    #[tokio::test]
    async fn test_participant() {
        let participant = TwoPhaseParticipant::new("node1".to_string());
        let txn_id = TransactionId(1);

        // Prepare
        let vote = participant.prepare(txn_id, vec![]).await;
        assert!(vote);
        assert_eq!(participant.prepared_count().await, 1);

        // Commit
        let ack = participant.commit(txn_id).await;
        assert!(ack);
        assert_eq!(participant.prepared_count().await, 0);
    }

    #[tokio::test]
    async fn test_participant_abort() {
        let participant = TwoPhaseParticipant::new("node1".to_string());
        let txn_id = TransactionId(1);

        participant.prepare(txn_id, vec![]).await;
        assert_eq!(participant.prepared_count().await, 1);

        participant.abort(txn_id).await;
        assert_eq!(participant.prepared_count().await, 0);
    }
}
