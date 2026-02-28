//! Live Kafka consumer using wire protocol client
//!
//! Wraps [`KafkaWireClient`] to provide a high-level consumer interface
//! compatible with the vector ingest pipeline. Connects to real Kafka
//! brokers over TCP, fetches metadata, polls messages, and tracks offsets.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::kafka_wire::{
    FetchRecord, FetchResponse, KafkaWireClient, KafkaWireConfig, KafkaWireError, MetadataResponse,
    PartitionMetadata,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the live Kafka consumer.
#[derive(Debug, Clone)]
pub struct LiveKafkaConsumerConfig {
    /// Wire-level configuration (brokers, client id, etc.).
    pub wire_config: KafkaWireConfig,
    /// Whether to automatically commit offsets after each poll.
    pub auto_commit: bool,
    /// Interval between automatic offset commits.
    pub auto_commit_interval: Duration,
    /// Whether to start consuming from the beginning of the topic.
    pub from_beginning: bool,
}

impl Default for LiveKafkaConsumerConfig {
    fn default() -> Self {
        Self {
            wire_config: KafkaWireConfig::default(),
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            from_beginning: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Partition state
// ---------------------------------------------------------------------------

/// Tracks per-partition offset state.
#[derive(Debug)]
struct PartitionState {
    /// Next offset to fetch.
    next_offset: i64,
    /// Last offset successfully committed.
    committed_offset: i64,
}

// ---------------------------------------------------------------------------
// LiveKafkaConsumer
// ---------------------------------------------------------------------------

/// High-level Kafka consumer backed by the wire protocol client.
///
/// Connects to a Kafka broker, discovers topic partitions via the Metadata
/// API, and polls records using the Fetch API.
pub struct LiveKafkaConsumer {
    client: KafkaWireClient,
    config: LiveKafkaConsumerConfig,
    /// Per-partition offset tracking.
    partitions: HashMap<i32, PartitionState>,
    /// Cached partition metadata for the target topic.
    topic_partitions: Vec<PartitionMetadata>,
    /// Timestamp of the last auto-commit.
    last_commit: Instant,
    /// Whether the consumer has been closed.
    closed: bool,
}

impl LiveKafkaConsumer {
    /// Create a new live Kafka consumer.
    ///
    /// Connects to the broker and fetches metadata for the configured topic.
    pub async fn new(config: LiveKafkaConsumerConfig) -> Result<Self, KafkaWireError> {
        let mut client = KafkaWireClient::connect(&config.wire_config).await?;

        // Fetch metadata to discover partitions
        let metadata = client.fetch_metadata().await?;
        let topic_name = &config.wire_config.topic;

        let topic_meta = metadata
            .topics
            .iter()
            .find(|t| t.name == *topic_name)
            .ok_or_else(|| {
                KafkaWireError::ProtocolError(format!("topic '{topic_name}' not found in metadata"))
            })?;

        let topic_partitions = topic_meta.partitions.clone();

        // Initialise per-partition offsets
        let start_offset = if config.from_beginning { 0 } else { -1 };
        let partitions: HashMap<i32, PartitionState> = topic_partitions
            .iter()
            .map(|p| {
                (
                    p.id,
                    PartitionState {
                        next_offset: start_offset,
                        committed_offset: -1,
                    },
                )
            })
            .collect();

        Ok(Self {
            client,
            config,
            partitions,
            topic_partitions,
            last_commit: Instant::now(),
            closed: false,
        })
    }

    /// Poll the broker for the next batch of records.
    ///
    /// Iterates over all assigned partitions and fetches up to
    /// `max_poll_records` records total.
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<FetchRecord>, KafkaWireError> {
        if self.closed {
            return Err(KafkaWireError::ConnectionFailed(
                "consumer is closed".to_string(),
            ));
        }

        let topic = self.config.wire_config.topic.clone();
        let max_bytes = self.config.wire_config.fetch_max_bytes;
        let max_records = self.config.wire_config.max_poll_records as usize;
        let partition_ids: Vec<i32> = self.partitions.keys().copied().collect();

        let mut all_records = Vec::new();

        for pid in &partition_ids {
            if all_records.len() >= max_records {
                break;
            }

            let offset = match self.partitions.get(pid) {
                Some(state) => state.next_offset,
                None => continue,
            };

            // Skip partitions with unknown offset (latest mode, not yet resolved)
            if offset < 0 {
                continue;
            }

            let fetch_result = tokio::time::timeout(
                timeout,
                self.client.fetch_messages(&topic, *pid, offset, max_bytes),
            )
            .await;

            match fetch_result {
                Ok(Ok(response)) => {
                    if let Some(last) = response.records.last() {
                        if let Some(state) = self.partitions.get_mut(pid) {
                            state.next_offset = last.offset + 1;
                        }
                    }
                    all_records.extend(response.records);
                }
                Ok(Err(e)) => {
                    tracing::warn!(partition = pid, error = %e, "fetch failed for partition");
                }
                Err(_) => {
                    tracing::debug!(partition = pid, "fetch timed out for partition");
                }
            }
        }

        // Truncate to max_poll_records
        all_records.truncate(max_records);

        // Auto-commit if configured and interval has elapsed
        if self.config.auto_commit
            && !all_records.is_empty()
            && self.last_commit.elapsed() >= self.config.auto_commit_interval
        {
            if let Err(e) = self.commit().await {
                tracing::warn!(error = %e, "auto-commit failed");
            }
        }

        Ok(all_records)
    }

    /// Commit current offsets for all partitions to the broker.
    pub async fn commit(&mut self) -> Result<(), KafkaWireError> {
        if self.closed {
            return Err(KafkaWireError::ConnectionFailed(
                "consumer is closed".to_string(),
            ));
        }

        let topic = self.config.wire_config.topic.clone();

        for (pid, state) in &mut self.partitions {
            if state.next_offset > state.committed_offset {
                self.client
                    .commit_offset(&topic, *pid, state.next_offset)
                    .await?;
                state.committed_offset = state.next_offset;
            }
        }

        self.last_commit = Instant::now();
        Ok(())
    }

    /// Return the current offset (next offset to fetch) for partition 0,
    /// or the minimum offset across all partitions.
    pub fn current_offset(&self) -> i64 {
        self.partitions
            .values()
            .map(|s| s.next_offset)
            .min()
            .unwrap_or(0)
    }

    /// Close the consumer, committing final offsets if possible.
    pub async fn close(&mut self) -> Result<(), KafkaWireError> {
        if self.closed {
            return Ok(());
        }

        // Best-effort final commit
        let _ = self.commit().await;
        self.client.close().await;
        self.closed = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_live_kafka_consumer_config_default() {
        let config = LiveKafkaConsumerConfig::default();
        assert!(config.auto_commit);
        assert!(config.from_beginning);
        assert_eq!(config.auto_commit_interval, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_new_fails_no_broker() {
        let config = LiveKafkaConsumerConfig {
            wire_config: KafkaWireConfig {
                brokers: vec![],
                ..KafkaWireConfig::default()
            },
            ..LiveKafkaConsumerConfig::default()
        };

        let result = LiveKafkaConsumer::new(config).await;
        assert!(matches!(result, Err(KafkaWireError::ConnectionFailed(_))));
    }

    #[tokio::test]
    async fn test_new_fails_unreachable_broker() {
        let config = LiveKafkaConsumerConfig {
            wire_config: KafkaWireConfig {
                brokers: vec![SocketAddr::from(([127, 0, 0, 1], 19998))],
                topic: "test".to_string(),
                session_timeout_ms: 200,
                ..KafkaWireConfig::default()
            },
            ..LiveKafkaConsumerConfig::default()
        };

        let result = LiveKafkaConsumer::new(config).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_state_initial() {
        let state = PartitionState {
            next_offset: 0,
            committed_offset: -1,
        };
        assert_eq!(state.next_offset, 0);
        assert_eq!(state.committed_offset, -1);
    }
}
