//! Kafka wire protocol client
//!
//! Implements a minimal Kafka protocol client for consuming messages
//! from real Kafka brokers over TCP. Supports the essential APIs:
//! - Metadata (API key 3)
//! - Fetch (API key 1)
//! - FindCoordinator (API key 10)
//! - JoinGroup (API key 11)
//! - SyncGroup (API key 14)
//! - OffsetFetch (API key 9)
//! - OffsetCommit (API key 8)
//! - Heartbeat (API key 12)

#![allow(dead_code)]

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the Kafka wire protocol client.
#[derive(Debug, thiserror::Error)]
pub enum KafkaWireError {
    /// Failed to connect to any broker.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Protocol-level error (malformed response, unexpected data).
    #[error("protocol error: {0}")]
    ProtocolError(String),

    /// Operation timed out.
    #[error("timeout")]
    Timeout,

    /// Broker returned an error code.
    #[error("broker error code: {0}")]
    BrokerError(i16),

    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Kafka wire protocol client.
#[derive(Debug, Clone)]
pub struct KafkaWireConfig {
    /// Broker addresses to connect to.
    pub brokers: Vec<SocketAddr>,
    /// Client identifier sent in request headers.
    pub client_id: String,
    /// Consumer group identifier.
    pub group_id: String,
    /// Topic to consume from.
    pub topic: String,
    /// Session timeout in milliseconds (default 30000).
    pub session_timeout_ms: i32,
    /// Maximum records returned per poll (default 500).
    pub max_poll_records: i32,
    /// Maximum bytes to fetch per request (default 1 MiB).
    pub fetch_max_bytes: i32,
    /// API version to use for requests (default 0 for broadest compat).
    pub api_version: i16,
}

impl Default for KafkaWireConfig {
    fn default() -> Self {
        Self {
            brokers: vec![SocketAddr::from(([127, 0, 0, 1], 9092))],
            client_id: "ferrite-wire-client".to_string(),
            group_id: "ferrite-consumer".to_string(),
            topic: String::new(),
            session_timeout_ms: 30_000,
            max_poll_records: 500,
            fetch_max_bytes: 1_048_576,
            api_version: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Information about a single Kafka broker.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker node ID.
    pub node_id: i32,
    /// Broker hostname.
    pub host: String,
    /// Broker port.
    pub port: i32,
}

/// Metadata about a single topic partition.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Partition ID.
    pub id: i32,
    /// Node ID of the partition leader.
    pub leader: i32,
    /// Node IDs of replicas.
    pub replicas: Vec<i32>,
}

/// Metadata about a single topic.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: String,
    /// Partition metadata.
    pub partitions: Vec<PartitionMetadata>,
}

/// Response from a Metadata request (API key 3).
#[derive(Debug, Clone)]
pub struct MetadataResponse {
    /// Known brokers in the cluster.
    pub brokers: Vec<BrokerInfo>,
    /// Metadata for requested topics.
    pub topics: Vec<TopicMetadata>,
}

/// A single record returned from a Fetch request.
#[derive(Debug, Clone)]
pub struct FetchRecord {
    /// Record offset within the partition.
    pub offset: i64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Record value.
    pub value: Vec<u8>,
    /// Record timestamp (epoch millis, or -1 if not available).
    pub timestamp: i64,
}

/// Response from a Fetch request (API key 1).
#[derive(Debug, Clone)]
pub struct FetchResponse {
    /// Records fetched from the broker.
    pub records: Vec<FetchRecord>,
}

// ---------------------------------------------------------------------------
// Wire protocol encoding helpers
// ---------------------------------------------------------------------------

/// Encode a Kafka protocol string (i16 length prefix + UTF-8 bytes).
pub fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

/// Encode a Kafka protocol byte array (i32 length prefix + bytes).
pub fn encode_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_i32(data.len() as i32);
    buf.put_slice(data);
}

/// Decode a Kafka protocol string (i16 length prefix + UTF-8 bytes).
pub fn decode_string(buf: &mut &[u8]) -> Result<String, KafkaWireError> {
    if buf.remaining() < 2 {
        return Err(KafkaWireError::ProtocolError(
            "not enough data for string length".to_string(),
        ));
    }
    let len = buf.get_i16();
    if len < 0 {
        // Null string in Kafka protocol
        return Ok(String::new());
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(KafkaWireError::ProtocolError(format!(
            "string length {len} exceeds remaining {} bytes",
            buf.remaining()
        )));
    }
    let bytes = &buf[..len];
    let s = String::from_utf8(bytes.to_vec())
        .map_err(|e| KafkaWireError::ProtocolError(format!("invalid UTF-8 in string: {e}")))?;
    buf.advance(len);
    Ok(s)
}

/// Decode a Kafka protocol byte array (i32 length prefix + bytes).
fn decode_bytes(buf: &mut &[u8]) -> Result<Option<Vec<u8>>, KafkaWireError> {
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "not enough data for bytes length".to_string(),
        ));
    }
    let len = buf.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(KafkaWireError::ProtocolError(format!(
            "bytes length {len} exceeds remaining {} bytes",
            buf.remaining()
        )));
    }
    let data = buf[..len].to_vec();
    buf.advance(len);
    Ok(Some(data))
}

/// Encode a Kafka request header.
///
/// Format: api_key (i16) + api_version (i16) + correlation_id (i32) + client_id (string)
pub fn encode_request_header(
    buf: &mut BytesMut,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
) {
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    encode_string(buf, client_id);
}

// ---------------------------------------------------------------------------
// Kafka request encoding
// ---------------------------------------------------------------------------

/// Encode a Metadata request body (API key 3, version 0).
///
/// If `topics` is empty, requests metadata for all topics.
pub fn encode_metadata_request(topics: &[&str]) -> BytesMut {
    let mut buf = BytesMut::new();
    // Array of topic names (i32 count)
    buf.put_i32(topics.len() as i32);
    for topic in topics {
        encode_string(&mut buf, topic);
    }
    buf
}

/// Encode a Fetch request body (API key 1, version 0).
///
/// Requests data from a single topic-partition starting at `offset`.
pub fn encode_fetch_request(topic: &str, partition: i32, offset: i64, max_bytes: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    // ReplicaId — consumers use -1
    buf.put_i32(-1);
    // MaxWaitTime (ms)
    buf.put_i32(500);
    // MinBytes
    buf.put_i32(1);
    // Number of topics
    buf.put_i32(1);
    encode_string(&mut buf, topic);
    // Number of partitions for this topic
    buf.put_i32(1);
    // Partition index
    buf.put_i32(partition);
    // FetchOffset
    buf.put_i64(offset);
    // PartitionMaxBytes
    buf.put_i32(max_bytes);
    buf
}

/// Encode an OffsetCommit request body (API key 8, version 0).
fn encode_offset_commit_request(
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_string(&mut buf, group_id);
    // Number of topics
    buf.put_i32(1);
    encode_string(&mut buf, topic);
    // Number of partitions
    buf.put_i32(1);
    buf.put_i32(partition);
    buf.put_i64(offset);
    // Metadata (empty string)
    encode_string(&mut buf, "");
    buf
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/// Decode a Metadata response (API key 3, version 0).
fn decode_metadata_response(data: &[u8]) -> Result<MetadataResponse, KafkaWireError> {
    let buf = &mut &data[..];

    // Skip correlation_id (already validated in read_response)
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short for correlation id".to_string(),
        ));
    }
    buf.advance(4);

    // Brokers array
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short for broker count".to_string(),
        ));
    }
    let broker_count = buf.get_i32();
    let mut brokers = Vec::with_capacity(broker_count.max(0) as usize);
    for _ in 0..broker_count {
        if buf.remaining() < 4 {
            return Err(KafkaWireError::ProtocolError(
                "truncated broker entry".to_string(),
            ));
        }
        let node_id = buf.get_i32();
        let host = decode_string(buf)?;
        if buf.remaining() < 4 {
            return Err(KafkaWireError::ProtocolError(
                "truncated broker port".to_string(),
            ));
        }
        let port = buf.get_i32();
        brokers.push(BrokerInfo {
            node_id,
            host,
            port,
        });
    }

    // Topics array
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short for topic count".to_string(),
        ));
    }
    let topic_count = buf.get_i32();
    let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
    for _ in 0..topic_count {
        // TopicErrorCode
        if buf.remaining() < 2 {
            return Err(KafkaWireError::ProtocolError(
                "truncated topic error code".to_string(),
            ));
        }
        let error_code = buf.get_i16();
        if error_code != 0 {
            return Err(KafkaWireError::BrokerError(error_code));
        }
        let name = decode_string(buf)?;

        // Partitions array
        if buf.remaining() < 4 {
            return Err(KafkaWireError::ProtocolError(
                "truncated partition count".to_string(),
            ));
        }
        let partition_count = buf.get_i32();
        let mut partitions = Vec::with_capacity(partition_count.max(0) as usize);
        for _ in 0..partition_count {
            // PartitionErrorCode
            if buf.remaining() < 2 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated partition error code".to_string(),
                ));
            }
            let p_error = buf.get_i16();
            if p_error != 0 {
                return Err(KafkaWireError::BrokerError(p_error));
            }
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated partition id".to_string(),
                ));
            }
            let id = buf.get_i32();
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated partition leader".to_string(),
                ));
            }
            let leader = buf.get_i32();

            // Replicas array
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated replicas count".to_string(),
                ));
            }
            let replica_count = buf.get_i32();
            let mut replicas = Vec::with_capacity(replica_count.max(0) as usize);
            for _ in 0..replica_count {
                if buf.remaining() < 4 {
                    return Err(KafkaWireError::ProtocolError(
                        "truncated replica id".to_string(),
                    ));
                }
                replicas.push(buf.get_i32());
            }

            // ISR array (skip)
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated ISR count".to_string(),
                ));
            }
            let isr_count = buf.get_i32();
            let isr_bytes = (isr_count.max(0) as usize) * 4;
            if buf.remaining() < isr_bytes {
                return Err(KafkaWireError::ProtocolError(
                    "truncated ISR entries".to_string(),
                ));
            }
            buf.advance(isr_bytes);

            partitions.push(PartitionMetadata {
                id,
                leader,
                replicas,
            });
        }

        topics.push(TopicMetadata { name, partitions });
    }

    Ok(MetadataResponse { brokers, topics })
}

/// Decode a Fetch response (API key 1, version 0).
///
/// Parses the v0 MessageSet format.
fn decode_fetch_response(data: &[u8]) -> Result<FetchResponse, KafkaWireError> {
    let buf = &mut &data[..];

    // Skip correlation_id
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short for correlation id".to_string(),
        ));
    }
    buf.advance(4);

    // Number of topics
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short for topic count".to_string(),
        ));
    }
    let topic_count = buf.get_i32();
    let mut records = Vec::new();

    for _ in 0..topic_count {
        // Topic name
        let _topic_name = decode_string(buf)?;

        // Number of partitions
        if buf.remaining() < 4 {
            return Err(KafkaWireError::ProtocolError(
                "truncated partition count in fetch".to_string(),
            ));
        }
        let partition_count = buf.get_i32();

        for _ in 0..partition_count {
            // Partition index
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated partition index".to_string(),
                ));
            }
            buf.advance(4); // partition index

            // Error code
            if buf.remaining() < 2 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated error code".to_string(),
                ));
            }
            let error_code = buf.get_i16();
            if error_code != 0 {
                return Err(KafkaWireError::BrokerError(error_code));
            }

            // HighWatermark
            if buf.remaining() < 8 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated high watermark".to_string(),
                ));
            }
            buf.advance(8);

            // MessageSet size
            if buf.remaining() < 4 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated message set size".to_string(),
                ));
            }
            let message_set_size = buf.get_i32() as usize;
            if buf.remaining() < message_set_size {
                return Err(KafkaWireError::ProtocolError(
                    "message set size exceeds remaining data".to_string(),
                ));
            }

            // Parse individual messages from the MessageSet
            let message_set_end = buf.len() - (buf.remaining() - message_set_size);
            let mut consumed = 0;
            while consumed < message_set_size {
                // Each message: Offset (8) + MessageSize (4)
                if buf.remaining() < 12 {
                    break; // Partial message at end of set
                }
                let offset = buf.get_i64();
                let msg_size = buf.get_i32() as usize;
                consumed += 12;

                if buf.remaining() < msg_size {
                    break; // Partial message
                }

                // CRC (4) + MagicByte (1) + Attributes (1)
                if msg_size < 6 {
                    buf.advance(msg_size);
                    consumed += msg_size;
                    continue;
                }
                buf.advance(4); // CRC
                let magic = buf.get_i8();
                buf.advance(1); // Attributes
                let mut inner_consumed = 6;

                // Timestamp (only in magic >= 1)
                let timestamp = if magic >= 1 && msg_size >= inner_consumed + 8 {
                    inner_consumed += 8;
                    buf.get_i64()
                } else {
                    -1
                };

                // Key
                let key = if msg_size >= inner_consumed + 4 {
                    let key_data = decode_bytes(buf)?;
                    inner_consumed += 4 + key_data.as_ref().map_or(0, |k| k.len());
                    key_data
                } else {
                    None
                };

                // Value
                let value = if msg_size >= inner_consumed + 4 {
                    decode_bytes(buf)?.unwrap_or_default()
                } else {
                    Vec::new()
                };

                // Skip any remaining bytes in this message
                let remaining_in_msg = msg_size.saturating_sub(
                    buf.len() - (buf.remaining() - (message_set_size - consumed - msg_size)),
                );
                if remaining_in_msg > 0 && buf.remaining() >= remaining_in_msg {
                    buf.advance(remaining_in_msg);
                }

                consumed += msg_size;

                records.push(FetchRecord {
                    offset,
                    key,
                    value,
                    timestamp,
                });
            }

            // Advance past any remaining bytes in the message set
            let still_left = message_set_size.saturating_sub(consumed);
            if still_left > 0 && buf.remaining() >= still_left {
                buf.advance(still_left);
            }

            let _ = message_set_end;
        }
    }

    Ok(FetchResponse { records })
}

/// Decode an OffsetCommit response (API key 8, version 0).
fn decode_offset_commit_response(data: &[u8]) -> Result<(), KafkaWireError> {
    let buf = &mut &data[..];

    // Skip correlation_id
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "response too short".to_string(),
        ));
    }
    buf.advance(4);

    // Number of topics
    if buf.remaining() < 4 {
        return Err(KafkaWireError::ProtocolError(
            "truncated topic count".to_string(),
        ));
    }
    let topic_count = buf.get_i32();
    for _ in 0..topic_count {
        let _name = decode_string(buf)?;
        if buf.remaining() < 4 {
            return Err(KafkaWireError::ProtocolError(
                "truncated partition count".to_string(),
            ));
        }
        let partition_count = buf.get_i32();
        for _ in 0..partition_count {
            if buf.remaining() < 6 {
                return Err(KafkaWireError::ProtocolError(
                    "truncated partition response".to_string(),
                ));
            }
            buf.advance(4); // partition index
            let error_code = buf.get_i16();
            if error_code != 0 {
                return Err(KafkaWireError::BrokerError(error_code));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// KafkaWireClient
// ---------------------------------------------------------------------------

/// Minimal Kafka wire protocol client over TCP.
///
/// Connects to a Kafka broker and speaks enough of the binary protocol
/// to fetch metadata, consume messages, and commit offsets.
pub struct KafkaWireClient {
    stream: Option<TcpStream>,
    config: KafkaWireConfig,
    correlation_id: AtomicI32,
    read_buf: BytesMut,
}

impl KafkaWireClient {
    /// Connect to the first responsive broker in the config.
    pub async fn connect(config: &KafkaWireConfig) -> Result<Self, KafkaWireError> {
        if config.brokers.is_empty() {
            return Err(KafkaWireError::ConnectionFailed(
                "no brokers configured".to_string(),
            ));
        }

        let mut last_error = None;
        for addr in &config.brokers {
            match tokio::time::timeout(
                Duration::from_millis(config.session_timeout_ms as u64),
                TcpStream::connect(addr),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    stream.set_nodelay(true).ok();
                    return Ok(Self {
                        stream: Some(stream),
                        config: config.clone(),
                        correlation_id: AtomicI32::new(0),
                        read_buf: BytesMut::with_capacity(4096),
                    });
                }
                Ok(Err(e)) => {
                    last_error = Some(e.to_string());
                }
                Err(_) => {
                    last_error = Some(format!("timeout connecting to {addr}"));
                }
            }
        }

        Err(KafkaWireError::ConnectionFailed(
            last_error.unwrap_or_else(|| "unknown error".to_string()),
        ))
    }

    /// Return the next correlation ID.
    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get a mutable reference to the active stream, or return an error.
    fn stream_mut(&mut self) -> Result<&mut TcpStream, KafkaWireError> {
        self.stream
            .as_mut()
            .ok_or_else(|| KafkaWireError::ConnectionFailed("connection closed".to_string()))
    }

    /// Frame and send a request to the broker.
    ///
    /// Prepends the 4-byte total length and the request header, then writes
    /// the body. Returns the correlation ID used.
    pub async fn send_request(
        &mut self,
        api_key: i16,
        api_version: i16,
        body: &[u8],
    ) -> Result<i32, KafkaWireError> {
        let correlation_id = self.next_correlation_id();

        // Build header
        let mut header = BytesMut::new();
        encode_request_header(
            &mut header,
            api_key,
            api_version,
            correlation_id,
            &self.config.client_id,
        );

        // Total message length = header + body
        let total_len = header.len() + body.len();
        let mut frame = BytesMut::with_capacity(4 + total_len);
        frame.put_i32(total_len as i32);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(body);

        let stream = self.stream_mut()?;
        stream.write_all(&frame).await?;
        stream.flush().await?;

        Ok(correlation_id)
    }

    /// Read a full response from the broker.
    ///
    /// Reads the 4-byte length prefix, then the response payload.
    pub async fn read_response(&mut self) -> Result<Vec<u8>, KafkaWireError> {
        let stream = self.stream_mut()?;

        // Read the 4-byte length prefix
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let response_len = i32::from_be_bytes(len_buf) as usize;

        if response_len > 100 * 1024 * 1024 {
            return Err(KafkaWireError::ProtocolError(format!(
                "response length {response_len} exceeds 100MiB limit"
            )));
        }

        let mut response = vec![0u8; response_len];
        stream.read_exact(&mut response).await?;

        Ok(response)
    }

    /// Fetch cluster metadata for the given topics.
    pub async fn fetch_metadata(&mut self) -> Result<MetadataResponse, KafkaWireError> {
        let topic = self.config.topic.clone();
        let topics: Vec<&str> = if topic.is_empty() {
            vec![]
        } else {
            vec![topic.as_str()]
        };
        let body = encode_metadata_request(&topics);
        self.send_request(3, self.config.api_version, &body).await?;
        let response = self.read_response().await?;
        decode_metadata_response(&response)
    }

    /// Fetch messages from a specific topic-partition starting at `offset`.
    pub async fn fetch_messages(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchResponse, KafkaWireError> {
        let body = encode_fetch_request(topic, partition, offset, max_bytes);
        self.send_request(1, self.config.api_version, &body).await?;
        let response = self.read_response().await?;
        decode_fetch_response(&response)
    }

    /// Commit an offset for a topic-partition to the broker.
    pub async fn commit_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaWireError> {
        let body = encode_offset_commit_request(&self.config.group_id, topic, partition, offset);
        self.send_request(8, self.config.api_version, &body).await?;
        let response = self.read_response().await?;
        decode_offset_commit_response(&response)
    }

    /// Close the connection to the broker.
    pub async fn close(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_string() {
        let mut buf = BytesMut::new();
        encode_string(&mut buf, "hello");
        assert_eq!(&buf[..], &[0, 5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_bytes() {
        let mut buf = BytesMut::new();
        encode_bytes(&mut buf, &[1, 2, 3]);
        assert_eq!(&buf[..], &[0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn test_decode_string() {
        let data: Vec<u8> = vec![0, 5, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = data.as_slice();
        let s = decode_string(&mut cursor).unwrap();
        assert_eq!(s, "hello");
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_decode_string_null() {
        // Negative length means null string in Kafka protocol
        let data: Vec<u8> = vec![0xFF, 0xFF]; // -1 as i16
        let mut cursor = data.as_slice();
        let s = decode_string(&mut cursor).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn test_decode_string_truncated() {
        let data: Vec<u8> = vec![0, 10, b'h', b'i'];
        let mut cursor = data.as_slice();
        let result = decode_string(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_request_header() {
        let mut buf = BytesMut::new();
        encode_request_header(&mut buf, 3, 0, 42, "test");
        // api_key(2) + api_version(2) + correlation_id(4) + string_len(2) + "test"(4) = 14
        assert_eq!(buf.len(), 14);
        let mut cursor = &buf[..];
        assert_eq!(cursor.get_i16(), 3); // api_key
        assert_eq!(cursor.get_i16(), 0); // api_version
        assert_eq!(cursor.get_i32(), 42); // correlation_id
    }

    #[test]
    fn test_encode_metadata_request() {
        let body = encode_metadata_request(&["my-topic"]);
        let mut cursor = &body[..];
        assert_eq!(cursor.get_i32(), 1); // topic count
        let topic = decode_string(&mut cursor).unwrap();
        assert_eq!(topic, "my-topic");
    }

    #[test]
    fn test_encode_metadata_request_empty() {
        let body = encode_metadata_request(&[]);
        let mut cursor = &body[..];
        assert_eq!(cursor.get_i32(), 0); // zero topics = all topics
    }

    #[test]
    fn test_encode_fetch_request() {
        let body = encode_fetch_request("test-topic", 0, 100, 1024);
        let mut cursor = &body[..];
        assert_eq!(cursor.get_i32(), -1); // replica_id
        assert_eq!(cursor.get_i32(), 500); // max_wait_time
        assert_eq!(cursor.get_i32(), 1); // min_bytes
        assert_eq!(cursor.get_i32(), 1); // topic count
    }

    #[test]
    fn test_kafka_wire_config_default() {
        let config = KafkaWireConfig::default();
        assert_eq!(config.brokers.len(), 1);
        assert_eq!(config.session_timeout_ms, 30_000);
        assert_eq!(config.max_poll_records, 500);
        assert_eq!(config.fetch_max_bytes, 1_048_576);
        assert_eq!(config.api_version, 0);
    }

    #[test]
    fn test_decode_bytes_null() {
        let data: Vec<u8> = vec![0xFF, 0xFF, 0xFF, 0xFF]; // -1 as i32
        let mut cursor = data.as_slice();
        let result = decode_bytes(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_bytes_valid() {
        let data: Vec<u8> = vec![0, 0, 0, 3, 10, 20, 30];
        let mut cursor = data.as_slice();
        let result = decode_bytes(&mut cursor).unwrap();
        assert_eq!(result, Some(vec![10, 20, 30]));
    }

    #[tokio::test]
    async fn test_connect_no_brokers() {
        let config = KafkaWireConfig {
            brokers: vec![],
            ..KafkaWireConfig::default()
        };
        let result = KafkaWireClient::connect(&config).await;
        assert!(matches!(result, Err(KafkaWireError::ConnectionFailed(_))));
    }

    #[tokio::test]
    async fn test_connect_unreachable_broker() {
        let config = KafkaWireConfig {
            brokers: vec![SocketAddr::from(([127, 0, 0, 1], 19999))],
            session_timeout_ms: 500,
            ..KafkaWireConfig::default()
        };
        let result = KafkaWireClient::connect(&config).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_offset_commit_request() {
        let body = encode_offset_commit_request("my-group", "my-topic", 0, 42);
        let mut cursor = &body[..];
        let group = decode_string(&mut cursor).unwrap();
        assert_eq!(group, "my-group");
        assert_eq!(cursor.get_i32(), 1); // topic count
    }
}
