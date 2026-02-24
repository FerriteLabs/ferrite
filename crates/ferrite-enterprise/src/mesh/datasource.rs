//! Data source abstraction for the Data Mesh Gateway.

#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Supported external data source types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataSourceType {
    Ferrite,
    Postgres,
    Mysql,
    S3Parquet,
    Kafka,
    Http,
}

impl DataSourceType {
    /// Parse a string into a `DataSourceType` (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "ferrite" => Some(Self::Ferrite),
            "postgres" | "postgresql" => Some(Self::Postgres),
            "mysql" => Some(Self::Mysql),
            "s3" | "s3parquet" | "parquet" => Some(Self::S3Parquet),
            "kafka" => Some(Self::Kafka),
            "http" | "https" => Some(Self::Http),
            _ => None,
        }
    }
}

impl std::fmt::Display for DataSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ferrite => write!(f, "ferrite"),
            Self::Postgres => write!(f, "postgres"),
            Self::Mysql => write!(f, "mysql"),
            Self::S3Parquet => write!(f, "s3parquet"),
            Self::Kafka => write!(f, "kafka"),
            Self::Http => write!(f, "http"),
        }
    }
}

/// Health / connectivity status of a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSourceStatus {
    Connected,
    Disconnected,
    Error(String),
    Initializing,
}

impl std::fmt::Display for DataSourceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Error(e) => write!(f, "error: {e}"),
            Self::Initializing => write!(f, "initializing"),
        }
    }
}

/// Configuration for a single data source registered in the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    pub id: String,
    pub name: String,
    pub source_type: DataSourceType,
    pub uri: String,
    pub schema: Option<DataSchema>,
    pub health_check_interval_secs: u64,
    pub max_connections: u32,
    pub timeout_ms: u64,
    pub status: DataSourceStatus,
    pub added_at: DateTime<Utc>,
    pub last_health_check: Option<DateTime<Utc>>,
}

impl DataSourceConfig {
    /// Create a new data source with sensible defaults.
    pub fn new(id: String, name: String, source_type: DataSourceType, uri: String) -> Self {
        Self {
            id,
            name,
            source_type,
            uri,
            schema: None,
            health_check_interval_secs: 30,
            max_connections: 10,
            timeout_ms: 5000,
            status: DataSourceStatus::Initializing,
            added_at: Utc::now(),
            last_health_check: None,
        }
    }
}

/// Schema describing the fields exposed by a data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSchema {
    pub namespace: String,
    pub fields: Vec<SchemaField>,
    pub version: u32,
}

/// A single field in a [`DataSchema`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
}

/// Primitive field types used inside schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Bytes,
    Json,
    Timestamp,
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::Integer => write!(f, "integer"),
            Self::Float => write!(f, "float"),
            Self::Boolean => write!(f, "boolean"),
            Self::Bytes => write!(f, "bytes"),
            Self::Json => write!(f, "json"),
            Self::Timestamp => write!(f, "timestamp"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_type_from_str() {
        assert_eq!(
            DataSourceType::from_str_ci("Postgres"),
            Some(DataSourceType::Postgres)
        );
        assert_eq!(
            DataSourceType::from_str_ci("MYSQL"),
            Some(DataSourceType::Mysql)
        );
        assert_eq!(
            DataSourceType::from_str_ci("ferrite"),
            Some(DataSourceType::Ferrite)
        );
        assert_eq!(DataSourceType::from_str_ci("unknown"), None);
    }

    #[test]
    fn test_new_source_defaults() {
        let src = DataSourceConfig::new(
            "pg1".into(),
            "Primary PG".into(),
            DataSourceType::Postgres,
            "postgres://localhost/db".into(),
        );
        assert_eq!(src.max_connections, 10);
        assert_eq!(src.timeout_ms, 5000);
        assert!(matches!(src.status, DataSourceStatus::Initializing));
    }
}
