//! Usage Metering
//!
//! Records and aggregates resource consumption metrics per tenant
//! for billing and capacity planning in Ferrite Cloud.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::CloudError;

/// The type of resource metric being recorded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricType {
    /// Operations per second throughput.
    OpsPerSecond,
    /// Memory consumption in bytes.
    MemoryBytes,
    /// Persistent storage consumption in bytes.
    StorageBytes,
    /// Network transfer in bytes.
    NetworkBytes,
    /// Active client connections.
    Connections,
}

/// A single usage data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Tenant that generated this usage.
    pub tenant_id: String,
    /// Kind of metric.
    pub metric_type: MetricType,
    /// Recorded value.
    pub value: f64,
    /// When the measurement was taken.
    pub timestamp: DateTime<Utc>,
}

/// Aggregated usage summary for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSummary {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Start of the aggregation window.
    pub from: DateTime<Utc>,
    /// End of the aggregation window.
    pub to: DateTime<Utc>,
    /// Aggregated totals keyed by metric type.
    pub totals: HashMap<MetricType, f64>,
    /// Number of records per metric type.
    pub counts: HashMap<MetricType, u64>,
}

/// Service responsible for recording and querying usage data.
pub struct MeteringService {
    records: Arc<RwLock<Vec<UsageRecord>>>,
}

impl MeteringService {
    /// Creates a new [`MeteringService`].
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Records a usage data point.
    pub async fn record(&self, record: UsageRecord) -> Result<(), CloudError> {
        if record.tenant_id.is_empty() {
            return Err(CloudError::InvalidConfig("tenant_id is empty".to_string()));
        }
        self.records.write().push(record);
        Ok(())
    }

    /// Queries usage records for a tenant within a time range.
    pub async fn query(
        &self,
        tenant_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<UsageRecord>, CloudError> {
        let records = self.records.read();
        let results = records
            .iter()
            .filter(|r| r.tenant_id == tenant_id && r.timestamp >= from && r.timestamp <= to)
            .cloned()
            .collect();
        Ok(results)
    }

    /// Aggregates usage records for a tenant within a time range.
    pub async fn aggregate(
        &self,
        tenant_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<UsageSummary, CloudError> {
        let records = self.query(tenant_id, from, to).await?;

        let mut totals: HashMap<MetricType, f64> = HashMap::new();
        let mut counts: HashMap<MetricType, u64> = HashMap::new();

        for record in &records {
            *totals.entry(record.metric_type.clone()).or_default() += record.value;
            *counts.entry(record.metric_type.clone()).or_default() += 1;
        }

        Ok(UsageSummary {
            tenant_id: tenant_id.to_string(),
            from,
            to,
            totals,
            counts,
        })
    }
}

impl Default for MeteringService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_record(tenant: &str, metric: MetricType, value: f64) -> UsageRecord {
        UsageRecord {
            tenant_id: tenant.to_string(),
            metric_type: metric,
            value,
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_record_and_query() {
        let svc = MeteringService::new();
        let now = Utc::now();

        svc.record(make_record("t1", MetricType::OpsPerSecond, 1000.0))
            .await
            .unwrap();
        svc.record(make_record("t1", MetricType::MemoryBytes, 2048.0))
            .await
            .unwrap();
        svc.record(make_record("t2", MetricType::OpsPerSecond, 500.0))
            .await
            .unwrap();

        let results = svc
            .query(
                "t1",
                now - Duration::seconds(10),
                now + Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregate() {
        let svc = MeteringService::new();
        let now = Utc::now();

        svc.record(make_record("t1", MetricType::OpsPerSecond, 100.0))
            .await
            .unwrap();
        svc.record(make_record("t1", MetricType::OpsPerSecond, 200.0))
            .await
            .unwrap();

        let summary = svc
            .aggregate(
                "t1",
                now - Duration::seconds(10),
                now + Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(summary.totals[&MetricType::OpsPerSecond], 300.0);
        assert_eq!(summary.counts[&MetricType::OpsPerSecond], 2);
    }

    #[tokio::test]
    async fn test_record_empty_tenant_error() {
        let svc = MeteringService::new();
        let record = UsageRecord {
            tenant_id: String::new(),
            metric_type: MetricType::Connections,
            value: 1.0,
            timestamp: Utc::now(),
        };
        assert!(svc.record(record).await.is_err());
    }
}
