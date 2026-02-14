//! Billing Service
//!
//! Converts usage records from the metering service into invoices with
//! per-tier pricing, discounts, and payment tracking.

use std::collections::HashMap;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::metering::MetricType;
use super::CloudError;

/// Pricing per unit for each metric type within a tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierPricing {
    /// Tier name (free, standard, premium)
    pub tier: String,
    /// Price per ops-per-second-hour in cents
    pub ops_per_second_hour_cents: u64,
    /// Price per GB-hour of memory in cents
    pub memory_gb_hour_cents: u64,
    /// Price per GB-month of storage in cents
    pub storage_gb_month_cents: u64,
    /// Price per GB of network egress in cents
    pub network_gb_cents: u64,
    /// Price per connection-hour in cents
    pub connection_hour_cents: u64,
    /// Free tier limits (operations below this are free)
    pub free_ops_per_month: u64,
}

impl TierPricing {
    /// Standard tier pricing
    pub fn standard() -> Self {
        Self {
            tier: "standard".to_string(),
            ops_per_second_hour_cents: 5,
            memory_gb_hour_cents: 1,
            storage_gb_month_cents: 10,
            network_gb_cents: 9,
            connection_hour_cents: 1,
            free_ops_per_month: 100_000,
        }
    }

    /// Premium tier pricing
    pub fn premium() -> Self {
        Self {
            tier: "premium".to_string(),
            ops_per_second_hour_cents: 8,
            memory_gb_hour_cents: 2,
            storage_gb_month_cents: 15,
            network_gb_cents: 7,
            connection_hour_cents: 1,
            free_ops_per_month: 1_000_000,
        }
    }

    /// Free tier (no charges)
    pub fn free() -> Self {
        Self {
            tier: "free".to_string(),
            ops_per_second_hour_cents: 0,
            memory_gb_hour_cents: 0,
            storage_gb_month_cents: 0,
            network_gb_cents: 0,
            connection_hour_cents: 0,
            free_ops_per_month: 10_000_000,
        }
    }
}

/// A line item on an invoice
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvoiceLineItem {
    /// Description of the charge
    pub description: String,
    /// Metric type
    pub metric: MetricType,
    /// Quantity consumed
    pub quantity: f64,
    /// Unit label (e.g., "CPU-hours", "GB-months")
    pub unit: String,
    /// Amount in cents
    pub amount_cents: u64,
}

/// An invoice for a billing period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    /// Unique invoice ID
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Billing period start
    pub period_start: SystemTime,
    /// Billing period end
    pub period_end: SystemTime,
    /// Line items
    pub line_items: Vec<InvoiceLineItem>,
    /// Subtotal in cents
    pub subtotal_cents: u64,
    /// Discount in cents
    pub discount_cents: u64,
    /// Total amount due in cents
    pub total_cents: u64,
    /// Invoice status
    pub status: InvoiceStatus,
    /// When the invoice was generated
    pub generated_at: SystemTime,
}

/// Status of an invoice
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvoiceStatus {
    /// Invoice has been generated but not sent
    Draft,
    /// Invoice has been sent to the tenant
    Sent,
    /// Invoice has been paid
    Paid,
    /// Invoice is overdue
    Overdue,
    /// Invoice has been voided
    Voided,
}

/// Billing service that generates invoices from usage data
pub struct BillingService {
    /// Pricing per tier
    pricing: RwLock<HashMap<String, TierPricing>>,
    /// Generated invoices
    invoices: RwLock<HashMap<String, Invoice>>,
    /// Invoice counter
    counter: RwLock<u64>,
}

impl BillingService {
    /// Create a new billing service with default tier pricing
    pub fn new() -> Self {
        let mut pricing = HashMap::new();
        pricing.insert("free".to_string(), TierPricing::free());
        pricing.insert("standard".to_string(), TierPricing::standard());
        pricing.insert("premium".to_string(), TierPricing::premium());

        Self {
            pricing: RwLock::new(pricing),
            invoices: RwLock::new(HashMap::new()),
            counter: RwLock::new(0),
        }
    }

    /// Set pricing for a tier
    pub fn set_tier_pricing(&self, pricing: TierPricing) {
        self.pricing.write().insert(pricing.tier.clone(), pricing);
    }

    /// Generate an invoice from usage summary
    pub fn generate_invoice(
        &self,
        tenant_id: &str,
        tier: &str,
        usage: &HashMap<MetricType, f64>,
        period_start: SystemTime,
        period_end: SystemTime,
    ) -> Result<Invoice, CloudError> {
        let pricing = self.pricing.read();
        let tier_pricing = pricing
            .get(tier)
            .ok_or_else(|| CloudError::InvalidConfig(format!("unknown tier: {}", tier)))?;

        let mut line_items = Vec::new();
        let mut subtotal: u64 = 0;

        for (metric, &quantity) in usage {
            let (unit_price, unit_label) = match metric {
                MetricType::OpsPerSecond => (tier_pricing.ops_per_second_hour_cents, "ops/s-hours"),
                MetricType::MemoryBytes => (tier_pricing.memory_gb_hour_cents, "GB-hours"),
                MetricType::StorageBytes => (tier_pricing.storage_gb_month_cents, "GB-months"),
                MetricType::NetworkBytes => (tier_pricing.network_gb_cents, "GB egress"),
                MetricType::Connections => (tier_pricing.connection_hour_cents, "conn-hours"),
            };

            let amount = (quantity * unit_price as f64) as u64;
            if amount > 0 {
                line_items.push(InvoiceLineItem {
                    description: format!("{:?} usage", metric),
                    metric: metric.clone(),
                    quantity,
                    unit: unit_label.to_string(),
                    amount_cents: amount,
                });
                subtotal += amount;
            }
        }

        let invoice_id = {
            let mut counter = self.counter.write();
            *counter += 1;
            format!("inv-{:08x}", *counter)
        };

        let invoice = Invoice {
            id: invoice_id.clone(),
            tenant_id: tenant_id.to_string(),
            period_start,
            period_end,
            line_items,
            subtotal_cents: subtotal,
            discount_cents: 0,
            total_cents: subtotal,
            status: InvoiceStatus::Draft,
            generated_at: SystemTime::now(),
        };

        self.invoices.write().insert(invoice_id, invoice.clone());

        Ok(invoice)
    }

    /// Get an invoice by ID
    pub fn get_invoice(&self, invoice_id: &str) -> Option<Invoice> {
        self.invoices.read().get(invoice_id).cloned()
    }

    /// List invoices for a tenant
    pub fn list_invoices(&self, tenant_id: &str) -> Vec<Invoice> {
        self.invoices
            .read()
            .values()
            .filter(|inv| inv.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Mark an invoice as paid
    pub fn mark_paid(&self, invoice_id: &str) -> Result<(), CloudError> {
        let mut invoices = self.invoices.write();
        let invoice = invoices
            .get_mut(invoice_id)
            .ok_or_else(|| CloudError::InstanceNotFound(invoice_id.to_string()))?;
        invoice.status = InvoiceStatus::Paid;
        Ok(())
    }
}

impl Default for BillingService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_pricing() {
        let standard = TierPricing::standard();
        assert_eq!(standard.tier, "standard");
        assert!(standard.ops_per_second_hour_cents > 0);

        let free = TierPricing::free();
        assert_eq!(free.ops_per_second_hour_cents, 0);
    }

    #[test]
    fn test_generate_invoice() {
        let svc = BillingService::new();
        let mut usage = HashMap::new();
        usage.insert(MetricType::OpsPerSecond, 100.0);
        usage.insert(MetricType::StorageBytes, 50.0);

        let invoice = svc
            .generate_invoice(
                "t1",
                "standard",
                &usage,
                SystemTime::now(),
                SystemTime::now(),
            )
            .unwrap();

        assert_eq!(invoice.tenant_id, "t1");
        assert_eq!(invoice.status, InvoiceStatus::Draft);
        assert!(invoice.total_cents > 0);
        assert!(!invoice.line_items.is_empty());
    }

    #[test]
    fn test_mark_paid() {
        let svc = BillingService::new();
        let mut usage = HashMap::new();
        usage.insert(MetricType::OpsPerSecond, 10.0);

        let invoice = svc
            .generate_invoice(
                "t1",
                "standard",
                &usage,
                SystemTime::now(),
                SystemTime::now(),
            )
            .unwrap();
        svc.mark_paid(&invoice.id).unwrap();

        let fetched = svc.get_invoice(&invoice.id).unwrap();
        assert_eq!(fetched.status, InvoiceStatus::Paid);
    }

    #[test]
    fn test_list_invoices() {
        let svc = BillingService::new();
        let mut usage = HashMap::new();
        usage.insert(MetricType::OpsPerSecond, 10.0);

        svc.generate_invoice(
            "t1",
            "standard",
            &usage,
            SystemTime::now(),
            SystemTime::now(),
        )
        .unwrap();
        svc.generate_invoice(
            "t1",
            "standard",
            &usage,
            SystemTime::now(),
            SystemTime::now(),
        )
        .unwrap();
        svc.generate_invoice(
            "t2",
            "standard",
            &usage,
            SystemTime::now(),
            SystemTime::now(),
        )
        .unwrap();

        assert_eq!(svc.list_invoices("t1").len(), 2);
        assert_eq!(svc.list_invoices("t2").len(), 1);
    }

    #[test]
    fn test_unknown_tier() {
        let svc = BillingService::new();
        let usage = HashMap::new();
        assert!(svc
            .generate_invoice(
                "t1",
                "nonexistent",
                &usage,
                SystemTime::now(),
                SystemTime::now()
            )
            .is_err());
    }
}
