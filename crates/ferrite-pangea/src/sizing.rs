//! Sizing calculator — estimate $/GiB savings with CXL vs DRAM-only.

/// Cloud instance pricing (approximate, for sizing guidance).
#[derive(Debug, Clone)]
pub struct InstancePricing {
    pub instance_type: String,
    pub dram_gib: f64,
    pub cxl_gib: f64,
    pub hourly_cost_usd: f64,
    pub dram_cost_per_gib: f64,
    pub cxl_cost_per_gib: f64,
}

/// Sizing recommendation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SizingRecommendation {
    pub working_set_gib: f64,
    pub dram_only_cost_monthly_usd: f64,
    pub cxl_hybrid_cost_monthly_usd: f64,
    pub savings_pct: f64,
    pub recommended_dram_gib: f64,
    pub recommended_cxl_gib: f64,
    pub estimated_p99_impact_pct: f64,
}

/// Calculate sizing recommendation.
pub fn recommend(
    working_set_gib: f64,
    dram_price_per_gib_month: f64,
    cxl_price_per_gib_month: f64,
    hot_ratio: f64,
) -> SizingRecommendation {
    let hot_ratio = hot_ratio.clamp(0.0, 1.0);
    let hot_gib = working_set_gib * hot_ratio;
    let cold_gib = working_set_gib * (1.0 - hot_ratio);

    let dram_only = working_set_gib * dram_price_per_gib_month;
    let hybrid = hot_gib * dram_price_per_gib_month + cold_gib * cxl_price_per_gib_month;

    SizingRecommendation {
        working_set_gib,
        dram_only_cost_monthly_usd: dram_only,
        cxl_hybrid_cost_monthly_usd: hybrid,
        savings_pct: if dram_only > 0.0 {
            ((dram_only - hybrid) / dram_only * 100.0).max(0.0)
        } else {
            0.0
        },
        recommended_dram_gib: hot_gib,
        recommended_cxl_gib: cold_gib,
        estimated_p99_impact_pct: (1.0 - hot_ratio) * 5.0,
    }
}

/// Reference pricing for common cloud instances.
pub fn reference_pricing() -> Vec<InstancePricing> {
    vec![
        InstancePricing {
            instance_type: "r7i.metal-24xl".into(),
            dram_gib: 768.0,
            cxl_gib: 0.0,
            hourly_cost_usd: 6.048,
            dram_cost_per_gib: 5.68,
            cxl_cost_per_gib: 0.0,
        },
        InstancePricing {
            instance_type: "r7i.metal-48xl".into(),
            dram_gib: 1536.0,
            cxl_gib: 0.0,
            hourly_cost_usd: 12.096,
            dram_cost_per_gib: 5.68,
            cxl_cost_per_gib: 0.0,
        },
        InstancePricing {
            instance_type: "Mv3-series (Azure)".into(),
            dram_gib: 2048.0,
            cxl_gib: 1024.0,
            hourly_cost_usd: 14.50,
            dram_cost_per_gib: 5.10,
            cxl_cost_per_gib: 2.80,
        },
        InstancePricing {
            instance_type: "c4-standard-metal (GCP)".into(),
            dram_gib: 512.0,
            cxl_gib: 0.0,
            hourly_cost_usd: 8.20,
            dram_cost_per_gib: 11.56,
            cxl_cost_per_gib: 0.0,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommend_pure_dram_when_hot_ratio_one() {
        let rec = recommend(100.0, 5.0, 2.5, 1.0);
        assert_eq!(rec.working_set_gib, 100.0);
        assert!((rec.recommended_dram_gib - 100.0).abs() < f64::EPSILON);
        assert!(rec.recommended_cxl_gib.abs() < f64::EPSILON);
        assert!(rec.savings_pct.abs() < f64::EPSILON);
        assert!(rec.estimated_p99_impact_pct.abs() < f64::EPSILON);
    }

    #[test]
    fn recommend_hybrid_saves_money() {
        let rec = recommend(100.0, 5.0, 2.5, 0.5);
        assert!((rec.recommended_dram_gib - 50.0).abs() < f64::EPSILON);
        assert!((rec.recommended_cxl_gib - 50.0).abs() < f64::EPSILON);
        // DRAM-only: 100*5 = 500, hybrid: 50*5 + 50*2.5 = 375 → 25% savings
        assert!((rec.dram_only_cost_monthly_usd - 500.0).abs() < f64::EPSILON);
        assert!((rec.cxl_hybrid_cost_monthly_usd - 375.0).abs() < f64::EPSILON);
        assert!((rec.savings_pct - 25.0).abs() < f64::EPSILON);
        assert!((rec.estimated_p99_impact_pct - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn reference_pricing_non_empty() {
        let prices = reference_pricing();
        assert!(prices.len() >= 3);
        assert!(prices.iter().all(|p| p.dram_gib > 0.0));
    }
}
