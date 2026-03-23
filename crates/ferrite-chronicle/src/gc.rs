//! Branch garbage collector — reclaims storage from unreachable branch versions.

use crate::registry::BranchRegistry;

/// GC configuration.
#[derive(Debug, Clone)]
pub struct GcPolicy {
    /// Maximum total branches across all tenants.
    pub max_total_branches: usize,
    /// Maximum age of a branch in milliseconds (0 = no limit).
    pub max_branch_age_ms: u64,
    /// Whether to protect the main branch from GC.
    pub protect_main: bool,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            max_total_branches: 1000,
            max_branch_age_ms: 0,
            protect_main: true,
        }
    }
}

/// Result of a GC pass.
#[derive(Debug, Default)]
pub struct GcResult {
    pub branches_scanned: usize,
    pub branches_collected: usize,
    pub overlays_freed: usize,
}

/// Run a GC pass against the branch registry.
pub fn collect(registry: &BranchRegistry, policy: &GcPolicy, now_ms: u64) -> GcResult {
    let all = registry.all();
    let mut result = GcResult {
        branches_scanned: all.len(),
        ..Default::default()
    };

    // Collect expired branches (TTL-based)
    for meta in &all {
        if let Some(ttl) = meta.ttl_ms {
            if now_ms > meta.created_at_ms + ttl && registry.drop(&meta.id) {
                result.branches_collected += 1;
            }
        }
    }

    // Collect branches exceeding max age
    if policy.max_branch_age_ms > 0 {
        let current = registry.all();
        for meta in &current {
            if now_ms.saturating_sub(meta.created_at_ms) > policy.max_branch_age_ms
                && registry.drop(&meta.id)
            {
                result.branches_collected += 1;
            }
        }
    }

    // If still over limit, collect oldest branches
    let current = registry.all();
    if current.len() > policy.max_total_branches {
        let mut sorted = current;
        sorted.sort_by_key(|m| m.created_at_ms);
        let excess = sorted.len().saturating_sub(policy.max_total_branches);
        for meta in sorted.iter().take(excess) {
            if registry.drop(&meta.id) {
                result.branches_collected += 1;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_collects_expired_ttl_branches() {
        let reg = BranchRegistry::new();
        let a = reg.create(None, "t", 100).unwrap();
        let b = reg.create(None, "t", 100).unwrap();
        reg.set_ttl(&a.id, Some(50)).unwrap();
        // b has no TTL — should survive.

        let policy = GcPolicy::default();
        let result = collect(&reg, &policy, 200);
        assert_eq!(result.branches_scanned, 2);
        assert_eq!(result.branches_collected, 1);
        assert!(reg.get(&a.id).is_none());
        assert!(reg.get(&b.id).is_some());
    }

    #[test]
    fn gc_collects_branches_exceeding_max_age() {
        let reg = BranchRegistry::new();
        let old = reg.create(None, "t", 10).unwrap();
        let young = reg.create(None, "t", 500).unwrap();

        let policy = GcPolicy {
            max_branch_age_ms: 100,
            ..Default::default()
        };
        let result = collect(&reg, &policy, 600);
        assert_eq!(result.branches_collected, 1);
        assert!(reg.get(&old.id).is_none());
        assert!(reg.get(&young.id).is_some());
    }

    #[test]
    fn gc_enforces_max_total_branches() {
        let reg = BranchRegistry::new();
        for i in 0..5 {
            reg.create(None, "t", i * 10).unwrap();
        }
        assert_eq!(reg.all().len(), 5);

        let policy = GcPolicy {
            max_total_branches: 3,
            ..Default::default()
        };
        let result = collect(&reg, &policy, 100);
        // Should have collected the 2 oldest
        assert_eq!(result.branches_collected, 2);
        assert_eq!(reg.all().len(), 3);
    }
}
