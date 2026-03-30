//! Moonshot feature configuration — runtime toggles and limits.
//!
//! Loaded from ferrite.toml `[moonshots]` section or environment variables.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;

use super::err_frame;

/// Global moonshot configuration.
static CONFIG: OnceLock<MoonshotConfig> = OnceLock::new();

/// Configuration for all moonshot features.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MoonshotConfig {
    pub forge: ForgeConfig,
    pub mnemo: MnemoConfig,
    pub chronicle: ChronicleConfig,
    pub lucidity: LucidityConfig,
    pub concord: ConcordConfig,
    pub pangea: PangeaConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ForgeConfig {
    pub enabled: bool,
    pub require_signing: bool,
    pub max_module_size_bytes: usize,
    pub calls_per_second: f64,
    pub burst_capacity: u64,
    pub default_fuel: u64,
    pub default_wall_time_ms: u64,
    pub max_memory_bytes: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MnemoConfig {
    pub enabled: bool,
    pub max_records_per_tenant: u64,
    pub max_bytes_per_tenant: u64,
    pub sweep_interval_seconds: u64,
    pub default_importance: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChronicleConfig {
    pub enabled: bool,
    pub max_total_branches: usize,
    pub max_branch_age_seconds: u64,
    pub retention_max_entries_per_key: usize,
    pub retention_max_age_seconds: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct LucidityConfig {
    pub enabled: bool,
    pub checkpoint_interval_epochs: u64,
    pub require_pq_signing: bool,
    pub max_proof_size: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ConcordConfig {
    pub enabled: bool,
    pub gossip_interval_ms: u64,
    pub gossip_fan_out: usize,
    pub max_crdt_metadata_bytes: usize,
    pub dvv_compaction_threshold: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct PangeaConfig {
    pub enabled: bool,
    pub promote_threshold: u64,
    pub demote_after_seconds: u64,
    pub dram_pressure_threshold: f64,
}

impl Default for ForgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_signing: false,
            max_module_size_bytes: 16 * 1024 * 1024, // 16 MiB
            calls_per_second: 1000.0,
            burst_capacity: 100,
            default_fuel: 1_000_000,
            default_wall_time_ms: 50,
            max_memory_bytes: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

impl Default for MnemoConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_records_per_tenant: 1_000_000,
            max_bytes_per_tenant: 1024 * 1024 * 1024, // 1 GiB
            sweep_interval_seconds: 300,
            default_importance: 0.5,
        }
    }
}

impl Default for ChronicleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_total_branches: 1000,
            max_branch_age_seconds: 30 * 24 * 3600, // 30 days
            retention_max_entries_per_key: 100,
            retention_max_age_seconds: 7 * 24 * 3600, // 7 days
        }
    }
}

impl Default for LucidityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            checkpoint_interval_epochs: 100,
            require_pq_signing: false,
            max_proof_size: 65536,
        }
    }
}

impl Default for ConcordConfig {
    fn default() -> Self {
        Self {
            enabled: true, // All features enabled by default
            gossip_interval_ms: 1000,
            gossip_fan_out: 3,
            max_crdt_metadata_bytes: 1024 * 1024, // 1 MiB
            dvv_compaction_threshold: 1000,
        }
    }
}

impl Default for PangeaConfig {
    fn default() -> Self {
        Self {
            enabled: true, // All features enabled by default
            promote_threshold: 10,
            demote_after_seconds: 60,
            dram_pressure_threshold: 0.8,
        }
    }
}

/// Initialize global config. Call once at startup.
pub fn init(config: MoonshotConfig) {
    let _ = CONFIG.set(config);
}

/// Get current config (returns default if not initialized).
pub fn get() -> &'static MoonshotConfig {
    CONFIG.get_or_init(MoonshotConfig::default)
}

/// Check if a moonshot family is enabled.
///
/// In test builds, all families are always enabled to avoid OnceLock
/// ordering issues with global configuration.
pub fn is_enabled(family: &str) -> bool {
    #[cfg(test)]
    {
        let _ = family;
        return true;
    }
    #[cfg(not(test))]
    {
        let cfg = get();
        match family.to_uppercase().as_str() {
            "FN" | "FORGE" => cfg.forge.enabled,
            "MEM" | "MNEMO" => cfg.mnemo.enabled,
            "CHR" | "CHRONICLE" => cfg.chronicle.enabled,
            "LUC" | "LUCIDITY" => cfg.lucidity.enabled,
            "CON" | "CONCORD" => cfg.concord.enabled,
            "PNG" | "PANGEA" => cfg.pangea.enabled,
            _ => false,
        }
    }
}

/// MOONSHOT.CONFIG command handler.
pub fn moonshot_config_command(args: &[String]) -> Frame {
    match args.first().map(|s| s.to_uppercase()).as_deref() {
        Some("SHOW") | None => show_config(),
        Some("GET") => {
            if let Some(key) = args.get(1) {
                get_config_key(key)
            } else {
                err_frame("usage: MOONSHOT.CONFIG GET <key>")
            }
        }
        Some("HELP") => help(),
        _ => err_frame("usage: MOONSHOT.CONFIG [SHOW|GET key|HELP]"),
    }
}

fn show_config() -> Frame {
    let cfg = get();
    let pairs: Vec<Frame> = vec![
        // Forge
        kv("forge.enabled", &cfg.forge.enabled.to_string()),
        kv(
            "forge.require_signing",
            &cfg.forge.require_signing.to_string(),
        ),
        kv(
            "forge.max_module_size_bytes",
            &cfg.forge.max_module_size_bytes.to_string(),
        ),
        kv(
            "forge.calls_per_second",
            &cfg.forge.calls_per_second.to_string(),
        ),
        kv(
            "forge.burst_capacity",
            &cfg.forge.burst_capacity.to_string(),
        ),
        kv("forge.default_fuel", &cfg.forge.default_fuel.to_string()),
        kv(
            "forge.default_wall_time_ms",
            &cfg.forge.default_wall_time_ms.to_string(),
        ),
        kv(
            "forge.max_memory_bytes",
            &cfg.forge.max_memory_bytes.to_string(),
        ),
        // Mnemo
        kv("mnemo.enabled", &cfg.mnemo.enabled.to_string()),
        kv(
            "mnemo.max_records_per_tenant",
            &cfg.mnemo.max_records_per_tenant.to_string(),
        ),
        kv(
            "mnemo.max_bytes_per_tenant",
            &cfg.mnemo.max_bytes_per_tenant.to_string(),
        ),
        kv(
            "mnemo.sweep_interval_seconds",
            &cfg.mnemo.sweep_interval_seconds.to_string(),
        ),
        kv(
            "mnemo.default_importance",
            &cfg.mnemo.default_importance.to_string(),
        ),
        // Chronicle
        kv("chronicle.enabled", &cfg.chronicle.enabled.to_string()),
        kv(
            "chronicle.max_total_branches",
            &cfg.chronicle.max_total_branches.to_string(),
        ),
        kv(
            "chronicle.max_branch_age_seconds",
            &cfg.chronicle.max_branch_age_seconds.to_string(),
        ),
        kv(
            "chronicle.retention_max_entries_per_key",
            &cfg.chronicle.retention_max_entries_per_key.to_string(),
        ),
        kv(
            "chronicle.retention_max_age_seconds",
            &cfg.chronicle.retention_max_age_seconds.to_string(),
        ),
        // Lucidity
        kv("lucidity.enabled", &cfg.lucidity.enabled.to_string()),
        kv(
            "lucidity.checkpoint_interval_epochs",
            &cfg.lucidity.checkpoint_interval_epochs.to_string(),
        ),
        kv(
            "lucidity.require_pq_signing",
            &cfg.lucidity.require_pq_signing.to_string(),
        ),
        kv(
            "lucidity.max_proof_size",
            &cfg.lucidity.max_proof_size.to_string(),
        ),
        // Concord
        kv("concord.enabled", &cfg.concord.enabled.to_string()),
        kv(
            "concord.gossip_interval_ms",
            &cfg.concord.gossip_interval_ms.to_string(),
        ),
        kv(
            "concord.gossip_fan_out",
            &cfg.concord.gossip_fan_out.to_string(),
        ),
        kv(
            "concord.max_crdt_metadata_bytes",
            &cfg.concord.max_crdt_metadata_bytes.to_string(),
        ),
        kv(
            "concord.dvv_compaction_threshold",
            &cfg.concord.dvv_compaction_threshold.to_string(),
        ),
        // Pangea
        kv("pangea.enabled", &cfg.pangea.enabled.to_string()),
        kv(
            "pangea.promote_threshold",
            &cfg.pangea.promote_threshold.to_string(),
        ),
        kv(
            "pangea.demote_after_seconds",
            &cfg.pangea.demote_after_seconds.to_string(),
        ),
        kv(
            "pangea.dram_pressure_threshold",
            &cfg.pangea.dram_pressure_threshold.to_string(),
        ),
    ];
    // Flatten key-value pairs into a single array
    let flat: Vec<Frame> = pairs
        .into_iter()
        .flat_map(|f| {
            if let Frame::Array(Some(items)) = f {
                items
            } else {
                vec![f]
            }
        })
        .collect();
    Frame::Array(Some(flat))
}

fn get_config_key(key: &str) -> Frame {
    let cfg = get();
    let val = match key.to_lowercase().as_str() {
        // Forge
        "forge.enabled" => cfg.forge.enabled.to_string(),
        "forge.require_signing" => cfg.forge.require_signing.to_string(),
        "forge.max_module_size_bytes" => cfg.forge.max_module_size_bytes.to_string(),
        "forge.calls_per_second" => cfg.forge.calls_per_second.to_string(),
        "forge.burst_capacity" => cfg.forge.burst_capacity.to_string(),
        "forge.default_fuel" => cfg.forge.default_fuel.to_string(),
        "forge.default_wall_time_ms" => cfg.forge.default_wall_time_ms.to_string(),
        "forge.max_memory_bytes" => cfg.forge.max_memory_bytes.to_string(),
        // Mnemo
        "mnemo.enabled" => cfg.mnemo.enabled.to_string(),
        "mnemo.max_records_per_tenant" => cfg.mnemo.max_records_per_tenant.to_string(),
        "mnemo.max_bytes_per_tenant" => cfg.mnemo.max_bytes_per_tenant.to_string(),
        "mnemo.sweep_interval_seconds" => cfg.mnemo.sweep_interval_seconds.to_string(),
        "mnemo.default_importance" => cfg.mnemo.default_importance.to_string(),
        // Chronicle
        "chronicle.enabled" => cfg.chronicle.enabled.to_string(),
        "chronicle.max_total_branches" => cfg.chronicle.max_total_branches.to_string(),
        "chronicle.max_branch_age_seconds" => cfg.chronicle.max_branch_age_seconds.to_string(),
        "chronicle.retention_max_entries_per_key" => {
            cfg.chronicle.retention_max_entries_per_key.to_string()
        }
        "chronicle.retention_max_age_seconds" => {
            cfg.chronicle.retention_max_age_seconds.to_string()
        }
        // Lucidity
        "lucidity.enabled" => cfg.lucidity.enabled.to_string(),
        "lucidity.checkpoint_interval_epochs" => {
            cfg.lucidity.checkpoint_interval_epochs.to_string()
        }
        "lucidity.require_pq_signing" => cfg.lucidity.require_pq_signing.to_string(),
        "lucidity.max_proof_size" => cfg.lucidity.max_proof_size.to_string(),
        // Concord
        "concord.enabled" => cfg.concord.enabled.to_string(),
        "concord.gossip_interval_ms" => cfg.concord.gossip_interval_ms.to_string(),
        "concord.gossip_fan_out" => cfg.concord.gossip_fan_out.to_string(),
        "concord.max_crdt_metadata_bytes" => cfg.concord.max_crdt_metadata_bytes.to_string(),
        "concord.dvv_compaction_threshold" => cfg.concord.dvv_compaction_threshold.to_string(),
        // Pangea
        "pangea.enabled" => cfg.pangea.enabled.to_string(),
        "pangea.promote_threshold" => cfg.pangea.promote_threshold.to_string(),
        "pangea.demote_after_seconds" => cfg.pangea.demote_after_seconds.to_string(),
        "pangea.dram_pressure_threshold" => cfg.pangea.dram_pressure_threshold.to_string(),
        _ => return err_frame(&format!("unknown config key: {key}")),
    };
    Frame::Bulk(Some(Bytes::from(val)))
}

fn help() -> Frame {
    let lines = vec![
        "MOONSHOT.CONFIG [SHOW|GET key|HELP]",
        "  SHOW  — Display all moonshot configuration key-value pairs.",
        "  GET   — Get a single config value, e.g. MOONSHOT.CONFIG GET forge.enabled",
        "  HELP  — Show this help message.",
        "",
        "Configuration is loaded from the [moonshots] section of ferrite.toml.",
    ];
    Frame::Array(Some(
        lines
            .into_iter()
            .map(|l| Frame::Bulk(Some(Bytes::from(l.to_owned()))))
            .collect(),
    ))
}

fn kv(key: &str, val: &str) -> Frame {
    Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from(key.to_owned()))),
        Frame::Bulk(Some(Bytes::from(val.to_owned()))),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_feature_flags() {
        let cfg = MoonshotConfig::default();
        assert!(cfg.forge.enabled);
        assert!(cfg.mnemo.enabled);
        assert!(cfg.chronicle.enabled);
        assert!(cfg.lucidity.enabled);
        assert!(cfg.concord.enabled);
        assert!(cfg.pangea.enabled);
    }

    #[test]
    fn is_enabled_returns_correct_values() {
        // We can't call init() here because CONFIG is global and may
        // already be set by another test or by get(). Instead, verify
        // the default config behaviour via the struct directly.
        let cfg = MoonshotConfig::default();

        // Simulate is_enabled logic for each alias
        let check = |family: &str, cfg: &MoonshotConfig| -> bool {
            match family.to_uppercase().as_str() {
                "FN" | "FORGE" => cfg.forge.enabled,
                "MEM" | "MNEMO" => cfg.mnemo.enabled,
                "CHR" | "CHRONICLE" => cfg.chronicle.enabled,
                "LUC" | "LUCIDITY" => cfg.lucidity.enabled,
                "CON" | "CONCORD" => cfg.concord.enabled,
                "PNG" | "PANGEA" => cfg.pangea.enabled,
                _ => false,
            }
        };

        assert!(check("FN", &cfg));
        assert!(check("FORGE", &cfg));
        assert!(check("MEM", &cfg));
        assert!(check("MNEMO", &cfg));
        assert!(check("CHR", &cfg));
        assert!(check("CHRONICLE", &cfg));
        assert!(check("LUC", &cfg));
        assert!(check("LUCIDITY", &cfg));
        assert!(check("CON", &cfg));
        assert!(check("CONCORD", &cfg));
        assert!(check("PNG", &cfg));
        assert!(check("PANGEA", &cfg));
        assert!(!check("UNKNOWN", &cfg));
    }

    #[test]
    fn config_serde_roundtrip() {
        let cfg = MoonshotConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let deserialized: MoonshotConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(cfg.forge.enabled, deserialized.forge.enabled);
        assert_eq!(
            cfg.forge.max_module_size_bytes,
            deserialized.forge.max_module_size_bytes
        );
        assert_eq!(
            cfg.mnemo.max_records_per_tenant,
            deserialized.mnemo.max_records_per_tenant
        );
        assert_eq!(
            cfg.chronicle.max_total_branches,
            deserialized.chronicle.max_total_branches
        );
        assert_eq!(
            cfg.lucidity.max_proof_size,
            deserialized.lucidity.max_proof_size
        );
        assert_eq!(
            cfg.concord.gossip_fan_out,
            deserialized.concord.gossip_fan_out
        );
        assert_eq!(
            cfg.pangea.promote_threshold,
            deserialized.pangea.promote_threshold
        );
    }

    #[test]
    fn moonshot_config_command_help() {
        let result = moonshot_config_command(&["HELP".to_string()]);
        match result {
            Frame::Array(Some(items)) => assert!(!items.is_empty()),
            _ => panic!("expected array frame from HELP"),
        }
    }

    #[test]
    fn moonshot_config_command_get_known_key() {
        let result = moonshot_config_command(&["GET".to_string(), "forge.enabled".to_string()]);
        match result {
            Frame::Bulk(Some(val)) => {
                let s = String::from_utf8_lossy(&val);
                assert!(s == "true" || s == "false");
            }
            _ => panic!("expected bulk frame from GET"),
        }
    }

    #[test]
    fn moonshot_config_command_get_unknown_key() {
        let result = moonshot_config_command(&["GET".to_string(), "nonexistent.key".to_string()]);
        match result {
            Frame::Error(_) => {} // expected
            _ => panic!("expected error frame for unknown key"),
        }
    }

    #[test]
    fn moonshot_config_command_show() {
        let result = moonshot_config_command(&[]);
        match result {
            Frame::Array(Some(items)) => {
                // Should contain key-value pairs (even number of elements)
                assert!(items.len() > 10);
                assert_eq!(items.len() % 2, 0);
            }
            _ => panic!("expected array frame from SHOW"),
        }
    }

    #[test]
    fn default_forge_values() {
        let cfg = ForgeConfig::default();
        assert_eq!(cfg.max_module_size_bytes, 16 * 1024 * 1024);
        assert!((cfg.calls_per_second - 1000.0).abs() < f64::EPSILON);
        assert_eq!(cfg.default_fuel, 1_000_000);
        assert_eq!(cfg.max_memory_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn default_concord_values() {
        let cfg = ConcordConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.gossip_interval_ms, 1000);
        assert_eq!(cfg.gossip_fan_out, 3);
    }

    #[test]
    fn default_pangea_values() {
        let cfg = PangeaConfig::default();
        assert!(cfg.enabled);
        assert!((cfg.dram_pressure_threshold - 0.8).abs() < f64::EPSILON);
    }
}
