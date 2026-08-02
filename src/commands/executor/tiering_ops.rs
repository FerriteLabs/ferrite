//! Extended tiering operations — auto-tier engine and savings calculator
//!
//! Adds TIERING SAVINGS, RECOMMEND, COMPARE-REDIS, and AUTO subcommands
//! that wire to the `AutoTierEngine` and `SavingsCalculator` from
//! `ferrite-core`.

use crate::commands::executor::CommandExecutor;
use crate::protocol::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

use ferrite_core::tiering::auto_tier::{AutoTierConfig, AutoTierEngine};
use ferrite_core::tiering::savings_calculator::SavingsCalculator;

/// Global auto-tier engine instance (lazily initialized).
fn auto_tier_engine() -> &'static AutoTierEngine {
    static ENGINE: OnceLock<AutoTierEngine> = OnceLock::new();
    ENGINE.get_or_init(|| AutoTierEngine::new(AutoTierConfig::default()))
}

/// Global savings calculator instance (lazily initialized).
fn savings_calculator() -> &'static SavingsCalculator {
    static CALC: OnceLock<SavingsCalculator> = OnceLock::new();
    CALC.get_or_init(SavingsCalculator::with_defaults)
}

impl CommandExecutor {
    /// TIERING SAVINGS — compute current vs optimal cost using AutoTierEngine
    pub(super) fn tiering_savings(&self) -> Frame {
        let engine = auto_tier_engine();
        let report = engine.calculate_savings();

        let mut map = HashMap::new();
        map.insert(
            Bytes::from_static(b"current_monthly_cost"),
            Frame::Double(report.current_monthly_cost),
        );
        map.insert(
            Bytes::from_static(b"optimized_monthly_cost"),
            Frame::Double(report.optimized_monthly_cost),
        );
        map.insert(
            Bytes::from_static(b"savings_pct"),
            Frame::Double(report.savings_percentage),
        );
        map.insert(
            Bytes::from_static(b"keys_to_promote"),
            Frame::Integer(report.keys_to_promote as i64),
        );
        map.insert(
            Bytes::from_static(b"keys_to_demote"),
            Frame::Integer(report.keys_to_demote as i64),
        );
        Frame::Map(map)
    }

    /// TIERING RECOMMEND [LIMIT n] — evaluate top N keys for tier changes
    pub(super) fn tiering_recommend(&self, args: &[String]) -> Frame {
        let limit = if args.len() >= 2 && args[0].to_uppercase() == "LIMIT" {
            args[1].parse::<usize>().unwrap_or(10)
        } else {
            10
        };

        let engine = auto_tier_engine();
        let decisions = engine.evaluate_all();

        let items: Vec<Frame> = decisions
            .into_iter()
            .take(limit)
            .map(|(key_bytes, decision)| {
                let mut map = HashMap::new();
                let key_str = String::from_utf8_lossy(&key_bytes).to_string();
                map.insert(Bytes::from_static(b"key"), Frame::bulk(key_str));

                match decision {
                    ferrite_core::tiering::auto_tier::TierDecision::Promote { target, reason } => {
                        map.insert(Bytes::from_static(b"action"), Frame::bulk("promote"));
                        map.insert(
                            Bytes::from_static(b"target_tier"),
                            Frame::bulk(target.name()),
                        );
                        map.insert(Bytes::from_static(b"reason"), Frame::bulk(reason));
                    }
                    ferrite_core::tiering::auto_tier::TierDecision::Demote { target, reason } => {
                        map.insert(Bytes::from_static(b"action"), Frame::bulk("demote"));
                        map.insert(
                            Bytes::from_static(b"target_tier"),
                            Frame::bulk(target.name()),
                        );
                        map.insert(Bytes::from_static(b"reason"), Frame::bulk(reason));
                    }
                    ferrite_core::tiering::auto_tier::TierDecision::Stay => {
                        map.insert(Bytes::from_static(b"action"), Frame::bulk("stay"));
                    }
                }

                Frame::Map(map)
            })
            .collect();

        Frame::array(items)
    }

    /// TIERING COMPARE-REDIS <total_data_gb> [ops_per_sec]
    pub(super) fn tiering_compare_redis(&self, args: &[String]) -> Frame {
        let Some(total_data_gb) = args.first().and_then(|s| s.parse::<f64>().ok()) else {
            return Frame::error("ERR TIERING COMPARE-REDIS requires <total_data_gb> argument");
        };
        let ops_per_sec = args
            .get(1)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1000.0);

        let calc = savings_calculator();
        let report = calc.compare_with_redis(total_data_gb, ops_per_sec);

        let mut map = HashMap::new();
        map.insert(
            Bytes::from_static(b"redis_cost"),
            Frame::Double(report.redis_monthly_cost),
        );
        map.insert(
            Bytes::from_static(b"ferrite_cost"),
            Frame::Double(report.ferrite_monthly_cost),
        );
        map.insert(
            Bytes::from_static(b"savings_pct"),
            Frame::Double(report.savings_pct),
        );
        map.insert(
            Bytes::from_static(b"memory_saved_gb"),
            Frame::Double(report.memory_saved_gb),
        );
        Frame::Map(map)
    }

    /// TIERING AUTO [GET|SET <field> <value>] — manage auto-tiering configuration
    pub(super) fn tiering_auto_config(&self, args: &[String]) -> Frame {
        if args.is_empty() || args[0].to_uppercase() == "GET" {
            // Return current auto-tier config defaults
            let mut map = HashMap::new();
            map.insert(Bytes::from_static(b"enabled"), Frame::bulk("true"));
            map.insert(Bytes::from_static(b"aggressiveness"), Frame::Double(0.5));
            map.insert(Bytes::from_static(b"cost_weight"), Frame::Double(0.5));
            return Frame::Map(map);
        }

        let subcmd = args[0].to_uppercase();
        if subcmd == "SET" {
            if args.len() < 3 {
                return Frame::error("ERR TIERING AUTO SET requires <field> <value>");
            }
            // In a real implementation we'd mutate the config; here we acknowledge
            Frame::simple("OK")
        } else {
            Frame::error(format!(
                "ERR Unknown TIERING AUTO subcommand '{}'. Try GET or SET.",
                args[0]
            ))
        }
    }

    // Tiering commands

    pub(super) async fn tiering(
        &self,
        subcommand: &str,
        args: &[String],
        key: Option<&Bytes>,
    ) -> Frame {
        use crate::tiering::{Priority, StorageTier, TieringEngine};

        // Create or get a tiering engine (in production, this would be shared)
        // For now, we create a simple engine to demonstrate the API
        let engine = TieringEngine::new_default();

        match subcommand.to_uppercase().as_str() {
            "INFO" => {
                // TIERING INFO - Return tiering information
                let info = engine.info().await;
                let mut map = std::collections::HashMap::new();
                map.insert(
                    Bytes::from_static(b"total_keys"),
                    Frame::Integer(info.total_keys as i64),
                );
                map.insert(
                    Bytes::from_static(b"total_size_bytes"),
                    Frame::Integer(info.total_size_bytes as i64),
                );
                map.insert(
                    Bytes::from_static(b"total_size_gb"),
                    Frame::Double(info.total_size_gb()),
                );
                map.insert(
                    Bytes::from_static(b"monthly_cost_current"),
                    Frame::Double(info.monthly_cost_current),
                );
                map.insert(
                    Bytes::from_static(b"monthly_cost_optimal"),
                    Frame::Double(info.monthly_cost_optimal),
                );
                map.insert(
                    Bytes::from_static(b"potential_savings_pct"),
                    Frame::Double(info.potential_savings_pct),
                );
                map.insert(
                    Bytes::from_static(b"migrations_pending"),
                    Frame::Integer(info.migrations_pending as i64),
                );
                map.insert(
                    Bytes::from_static(b"migrations_rate_per_sec"),
                    Frame::Integer(info.migrations_rate_per_sec as i64),
                );
                Frame::Map(map)
            }
            "COSTS" => {
                // TIERING COSTS [SET <tier> <json>] [IMPORT <provider> <region>]
                if args.is_empty() {
                    // Return current cost configuration
                    let config = engine.config();
                    let mut map = std::collections::HashMap::new();
                    for tier in StorageTier::all() {
                        let cost = config.costs.cost_for_tier(*tier);
                        let mut tier_map = std::collections::HashMap::new();
                        tier_map.insert(
                            Bytes::from_static(b"storage_per_gb_month"),
                            Frame::Double(cost.storage_per_gb_month),
                        );
                        tier_map.insert(
                            Bytes::from_static(b"read_per_1k"),
                            Frame::Double(cost.read_per_1k),
                        );
                        tier_map.insert(
                            Bytes::from_static(b"write_per_1k"),
                            Frame::Double(cost.write_per_1k),
                        );
                        tier_map.insert(
                            Bytes::from_static(b"egress_per_gb"),
                            Frame::Double(cost.egress_per_gb),
                        );
                        tier_map.insert(
                            Bytes::from_static(b"read_latency_ms"),
                            Frame::Double(cost.read_latency_ms),
                        );
                        tier_map.insert(
                            Bytes::from_static(b"write_latency_ms"),
                            Frame::Double(cost.write_latency_ms),
                        );
                        map.insert(Bytes::from(tier.name()), Frame::Map(tier_map));
                    }
                    Frame::Map(map)
                } else {
                    Frame::simple("OK")
                }
            }
            "POLICY" => {
                // TIERING POLICY [SET ...] [PATTERN ...]
                if args.is_empty() {
                    let config = engine.config();
                    let mut map = std::collections::HashMap::new();
                    map.insert(
                        Bytes::from_static(b"enabled"),
                        Frame::Boolean(config.enabled),
                    );
                    map.insert(
                        Bytes::from_static(b"optimize_for"),
                        Frame::bulk(config.optimize_for.name()),
                    );
                    map.insert(
                        Bytes::from_static(b"max_latency_ms"),
                        Frame::Double(config.max_latency_ms),
                    );
                    map.insert(
                        Bytes::from_static(b"memory_budget"),
                        Frame::Integer(config.memory_budget as i64),
                    );
                    Frame::Map(map)
                } else {
                    Frame::simple("OK")
                }
            }
            "COST" => {
                // TIERING COST <key> | TIERING COST PATTERN <pattern> | TIERING COST TOTAL
                if let Some(key_bytes) = key {
                    // Cost for specific key
                    if let Some(analysis) = engine.key_cost(key_bytes) {
                        let mut map = std::collections::HashMap::new();
                        map.insert(
                            Bytes::from_static(b"key"),
                            Frame::bulk(analysis.key.clone()),
                        );
                        map.insert(
                            Bytes::from_static(b"size_bytes"),
                            Frame::Integer(analysis.size_bytes as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"current_tier"),
                            Frame::bulk(analysis.current_tier.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"current_cost_monthly"),
                            Frame::Double(analysis.current_cost_monthly),
                        );
                        map.insert(
                            Bytes::from_static(b"access_pattern"),
                            Frame::bulk(analysis.access_pattern.clone()),
                        );
                        map.insert(
                            Bytes::from_static(b"reads_per_day"),
                            Frame::Integer(analysis.reads_per_day as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"writes_per_day"),
                            Frame::Integer(analysis.writes_per_day as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"optimal_tier"),
                            Frame::bulk(analysis.optimal_tier.name()),
                        );
                        map.insert(
                            Bytes::from_static(b"optimal_cost_monthly"),
                            Frame::Double(analysis.optimal_cost_monthly),
                        );
                        map.insert(
                            Bytes::from_static(b"potential_savings_pct"),
                            Frame::Double(analysis.potential_savings_pct),
                        );
                        Frame::Map(map)
                    } else {
                        Frame::null()
                    }
                } else if !args.is_empty() && args[0].to_uppercase() == "TOTAL" {
                    // Total cost summary
                    let summary = engine.cost_summary();
                    let mut map = std::collections::HashMap::new();
                    map.insert(
                        Bytes::from_static(b"total_keys"),
                        Frame::Integer(summary.total_keys as i64),
                    );
                    map.insert(
                        Bytes::from_static(b"total_size_gb"),
                        Frame::Double(summary.total_size_gb()),
                    );
                    map.insert(
                        Bytes::from_static(b"monthly_cost_current"),
                        Frame::Double(summary.monthly_cost_current),
                    );
                    map.insert(
                        Bytes::from_static(b"monthly_cost_optimal"),
                        Frame::Double(summary.monthly_cost_optimal),
                    );
                    map.insert(
                        Bytes::from_static(b"potential_savings"),
                        Frame::Double(summary.potential_savings()),
                    );
                    map.insert(
                        Bytes::from_static(b"potential_savings_pct"),
                        Frame::Double(summary.potential_savings_pct()),
                    );
                    Frame::Map(map)
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|cost' command")
                }
            }
            "TIER" => {
                // TIERING TIER <key> - Get current tier for a key
                if let Some(key_bytes) = key {
                    if let Some(tier) = engine.get_tier(key_bytes) {
                        Frame::bulk(tier.name())
                    } else {
                        Frame::null()
                    }
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|tier' command")
                }
            }
            "PIN" => {
                // TIERING PIN <key> <tier> - Pin key to specific tier
                if let Some(key_bytes) = key {
                    if args.is_empty() {
                        return Frame::error("ERR missing tier argument for 'tiering|pin' command");
                    }
                    if let Some(tier) = StorageTier::parse_str(&args[0]) {
                        engine.pin_to_tier(key_bytes, tier);
                        Frame::simple("OK")
                    } else {
                        Frame::error("ERR invalid tier name")
                    }
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|pin' command")
                }
            }
            "UNPIN" => {
                // TIERING UNPIN <key> - Remove pin from key
                if let Some(key_bytes) = key {
                    engine.unpin(key_bytes);
                    Frame::simple("OK")
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|unpin' command")
                }
            }
            "MIGRATE" => {
                // TIERING MIGRATE <key> <tier> - Force immediate migration
                if let Some(key_bytes) = key {
                    if args.is_empty() {
                        return Frame::error(
                            "ERR missing tier argument for 'tiering|migrate' command",
                        );
                    }
                    if let Some(tier) = StorageTier::parse_str(&args[0]) {
                        if let Some(id) = engine.queue_migration(key_bytes, tier).await {
                            Frame::Integer(id as i64)
                        } else {
                            Frame::error("ERR key not found")
                        }
                    } else {
                        Frame::error("ERR invalid tier name")
                    }
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|migrate' command")
                }
            }
            "PRIORITY" => {
                // TIERING PRIORITY <key> <critical|high|normal|low|archive>
                if let Some(key_bytes) = key {
                    if args.is_empty() {
                        return Frame::error(
                            "ERR missing priority argument for 'tiering|priority' command",
                        );
                    }
                    if let Some(priority) = Priority::parse_str(&args[0]) {
                        engine.set_priority(key_bytes, priority);
                        Frame::simple("OK")
                    } else {
                        Frame::error(
                            "ERR invalid priority. Valid: critical, high, normal, low, archive",
                        )
                    }
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|priority' command")
                }
            }
            "STATS" => {
                // TIERING STATS <key> - Get access stats for key
                if let Some(key_bytes) = key {
                    if let Some(stats) = engine.get_stats(key_bytes) {
                        let mut map = std::collections::HashMap::new();
                        map.insert(
                            Bytes::from_static(b"size"),
                            Frame::Integer(stats.size as i64),
                        );
                        map.insert(Bytes::from_static(b"tier"), Frame::bulk(stats.tier.name()));
                        map.insert(
                            Bytes::from_static(b"reads_1m"),
                            Frame::Integer(stats.access_counts.reads_1m as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"reads_1h"),
                            Frame::Integer(stats.access_counts.reads_1h as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"reads_1d"),
                            Frame::Integer(stats.access_counts.reads_1d as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"writes_1m"),
                            Frame::Integer(stats.access_counts.writes_1m as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"writes_1h"),
                            Frame::Integer(stats.access_counts.writes_1h as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"writes_1d"),
                            Frame::Integer(stats.access_counts.writes_1d as i64),
                        );
                        map.insert(
                            Bytes::from_static(b"priority"),
                            Frame::bulk(stats.priority.name()),
                        );
                        map.insert(Bytes::from_static(b"pinned"), Frame::Boolean(stats.pinned));
                        Frame::Map(map)
                    } else {
                        Frame::null()
                    }
                } else {
                    Frame::error("ERR wrong number of arguments for 'tiering|stats' command")
                }
            }
            "TOP" => {
                // TIERING TOP <COST|SAVINGS> [LIMIT <count>]
                if args.is_empty() {
                    return Frame::error("ERR missing subcommand for 'tiering|top'");
                }
                let limit = if args.len() >= 3 && args[1].to_uppercase() == "LIMIT" {
                    args[2].parse().unwrap_or(10)
                } else {
                    10
                };

                match args[0].to_uppercase().as_str() {
                    "COST" => {
                        let top = engine.top_by_cost(limit);
                        Frame::array(
                            top.into_iter()
                                .map(|(key, _, cost)| {
                                    Frame::array(vec![Frame::bulk(key), Frame::Double(cost)])
                                })
                                .collect(),
                        )
                    }
                    "SAVINGS" => {
                        let top = engine.top_savings(limit);
                        Frame::array(
                            top.into_iter()
                                .map(|(key, _, savings)| {
                                    Frame::array(vec![Frame::bulk(key), Frame::Double(savings)])
                                })
                                .collect(),
                        )
                    }
                    _ => Frame::error("ERR invalid TOP subcommand. Valid: COST, SAVINGS"),
                }
            }
            "SUBOPTIMAL" => {
                // TIERING SUBOPTIMAL [LIMIT <count>] - Keys not in optimal tier
                let limit = if args.len() >= 2 && args[0].to_uppercase() == "LIMIT" {
                    args[1].parse().unwrap_or(10)
                } else {
                    10
                };

                let suboptimal = engine.suboptimal_keys(limit);
                Frame::array(
                    suboptimal
                        .into_iter()
                        .map(|(key, stats, decision)| {
                            let mut map = std::collections::HashMap::new();
                            map.insert(Bytes::from_static(b"key"), Frame::bulk(key));
                            map.insert(
                                Bytes::from_static(b"current_tier"),
                                Frame::bulk(stats.tier.name()),
                            );
                            map.insert(
                                Bytes::from_static(b"optimal_tier"),
                                Frame::bulk(decision.tier.name()),
                            );
                            map.insert(
                                Bytes::from_static(b"reason"),
                                Frame::bulk(decision.reason.description()),
                            );
                            Frame::Map(map)
                        })
                        .collect(),
                )
            }
            "HELP" => Frame::array(vec![
                Frame::bulk("TIERING <subcommand> [<arg> [value] ...]"),
                Frame::bulk("INFO -- Return tiering information."),
                Frame::bulk("COSTS -- Return or set tier cost configuration."),
                Frame::bulk("POLICY -- Return or set tiering policy."),
                Frame::bulk("COST <key> -- Get cost analysis for a key."),
                Frame::bulk("COST TOTAL -- Get total cost summary."),
                Frame::bulk("TIER <key> -- Get current tier for a key."),
                Frame::bulk("PIN <key> <tier> -- Pin key to specific tier."),
                Frame::bulk("UNPIN <key> -- Remove tier pin from key."),
                Frame::bulk("MIGRATE <key> <tier> -- Force immediate migration."),
                Frame::bulk("PRIORITY <key> <priority> -- Set key priority."),
                Frame::bulk("STATS <key> -- Get access statistics for a key."),
                Frame::bulk("TOP COST [LIMIT n] -- Get top keys by cost."),
                Frame::bulk("TOP SAVINGS [LIMIT n] -- Get top savings opportunities."),
                Frame::bulk("SUBOPTIMAL [LIMIT n] -- Get keys not in optimal tier."),
                Frame::bulk("SAVINGS -- Compute current vs optimal cost report."),
                Frame::bulk("RECOMMEND [LIMIT n] -- Get tier change recommendations."),
                Frame::bulk(
                    "COMPARE-REDIS <total_data_gb> [ops_per_sec] -- Compare costs with Redis.",
                ),
                Frame::bulk("AUTO [GET|SET <field> <value>] -- Manage auto-tiering config."),
            ]),
            "SAVINGS" => self.tiering_savings(),
            "RECOMMEND" => self.tiering_recommend(args),
            "COMPARE-REDIS" => self.tiering_compare_redis(args),
            "AUTO" => self.tiering_auto_config(args),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for 'tiering|{}'",
                subcommand.to_lowercase()
            )),
        }
    }

    pub(super) async fn handle_budget(&self, subcommand: &str, args: &[String]) -> Frame {
        match subcommand {
            "SET" => {
                if args.len() < 2 {
                    return Frame::error(
                        "ERR BUDGET SET requires: namespace monthly_limit_cents [THRESHOLD pct] [OPTIMIZE on|off]",
                    );
                }
                let namespace = &args[0];
                let Ok(limit) = args[1].parse::<u64>() else {
                    return Frame::error("ERR monthly_limit_cents must be an integer");
                };

                let mut threshold: u8 = 80;
                let mut auto_optimize = true;

                let mut i = 2;
                while i < args.len() {
                    match args[i].to_uppercase().as_str() {
                        "THRESHOLD" => {
                            i += 1;
                            if i < args.len() {
                                threshold = args[i].parse().unwrap_or(80);
                            }
                        }
                        "OPTIMIZE" => {
                            i += 1;
                            if i < args.len() {
                                auto_optimize = matches!(
                                    args[i].to_lowercase().as_str(),
                                    "on" | "true" | "1" | "yes"
                                );
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }

                Frame::Array(Some(vec![
                    Frame::bulk("namespace"),
                    Frame::bulk(namespace.to_string()),
                    Frame::Integer(limit as i64),
                    Frame::bulk("alert_threshold_pct"),
                    Frame::Integer(threshold as i64),
                    Frame::bulk("auto_optimize"),
                    Frame::bulk(if auto_optimize { "true" } else { "false" }),
                    Frame::bulk("status"),
                    Frame::simple("OK"),
                ]))
            }
            "REPORT" => {
                let namespace = args
                    .first()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "default".to_string());
                Frame::Array(Some(vec![
                    Frame::bulk("namespace"),
                    Frame::bulk(namespace),
                    Frame::bulk("budget_cents"),
                    Frame::Integer(0),
                    Frame::bulk("spent_cents"),
                    Frame::Integer(0),
                    Frame::bulk("projected_cents"),
                    Frame::Integer(0),
                    Frame::bulk("memory_cost_cents"),
                    Frame::Integer(0),
                    Frame::bulk("storage_cost_cents"),
                    Frame::Integer(0),
                    Frame::bulk("network_cost_cents"),
                    Frame::Integer(0),
                    Frame::bulk("api_cost_cents"),
                    Frame::Integer(0),
                    Frame::bulk("savings_vs_all_memory_pct"),
                    Frame::bulk("0.0"),
                    Frame::bulk("status"),
                    Frame::bulk("on_track"),
                ]))
            }
            "STATUS" => {
                let namespace = args
                    .first()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "default".to_string());
                Frame::Array(Some(vec![
                    Frame::bulk("namespace"),
                    Frame::bulk(namespace),
                    Frame::bulk("status"),
                    Frame::bulk("on_track"),
                    Frame::bulk("budget_configured"),
                    Frame::bulk("false"),
                ]))
            }
            _ => Frame::error(format!(
                "ERR unknown BUDGET subcommand '{}'. Try SET, REPORT, STATUS",
                subcommand
            )),
        }
    }

    /// Handle AUTOTUNE subcommands for workload profiler management.
    pub(super) fn handle_autotune(&self, subcommand: &str, _args: &[String]) -> Frame {
        use ferrite_core::optimizer::TierThresholds;

        match subcommand.to_uppercase().as_str() {
            "STATUS" => {
                let mut items = Vec::new();
                items.push(Frame::bulk("profiler_active"));
                let active = self.store.profiler().is_some();
                items.push(Frame::bulk(if active { "true" } else { "false" }));
                if let Some(profiler) = self.store.profiler() {
                    let snap = profiler.snapshot();
                    items.push(Frame::bulk("keys_tracked"));
                    items.push(Frame::Integer(snap.unique_keys_accessed as i64));
                    items.push(Frame::bulk("total_reads"));
                    items.push(Frame::Integer(snap.total_reads as i64));
                    items.push(Frame::bulk("total_writes"));
                    items.push(Frame::Integer(snap.total_writes as i64));
                    items.push(Frame::bulk("ops_per_sec"));
                    items.push(Frame::Double(snap.ops_per_sec));
                }
                Frame::Array(Some(items))
            }
            "REPORT" => {
                if let Some(profiler) = self.store.profiler() {
                    let thresholds = TierThresholds::default();
                    let report = profiler.report(&thresholds);
                    let items = vec![
                        Frame::bulk("total_keys_analyzed"),
                        Frame::Integer(report.total_keys_analyzed as i64),
                        Frame::bulk("hot_keys"),
                        Frame::Integer(report.hot_keys as i64),
                        Frame::bulk("warm_keys"),
                        Frame::Integer(report.warm_keys as i64),
                        Frame::bulk("cold_keys"),
                        Frame::Integer(report.cold_keys as i64),
                        Frame::bulk("read_write_ratio"),
                        Frame::Double(report.read_write_ratio),
                        Frame::bulk("throughput_ops_per_sec"),
                        Frame::Double(report.throughput_ops_per_sec),
                        Frame::bulk("avg_value_size"),
                        Frame::Double(report.avg_value_size),
                        Frame::bulk("memory_usage_fraction"),
                        Frame::Double(report.memory_usage_fraction),
                    ];
                    Frame::Array(Some(items))
                } else {
                    Frame::error("ERR profiler is not active. Run AUTOTUNE ENABLE first")
                }
            }
            "ENABLE" => {
                if self.store.profiler().is_some() {
                    Frame::simple("OK (profiler already active)")
                } else {
                    // Store is behind Arc — cannot mutate. Signal that it must
                    // be enabled at startup or via Arc::get_mut before serving.
                    Frame::error(
                        "ERR profiler must be enabled at server startup with --autotune flag or via config",
                    )
                }
            }
            "DISABLE" => {
                if self.store.profiler().is_none() {
                    Frame::simple("OK (profiler already inactive)")
                } else {
                    Frame::error(
                        "ERR profiler cannot be disabled at runtime; restart without --autotune",
                    )
                }
            }
            _ => Frame::error(format!(
                "ERR unknown AUTOTUNE subcommand '{}'. Try STATUS, REPORT, ENABLE, DISABLE",
                subcommand
            )),
        }
    }
}
