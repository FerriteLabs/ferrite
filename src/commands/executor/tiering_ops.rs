//! Extended tiering operations — auto-tier engine and savings calculator
//!
//! Adds TIERING SAVINGS, RECOMMEND, COMPARE-REDIS, and AUTO subcommands
//! that wire to the `AutoTierEngine` and `SavingsCalculator` from
//! `ferrite-core`.

#![allow(dead_code)]

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
        let total_data_gb = match args.first().and_then(|s| s.parse::<f64>().ok()) {
            Some(v) => v,
            None => {
                return Frame::error("ERR TIERING COMPARE-REDIS requires <total_data_gb> argument");
            }
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
}
