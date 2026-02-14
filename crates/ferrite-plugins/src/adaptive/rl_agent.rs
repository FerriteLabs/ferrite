//! Reinforcement Learning Tiering Agent
//!
//! Q-learning based agent that learns optimal tiering decisions from experience,
//! replacing static LRU with workload-aware policies. Uses discretized state space
//! with tile coding for generalization across similar states.

use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::tier_classifier::Tier;

// ── State ────────────────────────────────────────────────────────────────────

/// Captures current system state as a feature vector for the RL agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TieringState {
    /// Memory utilization (0.0–1.0)
    pub memory_utilization: f64,
    /// Read/write ratio (reads / total ops)
    pub read_write_ratio: f64,
    /// Average key size in bytes
    pub avg_key_size: f64,
    /// Fraction of keys accessed in the last minute
    pub hot_key_fraction: f64,
    /// Fraction of keys per tier: \[Memory, Mmap, Ssd, Cloud, Archive\]
    pub tier_distribution: [f64; 5],
    /// Cyclical encoding of time of day (sin component)
    pub time_of_day_sin: f64,
    /// Cyclical encoding of time of day (cos component)
    pub time_of_day_cos: f64,
    /// Cyclical encoding of day of week (sin component)
    pub day_of_week_sin: f64,
    /// Cyclical encoding of day of week (cos component)
    pub day_of_week_cos: f64,
}

impl TieringState {
    /// Number of continuous features in the state vector.
    const NUM_FEATURES: usize = 13;

    /// Convert state into a flat feature vector for discretization.
    pub fn to_feature_vector(&self) -> [f64; Self::NUM_FEATURES] {
        [
            self.memory_utilization,
            self.read_write_ratio,
            self.avg_key_size,
            self.hot_key_fraction,
            self.tier_distribution[0],
            self.tier_distribution[1],
            self.tier_distribution[2],
            self.tier_distribution[3],
            self.tier_distribution[4],
            self.time_of_day_sin,
            self.time_of_day_cos,
            self.day_of_week_sin,
            self.day_of_week_cos,
        ]
    }

    /// Discretize state into bin indices using uniform tile coding.
    ///
    /// Each feature is mapped to a bin in `[0, bins)`. Features in
    /// `[-1.0, 1.0]` (cyclical encodings) are rescaled to `[0, 1]` first.
    pub fn discretize(&self, bins: usize) -> Vec<usize> {
        let features = self.to_feature_vector();
        features
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                // Cyclical features (indices 9–12) range [-1, 1]; normalise to [0, 1].
                let normalised = if i >= 9 { (v + 1.0) / 2.0 } else { v };
                let clamped = normalised.clamp(0.0, 1.0);
                let bin = (clamped * bins as f64) as usize;
                bin.min(bins - 1)
            })
            .collect()
    }
}

// ── Action ───────────────────────────────────────────────────────────────────

/// Action the RL agent can take to adjust tiering.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TieringAction {
    /// Promote keys from a colder tier to a warmer tier.
    PromoteKeys {
        /// Number of keys to promote
        count: usize,
        /// Source tier
        from_tier: Tier,
        /// Destination tier
        to_tier: Tier,
    },
    /// Demote keys from a warmer tier to a colder tier.
    DemoteKeys {
        /// Number of keys to demote
        count: usize,
        /// Source tier
        from_tier: Tier,
        /// Destination tier
        to_tier: Tier,
    },
    /// Adjust the classification threshold for a tier.
    AdjustThreshold {
        /// Target tier
        tier: Tier,
        /// New threshold value
        new_threshold: f64,
    },
    /// Take no action this step.
    NoOp,
}

/// Fixed action palette used by the agent.
///
/// We enumerate a small set of representative actions so the Q-table stays
/// tractable. Each action maps to an integer id via its position.
fn action_palette() -> Vec<TieringAction> {
    vec![
        TieringAction::NoOp,
        TieringAction::PromoteKeys {
            count: 10,
            from_tier: Tier::Cold,
            to_tier: Tier::Warm,
        },
        TieringAction::PromoteKeys {
            count: 10,
            from_tier: Tier::Warm,
            to_tier: Tier::Hot,
        },
        TieringAction::DemoteKeys {
            count: 10,
            from_tier: Tier::Hot,
            to_tier: Tier::Warm,
        },
        TieringAction::DemoteKeys {
            count: 10,
            from_tier: Tier::Warm,
            to_tier: Tier::Cold,
        },
        TieringAction::AdjustThreshold {
            tier: Tier::Hot,
            new_threshold: 0.8,
        },
        TieringAction::AdjustThreshold {
            tier: Tier::Hot,
            new_threshold: 0.6,
        },
        TieringAction::AdjustThreshold {
            tier: Tier::Cold,
            new_threshold: 0.4,
        },
        TieringAction::AdjustThreshold {
            tier: Tier::Cold,
            new_threshold: 0.2,
        },
    ]
}

// ── Reward ────────────────────────────────────────────────────────────────────

/// Computes the scalar reward for a transition.
///
/// Positive contributions come from cache-hit-rate improvement, latency
/// reduction, and cost savings. SLA violations produce a negative penalty.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TieringReward {
    /// Change in cache hit rate (positive = improvement)
    pub cache_hit_rate_delta: f64,
    /// Change in average latency in µs (negative = improvement)
    pub latency_delta_us: f64,
    /// Change in estimated cost (negative = cheaper)
    pub cost_delta: f64,
    /// Number of SLA violations observed in this step
    pub sla_violations: u64,
}

impl TieringReward {
    /// Weight for cache-hit-rate improvement
    const W_HIT_RATE: f64 = 10.0;
    /// Weight for latency improvement (reward is *negative* delta, so invert)
    const W_LATENCY: f64 = 0.001;
    /// Weight for cost reduction
    const W_COST: f64 = 1.0;
    /// Per-violation penalty
    const W_SLA: f64 = 5.0;

    /// Compute the scalar reward.
    pub fn compute(&self) -> f64 {
        let hit_reward = self.cache_hit_rate_delta * Self::W_HIT_RATE;
        let latency_reward = -self.latency_delta_us * Self::W_LATENCY;
        let cost_reward = -self.cost_delta * Self::W_COST;
        let sla_penalty = self.sla_violations as f64 * Self::W_SLA;

        hit_reward + latency_reward + cost_reward - sla_penalty
    }
}

// ── Experience ────────────────────────────────────────────────────────────────

/// A single SARS (state, action, reward, next-state) experience tuple.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Experience {
    /// State before the action
    pub state: TieringState,
    /// Action taken
    pub action: TieringAction,
    /// Scalar reward received
    pub reward: f64,
    /// State observed after the action
    pub next_state: TieringState,
}

// ── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the reinforcement-learning tiering agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RLTieringConfig {
    /// Enable the RL agent (false = fall back to static LRU)
    pub enabled: bool,
    /// Q-learning step size
    pub learning_rate: f64,
    /// Discount factor γ for future rewards
    pub discount_factor: f64,
    /// Initial exploration probability
    pub epsilon_start: f64,
    /// Multiplicative decay applied to epsilon after each step
    pub epsilon_decay: f64,
    /// Minimum exploration probability
    pub epsilon_min: f64,
    /// Number of uniform bins per feature dimension for state discretization
    pub state_discretization_bins: usize,
    /// Seconds between agent evaluation steps
    pub evaluation_interval_secs: u64,
    /// Steps of pure LRU before the agent starts acting
    pub warmup_steps: usize,
}

impl Default for RLTieringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            learning_rate: 0.01,
            discount_factor: 0.95,
            epsilon_start: 1.0,
            epsilon_decay: 0.995,
            epsilon_min: 0.05,
            state_discretization_bins: 10,
            evaluation_interval_secs: 60,
            warmup_steps: 100,
        }
    }
}

// ── Training statistics ──────────────────────────────────────────────────────

/// Running statistics tracked during training.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TrainingStats {
    /// Total training steps completed
    pub episodes: u64,
    /// Exponential moving average of reward
    pub avg_reward: f64,
    /// Current epsilon value
    pub epsilon_current: f64,
    /// Number of Q-table entries
    pub q_table_size: usize,
}

// ── Agent ────────────────────────────────────────────────────────────────────

/// Q-learning based reinforcement-learning agent for tiering decisions.
///
/// The agent discretizes continuous [`TieringState`] features via tile coding,
/// selects actions with an ε-greedy policy, and learns Q-values from
/// experience tuples.
pub struct RLTieringAgent {
    config: RLTieringConfig,
    /// Q-table: maps (discretized state, action index) → Q-value
    q_table: RwLock<HashMap<(Vec<usize>, usize), f64>>,
    /// Current exploration rate
    epsilon: RwLock<f64>,
    /// Total training steps completed
    episodes: RwLock<u64>,
    /// Exponential moving average of reward
    avg_reward: RwLock<f64>,
    /// Pre-computed action palette
    actions: Vec<TieringAction>,
}

impl RLTieringAgent {
    /// Create a new agent with the given configuration.
    pub fn new(config: RLTieringConfig) -> Self {
        let epsilon = config.epsilon_start;
        Self {
            config,
            q_table: RwLock::new(HashMap::new()),
            epsilon: RwLock::new(epsilon),
            episodes: RwLock::new(0),
            avg_reward: RwLock::new(0.0),
            actions: action_palette(),
        }
    }

    /// Select an action for the given state using ε-greedy exploration.
    ///
    /// During the warmup period ([`RLTieringConfig::warmup_steps`]) the agent
    /// always returns [`TieringAction::NoOp`] so the system falls back to LRU.
    pub fn select_action(&self, state: &TieringState) -> TieringAction {
        let episodes = *self.episodes.read();
        if (episodes as usize) < self.config.warmup_steps {
            return TieringAction::NoOp;
        }

        let epsilon = *self.epsilon.read();
        let mut rng = rand::thread_rng();

        if rng.gen::<f64>() < epsilon {
            // Explore: pick a random action
            let idx = rng.gen_range(0..self.actions.len());
            self.actions[idx].clone()
        } else {
            // Exploit: pick the action with the highest Q-value
            let disc = state.discretize(self.config.state_discretization_bins);
            let q = self.q_table.read();
            let best_idx = (0..self.actions.len())
                .max_by(|&a, &b| {
                    let qa = q.get(&(disc.clone(), a)).unwrap_or(&0.0);
                    let qb = q.get(&(disc.clone(), b)).unwrap_or(&0.0);
                    qa.partial_cmp(qb).unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or(0);
            self.actions[best_idx].clone()
        }
    }

    /// Perform a Q-learning update for a single transition.
    ///
    /// ```text
    /// Q(s, a) ← Q(s, a) + α [ r + γ max_a' Q(s', a') − Q(s, a) ]
    /// ```
    pub fn update(
        &self,
        state: &TieringState,
        action: &TieringAction,
        reward: f64,
        next_state: &TieringState,
    ) {
        let bins = self.config.state_discretization_bins;
        let disc_s = state.discretize(bins);
        let disc_s_next = next_state.discretize(bins);

        let action_idx = self.actions.iter().position(|a| a == action).unwrap_or(0);

        let mut q = self.q_table.write();

        // max_a' Q(s', a')
        let max_next_q = (0..self.actions.len())
            .map(|a| *q.get(&(disc_s_next.clone(), a)).unwrap_or(&0.0))
            .fold(f64::NEG_INFINITY, f64::max);
        let max_next_q = if max_next_q == f64::NEG_INFINITY {
            0.0
        } else {
            max_next_q
        };

        let key = (disc_s, action_idx);
        let current_q = *q.get(&key).unwrap_or(&0.0);
        let td_target = reward + self.config.discount_factor * max_next_q;
        let new_q = current_q + self.config.learning_rate * (td_target - current_q);
        q.insert(key, new_q);

        drop(q);

        // Decay epsilon
        {
            let mut eps = self.epsilon.write();
            *eps = (*eps * self.config.epsilon_decay).max(self.config.epsilon_min);
        }

        // Update running statistics
        {
            let mut episodes = self.episodes.write();
            *episodes += 1;
        }
        {
            let mut avg = self.avg_reward.write();
            // EMA with α = 0.01
            *avg = *avg * 0.99 + reward * 0.01;
        }
    }

    /// Convenience wrapper that performs a single training step from an
    /// [`Experience`] tuple.
    pub fn train_step(&self, experience: &Experience) {
        self.update(
            &experience.state,
            &experience.action,
            experience.reward,
            &experience.next_state,
        );
    }

    /// Return current training statistics.
    pub fn stats(&self) -> TrainingStats {
        TrainingStats {
            episodes: *self.episodes.read(),
            avg_reward: *self.avg_reward.read(),
            epsilon_current: *self.epsilon.read(),
            q_table_size: self.q_table.read().len(),
        }
    }

    /// Return a reference to the agent's configuration.
    pub fn config(&self) -> &RLTieringConfig {
        &self.config
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_state() -> TieringState {
        TieringState {
            memory_utilization: 0.6,
            read_write_ratio: 0.8,
            avg_key_size: 0.5,
            hot_key_fraction: 0.3,
            tier_distribution: [0.4, 0.3, 0.2, 0.05, 0.05],
            time_of_day_sin: 0.5,
            time_of_day_cos: -0.5,
            day_of_week_sin: 0.0,
            day_of_week_cos: 1.0,
        }
    }

    #[test]
    fn test_state_discretization() {
        let state = sample_state();
        let bins = 10;
        let disc = state.discretize(bins);

        assert_eq!(disc.len(), TieringState::NUM_FEATURES);
        for &b in &disc {
            assert!(b < bins, "bin index {b} out of range [0, {bins})");
        }
    }

    #[test]
    fn test_state_discretization_boundary() {
        let mut state = sample_state();
        state.memory_utilization = 1.0;
        state.time_of_day_sin = 1.0; // max cyclical
        let disc = state.discretize(10);
        // Max value should land in the last bin (index 9)
        assert_eq!(disc[0], 9);
    }

    #[test]
    fn test_action_selection_warmup_returns_noop() {
        let config = RLTieringConfig {
            warmup_steps: 1000,
            ..Default::default()
        };
        let agent = RLTieringAgent::new(config);
        let state = sample_state();
        // During warmup every call should return NoOp
        for _ in 0..10 {
            assert_eq!(agent.select_action(&state), TieringAction::NoOp);
        }
    }

    #[test]
    fn test_epsilon_greedy_exploration() {
        let config = RLTieringConfig {
            warmup_steps: 0,
            epsilon_start: 1.0, // always explore
            epsilon_min: 1.0,
            ..Default::default()
        };
        let agent = RLTieringAgent::new(config);
        let state = sample_state();

        // With epsilon=1.0 we expect random actions — not always NoOp.
        let mut saw_non_noop = false;
        for _ in 0..100 {
            if agent.select_action(&state) != TieringAction::NoOp {
                saw_non_noop = true;
                break;
            }
        }
        assert!(
            saw_non_noop,
            "expected at least one non-NoOp with full exploration"
        );
    }

    #[test]
    fn test_q_learning_update_convergence() {
        let config = RLTieringConfig {
            warmup_steps: 0,
            learning_rate: 0.5,
            discount_factor: 0.0, // no future reward — simplifies convergence check
            epsilon_start: 0.0,
            epsilon_min: 0.0,
            ..Default::default()
        };
        let agent = RLTieringAgent::new(config);
        let state = sample_state();
        let next_state = sample_state();
        let action = TieringAction::NoOp;

        // Repeatedly update with reward=1.0; Q should converge toward 1.0
        for _ in 0..100 {
            agent.update(&state, &action, 1.0, &next_state);
        }

        let disc = state.discretize(10);
        let q = agent.q_table.read();
        let q_val = *q.get(&(disc, 0)).unwrap_or(&0.0);
        assert!(
            (q_val - 1.0).abs() < 0.01,
            "Q-value should converge to 1.0, got {q_val}"
        );
    }

    #[test]
    fn test_reward_calculation() {
        let reward = TieringReward {
            cache_hit_rate_delta: 0.05, // +5% hit rate
            latency_delta_us: -100.0,   // 100µs improvement
            cost_delta: -0.5,           // cost reduced
            sla_violations: 0,
        };
        let score = reward.compute();
        // hit: 0.05*10=0.5, latency: -(-100)*0.001=0.1, cost: -(-0.5)*1=0.5, sla: 0
        assert!(
            score > 0.0,
            "positive improvements should give positive reward"
        );
        assert!((score - 1.1).abs() < 1e-9);

        let bad_reward = TieringReward {
            cache_hit_rate_delta: 0.0,
            latency_delta_us: 0.0,
            cost_delta: 0.0,
            sla_violations: 3,
        };
        assert!(
            bad_reward.compute() < 0.0,
            "SLA violations should give negative reward"
        );
    }

    #[test]
    fn test_train_step_updates_stats() {
        let agent = RLTieringAgent::new(RLTieringConfig::default());
        let exp = Experience {
            state: sample_state(),
            action: TieringAction::NoOp,
            reward: 1.0,
            next_state: sample_state(),
        };

        assert_eq!(agent.stats().episodes, 0);
        agent.train_step(&exp);
        assert_eq!(agent.stats().episodes, 1);
        assert!(agent.stats().q_table_size > 0);
    }

    #[test]
    fn test_epsilon_decay() {
        let config = RLTieringConfig {
            warmup_steps: 0,
            epsilon_start: 1.0,
            epsilon_decay: 0.5, // aggressive decay for testing
            epsilon_min: 0.01,
            ..Default::default()
        };
        let agent = RLTieringAgent::new(config);
        let state = sample_state();
        let action = TieringAction::NoOp;

        agent.update(&state, &action, 0.0, &state);
        let eps_after = agent.stats().epsilon_current;
        assert!(
            (eps_after - 0.5).abs() < 1e-9,
            "epsilon should decay to 0.5, got {eps_after}"
        );
    }

    #[test]
    fn test_default_config_values() {
        let config = RLTieringConfig::default();
        assert!(config.enabled);
        assert!((config.learning_rate - 0.01).abs() < 1e-9);
        assert!((config.discount_factor - 0.95).abs() < 1e-9);
        assert!((config.epsilon_start - 1.0).abs() < 1e-9);
        assert!((config.epsilon_decay - 0.995).abs() < 1e-9);
        assert!((config.epsilon_min - 0.05).abs() < 1e-9);
        assert_eq!(config.state_discretization_bins, 10);
        assert_eq!(config.evaluation_interval_secs, 60);
        assert_eq!(config.warmup_steps, 100);
    }
}
