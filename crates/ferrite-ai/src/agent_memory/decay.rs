//! Memory decay engine implementing Ebbinghaus forgetting curves.
//!
//! Models how memories lose strength over time unless reinforced through access.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Decay model controlling how memory strength diminishes over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecayModel {
    /// Exponential decay: strength = initial * e^(-λt)
    Exponential,
    /// Power-law decay: strength = initial * t^(-α)
    PowerLaw,
    /// Step function: full strength until TTL, then zero.
    StepFunction,
    /// No decay — memory persists indefinitely.
    None,
}

impl Default for DecayModel {
    fn default() -> Self {
        Self::Exponential
    }
}

/// Parameters for decay computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecayParams {
    /// The decay model to use.
    pub model: DecayModel,
    /// Decay rate parameter (lambda for exponential, alpha for power-law).
    pub rate: f64,
    /// Base strength before decay.
    pub initial_strength: f64,
    /// Number of rehearsals (accesses) which slow decay.
    pub rehearsals: u64,
    /// Each rehearsal multiplies the effective time constant by this factor.
    pub rehearsal_factor: f64,
}

impl Default for DecayParams {
    fn default() -> Self {
        Self {
            model: DecayModel::Exponential,
            rate: 0.1,
            initial_strength: 1.0,
            rehearsals: 0,
            rehearsal_factor: 1.5,
        }
    }
}

/// Computes the current strength of a memory given elapsed time and parameters.
pub fn compute_strength(params: &DecayParams, elapsed: Duration) -> f64 {
    let hours = elapsed.as_secs_f64() / 3600.0;
    let effective_rate = params.rate / (1.0 + params.rehearsals as f64 * params.rehearsal_factor);

    let strength = match params.model {
        DecayModel::Exponential => params.initial_strength * (-effective_rate * hours).exp(),
        DecayModel::PowerLaw => {
            if hours < 1.0 {
                params.initial_strength
            } else {
                params.initial_strength * hours.powf(-effective_rate)
            }
        }
        DecayModel::StepFunction => {
            // TTL = 1/rate hours
            let ttl_hours = if effective_rate > 0.0 {
                1.0 / effective_rate
            } else {
                f64::MAX
            };
            if hours < ttl_hours {
                params.initial_strength
            } else {
                0.0
            }
        }
        DecayModel::None => params.initial_strength,
    };

    strength.clamp(0.0, 1.0)
}

/// Computes the strength boost from accessing a memory.
pub fn rehearsal_boost(current_strength: f64, base_boost: f64) -> f64 {
    // Boost is stronger when memory is weaker (spacing effect)
    let boost = base_boost * (1.0 - current_strength * 0.5);
    (current_strength + boost).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_decay() {
        let params = DecayParams {
            model: DecayModel::Exponential,
            rate: 0.1,
            initial_strength: 1.0,
            rehearsals: 0,
            ..Default::default()
        };

        let strength_0 = compute_strength(&params, Duration::from_secs(0));
        assert!((strength_0 - 1.0).abs() < 0.01);

        let strength_24h = compute_strength(&params, Duration::from_secs(86400));
        assert!(strength_24h < 1.0);
        assert!(strength_24h > 0.0);
    }

    #[test]
    fn test_rehearsal_slows_decay() {
        let base = DecayParams {
            model: DecayModel::Exponential,
            rate: 0.1,
            initial_strength: 1.0,
            rehearsals: 0,
            ..Default::default()
        };
        let rehearsed = DecayParams {
            rehearsals: 5,
            ..base.clone()
        };

        let elapsed = Duration::from_secs(86400);
        let s_base = compute_strength(&base, elapsed);
        let s_rehearsed = compute_strength(&rehearsed, elapsed);

        assert!(s_rehearsed > s_base); // More rehearsals = slower decay
    }

    #[test]
    fn test_no_decay() {
        let params = DecayParams {
            model: DecayModel::None,
            initial_strength: 0.8,
            ..Default::default()
        };

        let strength = compute_strength(&params, Duration::from_secs(999_999));
        assert!((strength - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_step_function() {
        let params = DecayParams {
            model: DecayModel::StepFunction,
            rate: 0.1, // TTL = 10 hours
            initial_strength: 1.0,
            rehearsals: 0,
            rehearsal_factor: 1.0,
        };

        let before = compute_strength(&params, Duration::from_secs(5 * 3600));
        assert!((before - 1.0).abs() < f64::EPSILON);

        let after = compute_strength(&params, Duration::from_secs(15 * 3600));
        assert!((after - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rehearsal_boost() {
        let boosted = rehearsal_boost(0.3, 0.2);
        assert!(boosted > 0.3);
        assert!(boosted <= 1.0);

        // Stronger memories get less boost (spacing effect)
        let weak_boost = rehearsal_boost(0.1, 0.2);
        let strong_boost = rehearsal_boost(0.9, 0.2);
        assert!(weak_boost - 0.1 > strong_boost - 0.9);
    }
}
