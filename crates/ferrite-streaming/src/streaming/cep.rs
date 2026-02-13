//! Complex Event Processing (CEP) for pattern matching on event sequences
//!
//! CEP enables detecting patterns in event streams, such as:
//! - Sequence patterns: A followed by B followed by C
//! - Time-bounded patterns: A then B within 5 seconds
//! - Conditional patterns: A where x > 10, then B where y == "click"
//! - Quantifier patterns: 3 or more A events in a row
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::streaming::cep::{Pattern, PatternMatcher};
//!
//! // Detect fraud: login failure followed by login success from different IP
//! let fraud_pattern = Pattern::begin("login_fail")
//!     .where_(|e| e.get_str("type") == Some("login_failed"))
//!     .followed_by("login_success")
//!     .where_(|e| e.get_str("type") == Some("login_success"))
//!     .where_(|e, ctx| e.get_str("ip") != ctx.get("login_fail").get_str("ip"))
//!     .within(Duration::from_secs(60));
//!
//! let matcher = PatternMatcher::new(fraud_pattern);
//! let matches = matcher.process(events).await;
//! ```

use super::StreamEvent;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

/// A CEP pattern definition
#[derive(Clone)]
pub struct Pattern {
    /// Pattern name
    pub name: String,
    /// Pattern stages
    stages: Vec<PatternStage>,
    /// Maximum time window for the pattern
    within: Option<Duration>,
    /// Pattern quantifier
    quantifier: Quantifier,
}

impl Pattern {
    /// Begin a new pattern with a named stage
    pub fn begin(name: &str) -> PatternBuilder {
        PatternBuilder::new(name)
    }

    /// Get pattern stages
    pub fn stages(&self) -> &[PatternStage] {
        &self.stages
    }

    /// Get time window
    pub fn time_window(&self) -> Option<Duration> {
        self.within
    }

    /// Check if this is a complete pattern
    pub fn is_complete(&self) -> bool {
        !self.stages.is_empty()
    }
}

/// Builder for creating patterns
pub struct PatternBuilder {
    name: String,
    stages: Vec<PatternStage>,
    current_stage: Option<PatternStage>,
    within: Option<Duration>,
    quantifier: Quantifier,
}

impl PatternBuilder {
    /// Create a new pattern builder
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            stages: Vec::new(),
            current_stage: Some(PatternStage::new(name)),
            within: None,
            quantifier: Quantifier::Once,
        }
    }

    /// Add a condition to the current stage
    pub fn where_<F>(mut self, condition: F) -> Self
    where
        F: Fn(&StreamEvent) -> bool + Send + Sync + 'static,
    {
        if let Some(ref mut stage) = self.current_stage {
            stage.add_condition(Box::new(condition));
        }
        self
    }

    /// Add a condition that references previous matches
    pub fn where_with_context<F>(mut self, condition: F) -> Self
    where
        F: Fn(&StreamEvent, &PatternContext) -> bool + Send + Sync + 'static,
    {
        if let Some(ref mut stage) = self.current_stage {
            stage.add_context_condition(Box::new(condition));
        }
        self
    }

    /// Add a "followed by" stage (strict contiguity)
    pub fn followed_by(mut self, name: &str) -> Self {
        self.commit_stage();
        self.current_stage = Some(PatternStage::new(name).with_contiguity(Contiguity::Strict));
        self
    }

    /// Add a "followed by any" stage (relaxed contiguity)
    pub fn followed_by_any(mut self, name: &str) -> Self {
        self.commit_stage();
        self.current_stage = Some(PatternStage::new(name).with_contiguity(Contiguity::Relaxed));
        self
    }

    /// Add a "not followed by" stage (negative pattern)
    pub fn not_followed_by(mut self, name: &str) -> Self {
        self.commit_stage();
        self.current_stage = Some(PatternStage::new(name).as_negative());
        self
    }

    /// Add a "or" stage (alternative patterns)
    pub fn or(mut self, name: &str) -> Self {
        self.commit_stage();
        self.current_stage = Some(PatternStage::new(name).as_alternative());
        self
    }

    /// Set times quantifier (exact count)
    pub fn times(mut self, count: usize) -> Self {
        self.quantifier = Quantifier::Times(count);
        self
    }

    /// Set times or more quantifier (at least count)
    pub fn times_or_more(mut self, min: usize) -> Self {
        self.quantifier = Quantifier::TimesOrMore(min);
        self
    }

    /// Set optional quantifier
    pub fn optional(mut self) -> Self {
        self.quantifier = Quantifier::Optional;
        self
    }

    /// Set one or more quantifier
    pub fn one_or_more(mut self) -> Self {
        self.quantifier = Quantifier::OneOrMore;
        self
    }

    /// Set time window for the pattern
    pub fn within(mut self, duration: Duration) -> Self {
        self.within = Some(duration);
        self
    }

    /// Build the pattern
    pub fn build(mut self) -> Pattern {
        self.commit_stage();
        Pattern {
            name: self.name,
            stages: self.stages,
            within: self.within,
            quantifier: self.quantifier,
        }
    }

    fn commit_stage(&mut self) {
        if let Some(stage) = self.current_stage.take() {
            self.stages.push(stage);
        }
    }
}

/// A single stage in a pattern
#[derive(Clone)]
pub struct PatternStage {
    /// Stage name
    pub name: String,
    /// Conditions for matching
    #[allow(clippy::type_complexity)]
    conditions: Vec<Arc<dyn Fn(&StreamEvent) -> bool + Send + Sync>>,
    /// Context-aware conditions
    #[allow(clippy::type_complexity)]
    context_conditions: Vec<Arc<dyn Fn(&StreamEvent, &PatternContext) -> bool + Send + Sync>>,
    /// Contiguity requirement
    contiguity: Contiguity,
    /// Whether this is a negative pattern
    negative: bool,
    /// Whether this is an alternative pattern
    alternative: bool,
    /// Stage quantifier
    quantifier: Quantifier,
}

impl PatternStage {
    /// Create a new pattern stage
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            conditions: Vec::new(),
            context_conditions: Vec::new(),
            contiguity: Contiguity::Relaxed,
            negative: false,
            alternative: false,
            quantifier: Quantifier::Once,
        }
    }

    /// Set contiguity
    pub fn with_contiguity(mut self, contiguity: Contiguity) -> Self {
        self.contiguity = contiguity;
        self
    }

    /// Mark as negative pattern
    pub fn as_negative(mut self) -> Self {
        self.negative = true;
        self
    }

    /// Mark as alternative pattern
    pub fn as_alternative(mut self) -> Self {
        self.alternative = true;
        self
    }

    /// Add a condition
    pub fn add_condition(&mut self, condition: Box<dyn Fn(&StreamEvent) -> bool + Send + Sync>) {
        self.conditions.push(Arc::from(condition));
    }

    /// Add a context-aware condition
    #[allow(clippy::type_complexity)]
    pub fn add_context_condition(
        &mut self,
        condition: Box<dyn Fn(&StreamEvent, &PatternContext) -> bool + Send + Sync>,
    ) {
        self.context_conditions.push(Arc::from(condition));
    }

    /// Check if an event matches this stage
    pub fn matches(&self, event: &StreamEvent, context: &PatternContext) -> bool {
        let basic_match = self.conditions.iter().all(|c| c(event));
        let context_match = self.context_conditions.iter().all(|c| c(event, context));
        basic_match && context_match
    }
}

/// Contiguity requirement for pattern stages
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Contiguity {
    /// Strict: events must be immediately consecutive
    Strict,
    /// Relaxed: other events may occur in between
    Relaxed,
    /// Non-deterministic: allows for multiple matches
    NonDeterministic,
}

/// Pattern quantifier
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Quantifier {
    /// Exactly once
    Once,
    /// Zero or one time
    Optional,
    /// Zero or more times
    ZeroOrMore,
    /// One or more times
    OneOrMore,
    /// Exactly N times
    Times(usize),
    /// At least N times
    TimesOrMore(usize),
    /// Between min and max times
    Range(usize, usize),
}

/// Context for pattern matching (previously matched events)
#[derive(Clone, Debug, Default)]
pub struct PatternContext {
    /// Matched events by stage name
    matches: HashMap<String, Vec<StreamEvent>>,
}

impl PatternContext {
    /// Create a new pattern context
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a matched event
    pub fn add_match(&mut self, stage_name: &str, event: StreamEvent) {
        self.matches
            .entry(stage_name.to_string())
            .or_default()
            .push(event);
    }

    /// Get matched events for a stage
    pub fn get(&self, stage_name: &str) -> Option<&Vec<StreamEvent>> {
        self.matches.get(stage_name)
    }

    /// Get the first matched event for a stage
    pub fn get_first(&self, stage_name: &str) -> Option<&StreamEvent> {
        self.matches.get(stage_name).and_then(|v| v.first())
    }

    /// Get the last matched event for a stage
    pub fn get_last(&self, stage_name: &str) -> Option<&StreamEvent> {
        self.matches.get(stage_name).and_then(|v| v.last())
    }

    /// Get all matched events
    pub fn all_matches(&self) -> &HashMap<String, Vec<StreamEvent>> {
        &self.matches
    }

    /// Clone context for branching
    pub fn branch(&self) -> Self {
        self.clone()
    }
}

/// Pattern match result
#[derive(Clone, Debug)]
pub struct PatternMatch {
    /// Pattern name
    pub pattern_name: String,
    /// Matched events by stage name
    pub events: HashMap<String, Vec<StreamEvent>>,
    /// Match start time
    pub start_time: u64,
    /// Match end time
    pub end_time: u64,
}

impl PatternMatch {
    /// Get events for a stage
    pub fn get(&self, stage_name: &str) -> Option<&Vec<StreamEvent>> {
        self.events.get(stage_name)
    }

    /// Get all events in order
    pub fn all_events(&self) -> Vec<&StreamEvent> {
        let mut all: Vec<_> = self.events.values().flatten().collect();
        all.sort_by_key(|e| e.timestamp);
        all
    }

    /// Get match duration
    pub fn duration_ms(&self) -> u64 {
        self.end_time.saturating_sub(self.start_time)
    }
}

/// Pattern matcher for detecting patterns in event streams
pub struct PatternMatcher {
    /// Pattern to match
    pattern: Pattern,
    /// Active partial matches
    partial_matches: Vec<PartialMatch>,
    /// Maximum partial matches to track
    max_partial_matches: usize,
    /// Completed matches
    completed_matches: VecDeque<PatternMatch>,
    /// Maximum completed matches to buffer
    max_completed_matches: usize,
}

/// A partial match in progress
struct PartialMatch {
    /// Current stage index
    stage_index: usize,
    /// Pattern context with matched events
    context: PatternContext,
    /// Start time of the match
    start_time: u64,
    /// Last event time
    last_event_time: u64,
}

impl PatternMatcher {
    /// Create a new pattern matcher
    pub fn new(pattern: Pattern) -> Self {
        Self {
            pattern,
            partial_matches: Vec::new(),
            max_partial_matches: 10000,
            completed_matches: VecDeque::new(),
            max_completed_matches: 1000,
        }
    }

    /// Set maximum partial matches to track
    pub fn with_max_partial_matches(mut self, max: usize) -> Self {
        self.max_partial_matches = max;
        self
    }

    /// Process an event and return any completed matches
    pub fn process(&mut self, event: &StreamEvent) -> Vec<PatternMatch> {
        let mut new_matches = Vec::new();

        // Clean up expired partial matches
        self.cleanup_expired(event.timestamp);

        // Try to advance existing partial matches
        let mut advanced_matches = Vec::new();
        let mut i = 0;
        while i < self.partial_matches.len() {
            let pm = &self.partial_matches[i];
            let stage = &self.pattern.stages[pm.stage_index];

            if stage.matches(event, &pm.context) {
                // Event matches current stage
                let mut new_context = pm.context.branch();
                new_context.add_match(&stage.name, event.clone());

                if pm.stage_index + 1 >= self.pattern.stages.len() {
                    // Pattern complete!
                    let match_result = PatternMatch {
                        pattern_name: self.pattern.name.clone(),
                        events: new_context.all_matches().clone(),
                        start_time: pm.start_time,
                        end_time: event.timestamp,
                    };
                    new_matches.push(match_result.clone());
                    self.completed_matches.push_back(match_result);
                    if self.completed_matches.len() > self.max_completed_matches {
                        self.completed_matches.pop_front();
                    }
                } else {
                    // Advance to next stage
                    advanced_matches.push(PartialMatch {
                        stage_index: pm.stage_index + 1,
                        context: new_context,
                        start_time: pm.start_time,
                        last_event_time: event.timestamp,
                    });
                }

                // For non-deterministic contiguity, keep the original partial match
                if self
                    .pattern
                    .stages
                    .get(pm.stage_index + 1)
                    .map(|s| s.contiguity == Contiguity::NonDeterministic)
                    .unwrap_or(false)
                {
                    i += 1;
                    continue;
                }
            }
            i += 1;
        }

        // Add advanced matches
        for am in advanced_matches {
            if self.partial_matches.len() < self.max_partial_matches {
                self.partial_matches.push(am);
            }
        }

        // Try to start new matches
        if !self.pattern.stages.is_empty() {
            let first_stage = &self.pattern.stages[0];
            let empty_context = PatternContext::new();

            if first_stage.matches(event, &empty_context) {
                let mut context = PatternContext::new();
                context.add_match(&first_stage.name, event.clone());

                if self.pattern.stages.len() == 1 {
                    // Single-stage pattern complete immediately
                    let match_result = PatternMatch {
                        pattern_name: self.pattern.name.clone(),
                        events: context.all_matches().clone(),
                        start_time: event.timestamp,
                        end_time: event.timestamp,
                    };
                    new_matches.push(match_result.clone());
                    self.completed_matches.push_back(match_result);
                } else if self.partial_matches.len() < self.max_partial_matches {
                    self.partial_matches.push(PartialMatch {
                        stage_index: 1,
                        context,
                        start_time: event.timestamp,
                        last_event_time: event.timestamp,
                    });
                }
            }
        }

        new_matches
    }

    /// Get recent completed matches
    pub fn recent_matches(&self) -> impl Iterator<Item = &PatternMatch> {
        self.completed_matches.iter()
    }

    /// Get count of active partial matches
    pub fn partial_match_count(&self) -> usize {
        self.partial_matches.len()
    }

    /// Clear all state
    pub fn reset(&mut self) {
        self.partial_matches.clear();
        self.completed_matches.clear();
    }

    fn cleanup_expired(&mut self, current_time: u64) {
        if let Some(window) = self.pattern.within {
            let window_ms = window.as_millis() as u64;
            self.partial_matches
                .retain(|pm| current_time.saturating_sub(pm.start_time) <= window_ms);
        }
    }
}

/// CEP statistics
#[derive(Clone, Debug, Default)]
pub struct CepStats {
    /// Total events processed
    pub events_processed: u64,
    /// Patterns matched
    pub patterns_matched: u64,
    /// Active partial matches
    pub active_partial_matches: u64,
    /// Expired partial matches
    pub expired_partial_matches: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(event_type: &str, timestamp: u64) -> StreamEvent {
        StreamEvent::with_timestamp(None, json!({ "type": event_type }), timestamp)
    }

    #[test]
    fn test_simple_pattern() {
        let pattern = Pattern::begin("start")
            .where_(|e| e.get_str("type") == Some("A"))
            .followed_by("end")
            .where_(|e| e.get_str("type") == Some("B"))
            .build();

        let mut matcher = PatternMatcher::new(pattern);

        // No match yet
        let matches = matcher.process(&make_event("A", 1000));
        assert!(matches.is_empty());

        // Match!
        let matches = matcher.process(&make_event("B", 2000));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "start");
    }

    #[test]
    fn test_pattern_with_time_window() {
        let pattern = Pattern::begin("start")
            .where_(|e| e.get_str("type") == Some("A"))
            .followed_by("end")
            .where_(|e| e.get_str("type") == Some("B"))
            .within(Duration::from_secs(1))
            .build();

        let mut matcher = PatternMatcher::new(pattern);

        // Start pattern
        matcher.process(&make_event("A", 1000));

        // Too late - expired
        let matches = matcher.process(&make_event("B", 3000));
        assert!(matches.is_empty());
    }

    #[test]
    fn test_three_stage_pattern() {
        let pattern = Pattern::begin("first")
            .where_(|e| e.get_str("type") == Some("A"))
            .followed_by("second")
            .where_(|e| e.get_str("type") == Some("B"))
            .followed_by("third")
            .where_(|e| e.get_str("type") == Some("C"))
            .build();

        let mut matcher = PatternMatcher::new(pattern);

        assert!(matcher.process(&make_event("A", 1000)).is_empty());
        assert!(matcher.process(&make_event("B", 2000)).is_empty());
        let matches = matcher.process(&make_event("C", 3000));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_pattern_context() {
        let pattern = Pattern::begin("login_fail")
            .where_(|e| e.get_str("type") == Some("login_failed"))
            .followed_by("login_success")
            .where_(|e| e.get_str("type") == Some("login_success"))
            .build();

        let mut matcher = PatternMatcher::new(pattern);

        let fail_event = StreamEvent::with_timestamp(
            Some("user1".to_string()),
            json!({ "type": "login_failed", "ip": "1.2.3.4" }),
            1000,
        );

        let success_event = StreamEvent::with_timestamp(
            Some("user1".to_string()),
            json!({ "type": "login_success", "ip": "5.6.7.8" }),
            2000,
        );

        matcher.process(&fail_event);
        let matches = matcher.process(&success_event);
        assert_eq!(matches.len(), 1);

        // Check we can access the matched events
        let m = &matches[0];
        assert!(m.get("login_fail").is_some());
        assert!(m.get("login_success").is_some());
    }
}
