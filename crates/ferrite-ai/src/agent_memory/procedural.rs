//! Procedural memory for action sequences and learned skills.
//!
//! Tracks action history and learned skills (reusable action sequences),
//! with feedback-driven improvement and context-based skill suggestion.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single step within a skill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionStep {
    /// The tool or mechanism used (e.g., "search", "code_edit", "api_call").
    pub tool: String,
    /// Parameters passed to the tool.
    pub parameters: HashMap<String, String>,
    /// What the step is expected to produce.
    pub expected_outcome: Option<String>,
}

/// A learned skill composed of a sequence of action steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Skill {
    /// Unique identifier.
    pub id: String,
    /// Human-readable skill name.
    pub name: String,
    /// Description of what the skill does.
    pub description: String,
    /// Ordered steps to execute this skill.
    pub steps: Vec<ActionStep>,
    /// Success rate based on feedback (0.0 - 1.0).
    pub success_rate: f64,
    /// Number of times this skill has been used.
    pub usage_count: u64,
    /// When this skill was last used.
    pub last_used: SystemTime,
    /// Contexts in which this skill is applicable.
    pub trigger_contexts: Vec<String>,
}

/// A record of a single action taken.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRecord {
    /// When the action occurred.
    pub timestamp: SystemTime,
    /// Name of the action.
    pub action: String,
    /// Tool used, if any.
    pub tool: Option<String>,
    /// Input provided to the action.
    pub input: String,
    /// Output from the action, if captured.
    pub output: Option<String>,
    /// Whether the action succeeded.
    pub success: bool,
    /// How long the action took.
    pub duration: Duration,
}

/// Feedback about a skill execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillFeedback {
    /// Whether the skill execution was successful.
    pub success: bool,
    /// Optional notes about the outcome.
    pub notes: Option<String>,
}

// ---------------------------------------------------------------------------
// ProceduralMemory
// ---------------------------------------------------------------------------

/// Procedural memory store for skills and action history.
pub struct ProceduralMemory {
    skills: RwLock<HashMap<String, Skill>>,
    action_log: RwLock<VecDeque<ActionRecord>>,
    max_log_size: usize,
}

impl ProceduralMemory {
    /// Creates a new procedural memory store.
    pub fn new(max_log_size: usize) -> Self {
        Self {
            skills: RwLock::new(HashMap::new()),
            action_log: RwLock::new(VecDeque::new()),
            max_log_size,
        }
    }

    /// Records an action in the action log.
    pub fn record_action(&self, action: ActionRecord) {
        let mut log = self.action_log.write();
        if log.len() >= self.max_log_size {
            log.pop_front();
        }
        log.push_back(action);
    }

    /// Learns a new skill from a sequence of steps. Returns the skill ID.
    pub fn learn_skill(&self, name: &str, steps: Vec<ActionStep>, description: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let skill = Skill {
            id: id.clone(),
            name: name.to_string(),
            description: description.to_string(),
            steps,
            success_rate: 1.0,
            usage_count: 0,
            last_used: SystemTime::now(),
            trigger_contexts: Vec::new(),
        };

        self.skills.write().insert(name.to_string(), skill);
        id
    }

    /// Retrieves a skill by name.
    pub fn get_skill(&self, name: &str) -> Option<Skill> {
        self.skills.read().get(name).cloned()
    }

    /// Updates a skill's success rate based on feedback.
    pub fn improve_skill(&self, name: &str, feedback: SkillFeedback) {
        let mut skills = self.skills.write();
        if let Some(skill) = skills.get_mut(name) {
            skill.usage_count += 1;
            skill.last_used = SystemTime::now();

            // Exponential moving average for success rate
            let outcome = if feedback.success { 1.0 } else { 0.0 };
            let alpha = 0.3; // Weight of new observation
            skill.success_rate = skill.success_rate * (1.0 - alpha) + outcome * alpha;
        }
    }

    /// Suggests the best skill for a given context string.
    /// Matches against skill trigger contexts and descriptions.
    pub fn suggest_skill(&self, context: &str) -> Option<Skill> {
        let skills = self.skills.read();
        let context_lower = context.to_lowercase();

        skills
            .values()
            .filter(|skill| {
                // Match if any trigger context is a substring of the query context
                skill
                    .trigger_contexts
                    .iter()
                    .any(|tc| context_lower.contains(&tc.to_lowercase()))
                    || skill.description.to_lowercase().contains(&context_lower)
                    || context_lower.contains(&skill.name.to_lowercase())
            })
            .max_by(|a, b| {
                a.success_rate
                    .partial_cmp(&b.success_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Lists all skills with their names and success rates.
    pub fn list_skills(&self) -> Vec<(String, f64)> {
        self.skills
            .read()
            .values()
            .map(|s| (s.name.clone(), s.success_rate))
            .collect()
    }

    /// Returns the most recent actions, up to `limit`.
    pub fn recent_actions(&self, limit: usize) -> Vec<ActionRecord> {
        let log = self.action_log.read();
        log.iter().rev().take(limit).cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_step(tool: &str) -> ActionStep {
        ActionStep {
            tool: tool.to_string(),
            parameters: HashMap::new(),
            expected_outcome: None,
        }
    }

    fn make_action(name: &str, success: bool) -> ActionRecord {
        ActionRecord {
            timestamp: SystemTime::now(),
            action: name.to_string(),
            tool: None,
            input: "test input".to_string(),
            output: None,
            success,
            duration: Duration::from_millis(100),
        }
    }

    #[test]
    fn test_learn_and_get_skill() {
        let mem = ProceduralMemory::new(100);

        let steps = vec![make_step("search"), make_step("code_edit")];
        let id = mem.learn_skill("find_and_fix", steps, "Search and fix code");

        assert!(!id.is_empty());

        let skill = mem.get_skill("find_and_fix").unwrap();
        assert_eq!(skill.name, "find_and_fix");
        assert_eq!(skill.steps.len(), 2);
        assert_eq!(skill.success_rate, 1.0);
    }

    #[test]
    fn test_get_nonexistent_skill() {
        let mem = ProceduralMemory::new(100);
        assert!(mem.get_skill("nonexistent").is_none());
    }

    #[test]
    fn test_improve_skill() {
        let mem = ProceduralMemory::new(100);
        mem.learn_skill("test_skill", vec![make_step("run")], "A test skill");

        // Record some failures to bring down success rate
        for _ in 0..5 {
            mem.improve_skill(
                "test_skill",
                SkillFeedback {
                    success: false,
                    notes: None,
                },
            );
        }

        let skill = mem.get_skill("test_skill").unwrap();
        assert!(skill.success_rate < 1.0);
        assert_eq!(skill.usage_count, 5);
    }

    #[test]
    fn test_suggest_skill() {
        let mem = ProceduralMemory::new(100);
        mem.learn_skill(
            "code_review",
            vec![make_step("lint")],
            "Review code quality",
        );

        // Add trigger contexts
        {
            let mut skills = mem.skills.write();
            if let Some(skill) = skills.get_mut("code_review") {
                skill.trigger_contexts.push("review".to_string());
            }
        }

        let suggested = mem.suggest_skill("review");
        assert!(suggested.is_some());
        assert_eq!(suggested.unwrap().name, "code_review");

        let none = mem.suggest_skill("completely_unrelated_xyz_123");
        assert!(none.is_none());
    }

    #[test]
    fn test_record_and_recall_actions() {
        let mem = ProceduralMemory::new(5);

        for i in 0..7 {
            mem.record_action(make_action(&format!("action_{i}"), true));
        }

        let recent = mem.recent_actions(3);
        assert_eq!(recent.len(), 3);
        // Most recent first
        assert_eq!(recent[0].action, "action_6");
    }

    #[test]
    fn test_max_log_size() {
        let mem = ProceduralMemory::new(3);
        for i in 0..10 {
            mem.record_action(make_action(&format!("a{i}"), true));
        }

        let all = mem.recent_actions(100);
        assert!(all.len() <= 3);
    }

    #[test]
    fn test_list_skills() {
        let mem = ProceduralMemory::new(100);
        mem.learn_skill("skill_a", vec![], "A");
        mem.learn_skill("skill_b", vec![], "B");

        let list = mem.list_skills();
        assert_eq!(list.len(), 2);

        let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"skill_a"));
        assert!(names.contains(&"skill_b"));
    }

    #[test]
    fn test_skill_success_rate_update() {
        let mem = ProceduralMemory::new(100);
        mem.learn_skill("s", vec![], "test");

        // Success -> success rate stays high
        mem.improve_skill(
            "s",
            SkillFeedback {
                success: true,
                notes: None,
            },
        );
        let rate = mem.get_skill("s").unwrap().success_rate;
        assert!((rate - 1.0).abs() < 1e-6);

        // Failure -> success rate decreases
        mem.improve_skill(
            "s",
            SkillFeedback {
                success: false,
                notes: Some("failed due to timeout".into()),
            },
        );
        let rate = mem.get_skill("s").unwrap().success_rate;
        assert!(rate < 1.0);
    }
}
