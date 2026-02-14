//! Tutorial System
//!
//! Interactive tutorials for learning Ferrite through the playground.

use super::{Difficulty, PlaygroundError, TutorialInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

/// A tutorial with steps
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tutorial {
    /// Tutorial ID
    pub id: String,
    /// Title
    pub title: String,
    /// Description
    pub description: String,
    /// Difficulty level
    pub difficulty: Difficulty,
    /// Estimated time in minutes
    pub estimated_minutes: u32,
    /// Tutorial steps
    pub steps: Vec<TutorialStep>,
    /// Tags
    pub tags: Vec<String>,
}

impl Tutorial {
    /// Create a new tutorial
    pub fn new(id: &str, title: &str, description: &str) -> Self {
        Self {
            id: id.to_string(),
            title: title.to_string(),
            description: description.to_string(),
            difficulty: Difficulty::Beginner,
            estimated_minutes: 5,
            steps: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Create a builder
    pub fn builder(id: &str) -> TutorialBuilder {
        TutorialBuilder::new(id)
    }

    /// Get tutorial info (summary)
    pub fn info(&self) -> TutorialInfo {
        TutorialInfo {
            id: self.id.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            difficulty: self.difficulty.clone(),
            estimated_minutes: self.estimated_minutes,
            step_count: self.steps.len(),
            tags: self.tags.clone(),
        }
    }
}

/// A single step in a tutorial
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TutorialStep {
    /// Step title
    pub title: String,
    /// Step description/instructions
    pub description: String,
    /// Command to execute (if any)
    pub command: Option<String>,
    /// Expected result (for validation)
    pub expected_result: Option<String>,
    /// Hints for the user
    pub hints: Vec<String>,
}

impl TutorialStep {
    /// Create a new step
    pub fn new(title: &str, description: &str) -> Self {
        Self {
            title: title.to_string(),
            description: description.to_string(),
            command: None,
            expected_result: None,
            hints: Vec::new(),
        }
    }

    /// Set command
    pub fn with_command(mut self, cmd: &str) -> Self {
        self.command = Some(cmd.to_string());
        self
    }

    /// Set expected result
    pub fn with_expected(mut self, result: &str) -> Self {
        self.expected_result = Some(result.to_string());
        self
    }

    /// Add hint
    pub fn with_hint(mut self, hint: &str) -> Self {
        self.hints.push(hint.to_string());
        self
    }
}

/// Builder for tutorials
pub struct TutorialBuilder {
    id: String,
    title: String,
    description: String,
    difficulty: Difficulty,
    estimated_minutes: u32,
    steps: Vec<TutorialStep>,
    tags: Vec<String>,
}

impl TutorialBuilder {
    /// Create new builder
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            title: String::new(),
            description: String::new(),
            difficulty: Difficulty::Beginner,
            estimated_minutes: 5,
            steps: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Set title
    pub fn title(mut self, title: &str) -> Self {
        self.title = title.to_string();
        self
    }

    /// Set description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Set difficulty
    pub fn difficulty(mut self, difficulty: Difficulty) -> Self {
        self.difficulty = difficulty;
        self
    }

    /// Set estimated time
    pub fn estimated_minutes(mut self, minutes: u32) -> Self {
        self.estimated_minutes = minutes;
        self
    }

    /// Add a step
    pub fn step(mut self, step: TutorialStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Add a simple step (title, description, command)
    pub fn add_step(mut self, title: &str, desc: &str, cmd: Option<&str>) -> Self {
        let mut step = TutorialStep::new(title, desc);
        if let Some(c) = cmd {
            step.command = Some(c.to_string());
        }
        self.steps.push(step);
        self
    }

    /// Add tag
    pub fn tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Build the tutorial
    pub fn build(self) -> Tutorial {
        Tutorial {
            id: self.id,
            title: self.title,
            description: self.description,
            difficulty: self.difficulty,
            estimated_minutes: self.estimated_minutes,
            steps: self.steps,
            tags: self.tags,
        }
    }
}

/// Manages tutorials
pub struct TutorialManager {
    /// Tutorials by ID
    tutorials: RwLock<HashMap<String, Tutorial>>,
}

impl TutorialManager {
    /// Create a new tutorial manager
    pub fn new() -> Self {
        let manager = Self {
            tutorials: RwLock::new(HashMap::new()),
        };
        manager.load_builtin_tutorials();
        manager
    }

    /// Get a tutorial by ID
    pub fn get(&self, id: &str) -> Result<Tutorial, PlaygroundError> {
        let tutorials = self
            .tutorials
            .read()
            .map_err(|_| PlaygroundError::Runtime("lock error".to_string()))?;

        tutorials
            .get(id)
            .cloned()
            .ok_or_else(|| PlaygroundError::TutorialNotFound(id.to_string()))
    }

    /// List all tutorials
    pub fn list(&self) -> Vec<TutorialInfo> {
        let tutorials = self.tutorials.read().expect("tutorials lock");
        tutorials.values().map(|t| t.info()).collect()
    }

    /// List tutorials by difficulty
    pub fn by_difficulty(&self, difficulty: Difficulty) -> Vec<TutorialInfo> {
        let tutorials = self.tutorials.read().expect("tutorials lock");
        tutorials
            .values()
            .filter(|t| t.difficulty == difficulty)
            .map(|t| t.info())
            .collect()
    }

    /// List tutorials by tag
    pub fn by_tag(&self, tag: &str) -> Vec<TutorialInfo> {
        let tag_lower = tag.to_lowercase();
        let tutorials = self.tutorials.read().expect("tutorials lock");
        tutorials
            .values()
            .filter(|t| t.tags.iter().any(|t| t.to_lowercase() == tag_lower))
            .map(|t| t.info())
            .collect()
    }

    /// Add a custom tutorial
    pub fn add(&self, tutorial: Tutorial) -> Result<(), PlaygroundError> {
        let mut tutorials = self
            .tutorials
            .write()
            .map_err(|_| PlaygroundError::Runtime("lock error".to_string()))?;
        tutorials.insert(tutorial.id.clone(), tutorial);
        Ok(())
    }

    /// Load built-in tutorials
    fn load_builtin_tutorials(&self) {
        let tutorials = vec![
            // Getting Started
            Tutorial::builder("getting-started")
                .title("Getting Started with Ferrite")
                .description("Learn the basics of Ferrite - a Redis-compatible key-value store")
                .difficulty(Difficulty::Beginner)
                .estimated_minutes(5)
                .tag("basics")
                .step(TutorialStep::new(
                    "Welcome",
                    "Welcome to Ferrite! This tutorial will teach you the basics of working with \
                     key-value data. Ferrite is fully compatible with Redis, so if you know Redis, \
                     you already know Ferrite!",
                ))
                .step(TutorialStep::new(
                    "Your First Command",
                    "Let's start with the PING command. It's the simplest command - it just returns PONG. \
                     This is useful for testing if the server is responding.",
                ).with_command("PING").with_expected("PONG"))
                .step(TutorialStep::new(
                    "Storing Data",
                    "Use the SET command to store a value. The syntax is: SET key value",
                ).with_command("SET greeting \"Hello, World!\""))
                .step(TutorialStep::new(
                    "Retrieving Data",
                    "Use GET to retrieve the value you just stored.",
                ).with_command("GET greeting").with_expected("Hello, World!"))
                .step(TutorialStep::new(
                    "Deleting Data",
                    "Use DEL to remove a key. It returns the number of keys deleted.",
                ).with_command("DEL greeting"))
                .step(TutorialStep::new(
                    "Congratulations!",
                    "You've learned the basic operations: SET, GET, and DEL. \
                     These are the foundation of working with Ferrite!",
                ))
                .build(),

            // Strings Tutorial
            Tutorial::builder("strings")
                .title("Working with Strings")
                .description("Master string operations including counters and atomic operations")
                .difficulty(Difficulty::Beginner)
                .estimated_minutes(10)
                .tag("strings")
                .tag("basics")
                .step(TutorialStep::new(
                    "String Basics",
                    "Strings are the most basic data type in Ferrite. They can hold any data - \
                     text, numbers, or even binary data up to 512MB.",
                ))
                .step(TutorialStep::new(
                    "Multiple Keys",
                    "You can set multiple keys at once with MSET.",
                ).with_command("MSET name \"Alice\" age \"30\" city \"NYC\""))
                .step(TutorialStep::new(
                    "Get Multiple Keys",
                    "Similarly, get multiple values with MGET.",
                ).with_command("MGET name age city"))
                .step(TutorialStep::new(
                    "Atomic Counters",
                    "INCR atomically increments a number. This is perfect for counters!",
                ).with_command("SET visits 0"))
                .step(TutorialStep::new(
                    "Incrementing",
                    "Each INCR is atomic - safe for concurrent access.",
                ).with_command("INCR visits"))
                .step(TutorialStep::new(
                    "Increment By",
                    "Use INCRBY to increment by a specific amount.",
                ).with_command("INCRBY visits 10"))
                .step(TutorialStep::new(
                    "Conditional Set",
                    "SETNX sets a key only if it doesn't exist. Returns 1 if set, 0 if key exists.",
                ).with_command("SETNX name \"Bob\"").with_hint("The key 'name' already exists!"))
                .step(TutorialStep::new(
                    "String Operations",
                    "APPEND adds to the end of a string.",
                ).with_command("APPEND city \" USA\""))
                .step(TutorialStep::new(
                    "String Length",
                    "STRLEN returns the length of the string value.",
                ).with_command("STRLEN city"))
                .build(),

            // Lists Tutorial
            Tutorial::builder("lists")
                .title("Working with Lists")
                .description("Learn to use lists for queues, stacks, and message passing")
                .difficulty(Difficulty::Intermediate)
                .estimated_minutes(10)
                .tag("lists")
                .tag("data-structures")
                .step(TutorialStep::new(
                    "Introduction to Lists",
                    "Lists in Ferrite are linked lists of strings. They're perfect for queues, \
                     stacks, and message passing. Operations at both ends are O(1).",
                ))
                .step(TutorialStep::new(
                    "Adding to a List",
                    "LPUSH adds elements to the left (head) of the list.",
                ).with_command("LPUSH tasks \"task1\" \"task2\" \"task3\""))
                .step(TutorialStep::new(
                    "Right Push",
                    "RPUSH adds to the right (tail) of the list.",
                ).with_command("RPUSH tasks \"task4\""))
                .step(TutorialStep::new(
                    "List Length",
                    "LLEN returns the number of elements.",
                ).with_command("LLEN tasks"))
                .step(TutorialStep::new(
                    "View the List",
                    "LRANGE returns elements from start to stop index. Use -1 for the last element.",
                ).with_command("LRANGE tasks 0 -1"))
                .step(TutorialStep::new(
                    "Pop from List",
                    "LPOP removes and returns the first element (like a queue).",
                ).with_command("LPOP tasks"))
                .step(TutorialStep::new(
                    "Stack Behavior",
                    "Using LPUSH + LPOP gives you stack behavior (LIFO).",
                ).with_command("LPUSH stack \"a\" \"b\" \"c\""))
                .step(TutorialStep::new(
                    "Pop from Stack",
                    "Pop from the same end you pushed.",
                ).with_command("LPOP stack"))
                .step(TutorialStep::new(
                    "Index Access",
                    "LINDEX gets an element by index (0-based). Negative indexes count from the end.",
                ).with_command("LINDEX tasks 0"))
                .build(),

            // Hashes Tutorial
            Tutorial::builder("hashes")
                .title("Working with Hashes")
                .description("Store structured objects using hash data types")
                .difficulty(Difficulty::Intermediate)
                .estimated_minutes(10)
                .tag("hashes")
                .tag("data-structures")
                .step(TutorialStep::new(
                    "Introduction to Hashes",
                    "Hashes are maps of field-value pairs. They're ideal for storing objects \
                     like user profiles, where you want to access individual fields.",
                ))
                .step(TutorialStep::new(
                    "Creating a Hash",
                    "HSET sets one or more field-value pairs in a hash.",
                ).with_command("HSET user:1 name \"Alice\" email \"alice@example.com\" age \"30\""))
                .step(TutorialStep::new(
                    "Get a Field",
                    "HGET retrieves a single field value.",
                ).with_command("HGET user:1 name"))
                .step(TutorialStep::new(
                    "Get All Fields",
                    "HGETALL returns all fields and values.",
                ).with_command("HGETALL user:1"))
                .step(TutorialStep::new(
                    "Get Only Keys",
                    "HKEYS returns just the field names.",
                ).with_command("HKEYS user:1"))
                .step(TutorialStep::new(
                    "Get Only Values",
                    "HVALS returns just the values.",
                ).with_command("HVALS user:1"))
                .step(TutorialStep::new(
                    "Check Field Existence",
                    "HEXISTS checks if a field exists.",
                ).with_command("HEXISTS user:1 email"))
                .step(TutorialStep::new(
                    "Delete Fields",
                    "HDEL removes fields from a hash.",
                ).with_command("HDEL user:1 age"))
                .step(TutorialStep::new(
                    "Hash Length",
                    "HLEN returns the number of fields.",
                ).with_command("HLEN user:1"))
                .build(),

            // Sets Tutorial
            Tutorial::builder("sets")
                .title("Working with Sets")
                .description("Use sets for unique collections and membership testing")
                .difficulty(Difficulty::Intermediate)
                .estimated_minutes(8)
                .tag("sets")
                .tag("data-structures")
                .step(TutorialStep::new(
                    "Introduction to Sets",
                    "Sets are unordered collections of unique strings. They're great for \
                     tracking unique items, tags, and performing set operations.",
                ))
                .step(TutorialStep::new(
                    "Adding Members",
                    "SADD adds members to a set. Duplicates are automatically ignored.",
                ).with_command("SADD tags \"redis\" \"database\" \"nosql\" \"fast\""))
                .step(TutorialStep::new(
                    "Adding Duplicates",
                    "Try adding a duplicate - notice it returns 0 (no new members added).",
                ).with_command("SADD tags \"redis\""))
                .step(TutorialStep::new(
                    "View Members",
                    "SMEMBERS returns all members of the set.",
                ).with_command("SMEMBERS tags"))
                .step(TutorialStep::new(
                    "Check Membership",
                    "SISMEMBER checks if a value is in the set.",
                ).with_command("SISMEMBER tags \"redis\""))
                .step(TutorialStep::new(
                    "Set Size",
                    "SCARD returns the number of members (cardinality).",
                ).with_command("SCARD tags"))
                .step(TutorialStep::new(
                    "Remove Members",
                    "SREM removes members from the set.",
                ).with_command("SREM tags \"nosql\""))
                .build(),

            // Sorted Sets Tutorial
            Tutorial::builder("sorted-sets")
                .title("Working with Sorted Sets")
                .description("Build leaderboards and priority queues with sorted sets")
                .difficulty(Difficulty::Advanced)
                .estimated_minutes(12)
                .tag("sorted-sets")
                .tag("data-structures")
                .tag("leaderboards")
                .step(TutorialStep::new(
                    "Introduction",
                    "Sorted sets are like sets, but each member has a score. Members are \
                     automatically sorted by score. Perfect for leaderboards and priority queues!",
                ))
                .step(TutorialStep::new(
                    "Adding Scored Members",
                    "ZADD adds members with their scores.",
                ).with_command("ZADD leaderboard 100 \"player1\" 250 \"player2\" 175 \"player3\""))
                .step(TutorialStep::new(
                    "View by Rank",
                    "ZRANGE shows members by rank (lowest to highest score by default).",
                ).with_command("ZRANGE leaderboard 0 -1"))
                .step(TutorialStep::new(
                    "View with Scores",
                    "Add WITHSCORES to see the scores too.",
                ).with_command("ZRANGE leaderboard 0 -1 WITHSCORES"))
                .step(TutorialStep::new(
                    "Get Score",
                    "ZSCORE returns the score of a member.",
                ).with_command("ZSCORE leaderboard player2"))
                .step(TutorialStep::new(
                    "Get Rank",
                    "ZRANK returns the rank (0-indexed, lowest score = rank 0).",
                ).with_command("ZRANK leaderboard player1"))
                .step(TutorialStep::new(
                    "Update Score",
                    "ZADD with an existing member updates the score.",
                ).with_command("ZADD leaderboard 300 \"player1\""))
                .step(TutorialStep::new(
                    "Check New Ranking",
                    "Now player1 should be ranked higher!",
                ).with_command("ZRANGE leaderboard 0 -1 WITHSCORES"))
                .step(TutorialStep::new(
                    "Cardinality",
                    "ZCARD returns the number of members.",
                ).with_command("ZCARD leaderboard"))
                .build(),
        ];

        let mut store = self.tutorials.write().expect("tutorials lock");
        for tutorial in tutorials {
            store.insert(tutorial.id.clone(), tutorial);
        }
    }
}

impl Default for TutorialManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tutorial_creation() {
        let tutorial = Tutorial::new("test", "Test", "A test tutorial");
        assert_eq!(tutorial.id, "test");
        assert_eq!(tutorial.steps.len(), 0);
    }

    #[test]
    fn test_tutorial_builder() {
        let tutorial = Tutorial::builder("test")
            .title("Test Tutorial")
            .description("Testing")
            .difficulty(Difficulty::Intermediate)
            .estimated_minutes(15)
            .step(TutorialStep::new("Step 1", "First step").with_command("PING"))
            .tag("test")
            .build();

        assert_eq!(tutorial.title, "Test Tutorial");
        assert_eq!(tutorial.difficulty, Difficulty::Intermediate);
        assert_eq!(tutorial.steps.len(), 1);
    }

    #[test]
    fn test_tutorial_manager() {
        let manager = TutorialManager::new();

        // Should have builtin tutorials
        let all = manager.list();
        assert!(!all.is_empty());

        // Test get
        let tutorial = manager.get("getting-started").unwrap();
        assert!(!tutorial.steps.is_empty());

        // Test by difficulty
        let beginner = manager.by_difficulty(Difficulty::Beginner);
        assert!(!beginner.is_empty());
    }

    #[test]
    fn test_tutorial_step() {
        let step = TutorialStep::new("Test", "Description")
            .with_command("SET foo bar")
            .with_expected("OK")
            .with_hint("Try SET command");

        assert_eq!(step.command, Some("SET foo bar".to_string()));
        assert_eq!(step.expected_result, Some("OK".to_string()));
        assert_eq!(step.hints.len(), 1);
    }

    #[test]
    fn test_tutorial_info() {
        let tutorial = Tutorial::builder("info-test")
            .title("Info Test")
            .description("Test info method")
            .difficulty(Difficulty::Advanced)
            .estimated_minutes(20)
            .step(TutorialStep::new("S1", "Step 1"))
            .step(TutorialStep::new("S2", "Step 2"))
            .tag("test")
            .build();

        let info = tutorial.info();
        assert_eq!(info.id, "info-test");
        assert_eq!(info.title, "Info Test");
        assert_eq!(info.difficulty, Difficulty::Advanced);
        assert_eq!(info.step_count, 2);
        assert_eq!(info.estimated_minutes, 20);
    }

    #[test]
    fn test_tutorial_manager_get_not_found() {
        let manager = TutorialManager::new();
        assert!(manager.get("nonexistent-tutorial").is_err());
    }

    #[test]
    fn test_tutorial_manager_by_tag() {
        let manager = TutorialManager::new();
        let tagged = manager.by_tag("basics");
        assert!(!tagged.is_empty());
    }

    #[test]
    fn test_tutorial_manager_add_custom() {
        let manager = TutorialManager::new();
        let count_before = manager.list().len();
        let tutorial = Tutorial::builder("custom")
            .title("Custom Tutorial")
            .description("A custom tutorial")
            .build();
        manager.add(tutorial).unwrap();
        assert_eq!(manager.list().len(), count_before + 1);
    }

    #[test]
    fn test_tutorial_manager_default() {
        let manager = TutorialManager::default();
        assert!(!manager.list().is_empty());
    }

    #[test]
    fn test_builtin_tutorials_content() {
        let manager = TutorialManager::new();

        // Getting Started
        let gs = manager.get("getting-started").unwrap();
        assert!(!gs.steps.is_empty());
        assert_eq!(gs.difficulty, Difficulty::Beginner);

        // Strings
        let strings = manager.get("strings").unwrap();
        assert!(!strings.steps.is_empty());

        // Lists
        let lists = manager.get("lists").unwrap();
        assert!(!lists.steps.is_empty());
        assert_eq!(lists.difficulty, Difficulty::Intermediate);

        // Hashes
        let hashes = manager.get("hashes").unwrap();
        assert!(!hashes.steps.is_empty());

        // Sets
        let sets = manager.get("sets").unwrap();
        assert!(!sets.steps.is_empty());

        // Sorted sets
        let zsets = manager.get("sorted-sets").unwrap();
        assert!(!zsets.steps.is_empty());
        assert_eq!(zsets.difficulty, Difficulty::Advanced);
    }

    #[test]
    fn test_tutorial_builder_add_step() {
        let tutorial = Tutorial::builder("test")
            .title("Test")
            .description("Test")
            .add_step("Step 1", "Description 1", Some("PING"))
            .add_step("Step 2", "Description 2", None)
            .build();
        assert_eq!(tutorial.steps.len(), 2);
        assert!(tutorial.steps[0].command.is_some());
        assert!(tutorial.steps[1].command.is_none());
    }

    #[test]
    fn test_tutorial_by_difficulty_intermediate() {
        let manager = TutorialManager::new();
        let intermediate = manager.by_difficulty(Difficulty::Intermediate);
        assert!(!intermediate.is_empty());
        for info in &intermediate {
            assert_eq!(info.difficulty, Difficulty::Intermediate);
        }
    }

    #[test]
    fn test_tutorial_content_getting_started_has_commands() {
        let manager = TutorialManager::new();
        let tutorial = manager.get("getting-started").unwrap();
        // Should have steps with commands
        let steps_with_commands: Vec<_> = tutorial
            .steps
            .iter()
            .filter(|s| s.command.is_some())
            .collect();
        assert!(!steps_with_commands.is_empty());
        // Should contain PING, SET, GET, DEL
        let all_commands: Vec<_> = steps_with_commands
            .iter()
            .filter_map(|s| s.command.as_deref())
            .collect();
        assert!(all_commands.iter().any(|c| c.contains("PING")));
        assert!(all_commands.iter().any(|c| c.contains("SET")));
        assert!(all_commands.iter().any(|c| c.contains("GET")));
        assert!(all_commands.iter().any(|c| c.contains("DEL")));
    }

    #[test]
    fn test_tutorial_content_all_tutorials_have_steps() {
        let manager = TutorialManager::new();
        let all = manager.list();
        for info in &all {
            let tutorial = manager.get(&info.id).unwrap();
            assert!(
                !tutorial.steps.is_empty(),
                "Tutorial '{}' has no steps",
                info.id
            );
            assert_eq!(info.step_count, tutorial.steps.len());
        }
    }

    #[test]
    fn test_tutorial_content_strings_tutorial() {
        let manager = TutorialManager::new();
        let tutorial = manager.get("strings").unwrap();
        assert_eq!(tutorial.difficulty, Difficulty::Beginner);
        let commands: Vec<_> = tutorial
            .steps
            .iter()
            .filter_map(|s| s.command.as_deref())
            .collect();
        assert!(commands.iter().any(|c| c.contains("MSET")));
        assert!(commands.iter().any(|c| c.contains("INCR")));
    }

    #[test]
    fn test_tutorial_content_sorted_sets_advanced() {
        let manager = TutorialManager::new();
        let tutorial = manager.get("sorted-sets").unwrap();
        assert_eq!(tutorial.difficulty, Difficulty::Advanced);
        let commands: Vec<_> = tutorial
            .steps
            .iter()
            .filter_map(|s| s.command.as_deref())
            .collect();
        assert!(commands.iter().any(|c| c.contains("ZADD")));
        assert!(commands.iter().any(|c| c.contains("ZRANGE")));
    }

    #[test]
    fn test_tutorial_by_tag_data_structures() {
        let manager = TutorialManager::new();
        let tagged = manager.by_tag("data-structures");
        assert!(!tagged.is_empty());
        // Lists, hashes, sets, and sorted-sets should all have this tag
        assert!(tagged.len() >= 3);
    }

    #[test]
    fn test_tutorial_step_with_hints() {
        let step = TutorialStep::new("Hint Step", "Try this")
            .with_command("SET k v")
            .with_hint("Hint 1")
            .with_hint("Hint 2");
        assert_eq!(step.hints.len(), 2);
        assert_eq!(step.hints[0], "Hint 1");
    }

    #[test]
    fn test_tutorial_by_difficulty_advanced() {
        let manager = TutorialManager::new();
        let advanced = manager.by_difficulty(Difficulty::Advanced);
        assert!(!advanced.is_empty());
        for info in &advanced {
            assert_eq!(info.difficulty, Difficulty::Advanced);
        }
    }
}
