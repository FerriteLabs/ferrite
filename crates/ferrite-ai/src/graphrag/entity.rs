//! Entity extraction for GraphRAG
//!
//! Extracts entities (people, places, organizations, etc.) from text.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Type of entity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    /// Person name
    Person,
    /// Organization/company
    Organization,
    /// Location/place
    Location,
    /// Date or time
    DateTime,
    /// Product name
    Product,
    /// Event
    Event,
    /// Concept/topic
    Concept,
    /// Technical term
    Technical,
    /// Custom entity type
    Custom,
}

impl EntityType {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityType::Person => "person",
            EntityType::Organization => "organization",
            EntityType::Location => "location",
            EntityType::DateTime => "datetime",
            EntityType::Product => "product",
            EntityType::Event => "event",
            EntityType::Concept => "concept",
            EntityType::Technical => "technical",
            EntityType::Custom => "custom",
        }
    }

    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "person" | "per" => Some(EntityType::Person),
            "organization" | "org" | "company" => Some(EntityType::Organization),
            "location" | "loc" | "place" => Some(EntityType::Location),
            "datetime" | "date" | "time" => Some(EntityType::DateTime),
            "product" => Some(EntityType::Product),
            "event" => Some(EntityType::Event),
            "concept" | "topic" => Some(EntityType::Concept),
            "technical" | "tech" => Some(EntityType::Technical),
            _ => None,
        }
    }
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// An extracted entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Entity ID
    pub id: String,
    /// Entity name/text
    pub name: String,
    /// Entity type
    pub entity_type: EntityType,
    /// Confidence score (0-1)
    pub confidence: f32,
    /// Start position in source text
    pub start: usize,
    /// End position in source text
    pub end: usize,
    /// Normalized form
    pub normalized: Option<String>,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl Entity {
    /// Create a new entity
    pub fn new(name: impl Into<String>, entity_type: EntityType) -> Self {
        let name = name.into();
        let id = generate_entity_id(&name, entity_type);

        Self {
            id,
            name,
            entity_type,
            confidence: 1.0,
            start: 0,
            end: 0,
            normalized: None,
            attributes: HashMap::new(),
        }
    }

    /// Set confidence score
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Set position
    pub fn with_position(mut self, start: usize, end: usize) -> Self {
        self.start = start;
        self.end = end;
        self
    }

    /// Set normalized form
    pub fn with_normalized(mut self, normalized: impl Into<String>) -> Self {
        self.normalized = Some(normalized.into());
        self
    }

    /// Add attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Get display name
    pub fn display_name(&self) -> &str {
        self.normalized.as_deref().unwrap_or(&self.name)
    }
}

/// Entity extractor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorConfig {
    /// Minimum confidence threshold
    pub min_confidence: f32,
    /// Entity types to extract
    pub entity_types: Vec<EntityType>,
    /// Enable coreference resolution
    pub resolve_coreferences: bool,
    /// Maximum entities per document
    pub max_entities: usize,
    /// Custom patterns for extraction
    pub custom_patterns: Vec<PatternRule>,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.5,
            entity_types: vec![
                EntityType::Person,
                EntityType::Organization,
                EntityType::Location,
                EntityType::Product,
                EntityType::Concept,
                EntityType::Technical,
            ],
            resolve_coreferences: true,
            max_entities: 100,
            custom_patterns: Vec::new(),
        }
    }
}

/// Custom extraction pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternRule {
    /// Rule name
    pub name: String,
    /// Regex pattern
    pub pattern: String,
    /// Entity type for matches
    pub entity_type: EntityType,
    /// Confidence for matches
    pub confidence: f32,
}

/// Entity extractor
pub struct EntityExtractor {
    config: ExtractorConfig,
}

impl EntityExtractor {
    /// Create a new entity extractor
    pub fn new(config: ExtractorConfig) -> Self {
        Self { config }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(ExtractorConfig::default())
    }

    /// Extract entities from text
    pub fn extract(&self, text: &str) -> Vec<Entity> {
        let mut entities = Vec::new();

        // Simple rule-based extraction for demonstration
        // In production, this would use NER models (spaCy, HuggingFace, etc.)

        // Extract organizations (capitalized words followed by Inc/Corp/Ltd/etc.)
        entities.extend(self.extract_organizations(text));

        // Extract persons (capitalized name patterns)
        entities.extend(self.extract_persons(text));

        // Extract locations
        entities.extend(self.extract_locations(text));

        // Extract technical terms (camelCase, ACRONYMS)
        entities.extend(self.extract_technical(text));

        // Apply custom patterns
        entities.extend(self.apply_custom_patterns(text));

        // Filter by confidence
        entities.retain(|e| e.confidence >= self.config.min_confidence);

        // Limit entities
        if entities.len() > self.config.max_entities {
            entities.sort_by(|a, b| {
                b.confidence
                    .partial_cmp(&a.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            entities.truncate(self.config.max_entities);
        }

        // Deduplicate by normalized name
        self.deduplicate(entities)
    }

    /// Extract organization entities
    fn extract_organizations(&self, text: &str) -> Vec<Entity> {
        if !self.config.entity_types.contains(&EntityType::Organization) {
            return Vec::new();
        }

        let mut entities = Vec::new();
        let org_suffixes = [
            "Inc",
            "Corp",
            "Ltd",
            "LLC",
            "Company",
            "Co",
            "Group",
            "Foundation",
        ];

        for suffix in &org_suffixes {
            let pattern = format!(" {}", suffix);
            let mut search_start = 0;

            while let Some(pos) = text[search_start..].find(&pattern) {
                let abs_pos = search_start + pos;
                // Find the start of the organization name
                let name_start = text[..abs_pos]
                    .rfind(|c: char| c == '.' || c == ',' || c == '\n' || c == '(' || abs_pos == 0)
                    .map(|p| p + 1)
                    .unwrap_or(0);

                let name_end = abs_pos + suffix.len() + 1;
                if name_start < name_end && name_end <= text.len() {
                    let name = text[name_start..name_end].trim();
                    if !name.is_empty()
                        && name
                            .chars()
                            .next()
                            .map(|c| c.is_uppercase())
                            .unwrap_or(false)
                    {
                        entities.push(
                            Entity::new(name, EntityType::Organization)
                                .with_position(name_start, name_end)
                                .with_confidence(0.85),
                        );
                    }
                }

                search_start = abs_pos + 1;
            }
        }

        entities
    }

    /// Extract person entities
    fn extract_persons(&self, text: &str) -> Vec<Entity> {
        if !self.config.entity_types.contains(&EntityType::Person) {
            return Vec::new();
        }

        let mut entities = Vec::new();
        let titles = ["Mr", "Mrs", "Ms", "Dr", "Prof", "CEO", "CTO", "President"];

        for title in &titles {
            let pattern = format!("{}. ", title);
            let mut search_start = 0;

            while let Some(pos) = text[search_start..].find(&pattern) {
                let abs_pos = search_start + pos;
                let name_start = abs_pos + pattern.len();

                // Find end of name (next punctuation or lowercase word)
                let remaining = &text[name_start..];
                let name_end = remaining
                    .find([',', '.', '\n', '(', ':'])
                    .unwrap_or(remaining.len().min(50));

                if name_end > 0 {
                    let name = remaining[..name_end].trim();
                    // Check if it looks like a name (capitalized words)
                    if name
                        .split_whitespace()
                        .all(|w| w.chars().next().map(|c| c.is_uppercase()).unwrap_or(false))
                    {
                        let full_name = format!("{} {}", title, name);
                        entities.push(
                            Entity::new(&full_name, EntityType::Person)
                                .with_position(abs_pos, name_start + name_end)
                                .with_confidence(0.9),
                        );
                    }
                }

                search_start = abs_pos + 1;
            }
        }

        entities
    }

    /// Extract location entities
    fn extract_locations(&self, text: &str) -> Vec<Entity> {
        if !self.config.entity_types.contains(&EntityType::Location) {
            return Vec::new();
        }

        let mut entities = Vec::new();
        let location_markers = ["in", "at", "from", "to", "near"];

        for marker in &location_markers {
            let pattern = format!(" {} ", marker);
            let mut search_start = 0;

            while let Some(pos) = text[search_start..].find(&pattern) {
                let abs_pos = search_start + pos + pattern.len();

                // Check for capitalized word following the marker
                let remaining = &text[abs_pos..];
                if remaining
                    .chars()
                    .next()
                    .map(|c| c.is_uppercase())
                    .unwrap_or(false)
                {
                    let end = remaining
                        .find(|c: char| !c.is_alphabetic() && c != ' ')
                        .unwrap_or(remaining.len().min(30));

                    let location = remaining[..end].trim();
                    if !location.is_empty()
                        && location
                            .split_whitespace()
                            .all(|w| w.chars().next().map(|c| c.is_uppercase()).unwrap_or(false))
                    {
                        entities.push(
                            Entity::new(location, EntityType::Location)
                                .with_position(abs_pos, abs_pos + end)
                                .with_confidence(0.7),
                        );
                    }
                }

                search_start = abs_pos;
            }
        }

        entities
    }

    /// Extract technical terms
    fn extract_technical(&self, text: &str) -> Vec<Entity> {
        if !self.config.entity_types.contains(&EntityType::Technical) {
            return Vec::new();
        }

        let mut entities = Vec::new();

        // Find camelCase and PascalCase words
        let mut current_word = String::new();
        let mut word_start = 0;

        for (i, c) in text.char_indices() {
            if c.is_alphanumeric() || c == '_' {
                if current_word.is_empty() {
                    word_start = i;
                }
                current_word.push(c);
            } else {
                if current_word.len() > 2 && is_technical_term(&current_word) {
                    entities.push(
                        Entity::new(&current_word, EntityType::Technical)
                            .with_position(word_start, i)
                            .with_confidence(0.6),
                    );
                }
                current_word.clear();
            }
        }

        if current_word.len() > 2 && is_technical_term(&current_word) {
            entities.push(
                Entity::new(&current_word, EntityType::Technical)
                    .with_position(word_start, text.len())
                    .with_confidence(0.6),
            );
        }

        entities
    }

    /// Apply custom extraction patterns
    fn apply_custom_patterns(&self, text: &str) -> Vec<Entity> {
        let mut entities = Vec::new();

        for rule in &self.config.custom_patterns {
            if let Ok(re) = regex::Regex::new(&rule.pattern) {
                for cap in re.find_iter(text) {
                    entities.push(
                        Entity::new(cap.as_str(), rule.entity_type)
                            .with_position(cap.start(), cap.end())
                            .with_confidence(rule.confidence),
                    );
                }
            }
        }

        entities
    }

    /// Deduplicate entities by normalized name
    fn deduplicate(&self, entities: Vec<Entity>) -> Vec<Entity> {
        let mut seen: HashMap<String, Entity> = HashMap::new();

        for entity in entities {
            let key = entity.display_name().to_lowercase();
            if let Some(existing) = seen.get_mut(&key) {
                // Keep the one with higher confidence
                if entity.confidence > existing.confidence {
                    *existing = entity;
                }
            } else {
                seen.insert(key, entity);
            }
        }

        seen.into_values().collect()
    }

    /// Get configuration
    pub fn config(&self) -> &ExtractorConfig {
        &self.config
    }
}

impl Default for EntityExtractor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Check if word looks like a technical term
fn is_technical_term(word: &str) -> bool {
    // All uppercase (acronym)
    if word.chars().all(|c| c.is_uppercase() || c.is_numeric()) {
        return word.len() >= 2;
    }

    // camelCase or PascalCase (has mix of upper and lower)
    let has_upper = word.chars().any(|c| c.is_uppercase());
    let has_lower = word.chars().any(|c| c.is_lowercase());

    if has_upper && has_lower {
        let chars: Vec<char> = word.chars().collect();
        for i in 1..chars.len() {
            if chars[i - 1].is_lowercase() && chars[i].is_uppercase() {
                return true;
            }
        }
        return word.len() >= 4;
    }

    false
}

/// Generate entity ID
fn generate_entity_id(name: &str, entity_type: EntityType) -> String {
    let normalized: String = name
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect();

    format!("{}:{}", entity_type.as_str(), normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_creation() {
        let entity = Entity::new("Apple Inc", EntityType::Organization)
            .with_confidence(0.95)
            .with_position(10, 19);

        assert_eq!(entity.name, "Apple Inc");
        assert_eq!(entity.entity_type, EntityType::Organization);
        assert!((entity.confidence - 0.95).abs() < 0.01);
    }

    #[test]
    fn test_entity_type_display() {
        assert_eq!(EntityType::Person.to_string(), "person");
        assert_eq!(EntityType::Organization.to_string(), "organization");
    }

    #[test]
    fn test_extract_organizations() {
        let extractor = EntityExtractor::with_defaults();
        let text = "Apple Inc. released a new product. Microsoft Corp announced updates.";
        let entities = extractor.extract(text);

        let orgs: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Organization)
            .collect();

        assert!(!orgs.is_empty());
    }

    #[test]
    fn test_extract_technical_terms() {
        let extractor = EntityExtractor::with_defaults();
        let text = "Use the GraphRAG system with PostgreSQL database.";
        let entities = extractor.extract(text);

        let tech: Vec<_> = entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Technical)
            .collect();

        assert!(!tech.is_empty());
    }

    #[test]
    fn test_is_technical_term() {
        assert!(is_technical_term("GraphRAG"));
        assert!(is_technical_term("PostgreSQL"));
        assert!(is_technical_term("API"));
        assert!(!is_technical_term("the"));
        assert!(!is_technical_term("hello"));
    }
}
