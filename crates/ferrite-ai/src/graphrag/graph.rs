//! Knowledge graph for GraphRAG
//!
//! Stores entities and their relationships for enhanced retrieval.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::entity::{Entity, EntityType};
use super::{GraphRagError, Result};

/// Type of relationship between entities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationType {
    /// Entity mentions document
    MentionedIn,
    /// Entity is related to another
    RelatedTo,
    /// Entity is part of another
    PartOf,
    /// Entity owns or has another
    HasA,
    /// Entity created by another
    CreatedBy,
    /// Entity located at another
    LocatedIn,
    /// Entity works for another
    WorksFor,
    /// Entities co-occur in text
    CoOccurs,
    /// Custom relationship
    Custom,
}

impl RelationType {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationType::MentionedIn => "mentioned_in",
            RelationType::RelatedTo => "related_to",
            RelationType::PartOf => "part_of",
            RelationType::HasA => "has_a",
            RelationType::CreatedBy => "created_by",
            RelationType::LocatedIn => "located_in",
            RelationType::WorksFor => "works_for",
            RelationType::CoOccurs => "co_occurs",
            RelationType::Custom => "custom",
        }
    }
}

impl std::fmt::Display for RelationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A relationship between two entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Source entity ID
    pub source_id: String,
    /// Target entity ID
    pub target_id: String,
    /// Relationship type
    pub relation_type: RelationType,
    /// Relationship strength (0-1)
    pub weight: f32,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

impl Relationship {
    /// Create a new relationship
    pub fn new(
        source_id: impl Into<String>,
        target_id: impl Into<String>,
        relation_type: RelationType,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            target_id: target_id.into(),
            relation_type,
            weight: 1.0,
            properties: HashMap::new(),
        }
    }

    /// Set weight
    pub fn with_weight(mut self, weight: f32) -> Self {
        self.weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Add property
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Knowledge graph configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// Maximum entities to store
    pub max_entities: usize,
    /// Maximum relationships per entity
    pub max_relationships: usize,
    /// Enable co-occurrence relationships
    pub track_cooccurrence: bool,
    /// Minimum co-occurrence count for relationship
    pub min_cooccurrence: usize,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            max_entities: 100_000,
            max_relationships: 1000,
            track_cooccurrence: true,
            min_cooccurrence: 2,
        }
    }
}

/// Node in the knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    /// Entity data
    pub entity: Entity,
    /// Document IDs where entity appears
    pub document_ids: HashSet<String>,
    /// Occurrence count
    pub occurrence_count: u64,
}

impl GraphNode {
    /// Create a new graph node
    pub fn new(entity: Entity) -> Self {
        Self {
            entity,
            document_ids: HashSet::new(),
            occurrence_count: 1,
        }
    }

    /// Add document reference
    pub fn add_document(&mut self, doc_id: impl Into<String>) {
        self.document_ids.insert(doc_id.into());
        self.occurrence_count += 1;
    }
}

/// Knowledge graph for storing entities and relationships
pub struct KnowledgeGraph {
    config: GraphConfig,
    nodes: RwLock<HashMap<String, GraphNode>>,
    outgoing_edges: RwLock<HashMap<String, Vec<Relationship>>>,
    incoming_edges: RwLock<HashMap<String, Vec<Relationship>>>,
    document_entities: RwLock<HashMap<String, Vec<String>>>,
    // Stats
    total_entities: AtomicU64,
    total_relationships: AtomicU64,
}

impl KnowledgeGraph {
    /// Create a new knowledge graph
    pub fn new(config: GraphConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(HashMap::new()),
            outgoing_edges: RwLock::new(HashMap::new()),
            incoming_edges: RwLock::new(HashMap::new()),
            document_entities: RwLock::new(HashMap::new()),
            total_entities: AtomicU64::new(0),
            total_relationships: AtomicU64::new(0),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(GraphConfig::default())
    }

    /// Add an entity to the graph
    pub fn add_entity(&self, entity: Entity, document_id: Option<&str>) -> Result<String> {
        let entity_id = entity.id.clone();
        let mut nodes = self.nodes.write();

        if nodes.len() >= self.config.max_entities {
            return Err(GraphRagError::GraphError(
                "maximum entities reached".to_string(),
            ));
        }

        if let Some(existing) = nodes.get_mut(&entity_id) {
            // Entity exists, update
            if let Some(doc_id) = document_id {
                existing.add_document(doc_id);
            }
        } else {
            // New entity
            let mut node = GraphNode::new(entity);
            if let Some(doc_id) = document_id {
                node.add_document(doc_id);
            }
            nodes.insert(entity_id.clone(), node);
            self.total_entities.fetch_add(1, Ordering::Relaxed);
        }

        // Update document -> entity mapping
        if let Some(doc_id) = document_id {
            self.document_entities
                .write()
                .entry(doc_id.to_string())
                .or_default()
                .push(entity_id.clone());
        }

        Ok(entity_id)
    }

    /// Add a relationship between entities
    pub fn add_relationship(&self, relationship: Relationship) -> Result<()> {
        let source_id = &relationship.source_id;
        let target_id = &relationship.target_id;

        // Verify entities exist
        let nodes = self.nodes.read();
        if !nodes.contains_key(source_id) {
            return Err(GraphRagError::GraphError(format!(
                "source entity not found: {}",
                source_id
            )));
        }
        if !nodes.contains_key(target_id) {
            return Err(GraphRagError::GraphError(format!(
                "target entity not found: {}",
                target_id
            )));
        }
        drop(nodes);

        // Add to outgoing edges
        {
            let mut outgoing = self.outgoing_edges.write();
            let edges = outgoing.entry(source_id.clone()).or_default();
            if edges.len() < self.config.max_relationships {
                edges.push(relationship.clone());
            }
        }

        // Add to incoming edges
        {
            let mut incoming = self.incoming_edges.write();
            let edges = incoming.entry(target_id.clone()).or_default();
            if edges.len() < self.config.max_relationships {
                edges.push(relationship);
            }
        }

        self.total_relationships.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get an entity by ID
    pub fn get_entity(&self, id: &str) -> Option<Entity> {
        self.nodes.read().get(id).map(|n| n.entity.clone())
    }

    /// Get all entities
    pub fn get_entities(&self) -> Vec<Entity> {
        self.nodes
            .read()
            .values()
            .map(|n| n.entity.clone())
            .collect()
    }

    /// Get entities by type
    pub fn get_entities_by_type(&self, entity_type: EntityType) -> Vec<Entity> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.entity.entity_type == entity_type)
            .map(|n| n.entity.clone())
            .collect()
    }

    /// Get entities for a document
    pub fn get_document_entities(&self, doc_id: &str) -> Vec<Entity> {
        let doc_entities = self.document_entities.read();
        let entity_ids = match doc_entities.get(doc_id) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };
        drop(doc_entities);

        let nodes = self.nodes.read();
        entity_ids
            .iter()
            .filter_map(|id| nodes.get(id).map(|n| n.entity.clone()))
            .collect()
    }

    /// Get outgoing relationships for an entity
    pub fn get_outgoing(&self, entity_id: &str) -> Vec<Relationship> {
        self.outgoing_edges
            .read()
            .get(entity_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get incoming relationships for an entity
    pub fn get_incoming(&self, entity_id: &str) -> Vec<Relationship> {
        self.incoming_edges
            .read()
            .get(entity_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get related entities within N hops
    pub fn get_related(&self, entity_id: &str, max_hops: usize) -> Vec<Entity> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = vec![entity_id.to_string()];
        let mut result = Vec::new();

        for _ in 0..max_hops {
            let mut next_frontier = Vec::new();

            for id in &frontier {
                // Get outgoing neighbors
                if let Some(edges) = self.outgoing_edges.read().get(id) {
                    for edge in edges {
                        if visited.insert(edge.target_id.clone()) {
                            next_frontier.push(edge.target_id.clone());
                        }
                    }
                }

                // Get incoming neighbors
                if let Some(edges) = self.incoming_edges.read().get(id) {
                    for edge in edges {
                        if visited.insert(edge.source_id.clone()) {
                            next_frontier.push(edge.source_id.clone());
                        }
                    }
                }
            }

            frontier = next_frontier;
        }

        // Collect entities
        let nodes = self.nodes.read();
        for id in visited {
            if id != entity_id {
                if let Some(node) = nodes.get(&id) {
                    result.push(node.entity.clone());
                }
            }
        }

        result
    }

    /// Find entities by name (partial match)
    pub fn search_entities(&self, query: &str, limit: usize) -> Vec<Entity> {
        let query_lower = query.to_lowercase();

        self.nodes
            .read()
            .values()
            .filter(|n| n.entity.name.to_lowercase().contains(&query_lower))
            .take(limit)
            .map(|n| n.entity.clone())
            .collect()
    }

    /// Create co-occurrence relationships for entities in a document
    pub fn create_cooccurrences(&self, doc_id: &str) -> Result<usize> {
        if !self.config.track_cooccurrence {
            return Ok(0);
        }

        let entity_ids: Vec<String> = self
            .document_entities
            .read()
            .get(doc_id)
            .cloned()
            .unwrap_or_default();

        let mut count = 0;

        // Create pairwise co-occurrence relationships
        for i in 0..entity_ids.len() {
            for j in (i + 1)..entity_ids.len() {
                let rel = Relationship::new(&entity_ids[i], &entity_ids[j], RelationType::CoOccurs)
                    .with_property("document", doc_id);

                if self.add_relationship(rel).is_ok() {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Get graph statistics
    pub fn stats(&self) -> GraphStats {
        GraphStats {
            total_entities: self.total_entities.load(Ordering::Relaxed),
            total_relationships: self.total_relationships.load(Ordering::Relaxed),
            total_documents: self.document_entities.read().len() as u64,
        }
    }

    /// Clear all data
    pub fn clear(&self) {
        self.nodes.write().clear();
        self.outgoing_edges.write().clear();
        self.incoming_edges.write().clear();
        self.document_entities.write().clear();
        self.total_entities.store(0, Ordering::Relaxed);
        self.total_relationships.store(0, Ordering::Relaxed);
    }
}

impl Default for KnowledgeGraph {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Graph statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStats {
    /// Total entities
    pub total_entities: u64,
    /// Total relationships
    pub total_relationships: u64,
    /// Total documents indexed
    pub total_documents: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_entity() {
        let graph = KnowledgeGraph::with_defaults();

        let entity = Entity::new("Apple Inc", EntityType::Organization);
        let id = graph.add_entity(entity, Some("doc1")).unwrap();

        assert!(!id.is_empty());
        assert!(graph.get_entity(&id).is_some());
    }

    #[test]
    fn test_add_relationship() {
        let graph = KnowledgeGraph::with_defaults();

        let e1 = Entity::new("Apple", EntityType::Organization);
        let e2 = Entity::new("iPhone", EntityType::Product);

        let id1 = graph.add_entity(e1, None).unwrap();
        let id2 = graph.add_entity(e2, None).unwrap();

        let rel = Relationship::new(&id1, &id2, RelationType::HasA);
        graph.add_relationship(rel).unwrap();

        let outgoing = graph.get_outgoing(&id1);
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_id, id2);
    }

    #[test]
    fn test_get_related() {
        let graph = KnowledgeGraph::with_defaults();

        let e1 = Entity::new("Apple", EntityType::Organization);
        let e2 = Entity::new("iPhone", EntityType::Product);
        let e3 = Entity::new("iOS", EntityType::Technical);

        let id1 = graph.add_entity(e1, None).unwrap();
        let id2 = graph.add_entity(e2, None).unwrap();
        let id3 = graph.add_entity(e3, None).unwrap();

        graph
            .add_relationship(Relationship::new(&id1, &id2, RelationType::HasA))
            .unwrap();
        graph
            .add_relationship(Relationship::new(&id2, &id3, RelationType::HasA))
            .unwrap();

        let related = graph.get_related(&id1, 2);
        assert!(related.len() >= 2);
    }

    #[test]
    fn test_document_entities() {
        let graph = KnowledgeGraph::with_defaults();

        let e1 = Entity::new("Apple", EntityType::Organization);
        let e2 = Entity::new("Microsoft", EntityType::Organization);

        graph.add_entity(e1, Some("doc1")).unwrap();
        graph.add_entity(e2, Some("doc1")).unwrap();

        let doc_entities = graph.get_document_entities("doc1");
        assert_eq!(doc_entities.len(), 2);
    }

    #[test]
    fn test_search_entities() {
        let graph = KnowledgeGraph::with_defaults();

        graph
            .add_entity(Entity::new("Apple Inc", EntityType::Organization), None)
            .unwrap();
        graph
            .add_entity(Entity::new("Apple Watch", EntityType::Product), None)
            .unwrap();
        graph
            .add_entity(Entity::new("Microsoft", EntityType::Organization), None)
            .unwrap();

        let results = graph.search_entities("apple", 10);
        assert_eq!(results.len(), 2);
    }
}
