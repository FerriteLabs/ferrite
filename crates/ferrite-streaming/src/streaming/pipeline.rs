//! Stream processing pipeline implementation

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use super::operator::Operator;
use super::sink::Sink;
use super::source::Source;
use super::state::StateStore;
use super::{StreamConfig, StreamError, StreamEvent};

/// Processing pipeline
pub struct Pipeline {
    /// Pipeline name
    name: String,
    /// Configuration
    config: StreamConfig,
    /// Source
    source: Option<Arc<dyn Source>>,
    /// Operators in order
    operators: Vec<Arc<dyn Operator<String, serde_json::Value>>>,
    /// Sink
    sink: Option<Arc<dyn Sink>>,
    /// State stores
    state_stores: HashMap<String, Arc<StateStore>>,
    /// Running flag
    running: Arc<RwLock<bool>>,
    /// Event channel
    event_tx: Option<mpsc::Sender<StreamEvent>>,
    /// Output channel
    output_rx: Option<mpsc::Receiver<StreamEvent>>,
}

impl Pipeline {
    /// Get pipeline name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Start the pipeline
    pub async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;

        // Start source
        if let Some(ref source) = self.source {
            source.start().await?;
        }

        // Start sink
        if let Some(ref sink) = self.sink {
            sink.start().await?;
        }

        Ok(())
    }

    /// Run the pipeline event loop (blocking)
    /// This continuously polls the source, processes events, and writes to sink
    pub async fn run(&self) -> Result<PipelineMetrics, StreamError> {
        self.start().await?;

        let mut metrics = PipelineMetrics::default();
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| StreamError::ConfigError("No source configured".into()))?;
        let sink = self
            .sink
            .as_ref()
            .ok_or_else(|| StreamError::ConfigError("No sink configured".into()))?;

        // Main event loop
        while *self.running.read().await {
            // Poll for next event with a small timeout to allow checking running flag
            match tokio::time::timeout(tokio::time::Duration::from_millis(100), source.poll()).await
            {
                Ok(Some(event)) => {
                    metrics.events_in += 1;
                    let start = std::time::Instant::now();

                    // Process through operator chain
                    match self.process(event).await {
                        Ok(output_events) => {
                            for output_event in output_events {
                                if let Err(e) = sink.write(output_event).await {
                                    tracing::warn!("Failed to write to sink: {}", e);
                                    metrics.events_dropped += 1;
                                } else {
                                    metrics.events_out += 1;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to process event: {}", e);
                            metrics.events_dropped += 1;
                        }
                    }

                    metrics.processing_time_ns += start.elapsed().as_nanos() as u64;
                }
                Ok(None) => {
                    // No event available, continue loop
                }
                Err(_) => {
                    // Timeout, continue loop to check running flag
                }
            }
        }

        // Flush sink on shutdown
        sink.flush().await?;
        self.stop().await?;

        Ok(metrics)
    }

    /// Run the pipeline in a background task
    /// Returns a handle that can be used to stop the pipeline
    pub fn run_background(
        self: Arc<Self>,
    ) -> tokio::task::JoinHandle<Result<PipelineMetrics, StreamError>> {
        tokio::spawn(async move { self.run().await })
    }

    /// Stop the pipeline
    pub async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;

        // Stop source
        if let Some(ref source) = self.source {
            source.stop().await?;
        }

        // Stop sink
        if let Some(ref sink) = self.sink {
            sink.stop().await?;
        }

        Ok(())
    }

    /// Check if pipeline is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Process a single event
    pub async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        let mut events = vec![event];

        for operator in &self.operators {
            let mut new_events = Vec::new();
            for e in events {
                let processed = operator.process(e).await?;
                new_events.extend(processed);
            }
            events = new_events;
        }

        Ok(events)
    }

    /// Get state store
    pub fn get_state_store(&self, name: &str) -> Option<Arc<StateStore>> {
        self.state_stores.get(name).cloned()
    }

    /// Create a new state store
    pub async fn create_state_store(&mut self, name: &str) -> Arc<StateStore> {
        let store = Arc::new(StateStore::new(name.to_string()));
        self.state_stores.insert(name.to_string(), store.clone());
        store
    }
}

/// Pipeline builder for fluent construction
pub struct PipelineBuilder {
    name: String,
    config: StreamConfig,
    source: Option<Arc<dyn Source>>,
    operators: Vec<Arc<dyn Operator<String, serde_json::Value>>>,
    sink: Option<Arc<dyn Sink>>,
    state_stores: HashMap<String, Arc<StateStore>>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(name: String, config: StreamConfig) -> Self {
        Self {
            name,
            config,
            source: None,
            operators: Vec::new(),
            sink: None,
            state_stores: HashMap::new(),
        }
    }

    /// Set the source
    pub fn source<S: Source + 'static>(mut self, source: S) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// Add an operator
    pub fn operator<O: Operator<String, serde_json::Value> + 'static>(mut self, op: O) -> Self {
        self.operators.push(Arc::new(op));
        self
    }

    /// Set the sink
    pub fn sink<S: Sink + 'static>(mut self, sink: S) -> Self {
        self.sink = Some(Arc::new(sink));
        self
    }

    /// Add a state store
    pub fn state_store(mut self, name: &str) -> Self {
        let store = Arc::new(StateStore::new(name.to_string()));
        self.state_stores.insert(name.to_string(), store);
        self
    }

    /// Build the pipeline
    pub fn build(self) -> Pipeline {
        Pipeline {
            name: self.name,
            config: self.config,
            source: self.source,
            operators: self.operators,
            sink: self.sink,
            state_stores: self.state_stores,
            running: Arc::new(RwLock::new(false)),
            event_tx: None,
            output_rx: None,
        }
    }
}

/// Pipeline topology for visualization
#[derive(Debug, Clone)]
pub struct PipelineTopology {
    /// Nodes in the topology
    pub nodes: Vec<TopologyNode>,
    /// Edges between nodes
    pub edges: Vec<TopologyEdge>,
}

/// Node in the pipeline topology
#[derive(Debug, Clone)]
pub struct TopologyNode {
    /// Node ID
    pub id: String,
    /// Node name
    pub name: String,
    /// Node type
    pub node_type: TopologyNodeType,
}

/// Node type in topology
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyNodeType {
    /// Source node
    Source,
    /// Operator node
    Operator(String),
    /// Sink node
    Sink,
    /// State store
    StateStore,
}

/// Edge in the pipeline topology
#[derive(Debug, Clone)]
pub struct TopologyEdge {
    /// Source node ID
    pub from: String,
    /// Target node ID
    pub to: String,
    /// Edge type
    pub edge_type: TopologyEdgeType,
}

/// Edge type in topology
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyEdgeType {
    /// Data flow
    DataFlow,
    /// State access
    StateAccess,
}

impl PipelineTopology {
    /// Create a new topology
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Add a node
    pub fn add_node(&mut self, id: &str, name: &str, node_type: TopologyNodeType) {
        self.nodes.push(TopologyNode {
            id: id.to_string(),
            name: name.to_string(),
            node_type,
        });
    }

    /// Add an edge
    pub fn add_edge(&mut self, from: &str, to: &str, edge_type: TopologyEdgeType) {
        self.edges.push(TopologyEdge {
            from: from.to_string(),
            to: to.to_string(),
            edge_type,
        });
    }

    /// Get nodes of a specific type
    pub fn get_nodes_by_type(&self, node_type: &TopologyNodeType) -> Vec<&TopologyNode> {
        self.nodes
            .iter()
            .filter(|n| &n.node_type == node_type)
            .collect()
    }
}

impl Default for PipelineTopology {
    fn default() -> Self {
        Self::new()
    }
}

/// Pipeline metrics
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    /// Events received
    pub events_in: u64,
    /// Events emitted
    pub events_out: u64,
    /// Events dropped
    pub events_dropped: u64,
    /// Processing time (ns)
    pub processing_time_ns: u64,
    /// State store operations
    pub state_ops: u64,
    /// Commit count
    pub commits: u64,
}

/// Pipeline status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineStatus {
    /// Not started
    NotStarted,
    /// Starting up
    Starting,
    /// Running
    Running,
    /// Paused
    Paused,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
    /// Failed
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology() {
        let mut topology = PipelineTopology::new();

        topology.add_node("source", "Redis Source", TopologyNodeType::Source);
        topology.add_node(
            "filter",
            "Filter",
            TopologyNodeType::Operator("filter".to_string()),
        );
        topology.add_node("sink", "Redis Sink", TopologyNodeType::Sink);

        topology.add_edge("source", "filter", TopologyEdgeType::DataFlow);
        topology.add_edge("filter", "sink", TopologyEdgeType::DataFlow);

        assert_eq!(topology.nodes.len(), 3);
        assert_eq!(topology.edges.len(), 2);
    }
}
