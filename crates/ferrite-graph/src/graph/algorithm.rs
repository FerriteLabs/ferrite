//! Graph algorithms
//!
//! Provides common graph algorithms like shortest path, PageRank, etc.

use super::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

/// Graph algorithm trait
pub trait GraphAlgorithm {
    /// The result type
    type Result;

    /// Run the algorithm
    fn run(&self, storage: &GraphStorage) -> Self::Result;
}

/// Shortest path algorithms
pub struct ShortestPath;

impl ShortestPath {
    /// Find shortest path using BFS (unweighted)
    pub fn bfs(storage: &GraphStorage, from: VertexId, to: VertexId) -> Option<Vec<VertexId>> {
        if from == to {
            return Some(vec![from]);
        }

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent: HashMap<VertexId, VertexId> = HashMap::new();

        queue.push_back(from);
        visited.insert(from);

        while let Some(current) = queue.pop_front() {
            for neighbor in storage.out_neighbors(current) {
                if visited.insert(neighbor) {
                    parent.insert(neighbor, current);

                    if neighbor == to {
                        // Reconstruct path
                        let mut path = vec![to];
                        let mut node = to;
                        while let Some(&p) = parent.get(&node) {
                            path.push(p);
                            node = p;
                        }
                        path.reverse();
                        return Some(path);
                    }

                    queue.push_back(neighbor);
                }
            }
        }

        None
    }

    /// Find shortest path using Dijkstra (weighted)
    pub fn dijkstra(
        storage: &GraphStorage,
        from: VertexId,
        to: VertexId,
    ) -> Option<(Vec<VertexId>, f64)> {
        #[derive(Clone, PartialEq)]
        struct State {
            vertex: VertexId,
            cost: f64,
        }

        impl Eq for State {}

        impl Ord for State {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .cost
                    .partial_cmp(&self.cost)
                    .unwrap_or(Ordering::Equal)
            }
        }

        impl PartialOrd for State {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut dist: HashMap<VertexId, f64> = HashMap::new();
        let mut parent: HashMap<VertexId, VertexId> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(from, 0.0);
        heap.push(State {
            vertex: from,
            cost: 0.0,
        });

        while let Some(State { vertex, cost }) = heap.pop() {
            if vertex == to {
                // Reconstruct path
                let mut path = vec![to];
                let mut node = to;
                while let Some(&p) = parent.get(&node) {
                    path.push(p);
                    node = p;
                }
                path.reverse();
                return Some((path, cost));
            }

            if cost > *dist.get(&vertex).unwrap_or(&f64::INFINITY) {
                continue;
            }

            for eid in storage.get_out_edges(vertex) {
                if let Some(edge) = storage.get_edge_ref(eid) {
                    let next = edge.to;
                    let next_cost = cost + edge.weight;

                    if next_cost < *dist.get(&next).unwrap_or(&f64::INFINITY) {
                        dist.insert(next, next_cost);
                        parent.insert(next, vertex);
                        heap.push(State {
                            vertex: next,
                            cost: next_cost,
                        });
                    }
                }
            }
        }

        None
    }

    /// Find all shortest paths (BFS)
    pub fn all_shortest_paths(
        storage: &GraphStorage,
        from: VertexId,
    ) -> HashMap<VertexId, (Vec<VertexId>, usize)> {
        let mut result: HashMap<VertexId, (Vec<VertexId>, usize)> = HashMap::new();
        let mut queue = VecDeque::new();
        let mut parent: HashMap<VertexId, VertexId> = HashMap::new();
        let mut depth: HashMap<VertexId, usize> = HashMap::new();

        queue.push_back(from);
        depth.insert(from, 0);
        result.insert(from, (vec![from], 0));

        while let Some(current) = queue.pop_front() {
            let current_depth = depth.get(&current).copied().unwrap_or(0);

            for neighbor in storage.out_neighbors(current) {
                if let std::collections::hash_map::Entry::Vacant(e) = depth.entry(neighbor) {
                    e.insert(current_depth + 1);
                    parent.insert(neighbor, current);
                    queue.push_back(neighbor);

                    // Reconstruct path
                    let mut path = vec![neighbor];
                    let mut node = neighbor;
                    while let Some(&p) = parent.get(&node) {
                        path.push(p);
                        node = p;
                    }
                    path.reverse();
                    result.insert(neighbor, (path, current_depth + 1));
                }
            }
        }

        result
    }
}

/// PageRank algorithm
pub struct PageRank {
    /// Number of iterations
    iterations: usize,
    /// Damping factor
    damping: f64,
    /// Convergence threshold
    tolerance: f64,
}

impl PageRank {
    /// Create new PageRank instance
    pub fn new(iterations: usize, damping: f64) -> Self {
        Self {
            iterations,
            damping,
            tolerance: 1e-6,
        }
    }

    /// Set convergence tolerance
    pub fn with_tolerance(mut self, tolerance: f64) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Run PageRank
    pub fn run(&self, storage: &GraphStorage) -> HashMap<VertexId, f64> {
        let vertices: Vec<VertexId> = storage.vertex_ids();
        let n = vertices.len();

        if n == 0 {
            return HashMap::new();
        }

        let initial_rank = 1.0 / n as f64;
        let mut ranks: HashMap<VertexId, f64> =
            vertices.iter().map(|&v| (v, initial_rank)).collect();

        for _ in 0..self.iterations {
            let mut new_ranks: HashMap<VertexId, f64> = HashMap::new();
            let mut max_diff = 0.0f64;

            // Calculate rank contributions
            for &vertex in &vertices {
                let out_degree = storage.out_degree(vertex);
                let rank = *ranks.get(&vertex).unwrap_or(&initial_rank);

                if out_degree > 0 {
                    let contribution = rank / out_degree as f64;
                    for neighbor in storage.out_neighbors(vertex) {
                        *new_ranks.entry(neighbor).or_insert(0.0) += contribution;
                    }
                } else {
                    // Dangling node: distribute equally
                    let contribution = rank / n as f64;
                    for &v in &vertices {
                        *new_ranks.entry(v).or_insert(0.0) += contribution;
                    }
                }
            }

            // Apply damping
            for &vertex in &vertices {
                let new_rank = (1.0 - self.damping) / n as f64
                    + self.damping * new_ranks.get(&vertex).unwrap_or(&0.0);
                let old_rank = *ranks.get(&vertex).unwrap_or(&initial_rank);

                max_diff = max_diff.max((new_rank - old_rank).abs());
                ranks.insert(vertex, new_rank);
            }

            // Check convergence
            if max_diff < self.tolerance {
                break;
            }
        }

        ranks
    }

    /// Get top K vertices by rank
    pub fn top_k(&self, storage: &GraphStorage, k: usize) -> Vec<(VertexId, f64)> {
        let ranks = self.run(storage);
        let mut sorted: Vec<_> = ranks.into_iter().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        sorted.truncate(k);
        sorted
    }
}

impl GraphAlgorithm for PageRank {
    type Result = HashMap<VertexId, f64>;

    fn run(&self, storage: &GraphStorage) -> Self::Result {
        Self::run(self, storage)
    }
}

/// Connected components algorithm
pub struct ConnectedComponents;

impl ConnectedComponents {
    /// Find all connected components (undirected)
    pub fn find(storage: &GraphStorage) -> Vec<Vec<VertexId>> {
        let mut visited: HashSet<VertexId> = HashSet::new();
        let mut components: Vec<Vec<VertexId>> = Vec::new();

        for vertex in storage.vertex_ids() {
            if visited.contains(&vertex) {
                continue;
            }

            let mut component = Vec::new();
            let mut stack = vec![vertex];

            while let Some(v) = stack.pop() {
                if !visited.insert(v) {
                    continue;
                }

                component.push(v);

                // Add all neighbors (treating as undirected)
                for neighbor in storage.neighbors(v) {
                    if !visited.contains(&neighbor) {
                        stack.push(neighbor);
                    }
                }
            }

            if !component.is_empty() {
                components.push(component);
            }
        }

        components
    }

    /// Find strongly connected components (Tarjan's algorithm)
    pub fn find_strongly_connected(storage: &GraphStorage) -> Vec<Vec<VertexId>> {
        let mut index_counter = 0usize;
        let mut stack: Vec<VertexId> = Vec::new();
        let mut on_stack: HashSet<VertexId> = HashSet::new();
        let mut indices: HashMap<VertexId, usize> = HashMap::new();
        let mut low_links: HashMap<VertexId, usize> = HashMap::new();
        let mut components: Vec<Vec<VertexId>> = Vec::new();

        #[allow(clippy::too_many_arguments)]
        fn strong_connect(
            v: VertexId,
            storage: &GraphStorage,
            index_counter: &mut usize,
            stack: &mut Vec<VertexId>,
            on_stack: &mut HashSet<VertexId>,
            indices: &mut HashMap<VertexId, usize>,
            low_links: &mut HashMap<VertexId, usize>,
            components: &mut Vec<Vec<VertexId>>,
        ) {
            indices.insert(v, *index_counter);
            low_links.insert(v, *index_counter);
            *index_counter += 1;
            stack.push(v);
            on_stack.insert(v);

            for neighbor in storage.out_neighbors(v) {
                if !indices.contains_key(&neighbor) {
                    strong_connect(
                        neighbor,
                        storage,
                        index_counter,
                        stack,
                        on_stack,
                        indices,
                        low_links,
                        components,
                    );
                    let neighbor_low = low_links.get(&neighbor).copied().unwrap_or(0);
                    if let Some(v_low) = low_links.get_mut(&v) {
                        *v_low = (*v_low).min(neighbor_low);
                    }
                } else if on_stack.contains(&neighbor) {
                    let neighbor_idx = indices.get(&neighbor).copied().unwrap_or(0);
                    if let Some(v_low) = low_links.get_mut(&v) {
                        *v_low = (*v_low).min(neighbor_idx);
                    }
                }
            }

            // If v is a root node, pop the stack
            if low_links.get(&v) == indices.get(&v) {
                let mut component = Vec::new();
                loop {
                    let Some(w) = stack.pop() else {
                        break;
                    };
                    on_stack.remove(&w);
                    component.push(w);
                    if w == v {
                        break;
                    }
                }
                components.push(component);
            }
        }

        for vertex in storage.vertex_ids() {
            if !indices.contains_key(&vertex) {
                strong_connect(
                    vertex,
                    storage,
                    &mut index_counter,
                    &mut stack,
                    &mut on_stack,
                    &mut indices,
                    &mut low_links,
                    &mut components,
                );
            }
        }

        components
    }

    /// Get largest component
    pub fn largest(storage: &GraphStorage) -> Vec<VertexId> {
        Self::find(storage)
            .into_iter()
            .max_by_key(|c| c.len())
            .unwrap_or_default()
    }
}

/// Betweenness centrality
pub struct BetweennessCentrality;

impl BetweennessCentrality {
    /// Calculate betweenness centrality for all vertices
    pub fn calculate(storage: &GraphStorage) -> HashMap<VertexId, f64> {
        let vertices: Vec<VertexId> = storage.vertex_ids();
        let mut centrality: HashMap<VertexId, f64> = vertices.iter().map(|&v| (v, 0.0)).collect();

        for &s in &vertices {
            // Single-source shortest paths
            let mut stack: Vec<VertexId> = Vec::new();
            let mut predecessors: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
            let mut sigma: HashMap<VertexId, f64> = HashMap::new();
            let mut dist: HashMap<VertexId, i32> = HashMap::new();

            sigma.insert(s, 1.0);
            dist.insert(s, 0);

            let mut queue = VecDeque::new();
            queue.push_back(s);

            while let Some(v) = queue.pop_front() {
                stack.push(v);
                let v_dist = dist.get(&v).copied().unwrap_or(0);

                for w in storage.out_neighbors(v) {
                    // First visit?
                    if let std::collections::hash_map::Entry::Vacant(e) = dist.entry(w) {
                        e.insert(v_dist + 1);
                        queue.push_back(w);
                    }

                    // Shortest path to w via v?
                    if dist.get(&w).copied().unwrap_or(0) == v_dist + 1 {
                        let v_sigma = sigma.get(&v).copied().unwrap_or(1.0);
                        *sigma.entry(w).or_insert(0.0) += v_sigma;
                        predecessors.entry(w).or_default().push(v);
                    }
                }
            }

            // Accumulation
            let mut delta: HashMap<VertexId, f64> = vertices.iter().map(|&v| (v, 0.0)).collect();

            while let Some(w) = stack.pop() {
                if let Some(preds) = predecessors.get(&w) {
                    let w_sigma = *sigma.get(&w).unwrap_or(&1.0);
                    let w_delta = delta.get(&w).copied().unwrap_or(0.0);

                    for &v in preds {
                        let v_sigma = *sigma.get(&v).unwrap_or(&1.0);
                        let contribution = (v_sigma / w_sigma) * (1.0 + w_delta);
                        *delta.entry(v).or_insert(0.0) += contribution;
                    }
                }

                if w != s {
                    *centrality.entry(w).or_insert(0.0) += delta.get(&w).copied().unwrap_or(0.0);
                }
            }
        }

        // Normalize
        let n = vertices.len();
        if n > 2 {
            let factor = 1.0 / ((n - 1) * (n - 2)) as f64;
            for (_, c) in centrality.iter_mut() {
                *c *= factor;
            }
        }

        centrality
    }
}

/// Clustering coefficient
pub struct ClusteringCoefficient;

impl ClusteringCoefficient {
    /// Calculate local clustering coefficient for a vertex
    pub fn local(storage: &GraphStorage, vertex: VertexId) -> f64 {
        let neighbors: Vec<VertexId> = storage.neighbors(vertex);
        let k = neighbors.len();

        if k < 2 {
            return 0.0;
        }

        let mut triangles = 0;
        for i in 0..k {
            for j in (i + 1)..k {
                // Check if neighbors are connected
                if !storage.edges_between(neighbors[i], neighbors[j]).is_empty()
                    || !storage.edges_between(neighbors[j], neighbors[i]).is_empty()
                {
                    triangles += 1;
                }
            }
        }

        (2 * triangles) as f64 / (k * (k - 1)) as f64
    }

    /// Calculate global clustering coefficient
    pub fn global(storage: &GraphStorage) -> f64 {
        let vertices = storage.vertex_ids();
        if vertices.is_empty() {
            return 0.0;
        }

        let sum: f64 = vertices.iter().map(|&v| Self::local(storage, v)).sum();

        sum / vertices.len() as f64
    }
}

/// Graph diameter
pub struct GraphDiameter;

impl GraphDiameter {
    /// Calculate the diameter (longest shortest path)
    pub fn calculate(storage: &GraphStorage) -> usize {
        let vertices = storage.vertex_ids();
        let mut max_distance = 0;

        for &start in &vertices {
            let paths = ShortestPath::all_shortest_paths(storage, start);
            for (_, (_, dist)) in paths {
                max_distance = max_distance.max(dist);
            }
        }

        max_distance
    }

    /// Calculate eccentricity for a vertex
    pub fn eccentricity(storage: &GraphStorage, vertex: VertexId) -> usize {
        let paths = ShortestPath::all_shortest_paths(storage, vertex);
        paths.values().map(|(_, d)| *d).max().unwrap_or(0)
    }
}

/// DFS traversal with cycle detection
pub struct CycleDetectingDfs;

impl CycleDetectingDfs {
    /// Perform DFS from `start`, returning visited vertices with depths and whether a cycle was
    /// detected.
    pub fn run(
        storage: &GraphStorage,
        start: VertexId,
        max_depth: Option<usize>,
    ) -> DfsCycleResult {
        let mut state = DfsState {
            visited: HashSet::new(),
            on_stack: HashSet::new(),
            order: Vec::new(),
            has_cycle: false,
            cycle_edges: Vec::new(),
        };

        Self::dfs_visit(storage, start, 0, max_depth, &mut state);

        DfsCycleResult {
            visited: state.order,
            has_cycle: state.has_cycle,
            cycle_edges: state.cycle_edges,
        }
    }

    /// Detect whether the graph reachable from `start` contains a cycle.
    pub fn has_cycle(storage: &GraphStorage, start: VertexId) -> bool {
        Self::run(storage, start, None).has_cycle
    }

    fn dfs_visit(
        storage: &GraphStorage,
        vertex: VertexId,
        depth: usize,
        max_depth: Option<usize>,
        state: &mut DfsState,
    ) {
        if let Some(max) = max_depth {
            if depth > max {
                return;
            }
        }

        state.visited.insert(vertex);
        state.on_stack.insert(vertex);
        state.order.push((vertex, depth));

        for neighbor in storage.out_neighbors(vertex) {
            if state.on_stack.contains(&neighbor) {
                state.has_cycle = true;
                state.cycle_edges.push((vertex, neighbor));
            } else if !state.visited.contains(&neighbor) {
                Self::dfs_visit(storage, neighbor, depth + 1, max_depth, state);
            }
        }

        state.on_stack.remove(&vertex);
    }
}

/// Internal state for DFS cycle detection
struct DfsState {
    visited: HashSet<VertexId>,
    on_stack: HashSet<VertexId>,
    order: Vec<(VertexId, usize)>,
    has_cycle: bool,
    cycle_edges: Vec<(VertexId, VertexId)>,
}

/// Result of a DFS with cycle detection
#[derive(Debug, Clone)]
pub struct DfsCycleResult {
    /// Vertices visited in DFS order with their depth
    pub visited: Vec<(VertexId, usize)>,
    /// Whether a cycle was detected
    pub has_cycle: bool,
    /// Back-edges that form cycles (from, to)
    pub cycle_edges: Vec<(VertexId, VertexId)>,
}

/// Find all paths between two vertices
pub struct AllPaths;

impl AllPaths {
    /// Find all paths from `from` to `to` with a maximum depth limit.
    ///
    /// Returns a vector of paths, where each path is a vector of vertex IDs.
    pub fn find(
        storage: &GraphStorage,
        from: VertexId,
        to: VertexId,
        max_depth: usize,
    ) -> Vec<Vec<VertexId>> {
        let mut results = Vec::new();
        let mut current_path = vec![from];
        let mut visited = HashSet::new();
        visited.insert(from);

        Self::dfs_all_paths(
            storage,
            from,
            to,
            max_depth,
            &mut visited,
            &mut current_path,
            &mut results,
        );

        results
    }

    fn dfs_all_paths(
        storage: &GraphStorage,
        current: VertexId,
        target: VertexId,
        max_depth: usize,
        visited: &mut HashSet<VertexId>,
        current_path: &mut Vec<VertexId>,
        results: &mut Vec<Vec<VertexId>>,
    ) {
        if current == target {
            results.push(current_path.clone());
            return;
        }

        if current_path.len() > max_depth {
            return;
        }

        for neighbor in storage.out_neighbors(current) {
            if visited.insert(neighbor) {
                current_path.push(neighbor);

                Self::dfs_all_paths(
                    storage,
                    neighbor,
                    target,
                    max_depth,
                    visited,
                    current_path,
                    results,
                );

                current_path.pop();
                visited.remove(&neighbor);
            }
        }
    }
}

/// Triangle counting
pub struct TriangleCounting;

impl TriangleCounting {
    /// Count all triangles in the graph
    pub fn count(storage: &GraphStorage) -> usize {
        let vertices = storage.vertex_ids();
        let mut triangles = 0;

        for &v in &vertices {
            let neighbors: Vec<VertexId> = storage.neighbors(v);
            for i in 0..neighbors.len() {
                for j in (i + 1)..neighbors.len() {
                    let u = neighbors[i];
                    let w = neighbors[j];

                    // Check if u and w are connected
                    if !storage.edges_between(u, w).is_empty()
                        || !storage.edges_between(w, u).is_empty()
                    {
                        triangles += 1;
                    }
                }
            }
        }

        // Each triangle is counted 3 times
        triangles / 3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> GraphStorage {
        let mut storage = GraphStorage::new();

        for i in 1..=5 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }

        // Create edges: 1->2->3->4->5, 1->3
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "EDGE",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "EDGE",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(3),
                VertexId::new(4),
                "EDGE",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(4),
                VertexId::new(4),
                VertexId::new(5),
                "EDGE",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(5),
                VertexId::new(1),
                VertexId::new(3),
                "EDGE",
            ))
            .unwrap();

        storage
    }

    #[test]
    fn test_bfs_shortest_path() {
        let storage = create_test_graph();

        let path = ShortestPath::bfs(&storage, VertexId::new(1), VertexId::new(5));
        assert!(path.is_some());

        let path = path.unwrap();
        assert_eq!(path[0], VertexId::new(1));
        assert_eq!(*path.last().unwrap(), VertexId::new(5));
    }

    #[test]
    fn test_bfs_no_path() {
        let storage = create_test_graph();

        // No edge from 5 to anywhere
        let path = ShortestPath::bfs(&storage, VertexId::new(5), VertexId::new(1));
        assert!(path.is_none());
    }

    #[test]
    fn test_dijkstra() {
        let storage = create_test_graph();

        let result = ShortestPath::dijkstra(&storage, VertexId::new(1), VertexId::new(5));
        assert!(result.is_some());

        let (path, cost) = result.unwrap();
        assert_eq!(path[0], VertexId::new(1));
        assert!(cost > 0.0);
    }

    #[test]
    fn test_pagerank() {
        let storage = create_test_graph();

        let pr = PageRank::new(20, 0.85);
        let ranks = pr.run(&storage);

        assert_eq!(ranks.len(), 5);

        // All ranks should be positive
        for (_, rank) in &ranks {
            assert!(*rank > 0.0);
        }

        // Sum should be approximately 1
        let sum: f64 = ranks.values().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_pagerank_top_k() {
        let storage = create_test_graph();

        let pr = PageRank::new(20, 0.85);
        let top = pr.top_k(&storage, 3);

        assert_eq!(top.len(), 3);
        // Should be sorted by rank
        assert!(top[0].1 >= top[1].1);
        assert!(top[1].1 >= top[2].1);
    }

    #[test]
    fn test_connected_components() {
        let mut storage = GraphStorage::new();

        // Create two disconnected components
        for i in 1..=6 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }

        // Component 1: 1-2-3
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "E",
            ))
            .unwrap();

        // Component 2: 4-5-6
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(4),
                VertexId::new(5),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(4),
                VertexId::new(5),
                VertexId::new(6),
                "E",
            ))
            .unwrap();

        let components = ConnectedComponents::find(&storage);
        // Note: due to directed nature, might get more components
        assert!(!components.is_empty());
    }

    #[test]
    fn test_strongly_connected_components() {
        let mut storage = GraphStorage::new();

        // Create a cycle: 1->2->3->1
        for i in 1..=3 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(3),
                VertexId::new(1),
                "E",
            ))
            .unwrap();

        let sccs = ConnectedComponents::find_strongly_connected(&storage);
        // Should have one SCC with all 3 nodes
        assert!(!sccs.is_empty());
        assert!(sccs.iter().any(|c| c.len() == 3));
    }

    #[test]
    fn test_clustering_coefficient() {
        let mut storage = GraphStorage::new();

        // Create a triangle: 1-2-3-1
        for i in 1..=3 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(3),
                VertexId::new(1),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(4),
                VertexId::new(2),
                VertexId::new(1),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(5),
                VertexId::new(3),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(6),
                VertexId::new(1),
                VertexId::new(3),
                "E",
            ))
            .unwrap();

        let global = ClusteringCoefficient::global(&storage);
        assert!(global > 0.0);
    }

    #[test]
    fn test_triangle_counting() {
        let mut storage = GraphStorage::new();

        // Create a triangle
        for i in 1..=3 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(3),
                VertexId::new(1),
                "E",
            ))
            .unwrap();

        let triangles = TriangleCounting::count(&storage);
        assert_eq!(triangles, 1);
    }

    #[test]
    fn test_all_shortest_paths() {
        let storage = create_test_graph();

        let paths = ShortestPath::all_shortest_paths(&storage, VertexId::new(1));
        assert!(paths.contains_key(&VertexId::new(1)));
        assert_eq!(paths.get(&VertexId::new(1)).unwrap().1, 0);
    }

    #[test]
    fn test_graph_diameter() {
        let storage = create_test_graph();

        let diameter = GraphDiameter::calculate(&storage);
        assert!(diameter > 0);
    }

    #[test]
    fn test_eccentricity() {
        let storage = create_test_graph();

        let ecc = GraphDiameter::eccentricity(&storage, VertexId::new(1));
        assert!(ecc > 0);
    }

    #[test]
    fn test_cycle_detecting_dfs_no_cycle() {
        let storage = create_test_graph(); // DAG: 1->2->3->4->5, 1->3

        let result = CycleDetectingDfs::run(&storage, VertexId::new(1), None);
        assert!(!result.has_cycle);
        assert!(result.cycle_edges.is_empty());
        // Should visit all reachable vertices
        assert_eq!(result.visited.len(), 5);
    }

    #[test]
    fn test_cycle_detecting_dfs_with_cycle() {
        let mut storage = GraphStorage::new();

        for i in 1..=3 {
            storage
                .add_vertex(Vertex::new(VertexId::new(i), "Node"))
                .unwrap();
        }
        // Cycle: 1->2->3->1
        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(2),
                VertexId::new(2),
                VertexId::new(3),
                "E",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(3),
                VertexId::new(3),
                VertexId::new(1),
                "E",
            ))
            .unwrap();

        let result = CycleDetectingDfs::run(&storage, VertexId::new(1), None);
        assert!(result.has_cycle);
        assert!(!result.cycle_edges.is_empty());
    }

    #[test]
    fn test_cycle_detecting_dfs_max_depth() {
        let storage = create_test_graph();

        let result = CycleDetectingDfs::run(&storage, VertexId::new(1), Some(1));
        // Should only visit depth 0 and 1
        assert!(result.visited.iter().all(|(_, d)| *d <= 1));
    }

    #[test]
    fn test_has_cycle() {
        let storage = create_test_graph();
        assert!(!CycleDetectingDfs::has_cycle(&storage, VertexId::new(1)));
    }

    #[test]
    fn test_all_paths_simple() {
        let storage = create_test_graph(); // 1->2->3->4->5, 1->3

        let paths = AllPaths::find(&storage, VertexId::new(1), VertexId::new(3), 5);
        // Two paths: 1->2->3 and 1->3
        assert_eq!(paths.len(), 2);
        for path in &paths {
            assert_eq!(path[0], VertexId::new(1));
            assert_eq!(*path.last().unwrap(), VertexId::new(3));
        }
    }

    #[test]
    fn test_all_paths_depth_limited() {
        let storage = create_test_graph();

        // max_depth=1: only direct edges (path length at most 2)
        let paths = AllPaths::find(&storage, VertexId::new(1), VertexId::new(3), 1);
        // Only the direct 1->3 path fits within depth 1
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![VertexId::new(1), VertexId::new(3)]);
    }

    #[test]
    fn test_all_paths_no_path() {
        let storage = create_test_graph();

        let paths = AllPaths::find(&storage, VertexId::new(5), VertexId::new(1), 10);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_all_paths_same_node() {
        let storage = create_test_graph();

        let paths = AllPaths::find(&storage, VertexId::new(1), VertexId::new(1), 5);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![VertexId::new(1)]);
    }
}
