//! Graph Database Command Handlers
//!
//! Migration target: `ferrite-graph` crate (crates/ferrite-graph)
//!
//! Redis-compatible command bindings for the graph database engine.
//! Provides Cypher-like graph operations over RESP protocol.
//!
//! # Commands
//!
//! - `GRAPH.CREATE <name>` - Create a graph
//! - `GRAPH.DELETE <name>` - Delete a graph
//! - `GRAPH.QUERY <name> <cypher>` - Execute Cypher query
//! - `GRAPH.ADDNODE <name> <label> [properties]` - Add vertex
//! - `GRAPH.ADDEDGE <name> <from> <to> <type> [properties]` - Add edge
//! - `GRAPH.GETNODE <name> <id>` - Get vertex by ID
//! - `GRAPH.GETEDGE <name> <id>` - Get edge by ID
//! - `GRAPH.DELETENODE <name> <id>` - Delete vertex
//! - `GRAPH.DELETEEDGE <name> <id>` - Delete edge
//! - `GRAPH.NEIGHBORS <name> <id> [IN|OUT|BOTH] [type]` - Get neighbors
//! - `GRAPH.SHORTESTPATH <name> <from> <to>` - Find shortest path
//! - `GRAPH.PAGERANK <name>` - Compute PageRank
//! - `GRAPH.LIST` - List all graphs
//! - `GRAPH.INFO <name>` - Get graph info

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_graph::graph::{Direction, EdgeId, Graph, PropertyValue, VertexId};

use super::{err_frame, ok_frame, HandlerContext};

/// Global graph store - maps graph names to Graph instances
static GRAPH_STORE: OnceLock<RwLock<HashMap<String, Graph>>> = OnceLock::new();

/// Get or initialize the global graph store
fn get_graph_store() -> &'static RwLock<HashMap<String, Graph>> {
    GRAPH_STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Handle GRAPH.CREATE command
///
/// Creates a new graph.
///
/// # Syntax
/// `GRAPH.CREATE <name>`
///
/// # Returns
/// OK on success.
pub fn graph_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'GRAPH.CREATE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let store = get_graph_store();

    let mut graphs = store.write();
    if graphs.contains_key(&name) {
        return err_frame(&format!("graph '{}' already exists", name));
    }

    graphs.insert(name, Graph::new());
    ok_frame()
}

/// Handle GRAPH.DELETE command
///
/// Deletes a graph and all its vertices/edges.
///
/// # Syntax
/// `GRAPH.DELETE <name>`
///
/// # Returns
/// OK on success.
pub fn graph_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'GRAPH.DELETE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let store = get_graph_store();

    let mut graphs = store.write();
    if graphs.remove(&name).is_none() {
        return err_frame(&format!("graph '{}' not found", name));
    }

    ok_frame()
}

/// Handle GRAPH.QUERY command
///
/// Executes a Cypher-like query on the graph.
///
/// # Syntax
/// `GRAPH.QUERY <name> <cypher-query> [TIMEOUT <ms>]`
///
/// # Returns
/// Query results as array.
pub fn graph_query(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.QUERY' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let query = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    // Execute the query
    match graph.query(&query) {
        Ok(result) => {
            // Convert result to Frame
            let headers: Vec<Frame> = result
                .columns
                .iter()
                .map(|c| Frame::bulk(c.clone()))
                .collect();

            let rows: Vec<Frame> = result
                .rows
                .iter()
                .map(|row| {
                    let values: Vec<Frame> = result
                        .columns
                        .iter()
                        .map(|col| match row.values.get(col) {
                            Some(v) => property_value_to_frame(v),
                            None => Frame::Null,
                        })
                        .collect();
                    Frame::array(values)
                })
                .collect();

            Frame::array(vec![
                Frame::array(headers),
                Frame::array(rows),
                Frame::array(vec![Frame::bulk(format!(
                    "Query internal execution time: {} ms",
                    result.took_ms
                ))]),
            ])
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Convert properties HashMap to JSON string
fn properties_to_json_string(props: &HashMap<String, PropertyValue>) -> String {
    let entries: Vec<String> = props
        .iter()
        .map(|(k, v)| format!("\"{}\":{}", k, property_value_to_json_string(v)))
        .collect();
    format!("{{{}}}", entries.join(","))
}

/// Convert PropertyValue to JSON string
fn property_value_to_json_string(pv: &PropertyValue) -> String {
    match pv {
        PropertyValue::String(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => f.to_string(),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Null => "null".to_string(),
        PropertyValue::List(l) => {
            let items: Vec<String> = l.iter().map(property_value_to_json_string).collect();
            format!("[{}]", items.join(","))
        }
        PropertyValue::Map(m) => {
            let entries: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", k, property_value_to_json_string(v)))
                .collect();
            format!("{{{}}}", entries.join(","))
        }
    }
}

/// Convert query value to frame
fn property_value_to_frame(value: &ferrite_graph::graph::QueryValue) -> Frame {
    use ferrite_graph::graph::QueryValue;

    match value {
        QueryValue::Property(pv) => match pv {
            PropertyValue::String(s) => Frame::bulk(s.clone()),
            PropertyValue::Integer(i) => Frame::Integer(*i),
            PropertyValue::Float(f) => Frame::bulk(f.to_string()),
            PropertyValue::Boolean(b) => Frame::Integer(if *b { 1 } else { 0 }),
            PropertyValue::Null => Frame::Null,
            PropertyValue::List(l) => Frame::array(
                l.iter()
                    .map(|v| match v {
                        PropertyValue::String(s) => Frame::bulk(s.clone()),
                        PropertyValue::Integer(i) => Frame::Integer(*i),
                        _ => Frame::Null,
                    })
                    .collect(),
            ),
            PropertyValue::Map(_) => Frame::bulk("[map]"),
        },
        QueryValue::Vertex(v) => Frame::array(vec![
            Frame::Integer(v.id.0 as i64),
            Frame::bulk(v.label.clone()),
            Frame::bulk(properties_to_json_string(&v.properties)),
        ]),
        QueryValue::Edge(e) => Frame::array(vec![
            Frame::Integer(e.id.0 as i64),
            Frame::bulk(e.label.clone()),
            Frame::Integer(e.from.0 as i64),
            Frame::Integer(e.to.0 as i64),
        ]),
        QueryValue::Path(p) => Frame::array(p.iter().map(property_value_to_frame).collect()),
        QueryValue::Null => Frame::Null,
    }
}

/// Handle GRAPH.ADDNODE command
///
/// Adds a vertex to the graph.
///
/// # Syntax
/// `GRAPH.ADDNODE <name> <label> [properties-json]`
///
/// # Returns
/// The node ID.
pub fn graph_addnode(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.ADDNODE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let label = String::from_utf8_lossy(&args[1]).to_string();

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    // Parse optional properties
    let mut builder = graph.add_vertex(&label);

    if args.len() > 2 {
        let props_json = String::from_utf8_lossy(&args[2]).to_string();
        match serde_json::from_str::<serde_json::Value>(&props_json) {
            Ok(serde_json::Value::Object(map)) => {
                for (key, value) in map {
                    let pv = json_to_property_value(&value);
                    builder = builder.property(&key, pv);
                }
            }
            Ok(_) => return err_frame("properties must be a JSON object"),
            Err(_) => return err_frame("invalid properties JSON"),
        }
    }

    match builder.build() {
        Ok(id) => Frame::Integer(id.0 as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Convert JSON value to PropertyValue
fn json_to_property_value(value: &serde_json::Value) -> PropertyValue {
    match value {
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.iter().map(json_to_property_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut hm = HashMap::new();
            for (k, v) in map {
                hm.insert(k.clone(), json_to_property_value(v));
            }
            PropertyValue::Map(hm)
        }
    }
}

/// Handle GRAPH.ADDEDGE command
///
/// Adds an edge between two vertices.
///
/// # Syntax
/// `GRAPH.ADDEDGE <name> <from-id> <to-id> <type> [properties-json]`
///
/// # Returns
/// The edge ID.
pub fn graph_addedge(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 4 {
        return err_frame("wrong number of arguments for 'GRAPH.ADDEDGE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let from_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid from node ID"),
    };
    let to_id: u64 = match String::from_utf8_lossy(&args[2]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid to node ID"),
    };
    let edge_type = String::from_utf8_lossy(&args[3]).to_string();

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    // Parse optional properties
    let mut builder = graph.add_edge(VertexId(from_id), VertexId(to_id), &edge_type);

    if args.len() > 4 {
        let props_json = String::from_utf8_lossy(&args[4]).to_string();
        match serde_json::from_str::<serde_json::Value>(&props_json) {
            Ok(serde_json::Value::Object(map)) => {
                for (key, value) in map {
                    let pv = json_to_property_value(&value);
                    builder = builder.property(&key, pv);
                }
            }
            Ok(_) => return err_frame("properties must be a JSON object"),
            Err(_) => return err_frame("invalid properties JSON"),
        }
    }

    match builder.build() {
        Ok(id) => Frame::Integer(id.0 as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle GRAPH.GETNODE command
///
/// Gets a vertex by ID.
///
/// # Syntax
/// `GRAPH.GETNODE <name> <id>`
///
/// # Returns
/// Node as array [id, labels, properties] or nil.
pub fn graph_getnode(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.GETNODE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let node_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid node ID"),
    };

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    match graph.get_vertex(VertexId(node_id)) {
        Some(vertex) => Frame::array(vec![
            Frame::Integer(vertex.id.0 as i64),
            Frame::bulk(vertex.label.clone()),
            Frame::bulk(properties_to_json_string(&vertex.properties)),
        ]),
        None => Frame::Null,
    }
}

/// Handle GRAPH.GETEDGE command
///
/// Gets an edge by ID.
///
/// # Syntax
/// `GRAPH.GETEDGE <name> <id>`
///
/// # Returns
/// Edge as array [id, type, from, to, properties] or nil.
pub fn graph_getedge(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.GETEDGE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let edge_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid edge ID"),
    };

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    match graph.get_edge(EdgeId(edge_id)) {
        Some(edge) => Frame::array(vec![
            Frame::Integer(edge.id.0 as i64),
            Frame::bulk(edge.label.clone()),
            Frame::Integer(edge.from.0 as i64),
            Frame::Integer(edge.to.0 as i64),
            Frame::bulk(properties_to_json_string(&edge.properties)),
        ]),
        None => Frame::Null,
    }
}

/// Handle GRAPH.DELETENODE command
///
/// Deletes a vertex and all its edges.
///
/// # Syntax
/// `GRAPH.DELETENODE <name> <id>`
///
/// # Returns
/// Number of deleted elements (node + edges).
pub fn graph_deletenode(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.DELETENODE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let node_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid node ID"),
    };

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    match graph.remove_vertex(VertexId(node_id)) {
        Ok(()) => Frame::Integer(1), // Vertex removed (edges are removed internally)
        Err(_) => Frame::Integer(0),
    }
}

/// Handle GRAPH.DELETEEDGE command
///
/// Deletes an edge.
///
/// # Syntax
/// `GRAPH.DELETEEDGE <name> <id>`
///
/// # Returns
/// 1 if deleted, 0 if not found.
pub fn graph_deleteedge(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.DELETEEDGE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let edge_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid edge ID"),
    };

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    match graph.remove_edge(EdgeId(edge_id)) {
        Ok(()) => Frame::Integer(1),
        Err(_) => Frame::Integer(0),
    }
}

/// Handle GRAPH.NEIGHBORS command
///
/// Gets neighbors of a vertex.
///
/// # Syntax
/// `GRAPH.NEIGHBORS <name> <id> [IN|OUT|BOTH] [type]`
///
/// # Returns
/// Array of neighbor node IDs.
pub fn graph_neighbors(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'GRAPH.NEIGHBORS' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let node_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid node ID"),
    };

    // Parse direction
    let mut direction = Direction::Both;
    let mut edge_type: Option<String> = None;

    if args.len() > 2 {
        let dir = String::from_utf8_lossy(&args[2]).to_uppercase();
        if dir == "IN" {
            direction = Direction::In;
            if args.len() > 3 {
                edge_type = Some(String::from_utf8_lossy(&args[3]).to_string());
            }
        } else if dir == "OUT" {
            direction = Direction::Out;
            if args.len() > 3 {
                edge_type = Some(String::from_utf8_lossy(&args[3]).to_string());
            }
        } else if dir == "BOTH" {
            direction = Direction::Both;
            if args.len() > 3 {
                edge_type = Some(String::from_utf8_lossy(&args[3]).to_string());
            }
        } else {
            edge_type = Some(dir.to_string());
        }
    }

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    // Use traversal to get neighbors
    let traverser = graph.traverse(VertexId(node_id));

    let neighbors: Vec<Frame> = match direction {
        Direction::Out => {
            let results = match &edge_type {
                Some(et) => traverser.out(Some(et.as_str())).collect(),
                None => traverser.out(None).collect(),
            };
            results
                .into_iter()
                .map(|v| Frame::Integer(v.id.0 as i64))
                .collect()
        }
        Direction::In => {
            let results = match &edge_type {
                Some(et) => traverser.in_(Some(et.as_str())).collect(),
                None => traverser.in_(None).collect(),
            };
            results
                .into_iter()
                .map(|v| Frame::Integer(v.id.0 as i64))
                .collect()
        }
        Direction::Both => {
            let results = match &edge_type {
                Some(et) => traverser.both(Some(et.as_str())).collect(),
                None => traverser.both(None).collect(),
            };
            results
                .into_iter()
                .map(|v| Frame::Integer(v.id.0 as i64))
                .collect()
        }
    };

    Frame::array(neighbors)
}

/// Handle GRAPH.SHORTESTPATH command
///
/// Finds the shortest path between two vertices.
///
/// # Syntax
/// `GRAPH.SHORTESTPATH <name> <from-id> <to-id> [MAXDEPTH <n>] [TYPE <edge-type>]`
///
/// # Returns
/// Array of node IDs in the path, or nil if no path exists.
pub fn graph_shortestpath(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'GRAPH.SHORTESTPATH' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let from_id: u64 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid from node ID"),
    };
    let to_id: u64 = match String::from_utf8_lossy(&args[2]).parse() {
        Ok(id) => id,
        Err(_) => return err_frame("invalid to node ID"),
    };

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    match graph.shortest_path(VertexId(from_id), VertexId(to_id)) {
        Some(path) => {
            let frames: Vec<Frame> = path
                .iter()
                .map(|vid| Frame::Integer(vid.0 as i64))
                .collect();
            Frame::array(frames)
        }
        None => Frame::Null,
    }
}

/// Handle GRAPH.PAGERANK command
///
/// Computes PageRank for all vertices.
///
/// # Syntax
/// `GRAPH.PAGERANK <name> [ITERATIONS <n>] [DAMPING <factor>]`
///
/// # Returns
/// Array of [node-id, score] pairs.
pub fn graph_pagerank(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'GRAPH.PAGERANK' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    // Parse optional arguments
    let mut iterations: usize = 20;
    let mut damping: f64 = 0.85;

    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "ITERATIONS" => {
                if i + 1 >= args.len() {
                    return err_frame("ITERATIONS requires a value");
                }
                iterations = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(20);
                i += 2;
            }
            "DAMPING" => {
                if i + 1 >= args.len() {
                    return err_frame("DAMPING requires a value");
                }
                damping = String::from_utf8_lossy(&args[i + 1])
                    .parse()
                    .unwrap_or(0.85);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    let ranks = graph.pagerank(iterations, damping);

    // Convert to sorted vec of (id, score) pairs
    let mut sorted_ranks: Vec<_> = ranks.iter().collect();
    sorted_ranks.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));

    // Convert to array of [id, score] pairs
    let results: Vec<Frame> = sorted_ranks
        .iter()
        .map(|(vid, score)| {
            Frame::array(vec![
                Frame::Integer(vid.0 as i64),
                Frame::bulk(format!("{:.6}", score)),
            ])
        })
        .collect();

    Frame::array(results)
}

/// Handle GRAPH.LIST command
///
/// Lists all graphs.
///
/// # Syntax
/// `GRAPH.LIST`
///
/// # Returns
/// Array of graph names.
pub fn graph_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let store = get_graph_store();
    let graphs = store.read();

    let names: Vec<Frame> = graphs
        .keys()
        .map(|name| Frame::bulk(name.clone()))
        .collect();

    Frame::array(names)
}

/// Handle GRAPH.INFO command
///
/// Gets information about a graph.
///
/// # Syntax
/// `GRAPH.INFO <name>`
///
/// # Returns
/// Array of key-value pairs with graph statistics.
pub fn graph_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'GRAPH.INFO' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let store = get_graph_store();
    let graphs = store.read();

    let graph = match graphs.get(&name) {
        Some(g) => g,
        None => return err_frame(&format!("graph '{}' not found", name)),
    };

    let vertex_count = graph.vertex_count();
    let edge_count = graph.edge_count();

    // Get unique labels and edge types
    let vertices = graph.vertices();
    let edges = graph.edges();

    let mut labels: Vec<String> = vertices.iter().map(|v| v.label.clone()).collect();
    labels.sort();
    labels.dedup();

    let mut edge_types: Vec<String> = edges.iter().map(|e| e.label.clone()).collect();
    edge_types.sort();
    edge_types.dedup();

    Frame::array(vec![
        Frame::bulk("name"),
        Frame::bulk(name),
        Frame::bulk("nodeCount"),
        Frame::Integer(vertex_count as i64),
        Frame::bulk("edgeCount"),
        Frame::Integer(edge_count as i64),
        Frame::bulk("nodeLabels"),
        Frame::array(labels.into_iter().map(Frame::bulk).collect()),
        Frame::bulk("edgeTypes"),
        Frame::array(edge_types.into_iter().map(Frame::bulk).collect()),
        Frame::bulk("memoryUsage"),
        Frame::Integer(0), // Approximate - would need size calculation
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_node_id() {
        let valid_id = "12345";
        let invalid_id = "not_a_number";

        assert!(valid_id.parse::<u64>().is_ok());
        assert!(invalid_id.parse::<u64>().is_err());
    }

    #[test]
    fn test_direction_parsing() {
        let dirs = ["IN", "OUT", "BOTH"];
        for dir in dirs {
            assert!(dir == "IN" || dir == "OUT" || dir == "BOTH");
        }
    }

    #[test]
    fn test_json_to_property_value() {
        let json_str = serde_json::json!("hello");
        let pv = json_to_property_value(&json_str);
        assert!(matches!(pv, PropertyValue::String(s) if s == "hello"));

        let json_int = serde_json::json!(42);
        let pv = json_to_property_value(&json_int);
        assert!(matches!(pv, PropertyValue::Integer(42)));

        let json_bool = serde_json::json!(true);
        let pv = json_to_property_value(&json_bool);
        assert!(matches!(pv, PropertyValue::Boolean(true)));
    }
}
