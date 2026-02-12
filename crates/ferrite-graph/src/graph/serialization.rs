//! Graph serialization and deserialization
//!
//! Provides import/export in GraphML and adjacency list formats.

use super::*;
use std::collections::HashMap;
use std::io::{BufRead, Write};

/// GraphML import/export
pub struct GraphMl;

impl GraphMl {
    /// Export the graph storage to GraphML XML format.
    pub fn export<W: Write>(storage: &GraphStorage, writer: &mut W) -> std::io::Result<()> {
        writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(
            writer,
            r#"<graphml xmlns="http://graphml.graphstruct.org/graphml">"#
        )?;

        // Collect all property keys across vertices and edges
        let mut vertex_keys: HashMap<String, &str> = HashMap::new();
        let mut edge_keys: HashMap<String, &str> = HashMap::new();

        for v in storage.all_vertices() {
            for (k, val) in &v.properties {
                vertex_keys
                    .entry(k.clone())
                    .or_insert(property_type_name(val));
            }
        }
        for e in storage.all_edges() {
            for (k, val) in &e.properties {
                edge_keys
                    .entry(k.clone())
                    .or_insert(property_type_name(val));
            }
        }

        // Write key definitions
        for (name, type_name) in &vertex_keys {
            writeln!(
                writer,
                r#"  <key id="v_{name}" for="node" attr.name="{name}" attr.type="{type_name}"/>"#
            )?;
        }
        writeln!(
            writer,
            r#"  <key id="v_label" for="node" attr.name="_label" attr.type="string"/>"#
        )?;

        for (name, type_name) in &edge_keys {
            writeln!(
                writer,
                r#"  <key id="e_{name}" for="edge" attr.name="{name}" attr.type="{type_name}"/>"#
            )?;
        }
        writeln!(
            writer,
            r#"  <key id="e_label" for="edge" attr.name="_label" attr.type="string"/>"#
        )?;
        writeln!(
            writer,
            r#"  <key id="e_weight" for="edge" attr.name="_weight" attr.type="double"/>"#
        )?;

        writeln!(writer, r#"  <graph id="G" edgedefault="directed">"#)?;

        // Write vertices
        for v in storage.all_vertices() {
            writeln!(writer, r#"    <node id="n{}">"#, v.id.0)?;
            writeln!(
                writer,
                r#"      <data key="v_label">{}</data>"#,
                escape_xml(&v.label)
            )?;
            for (k, val) in &v.properties {
                writeln!(
                    writer,
                    r#"      <data key="v_{}">{}</data>"#,
                    escape_xml(k),
                    escape_xml(&property_to_string(val))
                )?;
            }
            writeln!(writer, r#"    </node>"#)?;
        }

        // Write edges
        for e in storage.all_edges() {
            writeln!(
                writer,
                r#"    <edge id="e{}" source="n{}" target="n{}">"#,
                e.id.0, e.from.0, e.to.0
            )?;
            writeln!(
                writer,
                r#"      <data key="e_label">{}</data>"#,
                escape_xml(&e.label)
            )?;
            writeln!(writer, r#"      <data key="e_weight">{}</data>"#, e.weight)?;
            for (k, val) in &e.properties {
                writeln!(
                    writer,
                    r#"      <data key="e_{}">{}</data>"#,
                    escape_xml(k),
                    escape_xml(&property_to_string(val))
                )?;
            }
            writeln!(writer, r#"    </edge>"#)?;
        }

        writeln!(writer, r#"  </graph>"#)?;
        writeln!(writer, r#"</graphml>"#)?;

        Ok(())
    }

    /// Export graph to a GraphML string.
    pub fn export_string(storage: &GraphStorage) -> String {
        let mut buf = Vec::new();
        Self::export(storage, &mut buf).expect("writing to Vec should not fail");
        String::from_utf8(buf).expect("GraphML output should be valid UTF-8")
    }

    /// Import a graph from GraphML XML format.
    ///
    /// This is a simplified parser that handles the subset of GraphML produced by [`export`].
    pub fn import<R: BufRead>(reader: R, storage: &mut GraphStorage) -> Result<ImportStats> {
        let content: String = reader
            .lines()
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|e| GraphError::InvalidQuery(format!("IO error: {}", e)))?
            .join("\n");

        let mut vertex_count = 0usize;
        let mut edge_count = 0usize;

        // Parse key definitions to understand property types
        let mut key_types: HashMap<String, String> = HashMap::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("<key ") {
                if let (Some(id), Some(attr_type)) = (
                    extract_attr(trimmed, "id"),
                    extract_attr(trimmed, "attr.type"),
                ) {
                    key_types.insert(id, attr_type);
                }
            }
        }

        // Parse nodes
        let mut in_node = false;
        let mut current_node_id: Option<u64> = None;
        let mut node_label = String::new();
        let mut node_props: HashMap<String, PropertyValue> = HashMap::new();

        // Parse edges
        let mut in_edge = false;
        let mut current_edge_src: Option<u64> = None;
        let mut current_edge_tgt: Option<u64> = None;
        let mut edge_label = String::new();
        let mut edge_weight = 1.0f64;
        let mut edge_props: HashMap<String, PropertyValue> = HashMap::new();

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("<node ") {
                in_node = true;
                if let Some(id_str) = extract_attr(trimmed, "id") {
                    current_node_id = id_str.strip_prefix('n').and_then(|s| s.parse::<u64>().ok());
                }
                node_label.clear();
                node_props.clear();
            } else if trimmed == "</node>" {
                if let Some(nid) = current_node_id {
                    let label = if node_label.is_empty() {
                        "Node".to_string()
                    } else {
                        node_label.clone()
                    };
                    let mut vertex = Vertex::new(VertexId::new(nid), &label);
                    vertex.properties = node_props.clone();
                    storage.add_vertex(vertex)?;
                    vertex_count += 1;
                }
                in_node = false;
                current_node_id = None;
            } else if trimmed.starts_with("<edge ") {
                in_edge = true;
                current_edge_src = extract_attr(trimmed, "source")
                    .and_then(|s| s.strip_prefix('n').and_then(|s| s.parse::<u64>().ok()));
                current_edge_tgt = extract_attr(trimmed, "target")
                    .and_then(|s| s.strip_prefix('n').and_then(|s| s.parse::<u64>().ok()));
                edge_label.clear();
                edge_weight = 1.0;
                edge_props.clear();
            } else if trimmed == "</edge>" {
                if let (Some(src), Some(tgt)) = (current_edge_src, current_edge_tgt) {
                    let label = if edge_label.is_empty() {
                        "RELATED_TO".to_string()
                    } else {
                        edge_label.clone()
                    };
                    let eid = edge::next_edge_id();
                    let mut edge = Edge::new(eid, VertexId::new(src), VertexId::new(tgt), &label);
                    edge.weight = edge_weight;
                    edge.properties = edge_props.clone();
                    storage.add_edge(edge)?;
                    edge_count += 1;
                }
                in_edge = false;
            } else if trimmed.starts_with("<data ") {
                if let Some(key) = extract_attr(trimmed, "key") {
                    let value_str = extract_data_value(trimmed);
                    let value_str = unescape_xml(&value_str);

                    if in_node {
                        if key == "v_label" {
                            node_label = value_str;
                        } else if let Some(prop_name) = key.strip_prefix("v_") {
                            let type_hint = key_types.get(&key).map(|s| s.as_str());
                            node_props
                                .insert(prop_name.to_string(), parse_value(&value_str, type_hint));
                        }
                    } else if in_edge {
                        if key == "e_label" {
                            edge_label = value_str;
                        } else if key == "e_weight" {
                            edge_weight = value_str.parse::<f64>().unwrap_or(1.0);
                        } else if let Some(prop_name) = key.strip_prefix("e_") {
                            let type_hint = key_types.get(&key).map(|s| s.as_str());
                            edge_props
                                .insert(prop_name.to_string(), parse_value(&value_str, type_hint));
                        }
                    }
                }
            }
        }

        Ok(ImportStats {
            vertices_imported: vertex_count,
            edges_imported: edge_count,
        })
    }
}

/// Adjacency list import/export
pub struct AdjList;

impl AdjList {
    /// Export the graph as an adjacency list.
    ///
    /// Format: `vertex_id label [prop=val ...] -> neighbor_id:edge_label:weight ...`
    pub fn export<W: Write>(storage: &GraphStorage, writer: &mut W) -> std::io::Result<()> {
        // Header
        writeln!(
            writer,
            "# Adjacency list: vertex_id label [props] -> neighbor_id:edge_label:weight ..."
        )?;
        writeln!(
            writer,
            "# Vertices: {}, Edges: {}",
            storage.vertex_count(),
            storage.edge_count()
        )?;

        let mut vertex_ids = storage.vertex_ids();
        vertex_ids.sort_by_key(|v| v.0);

        for vid in vertex_ids {
            if let Some(vertex) = storage.get_vertex_ref(vid) {
                // Write vertex info
                write!(writer, "{} {}", vid.0, vertex.label)?;

                // Write properties
                for (k, v) in &vertex.properties {
                    write!(writer, " {}={}", k, property_to_string(v))?;
                }

                // Write adjacency
                let out_edges = storage.get_out_edges(vid);
                if !out_edges.is_empty() {
                    write!(writer, " ->")?;
                    for eid in out_edges {
                        if let Some(edge) = storage.get_edge_ref(eid) {
                            write!(writer, " {}:{}:{}", edge.to.0, edge.label, edge.weight)?;
                        }
                    }
                }

                writeln!(writer)?;
            }
        }

        Ok(())
    }

    /// Export graph to an adjacency list string.
    pub fn export_string(storage: &GraphStorage) -> String {
        let mut buf = Vec::new();
        Self::export(storage, &mut buf).expect("writing to Vec should not fail");
        String::from_utf8(buf).expect("adjacency list output should be valid UTF-8")
    }

    /// Import a graph from adjacency list format.
    ///
    /// Lines starting with `#` are comments. Format:
    /// `vertex_id label [key=value ...] [-> neighbor_id:edge_label:weight ...]`
    pub fn import<R: BufRead>(reader: R, storage: &mut GraphStorage) -> Result<ImportStats> {
        let mut vertex_count = 0usize;
        let mut edge_count = 0usize;

        // First pass: collect all vertices
        let lines: Vec<String> = reader
            .lines()
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|e| GraphError::InvalidQuery(format!("IO error: {}", e)))?;

        // Parse vertices first
        for line in &lines {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (vertex_part, _) = line.split_once(" ->").unwrap_or((line, ""));
            let parts: Vec<&str> = vertex_part.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }

            let vid: u64 = parts[0].parse().map_err(|_| {
                GraphError::InvalidQuery(format!("Invalid vertex ID: {}", parts[0]))
            })?;
            let label = parts[1];

            let mut vertex = Vertex::new(VertexId::new(vid), label);

            // Parse properties
            for part in &parts[2..] {
                if let Some((k, v)) = part.split_once('=') {
                    vertex
                        .properties
                        .insert(k.to_string(), parse_value(v, None));
                }
            }

            storage.add_vertex(vertex)?;
            vertex_count += 1;
        }

        // Parse edges
        for line in &lines {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((vertex_part, adj_part)) = line.split_once(" ->") {
                let parts: Vec<&str> = vertex_part.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }

                let from_id: u64 = parts[0].parse().map_err(|_| {
                    GraphError::InvalidQuery(format!("Invalid vertex ID: {}", parts[0]))
                })?;

                for adj in adj_part.split_whitespace() {
                    let adj_parts: Vec<&str> = adj.split(':').collect();
                    if adj_parts.len() < 2 {
                        continue;
                    }

                    let to_id: u64 = adj_parts[0].parse().map_err(|_| {
                        GraphError::InvalidQuery(format!("Invalid neighbor ID: {}", adj_parts[0]))
                    })?;
                    let label = adj_parts[1];
                    let weight: f64 = adj_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(1.0);

                    let eid = edge::next_edge_id();
                    let mut edge =
                        Edge::new(eid, VertexId::new(from_id), VertexId::new(to_id), label);
                    edge.weight = weight;
                    storage.add_edge(edge)?;
                    edge_count += 1;
                }
            }
        }

        Ok(ImportStats {
            vertices_imported: vertex_count,
            edges_imported: edge_count,
        })
    }
}

/// Statistics from an import operation
#[derive(Debug, Clone)]
pub struct ImportStats {
    /// Number of vertices imported
    pub vertices_imported: usize,
    /// Number of edges imported
    pub edges_imported: usize,
}

// --- Helper functions ---

fn property_type_name(val: &PropertyValue) -> &'static str {
    match val {
        PropertyValue::String(_) => "string",
        PropertyValue::Integer(_) => "long",
        PropertyValue::Float(_) => "double",
        PropertyValue::Boolean(_) => "boolean",
        _ => "string",
    }
}

fn property_to_string(val: &PropertyValue) -> String {
    match val {
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => f.to_string(),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Null => "null".to_string(),
        PropertyValue::List(l) => format!("[{} items]", l.len()),
        PropertyValue::Map(m) => format!("{{{} entries}}", m.len()),
    }
}

fn parse_value(s: &str, type_hint: Option<&str>) -> PropertyValue {
    match type_hint {
        Some("long") | Some("int") => s
            .parse::<i64>()
            .map(PropertyValue::Integer)
            .unwrap_or_else(|_| PropertyValue::String(s.to_string())),
        Some("double") | Some("float") => s
            .parse::<f64>()
            .map(PropertyValue::Float)
            .unwrap_or_else(|_| PropertyValue::String(s.to_string())),
        Some("boolean") => s
            .parse::<bool>()
            .map(PropertyValue::Boolean)
            .unwrap_or_else(|_| PropertyValue::String(s.to_string())),
        _ => {
            // Auto-detect type
            if let Ok(i) = s.parse::<i64>() {
                PropertyValue::Integer(i)
            } else if let Ok(f) = s.parse::<f64>() {
                PropertyValue::Float(f)
            } else if s == "true" {
                PropertyValue::Boolean(true)
            } else if s == "false" {
                PropertyValue::Boolean(false)
            } else if s == "null" {
                PropertyValue::Null
            } else {
                PropertyValue::String(s.to_string())
            }
        }
    }
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn unescape_xml(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn extract_attr(tag: &str, attr_name: &str) -> Option<String> {
    let search = format!("{}=\"", attr_name);
    if let Some(start) = tag.find(&search) {
        let value_start = start + search.len();
        if let Some(end) = tag[value_start..].find('"') {
            return Some(tag[value_start..value_start + end].to_string());
        }
    }
    None
}

fn extract_data_value(tag: &str) -> String {
    // Extract value between > and </data>
    if let Some(start) = tag.find('>') {
        let rest = &tag[start + 1..];
        if let Some(end) = rest.find("</data>") {
            return rest[..end].to_string();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let mut v1 = Vertex::new(VertexId::new(200), "Person");
        v1.set("name", "Alice");
        v1.set("age", 30i64);
        storage.add_vertex(v1).unwrap();

        let mut v2 = Vertex::new(VertexId::new(201), "Person");
        v2.set("name", "Bob");
        v2.set("age", 25i64);
        storage.add_vertex(v2).unwrap();

        let mut v3 = Vertex::new(VertexId::new(202), "Company");
        v3.set("name", "Acme");
        storage.add_vertex(v3).unwrap();

        let mut e1 = Edge::new(
            EdgeId::new(200),
            VertexId::new(200),
            VertexId::new(201),
            "KNOWS",
        );
        e1.set("since", 2020i64);
        e1.weight = 0.8;
        storage.add_edge(e1).unwrap();

        let e2 = Edge::new(
            EdgeId::new(201),
            VertexId::new(200),
            VertexId::new(202),
            "WORKS_AT",
        );
        storage.add_edge(e2).unwrap();

        storage
    }

    // --- GraphML tests ---

    #[test]
    fn test_graphml_export() {
        let storage = create_test_graph();
        let xml = GraphMl::export_string(&storage);

        assert!(xml.contains("<graphml"));
        assert!(xml.contains("</graphml>"));
        assert!(xml.contains("n200"));
        assert!(xml.contains("n201"));
        assert!(xml.contains("Alice"));
    }

    #[test]
    fn test_graphml_roundtrip() {
        let original = create_test_graph();
        let xml = GraphMl::export_string(&original);

        let mut imported = GraphStorage::new();
        let stats = GraphMl::import(xml.as_bytes(), &mut imported).unwrap();

        assert_eq!(stats.vertices_imported, 3);
        assert_eq!(stats.edges_imported, 2);

        assert_eq!(imported.vertex_count(), original.vertex_count());
        assert_eq!(imported.edge_count(), original.edge_count());

        // Check a vertex
        let alice = imported.get_vertex(VertexId::new(200)).unwrap();
        assert_eq!(alice.label, "Person");
        assert_eq!(alice.get_string("name"), Some("Alice"));
    }

    #[test]
    fn test_graphml_properties_preserved() {
        let original = create_test_graph();
        let xml = GraphMl::export_string(&original);

        let mut imported = GraphStorage::new();
        GraphMl::import(xml.as_bytes(), &mut imported).unwrap();

        let alice = imported.get_vertex(VertexId::new(200)).unwrap();
        assert_eq!(alice.get_integer("age"), Some(30));
    }

    #[test]
    fn test_graphml_edge_weight_preserved() {
        let original = create_test_graph();
        let xml = GraphMl::export_string(&original);

        let mut imported = GraphStorage::new();
        GraphMl::import(xml.as_bytes(), &mut imported).unwrap();

        // Find the KNOWS edge
        let edges = imported.all_edges();
        let knows = edges.iter().find(|e| e.label == "KNOWS").unwrap();
        assert!((knows.weight - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_graphml_xml_escaping() {
        let mut storage = GraphStorage::new();
        let mut v = Vertex::new(VertexId::new(300), "Test");
        v.set("data", "a < b & c > d");
        storage.add_vertex(v).unwrap();

        let xml = GraphMl::export_string(&storage);
        assert!(xml.contains("&lt;"));
        assert!(xml.contains("&amp;"));
        assert!(xml.contains("&gt;"));

        // Roundtrip
        let mut imported = GraphStorage::new();
        GraphMl::import(xml.as_bytes(), &mut imported).unwrap();
        let v = imported.get_vertex(VertexId::new(300)).unwrap();
        assert_eq!(v.get_string("data"), Some("a < b & c > d"));
    }

    // --- Adjacency list tests ---

    #[test]
    fn test_adjlist_export() {
        let storage = create_test_graph();
        let output = AdjList::export_string(&storage);

        assert!(output.contains("200 Person"));
        assert!(output.contains("201 Person"));
        assert!(output.contains("202 Company"));
        assert!(output.contains("->"));
    }

    #[test]
    fn test_adjlist_roundtrip() {
        let original = create_test_graph();
        let output = AdjList::export_string(&original);

        let mut imported = GraphStorage::new();
        let stats = AdjList::import(output.as_bytes(), &mut imported).unwrap();

        assert_eq!(stats.vertices_imported, 3);
        assert_eq!(stats.edges_imported, 2);

        assert_eq!(imported.vertex_count(), original.vertex_count());
        assert_eq!(imported.edge_count(), original.edge_count());
    }

    #[test]
    fn test_adjlist_vertex_properties() {
        let original = create_test_graph();
        let output = AdjList::export_string(&original);

        let mut imported = GraphStorage::new();
        AdjList::import(output.as_bytes(), &mut imported).unwrap();

        let alice = imported.get_vertex(VertexId::new(200)).unwrap();
        assert_eq!(alice.label, "Person");
    }

    #[test]
    fn test_adjlist_comments_ignored() {
        let input = "# This is a comment\n1 Person name=Alice\n2 Person name=Bob\n1 -> 2:KNOWS:1\n";

        let mut storage = GraphStorage::new();
        let stats = AdjList::import(input.as_bytes(), &mut storage).unwrap();

        assert_eq!(stats.vertices_imported, 2);
        assert_eq!(stats.edges_imported, 1);
    }

    #[test]
    fn test_adjlist_empty() {
        let input = "# Empty graph\n";
        let mut storage = GraphStorage::new();
        let stats = AdjList::import(input.as_bytes(), &mut storage).unwrap();

        assert_eq!(stats.vertices_imported, 0);
        assert_eq!(stats.edges_imported, 0);
    }

    // --- Import stats tests ---

    #[test]
    fn test_import_stats() {
        let stats = ImportStats {
            vertices_imported: 5,
            edges_imported: 10,
        };
        assert_eq!(stats.vertices_imported, 5);
        assert_eq!(stats.edges_imported, 10);
    }
}
