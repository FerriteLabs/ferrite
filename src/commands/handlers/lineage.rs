//! LINEAGE.* command handlers
//!
//! LINEAGE.RECORD, LINEAGE.ANCESTORS, LINEAGE.DESCENDANTS, LINEAGE.IMPACT,
//! LINEAGE.PATH, LINEAGE.EXPORT, LINEAGE.STATS, LINEAGE.PRUNE, LINEAGE.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use crate::protocol::Frame;
use ferrite_core::audit::lineage::{
    EdgeType, LineageConfig, LineageEdge, LineageGraph, LineageNode, NodeType,
};

use super::{err_frame, ok_frame};

/// Global lineage graph singleton.
static LINEAGE_GRAPH: OnceLock<LineageGraph> = OnceLock::new();

fn get_graph() -> &'static LineageGraph {
    LINEAGE_GRAPH.get_or_init(|| LineageGraph::new(LineageConfig::default()))
}

/// Dispatch a `LINEAGE` subcommand.
pub fn lineage_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "RECORD" => handle_record(args),
        "ANCESTORS" => handle_ancestors(args),
        "DESCENDANTS" => handle_descendants(args),
        "IMPACT" => handle_impact(args),
        "PATH" => handle_path(args),
        "EXPORT" => handle_export(args),
        "STATS" => handle_stats(),
        "PRUNE" => handle_prune(args),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown LINEAGE subcommand '{}'", other)),
    }
}

/// LINEAGE.RECORD source_type source_name edge_type target_type target_name
fn handle_record(args: &[String]) -> Frame {
    if args.len() < 5 {
        return err_frame(
            "LINEAGE.RECORD requires: source_type source_name edge_type target_type target_name",
        );
    }

    let source_type = match NodeType::from_str_loose(&args[0]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown source node type '{}'", args[0])),
    };
    let source_name = &args[1];
    let edge_type = match EdgeType::from_str_loose(&args[2]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown edge type '{}'", args[2])),
    };
    let target_type = match NodeType::from_str_loose(&args[3]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown target node type '{}'", args[3])),
    };
    let target_name = &args[4];

    let mut metadata = HashMap::new();
    // Parse optional metadata key-value pairs after the 5 required args
    let mut i = 5;
    while i + 1 < args.len() {
        metadata.insert(args[i].clone(), args[i + 1].clone());
        i += 2;
    }

    get_graph().record_edge(LineageEdge {
        source: LineageNode::new(source_type, source_name.as_str()),
        target: LineageNode::new(target_type, target_name.as_str()),
        edge_type,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        metadata,
    });

    ok_frame()
}

/// LINEAGE.ANCESTORS node_type name [DEPTH n]
fn handle_ancestors(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("LINEAGE.ANCESTORS requires: node_type name [DEPTH n]");
    }

    let node_type = match NodeType::from_str_loose(&args[0]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown node type '{}'", args[0])),
    };
    let name = &args[1];
    let depth = parse_depth_arg(args, 2);

    let node = LineageNode::new(node_type, name.as_str());
    let ancestors = get_graph().ancestors(&node, depth);
    nodes_to_frame(&ancestors)
}

/// LINEAGE.DESCENDANTS node_type name [DEPTH n]
fn handle_descendants(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("LINEAGE.DESCENDANTS requires: node_type name [DEPTH n]");
    }

    let node_type = match NodeType::from_str_loose(&args[0]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown node type '{}'", args[0])),
    };
    let name = &args[1];
    let depth = parse_depth_arg(args, 2);

    let node = LineageNode::new(node_type, name.as_str());
    let descendants = get_graph().descendants(&node, depth);
    nodes_to_frame(&descendants)
}

/// LINEAGE.IMPACT key
fn handle_impact(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("LINEAGE.IMPACT requires: key");
    }

    let report = get_graph().impact_analysis(&args[0]);
    let source_key = report.source_key.clone();

    Frame::array(vec![
        Frame::bulk("source_key"),
        Frame::bulk(source_key),
        Frame::bulk("affected_views"),
        Frame::Integer(report.affected_views.len() as i64),
        Frame::bulk("affected_triggers"),
        Frame::Integer(report.affected_triggers.len() as i64),
        Frame::bulk("affected_functions"),
        Frame::Integer(report.affected_functions.len() as i64),
        Frame::bulk("affected_keys"),
        Frame::Integer(report.affected_keys.len() as i64),
        Frame::bulk("total_downstream"),
        Frame::Integer(report.total_downstream as i64),
        Frame::bulk("max_depth"),
        Frame::Integer(report.max_depth as i64),
    ])
}

/// LINEAGE.PATH from_type from_name to_type to_name
fn handle_path(args: &[String]) -> Frame {
    if args.len() < 4 {
        return err_frame("LINEAGE.PATH requires: from_type from_name to_type to_name");
    }

    let from_type = match NodeType::from_str_loose(&args[0]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown from node type '{}'", args[0])),
    };
    let from_name = &args[1];
    let to_type = match NodeType::from_str_loose(&args[2]) {
        Some(t) => t,
        None => return err_frame(&format!("unknown to node type '{}'", args[2])),
    };
    let to_name = &args[3];

    let from = LineageNode::new(from_type, from_name.as_str());
    let to = LineageNode::new(to_type, to_name.as_str());

    match get_graph().lineage_path(&from, &to) {
        Some(path) => {
            let items: Vec<Frame> = path
                .iter()
                .map(|edge| {
                    Frame::array(vec![
                        Frame::bulk(format!("{}:{}", edge.source.node_type, edge.source.name)),
                        Frame::bulk(edge.edge_type.to_string()),
                        Frame::bulk(format!("{}:{}", edge.target.node_type, edge.target.name)),
                    ])
                })
                .collect();
            Frame::array(items)
        }
        None => Frame::Null,
    }
}

/// LINEAGE.EXPORT [DOT]
fn handle_export(_args: &[String]) -> Frame {
    let dot = get_graph().export_dot();
    Frame::bulk(dot)
}

/// LINEAGE.STATS
fn handle_stats() -> Frame {
    let stats = get_graph().graph_stats();
    let most_connected = stats.most_connected_node.clone();

    Frame::array(vec![
        Frame::bulk("nodes"),
        Frame::Integer(stats.nodes as i64),
        Frame::bulk("edges"),
        Frame::Integer(stats.edges as i64),
        Frame::bulk("max_depth"),
        Frame::Integer(stats.max_depth as i64),
        Frame::bulk("orphaned_nodes"),
        Frame::Integer(stats.orphaned_nodes as i64),
        Frame::bulk("most_connected_node"),
        match most_connected {
            Some(n) => Frame::bulk(n),
            None => Frame::Null,
        },
        Frame::bulk("avg_degree"),
        Frame::bulk(format!("{:.2}", stats.avg_degree)),
    ])
}

/// LINEAGE.PRUNE [OLDER_THAN seconds]
fn handle_prune(args: &[String]) -> Frame {
    let mut older_than_secs = 86400u64; // default 1 day

    let mut i = 0;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "OLDER_THAN" => {
                if i + 1 < args.len() {
                    older_than_secs = args[i + 1].parse().unwrap_or(86400);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let pruned = get_graph().prune_old(Duration::from_secs(older_than_secs));
    Frame::Integer(pruned as i64)
}

/// LINEAGE.HELP
fn handle_help() -> Frame {
    let help = vec![
        "LINEAGE.RECORD source_type source_name edge_type target_type target_name — Record a lineage edge",
        "LINEAGE.ANCESTORS node_type name [DEPTH n] — Find upstream ancestors (BFS)",
        "LINEAGE.DESCENDANTS node_type name [DEPTH n] — Find downstream descendants (BFS)",
        "LINEAGE.IMPACT key — Impact analysis: what is affected if key changes",
        "LINEAGE.PATH from_type from_name to_type to_name — Find path between nodes",
        "LINEAGE.EXPORT [DOT] — Export graph as Graphviz DOT",
        "LINEAGE.STATS — Graph statistics",
        "LINEAGE.PRUNE [OLDER_THAN seconds] — Remove old edges",
        "LINEAGE.HELP — Show this help",
        "",
        "Node types: KEY, VIEW, TRIGGER, FUNCTION, CLIENT, EXTERNAL",
        "Edge types: DERIVED_FROM, WRITTEN_BY, READ_BY, TRIGGERED_BY, MATERIALIZED_FROM, DEPENDS, FEEDS",
    ];

    Frame::array(help.into_iter().map(Frame::bulk).collect())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_depth_arg(args: &[String], start: usize) -> usize {
    let mut i = start;
    while i + 1 < args.len() {
        if args[i].to_uppercase() == "DEPTH" {
            return args[i + 1].parse().unwrap_or(10);
        }
        i += 1;
    }
    10 // default depth
}

fn nodes_to_frame(nodes: &[LineageNode]) -> Frame {
    let items: Vec<Frame> = nodes
        .iter()
        .map(|n| Frame::bulk(format!("{}:{}", n.node_type, n.name)))
        .collect();
    Frame::array(items)
}
