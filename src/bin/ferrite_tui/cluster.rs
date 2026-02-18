//! Cluster Topology View
//!
//! Provides a visual representation of the Redis/Ferrite cluster topology,
//! including node hierarchy, slot assignments, and replication relationships.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};

/// Represents a node in the cluster
#[derive(Clone, Debug)]
pub struct ClusterNode {
    /// Unique node identifier (40-char hex string in Redis)
    pub id: String,
    /// Network address (host:port)
    pub addr: String,
    /// Node role (master/replica)
    pub role: NodeRole,
    /// Slot ranges this node is responsible for (masters only)
    pub slot_ranges: Vec<(u16, u16)>,
    /// ID of master node (for replicas)
    pub master_id: Option<String>,
    /// Connection status
    pub status: NodeStatus,
    /// Last ping sent timestamp (milliseconds)
    pub ping_sent: u64,
    /// Last pong received timestamp (milliseconds)
    pub pong_recv: u64,
}

/// Node role in the cluster
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Replica,
    Unknown,
}

impl NodeRole {
    // Reserved for cluster node role parsing
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "master" => NodeRole::Master,
            "slave" | "replica" => NodeRole::Replica,
            _ => NodeRole::Unknown,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Replica => "replica",
            NodeRole::Unknown => "unknown",
        }
    }

    pub fn color(&self) -> Color {
        match self {
            NodeRole::Master => Color::Green,
            NodeRole::Replica => Color::Yellow,
            NodeRole::Unknown => Color::Gray,
        }
    }
}

/// Connection status of a node
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Connected,
    Disconnected,
    Handshake,
    NoAddr,
    Fail,
}

impl NodeStatus {
    pub fn from_flags(flags: &str) -> Self {
        if flags.contains("fail") {
            NodeStatus::Fail
        } else if flags.contains("handshake") {
            NodeStatus::Handshake
        } else if flags.contains("noaddr") {
            NodeStatus::NoAddr
        } else if flags.contains("disconnected") {
            NodeStatus::Disconnected
        } else {
            NodeStatus::Connected
        }
    }

    pub fn icon(&self) -> &str {
        match self {
            NodeStatus::Connected => "●",
            NodeStatus::Disconnected => "○",
            NodeStatus::Handshake => "◐",
            NodeStatus::NoAddr => "◌",
            NodeStatus::Fail => "✗",
        }
    }

    pub fn color(&self) -> Color {
        match self {
            NodeStatus::Connected => Color::Green,
            NodeStatus::Disconnected => Color::Red,
            NodeStatus::Handshake => Color::Yellow,
            NodeStatus::NoAddr => Color::DarkGray,
            NodeStatus::Fail => Color::Red,
        }
    }
}

impl ClusterNode {
    /// Create a new cluster node
    pub fn new(
        id: String,
        addr: String,
        role: NodeRole,
        slot_ranges: Vec<(u16, u16)>,
        master_id: Option<String>,
        status: NodeStatus,
    ) -> Self {
        Self {
            id,
            addr,
            role,
            slot_ranges,
            master_id,
            status,
            ping_sent: 0,
            pong_recv: 0,
        }
    }

    /// Calculate ping latency in milliseconds
    pub fn ping_latency(&self) -> Option<u64> {
        if self.pong_recv > 0 && self.ping_sent > 0 && self.pong_recv >= self.ping_sent {
            Some(self.pong_recv - self.ping_sent)
        } else {
            None
        }
    }

    /// Get total number of slots assigned to this node
    pub fn slot_count(&self) -> u16 {
        self.slot_ranges
            .iter()
            .map(|(start, end)| end - start + 1)
            .sum()
    }

    /// Format slot ranges as a compact string
    pub fn format_slots(&self) -> String {
        if self.slot_ranges.is_empty() {
            return String::new();
        }

        let ranges: Vec<String> = self
            .slot_ranges
            .iter()
            .map(|(start, end)| {
                if start == end {
                    format!("{}", start)
                } else {
                    format!("{}-{}", start, end)
                }
            })
            .collect();

        if ranges.len() > 3 {
            format!("[{} ranges, {} slots]", ranges.len(), self.slot_count())
        } else {
            format!("[{}]", ranges.join(","))
        }
    }
}

/// State management for the cluster topology view
pub struct ClusterState {
    /// List of all nodes in the cluster
    pub nodes: Vec<ClusterNode>,
    /// Currently selected node index
    pub selected: usize,
    /// List state for rendering
    pub list_state: ListState,
    /// Whether cluster mode is enabled
    pub cluster_enabled: bool,
    /// Total slots assigned across all nodes
    pub slots_assigned: u16,
    /// Slots that are OK (not in migration/import)
    pub slots_ok: u16,
    /// Number of master nodes
    pub cluster_size: usize,
    /// Cluster state (ok, fail)
    pub cluster_state: String,
    /// Show detailed view for selected node
    pub show_detail: bool,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            selected: 0,
            list_state: ListState::default(),
            cluster_enabled: false,
            slots_assigned: 0,
            slots_ok: 0,
            cluster_size: 0,
            cluster_state: "unknown".to_string(),
            show_detail: false,
        }
    }
}

impl ClusterState {
    /// Create a new cluster state
    // Constructor reserved for TUI cluster view initialization
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the next node
    pub fn select_next(&mut self) {
        if !self.nodes.is_empty() {
            self.selected = (self.selected + 1) % self.nodes.len();
            self.list_state.select(Some(self.selected));
        }
    }

    /// Select the previous node
    pub fn select_previous(&mut self) {
        if !self.nodes.is_empty() {
            if self.selected > 0 {
                self.selected -= 1;
            } else {
                self.selected = self.nodes.len() - 1;
            }
            self.list_state.select(Some(self.selected));
        }
    }

    /// Get the currently selected node
    pub fn selected_node(&self) -> Option<&ClusterNode> {
        self.nodes.get(self.selected)
    }

    /// Get replicas for a given master node ID
    pub fn get_replicas(&self, master_id: &str) -> Vec<&ClusterNode> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Replica && n.master_id.as_deref() == Some(master_id))
            .collect()
    }

    /// Get all master nodes
    // Reserved for cluster topology queries
    #[allow(dead_code)]
    pub fn get_masters(&self) -> Vec<&ClusterNode> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Master)
            .collect()
    }

    /// Calculate cluster health percentage
    pub fn health_percentage(&self) -> f64 {
        if self.slots_assigned == 0 {
            0.0
        } else {
            (self.slots_ok as f64 / self.slots_assigned as f64) * 100.0
        }
    }

    /// Toggle detail view
    pub fn toggle_detail(&mut self) {
        self.show_detail = !self.show_detail;
    }
}

/// Render the cluster topology view
pub fn render_cluster(f: &mut Frame, state: &mut ClusterState, area: Rect) {
    if !state.cluster_enabled {
        let msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "Cluster mode is not enabled on this server.",
                Style::default().fg(Color::Yellow),
            )),
            Line::from(""),
            Line::from("Press 'r' to refresh cluster information."),
            Line::from("Press 'q' to quit."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Cluster Topology ")
                .border_style(Style::default().fg(Color::Yellow)),
        );
        f.render_widget(msg, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7), // Cluster stats
            Constraint::Min(0),    // Node list or detail
        ])
        .split(area);

    render_cluster_stats(f, state, chunks[0]);

    if state.show_detail {
        render_node_detail(f, state, chunks[1]);
    } else {
        render_node_topology(f, state, chunks[1]);
    }
}

/// Render cluster-level statistics
fn render_cluster_stats(f: &mut Frame, state: &ClusterState, area: Rect) {
    let health = state.health_percentage();
    let health_color = if health >= 99.9 {
        Color::Green
    } else if health >= 80.0 {
        Color::Yellow
    } else {
        Color::Red
    };

    let state_color = if state.cluster_state == "ok" {
        Color::Green
    } else {
        Color::Red
    };

    let stats = vec![
        Line::from(vec![
            Span::styled(
                "Cluster State: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                &state.cluster_state,
                Style::default()
                    .fg(state_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled("Health: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.1}%", health),
                Style::default()
                    .fg(health_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                "Total Nodes: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{}  ", state.nodes.len())),
            Span::styled("Masters: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{}  ", state.cluster_size)),
            Span::styled("Replicas: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{}", state.nodes.len() - state.cluster_size)),
        ]),
        Line::from(vec![
            Span::styled("Slots: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "{} assigned, {} OK",
                state.slots_assigned, state.slots_ok
            )),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Keys: [j/k] navigate  [Enter] details  [r] refresh  [Esc] back",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let stats_widget = Paragraph::new(stats).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Cluster Status ")
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(stats_widget, area);
}

/// Render node topology as a hierarchical list
fn render_node_topology(f: &mut Frame, state: &mut ClusterState, area: Rect) {
    let mut items = Vec::new();
    let node_count = state.nodes.len();

    // Clone nodes to avoid borrow issues with list_state
    let nodes: Vec<ClusterNode> = state.nodes.clone();

    // Group nodes by master -> replicas
    let masters: Vec<&ClusterNode> = nodes
        .iter()
        .filter(|n| n.role == NodeRole::Master)
        .collect();

    for master in masters {
        // Render master node
        items.push(create_node_list_item(master, 0));

        // Render replicas of this master
        let replicas: Vec<&ClusterNode> = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Replica && n.master_id.as_ref() == Some(&master.id))
            .collect();
        for replica in replicas {
            items.push(create_node_list_item(replica, 1));
        }
    }

    // Add orphaned replicas (replicas without a known master)
    for node in &nodes {
        if node.role == NodeRole::Replica {
            if let Some(ref master_id) = node.master_id {
                if !nodes.iter().any(|n| &n.id == master_id) {
                    items.push(create_node_list_item(node, 0));
                }
            }
        }
    }

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Cluster Topology ({} nodes) ", node_count))
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut state.list_state);
}

/// Create a list item for a cluster node
fn create_node_list_item(node: &ClusterNode, indent_level: usize) -> ListItem<'_> {
    let indent = "  ".repeat(indent_level);
    let prefix = if indent_level > 0 { "└─ " } else { "" };

    let status_icon = node.status.icon();
    let status_color = node.status.color();

    let role_str = match node.role {
        NodeRole::Master => "[M]",
        NodeRole::Replica => "[R]",
        NodeRole::Unknown => "[?]",
    };
    let role_color = node.role.color();

    let slots_str = node.format_slots();

    let latency_str = if let Some(latency) = node.ping_latency() {
        format!(" ({}ms)", latency)
    } else {
        String::new()
    };

    let line = Line::from(vec![
        Span::raw(indent),
        Span::raw(prefix),
        Span::styled(status_icon, Style::default().fg(status_color)),
        Span::raw(" "),
        Span::styled(role_str, Style::default().fg(role_color)),
        Span::raw(format!(" {} ", node.addr)),
        Span::styled(slots_str, Style::default().fg(Color::Cyan)),
        Span::styled(latency_str, Style::default().fg(Color::DarkGray)),
    ]);

    ListItem::new(line)
}

/// Render detailed view of selected node
fn render_node_detail(f: &mut Frame, state: &ClusterState, area: Rect) {
    let node = match state.selected_node() {
        Some(n) => n,
        None => {
            let msg = Paragraph::new("No node selected").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Node Details "),
            );
            f.render_widget(msg, area);
            return;
        }
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                "Node ID:      ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(&node.id),
        ]),
        Line::from(vec![
            Span::styled(
                "Address:      ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(&node.addr),
        ]),
        Line::from(vec![
            Span::styled(
                "Role:         ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(node.role.as_str(), Style::default().fg(node.role.color())),
        ]),
        Line::from(vec![
            Span::styled(
                "Status:       ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(node.status.icon(), Style::default().fg(node.status.color())),
            Span::raw(" "),
            Span::styled(
                format!("{:?}", node.status),
                Style::default().fg(node.status.color()),
            ),
        ]),
    ];

    if let Some(latency) = node.ping_latency() {
        lines.push(Line::from(vec![
            Span::styled(
                "Ping Latency: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{}ms", latency)),
        ]));
    }

    if node.role == NodeRole::Master {
        lines.push(Line::from(vec![
            Span::styled(
                "Slots:        ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{} total", node.slot_count()),
                Style::default().fg(Color::Cyan),
            ),
        ]));

        if !node.slot_ranges.is_empty() {
            lines.push(Line::from(vec![Span::styled(
                "Ranges:       ",
                Style::default().add_modifier(Modifier::BOLD),
            )]));

            for (start, end) in &node.slot_ranges {
                let range_str = if start == end {
                    format!("  - Slot {}", start)
                } else {
                    format!("  - Slots {}-{} ({} slots)", start, end, end - start + 1)
                };
                lines.push(Line::from(Span::raw(range_str)));
            }
        }

        // Show replicas
        let replicas = state.get_replicas(&node.id);
        if !replicas.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                format!("Replicas:     {} node(s)", replicas.len()),
                Style::default().add_modifier(Modifier::BOLD),
            )]));

            for replica in replicas {
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(
                        replica.status.icon(),
                        Style::default().fg(replica.status.color()),
                    ),
                    Span::raw(format!(" {}", replica.addr)),
                ]));
            }
        }
    } else if node.role == NodeRole::Replica {
        if let Some(ref master_id) = node.master_id {
            lines.push(Line::from(vec![
                Span::styled(
                    "Master ID:    ",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(&master_id[..12.min(master_id.len())]),
            ]));

            // Find and show master address
            if let Some(master) = state.nodes.iter().find(|n| &n.id == master_id) {
                lines.push(Line::from(vec![
                    Span::styled(
                        "Master Addr:  ",
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(&master.addr),
                ]));
            }
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Press [Esc] to return to topology view",
        Style::default().fg(Color::DarkGray),
    )));

    let detail_widget = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Node Details: {} ", node.addr))
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(detail_widget, area);
}
