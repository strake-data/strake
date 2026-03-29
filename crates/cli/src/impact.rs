use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::LazyLock;
use strake_common::models::{ContractsConfig, SourcesConfig};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize)]
#[allow(dead_code)]
pub enum NodeId {
    Source(String),
    Table(String, String),          // source, table
    Column(String, String, String), // source, table, column
    Contract(String, String),       // source, table
    Policy(String),
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub enum EdgeKind {
    TableOf,
    ColumnOf,
    ContractCovers,
    ColumnConstrained,
    PolicyCovers,
    ColumnMasked,
}

#[allow(dead_code)]
pub struct ReferenceGraph {
    pub nodes: HashMap<NodeId, GraphNode>,
    pub edges: Vec<(NodeId, NodeId, EdgeKind)>,
}

#[allow(dead_code)]
pub struct GraphNode {
    pub id: NodeId,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImpactRecord {
    pub trigger: String, // Path of the change
    pub affected: Vec<AffectedRef>,
    pub static_only: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AffectedRef {
    pub kind: AffectedKind,
    pub entity: String,
    pub severity: ImpactSeverity,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AffectedKind {
    Contract,
    Policy,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum ImpactSeverity {
    Info,
    Warning,
    RestrictionRemoved,
    ContractBreaking,
}

static COL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"sources\[(\d+)\]\.tables\[(\d+)\]\.(?:column_definitions|columns)\[(\d+)\]")
        .unwrap()
});

static TBL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"sources\[(\d+)\]\.tables\[(\d+)\]").unwrap());

impl ReferenceGraph {
    pub fn build(sources: &SourcesConfig, contracts: &ContractsConfig) -> Self {
        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        for source in &sources.sources {
            let s_id = NodeId::Source(source.name.to_string());
            nodes.insert(
                s_id.clone(),
                GraphNode {
                    id: s_id.clone(),
                    label: source.name.to_string(),
                },
            );

            for table in &source.tables {
                let t_id = NodeId::Table(source.name.to_string(), table.name.clone());
                nodes.insert(
                    t_id.clone(),
                    GraphNode {
                        id: t_id.clone(),
                        label: format!("{}.{}", table.schema, table.name),
                    },
                );
                edges.push((s_id.clone(), t_id.clone(), EdgeKind::TableOf));

                for col in &table.column_definitions {
                    let c_id = NodeId::Column(
                        source.name.to_string(),
                        table.name.clone(),
                        col.name.clone(),
                    );
                    nodes.insert(
                        c_id.clone(),
                        GraphNode {
                            id: c_id.clone(),
                            label: col.name.clone(),
                        },
                    );
                    edges.push((t_id.clone(), c_id.clone(), EdgeKind::ColumnOf));
                }
            }
        }

        // Contracts
        for contract in &contracts.contracts {
            // How to find source for a contract?
            // In Phase 3.3, contracts will have source/table field.
            // For now, let's assume table matches name.
            let parts: Vec<&str> = contract.table.split('.').collect();
            if parts.len() == 2 {
                let t_id = NodeId::Table("unknown".to_string(), parts[1].to_string()); // FIXME: resolve source
                let con_id = NodeId::Contract("unknown".to_string(), parts[1].to_string());
                nodes.insert(
                    con_id.clone(),
                    GraphNode {
                        id: con_id.clone(),
                        label: format!("Contract: {}", contract.table),
                    },
                );
                edges.push((con_id.clone(), t_id.clone(), EdgeKind::ContractCovers));
            }
        }

        Self { nodes, edges }
    }

    pub fn impact_of(
        &self,
        changes: &[crate::commands::DiffChange],
        sources: &SourcesConfig,
    ) -> ImpactRecord {
        let mut affected = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for change in changes {
            let (source, table, col) = self.parse_path(&change.path, sources);
            if source == "unknown" {
                continue;
            }

            let start_node = if let Some(c) = col {
                NodeId::Column(source, table, c)
            } else {
                NodeId::Table(source, table)
            };

            self.traverse_impact(&start_node, &mut affected, &mut seen);
        }

        // Sort by severity (descending)
        affected.sort_by(|a, b| b.severity.cmp(&a.severity));

        ImpactRecord {
            trigger: "multiple changes".to_string(),
            affected,
            static_only: true,
        }
    }

    fn parse_path(&self, path: &str, sources: &SourcesConfig) -> (String, String, Option<String>) {
        if let Some(caps) = COL_RE.captures(path) {
            let s_idx: usize = caps[1].parse().unwrap_or(0);
            let t_idx: usize = caps[2].parse().unwrap_or(0);
            let c_idx: usize = caps[3].parse().unwrap_or(0);

            let res = sources.sources.get(s_idx).and_then(|s| {
                s.tables.get(t_idx).and_then(|t| {
                    t.column_definitions
                        .get(c_idx)
                        .map(|c| (s.name.to_string(), t.name.clone(), Some(c.name.clone())))
                })
            });
            if let Some(res) = res {
                return res;
            }
        } else if let Some(caps) = TBL_RE.captures(path) {
            let s_idx: usize = caps[1].parse().unwrap_or(0);
            let t_idx: usize = caps[2].parse().unwrap_or(0);

            let res = sources.sources.get(s_idx).and_then(|s| {
                s.tables
                    .get(t_idx)
                    .map(|t| (s.name.to_string(), t.name.clone(), None))
            });
            if let Some(res) = res {
                return res;
            }
        }

        ("unknown".to_string(), "unknown".to_string(), None)
    }

    fn traverse_impact(
        &self,
        node: &NodeId,
        affected: &mut Vec<AffectedRef>,
        seen: &mut std::collections::HashSet<NodeId>,
    ) {
        if !seen.insert(node.clone()) {
            return;
        }

        // Find all edges leading OUT of this node (dependents)
        // Actually, in our graph, Contract -> Table, so we need to find edges leading TO this node?
        // Let's reverse the direction or search edges.
        for (from, to, kind) in &self.edges {
            if to == node {
                match kind {
                    EdgeKind::ContractCovers | EdgeKind::ColumnConstrained => {
                        if let NodeId::Contract(s, t) = from {
                            affected.push(AffectedRef {
                                kind: AffectedKind::Contract,
                                entity: format!("{}.{}", s, t),
                                severity: ImpactSeverity::Warning,
                            });
                        }
                    }
                    EdgeKind::PolicyCovers | EdgeKind::ColumnMasked => {
                        if let NodeId::Policy(p) = from {
                            affected.push(AffectedRef {
                                kind: AffectedKind::Policy,
                                entity: p.clone(),
                                severity: ImpactSeverity::Warning,
                            });
                        }
                    }
                    _ => {}
                }
                self.traverse_impact(from, affected, seen);
            }
        }
    }
}
