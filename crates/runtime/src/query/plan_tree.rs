//! Query execution plan tree visualization.
//!
//! Provides ASCII tree formatting of DataFusion execution plans with
//! federation pushdown indicators, timing metrics, and row counts.

use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Write;
use std::sync::Arc;

/// Formats an execution plan as an ASCII tree with federation details.
pub struct PlanTreeFormatter {
    /// Show federation indicators ([PUSHED] markers)
    show_federation: bool,
    /// Show pushdown details (filters, projections, limits)
    show_pushdown: bool,
    /// Show metrics (timing, row counts) if available
    show_metrics: bool,
}

impl Default for PlanTreeFormatter {
    fn default() -> Self {
        Self {
            show_federation: true,
            show_pushdown: true,
            show_metrics: true,
        }
    }
}

impl PlanTreeFormatter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Format an execution plan as an ASCII tree.
    pub fn format(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        let mut output = String::new();
        let _ = writeln!(
            output,
            "\n╔══════════════════════════════════════════════════════════════╗"
        );
        let _ = writeln!(
            output,
            "║              QUERY EXECUTION PLAN TREE                       ║"
        );
        let _ = writeln!(
            output,
            "╚══════════════════════════════════════════════════════════════╝\n"
        );

        self.format_node(plan, "", true, &mut output);

        output
    }

    fn format_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        prefix: &str,
        is_last: bool,
        output: &mut String,
    ) {
        let connector = if is_last { "└─" } else { "├─" };
        let child_prefix = if is_last { "  " } else { "│ " };

        // Get node name and details
        let node_name = self.get_node_name(plan);
        let federation_marker = self.get_federation_marker(plan);
        let metrics = self.get_metrics(plan);

        // Build the main line
        let _ = write!(output, "{}{} {}", prefix, connector, node_name);

        if !federation_marker.is_empty() {
            let _ = write!(output, " {}", federation_marker);
        }

        if self.show_metrics && !metrics.is_empty() {
            let _ = write!(output, " {}", metrics);
        }

        let _ = writeln!(output);

        // Show additional details for certain node types
        if self.show_pushdown {
            self.write_node_details(plan, prefix, child_prefix, output);
        }

        // Recurse into children
        let children = plan.children();
        for (i, child) in children.iter().enumerate() {
            let new_prefix = format!("{}{}", prefix, child_prefix);
            let is_last_child = i == children.len() - 1;
            self.format_node(child, &new_prefix, is_last_child, output);
        }
    }

    fn get_node_name(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        let name = plan.name().to_string();

        // Clean up common suffixes for readability
        name.replace("Exec", "")
    }

    fn get_federation_marker(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        if !self.show_federation {
            return String::new();
        }

        let name = plan.name();

        // Check for federation-related nodes
        if name.contains("SqlExec") || name.contains("Cooperative") {
            return "[PUSHED]".to_string();
        }

        String::new()
    }

    fn get_metrics(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        // Try to get metrics from the plan
        if let Some(metrics) = plan.metrics() {
            let mut parts = Vec::new();

            // Look for output rows
            if let Some(output_rows) = metrics.output_rows() {
                parts.push(format!("rows: {}", output_rows));
            }

            // Look for elapsed compute time
            if let Some(elapsed) = metrics.elapsed_compute() {
                let ms = elapsed as f64 / 1_000_000.0;
                if ms > 0.0 {
                    parts.push(format!("{:.1}ms", ms));
                }
            }

            if !parts.is_empty() {
                return format!("({})", parts.join(", "));
            }
        }

        String::new()
    }

    fn write_node_details(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        prefix: &str,
        child_prefix: &str,
        output: &mut String,
    ) {
        let name = plan.name();
        let detail_prefix = format!("{}{}   ", prefix, child_prefix);

        // Use displayable() to get the one-line representation
        let display = format!("{}", displayable(plan.as_ref()).one_line());

        // For SqlExec nodes, show the SQL query
        if name.contains("SqlExec") || name.contains("Cooperative") {
            if let Some(sql_start) = display.find("sql=") {
                let sql_part = &display[sql_start + 4..];

                // Wrap lines
                let chunk_size = 70;
                let chars: Vec<char> = sql_part.chars().collect();
                let chunks: Vec<_> = chars
                    .chunks(chunk_size)
                    .map(|chunk| chunk.iter().collect::<String>())
                    .collect();

                if let Some(first) = chunks.first() {
                    let _ = writeln!(output, "{}sql: {}", detail_prefix, first);
                    for chunk in &chunks[1..] {
                        // Align with "sql: " (5 chars)
                        let _ = writeln!(output, "{}     {}", detail_prefix, chunk);
                    }
                }
            }
        }

        // For join nodes, show join conditions
        if name.contains("Join") {
            if let Some(on_start) = display.find("on=") {
                let on_part = &display[on_start..];
                if let Some(end) = on_part.find(']') {
                    let _ = writeln!(output, "{}{}", detail_prefix, &on_part[..end + 1]);
                }
            }
            if let Some(type_start) = display.find("join_type=") {
                let type_part = &display[type_start..];
                let end = type_part
                    .find([',', ')', ' '])
                    .unwrap_or(type_part.len());
                let _ = writeln!(output, "{}{}", detail_prefix, &type_part[..end]);
            }
        }

        // For filter nodes, show the filter expression
        if name.contains("Filter") {
            if let Some(filter_start) = display.find("predicate=") {
                let filter_part = &display[filter_start..];
                let filter_display = if filter_part.len() > 60 {
                    format!("{}...", &filter_part[..57])
                } else {
                    filter_part.to_string()
                };
                let _ = writeln!(output, "{}filter: {} ✓", detail_prefix, filter_display);
            }
        }

        // For DataSource or Projection nodes, look for projection
        if name.contains("Projection") || name.contains("DataSource") || name.contains("Scan") {
            if let Some(proj_start) = display.find("projection=[") {
                let proj_part = &display[proj_start + 12..]; // skip "projection=["
                if let Some(end) = proj_part.find(']') {
                    let fields_str = &proj_part[..end];
                    // Only print if reasonable length
                    if fields_str.len() < 80 {
                        let _ = writeln!(output, "{}projection: [{}]", detail_prefix, fields_str);
                    } else {
                        // Fallback to schema summary if too long string
                        let schema = plan.schema();
                        let field_names: Vec<_> =
                            schema.fields().iter().map(|f| f.name().as_str()).collect();
                        let _ = writeln!(
                            output,
                            "{}projection: [{}, ... ({} fields)]",
                            detail_prefix,
                            field_names.first().unwrap_or(&""),
                            field_names.len()
                        );
                    }
                }
            }
        }

        // For DataSource, look for file_type and source limits
        if name.contains("DataSource") || name.contains("Scan") {
            // Source limit
            if let Some(limit_start) = display.find("limit=") {
                let limit_part = &display[limit_start + 6..]; // skip "limit="
                let end = limit_part.find(',').unwrap_or(limit_part.len());
                let _ = writeln!(
                    output,
                    "{}limit: {} (source)",
                    detail_prefix,
                    &limit_part[..end]
                );
            }

            // File type
            if let Some(ft_start) = display.find("file_type=") {
                let ft_part = &display[ft_start + 10..]; // skip "file_type="
                let end = ft_part.find(',').unwrap_or(ft_part.len());
                let _ = writeln!(output, "{}source: {}", detail_prefix, &ft_part[..end]);
            }
        }

        // For limit nodes or generic fetch
        if display.contains("fetch=") {
            if let Some(fetch_start) = display.find("fetch=") {
                let fetch_val_part = &display[fetch_start + 6..]; // skip "fetch="
                                                                  // Find end: comma, space, or end of string
                let end = fetch_val_part
                    .find(|c: char| !c.is_numeric())
                    .unwrap_or(fetch_val_part.len());

                // Only print if we look like a Limit/Coalesce node OR if explicitly requested
                if name.contains("Limit") || name.contains("Coalesce") {
                    let _ = writeln!(
                        output,
                        "{}limit: {} ✓",
                        detail_prefix,
                        &fetch_val_part[..end]
                    );
                }
            }
        }
    }
}

/// Format an execution plan as a detailed ASCII tree.
pub fn format_plan_tree(plan: &Arc<dyn ExecutionPlan>) -> String {
    PlanTreeFormatter::new().format(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formatter_creation() {
        let formatter = PlanTreeFormatter::new();
        assert!(formatter.show_federation);
        assert!(formatter.show_pushdown);
        assert!(formatter.show_metrics);
    }
}
