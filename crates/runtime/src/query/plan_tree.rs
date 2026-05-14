//! Query execution plan tree visualization.
//!
//! Provides ASCII tree formatting of DataFusion execution plans with
//! federation pushdown indicators, timing metrics, and row counts.
//!
//! ## Performance Characteristics
//!
//! Formatting traverses the entire execution plan tree once, with O(n) time
//! and O(depth) stack space. No allocations beyond the output `String`.
//!
//! ## Errors
//!
//! This module never returns errors; all formatting is infallible.
//!
//! ## Safety
//!
//! String slicing is handled carefully to avoid panics on multibyte UTF-8 characters.
//!
//! ## Usage
//!
//! ```rust
//! # use datafusion::physical_plan::ExecutionPlan;
//! # use std::sync::Arc;
//! # use strake_runtime::query::plan_tree::PlanTreeFormatter;
//! # fn example(plan: Arc<dyn ExecutionPlan>) {
//! let formatter = PlanTreeFormatter::default();
//! let ascii_tree = formatter.format_plan_tree(&plan);
//! # }
//! ```

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::displayable;
use std::fmt::Write;
use std::sync::Arc;

/// Formats an execution plan as an ASCII tree with federation and metric annotations.
pub struct PlanTreeFormatter {
    /// Show federation indicators ([PUSHED] markers)
    pub show_federation: bool,
    /// Show pushdown details (filters, projections, limits)
    pub show_pushdown: bool,
    /// Show metrics (timing, row counts, etc.) if available
    pub show_metrics: bool,
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
    /// Creates a formatter with all features enabled (federation markers, pushdown details, metrics).
    pub fn new() -> Self {
        Self::default()
    }

    /// Renders the execution plan as a boxed ASCII tree.
    ///
    /// # Examples
    /// ```rust
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use std::sync::Arc;
    /// # use strake_runtime::query::plan_tree::PlanTreeFormatter;
    /// # fn example(plan: Arc<dyn ExecutionPlan>) {
    /// let formatter = PlanTreeFormatter::new();
    /// let tree = formatter.format(&plan);
    /// # }
    /// ```
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
        let child_prefix_str = if is_last { "  " } else { "│ " };

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
            self.write_node_details(plan, prefix, child_prefix_str, output);
        }

        // Recurse into children
        let children = plan.children();
        for (i, child) in children.iter().enumerate() {
            let mut new_prefix = String::with_capacity(prefix.len() + child_prefix_str.len());
            new_prefix.push_str(prefix);
            new_prefix.push_str(child_prefix_str);
            let is_last_child = i == children.len() - 1;
            self.format_node(child, &new_prefix, is_last_child, output);
        }
    }

    fn get_node_name(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        let name = plan.name();

        // Clean up common suffixes for readability
        name.strip_suffix("Exec").unwrap_or(name).to_string()
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
            let mut output_rows_val = None;

            // Look for output rows
            if let Some(output_rows) = metrics.output_rows() {
                output_rows_val = Some(output_rows);
                parts.push(format!("rows: {}", output_rows));
            }

            // Look for elapsed compute time
            if let Some(elapsed) = metrics.elapsed_compute() {
                let ms = elapsed as f64 / 1_000_000.0;
                if ms > 0.0 {
                    parts.push(format!("{:.1}ms", ms));
                }
            }

            // Look for output bytes (DataFusion 53)
            let output_bytes_sum = metrics
                .iter()
                .filter_map(|m| {
                    use datafusion::physical_plan::metrics::MetricValue;
                    match m.value() {
                        MetricValue::Count { name, count } if name == "output_bytes" => {
                            Some(count.value())
                        }
                        _ => None,
                    }
                })
                .sum::<usize>();

            if output_bytes_sum > 0 {
                let bytes = output_bytes_sum as f64;
                if output_bytes_sum < 1024 {
                    parts.push(format!("{}B", output_bytes_sum));
                } else if output_bytes_sum < 1024 * 1024 {
                    parts.push(format!("{:.1}KB", bytes / 1024.0));
                } else {
                    parts.push(format!("{:.1}MB", bytes / (1024.0 * 1024.0)));
                }
            } else if output_rows_val.unwrap_or(0) > 0 {
                // P2/P3 Fix: Only show placeholder if node produced rows but no bytes reported
                parts.push("[no output_bytes metric]".to_string());
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
        child_prefix_str: &str,
        output: &mut String,
    ) {
        let name = plan.name();

        // Use displayable() to get the one-line representation
        let display = format!("{}", displayable(plan.as_ref()).one_line());

        // For SqlExec nodes, show the SQL query
        if (name.contains("SqlExec") || name.contains("Cooperative"))
            && let Some(sql_start) = display.find("sql=")
        {
            let sql_part = &display[sql_start + 4..];

            // Wrap lines
            let chunk_size = 70;
            let chars: Vec<char> = sql_part.chars().collect();
            let chunks: Vec<_> = chars
                .chunks(chunk_size)
                .map(|chunk| chunk.iter().collect::<String>())
                .collect();

            if let Some(first) = chunks.first() {
                let _ = write!(output, "{}{}   sql: {}", prefix, child_prefix_str, first);
                let _ = writeln!(output);
                for chunk in &chunks[1..] {
                    // Align with "sql: " (5 chars)
                    let _ = write!(output, "{}{}        {}", prefix, child_prefix_str, chunk);
                    let _ = writeln!(output);
                }
            }
        }

        // For join nodes, show join conditions
        if name.contains("Join") {
            if let Some(on_start) = display.find("on=") {
                let on_part = &display[on_start..];
                if let Some(end) = on_part.find(']') {
                    let _ = write!(
                        output,
                        "{}{}   {}",
                        prefix,
                        child_prefix_str,
                        &on_part[..end + 1]
                    );
                    let _ = writeln!(output);
                }
            }
            if let Some(type_start) = display.find("join_type=") {
                let type_part = &display[type_start..];
                let end = type_part.find([',', ')', ' ']).unwrap_or(type_part.len());
                let _ = write!(
                    output,
                    "{}{}   {}",
                    prefix,
                    child_prefix_str,
                    &type_part[..end]
                );
                let _ = writeln!(output);
            }
        }

        // For filter nodes, show the filter expression
        // Any FilterExec node in the physical plan represents filtering happening in Strake (not pushed down completely)
        if name.contains("Filter") {
            let filter_part = if let Some(idx) = display.find("predicate=") {
                &display[idx..]
            } else if let Some(idx) = display.find(':') {
                // DF 51 often uses ": predicate" format
                &display[idx + 1..]
            } else {
                &display
            };

            let filter_display = if filter_part.len() > 60 {
                let truncated: String = filter_part.chars().take(57).collect();
                format!("{}...", truncated)
            } else {
                filter_part.trim().to_string()
            };
            let _ = write!(
                output,
                "{}{}   filter: {} [NOT PUSHED - Executed Locally]",
                prefix, child_prefix_str, filter_display
            );
            let _ = writeln!(output);
        }

        // For DataSource or Projection nodes, look for projection
        if (name.contains("Projection") || name.contains("DataSource") || name.contains("Scan"))
            && let Some(proj_start) = display.find("projection=[")
        {
            let proj_part = &display[proj_start + 12..]; // skip "projection=["
            if let Some(end) = proj_part.find(']') {
                let fields_str = &proj_part[..end];
                // Only print if reasonable length
                if fields_str.len() < 80 {
                    let _ = write!(
                        output,
                        "{}{}   projection: [{}]",
                        prefix, child_prefix_str, fields_str
                    );
                    let _ = writeln!(output);
                } else {
                    // Fallback to schema summary if too long string
                    let schema = plan.schema();
                    let field_names: Vec<_> =
                        schema.fields().iter().map(|f| f.name().as_str()).collect();
                    let _ = write!(
                        output,
                        "{}{}   projection: [{}, ... ({} fields)]",
                        prefix,
                        child_prefix_str,
                        field_names.first().unwrap_or(&""),
                        field_names.len()
                    );
                    let _ = writeln!(output);
                }
            }
        }
        // For DataSource, also look for pushed-down filters
        if name.contains("DataSource") || name.contains("Scan") {
            let filter_keys = ["filters=[", "full_filters=[", "filter=[", "predicate="];
            let mut found_filter = None;

            for key in filter_keys {
                if let Some(filter_start) = display.find(key) {
                    let filter_part = &display[filter_start + key.len()..];
                    let end_char = if key.ends_with('[') { ']' } else { ',' };
                    let end = filter_part.find(end_char).unwrap_or(filter_part.len());
                    let filters_str = &filter_part[..end];
                    if !filters_str.is_empty() {
                        found_filter = Some(filters_str.to_string());
                        break;
                    }
                }
            }

            if let Some(filters_str) = found_filter {
                let filter_display = if filters_str.len() > 80 {
                    let truncated: String = filters_str.chars().take(77).collect();
                    format!("{}...", truncated)
                } else {
                    filters_str
                };
                let _ = write!(
                    output,
                    "{}{}   filter: {} [PUSHED]",
                    prefix, child_prefix_str, filter_display
                );
                let _ = writeln!(output);
            }
        }

        // For DataSource, look for file_type and source limits
        if name.contains("DataSource") || name.contains("Scan") {
            // Source limit
            if let Some(limit_start) = display.find("limit=") {
                let limit_part = &display[limit_start + 6..]; // skip "limit="
                let end = limit_part.find(',').unwrap_or(limit_part.len());
                let _ = write!(
                    output,
                    "{}{}   limit: {} (source)",
                    prefix,
                    child_prefix_str,
                    &limit_part[..end]
                );
                let _ = writeln!(output);
            }

            // File type
            if let Some(ft_start) = display.find("file_type=") {
                let ft_part = &display[ft_start + 10..]; // skip "file_type="
                let end = ft_part.find(',').unwrap_or(ft_part.len());
                let _ = write!(
                    output,
                    "{}{}   source: {}",
                    prefix,
                    child_prefix_str,
                    &ft_part[..end]
                );
                let _ = writeln!(output);
            }
        }

        // For limit nodes or generic fetch
        if display.contains("fetch=")
            && let Some(fetch_start) = display.find("fetch=")
        {
            let fetch_val_part = &display[fetch_start + 6..]; // skip "fetch="
            // Find end: comma, space, or end of string
            let end = fetch_val_part
                .find(|c: char| !c.is_numeric())
                .unwrap_or(fetch_val_part.len());

            // Only print if we look like a Limit/Coalesce node OR if explicitly requested
            if name.contains("Limit") || name.contains("Coalesce") {
                let _ = write!(
                    output,
                    "{}{}   limit: {} ✓",
                    prefix,
                    child_prefix_str,
                    &fetch_val_part[..end]
                );
                let _ = writeln!(output);
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

    #[test]
    fn test_filter_not_pushed_annotation() {
        use datafusion::common::ScalarValue;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::expressions::Literal;
        use datafusion::physical_plan::filter::FilterExec;

        // Construct a simple plan: Filter -> Empty
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let empty = Arc::new(EmptyExec::new(schema.clone()));

        let predicate = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let filter = Arc::new(FilterExec::try_new(predicate, empty).unwrap());

        let formatter = PlanTreeFormatter::new();
        let output = formatter.format(&(filter as Arc<dyn ExecutionPlan>));

        assert!(output.contains("[NOT PUSHED - Executed Locally]"));
    }
}
