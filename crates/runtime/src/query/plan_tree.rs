//! Query execution plan tree visualization.
//!
//! Provides ASCII tree formatting of DataFusion execution plans with
//! federation pushdown indicators, timing metrics, and row counts.
//!
//! ## Performance Characteristics
//!
//! Formatting traverses the entire execution plan tree once.
//! Allocations are O(n) proportional to the number of nodes in the plan.
//!
//! ## Errors
//!
//! This module never returns errors; all formatting is infallible.
//!
//! ## Safety
//!
//! String slicing is handled via character boundaries to avoid panics on multibyte UTF-8 characters.
//!
//! ## Usage
//!
//! ```rust
//! # use datafusion::physical_plan::ExecutionPlan;
//! # use std::sync::Arc;
//! # use strake_runtime::query::plan_tree::PlanTreeFormatter;
//! # fn example(plan: Arc<dyn ExecutionPlan>) {
//! let formatter = PlanTreeFormatter::default();
//! let ascii_tree = formatter.format(&plan);
//! # }
//! ```

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use std::borrow::Cow;
use std::sync::Arc;
use strake_connectors::sources::federated;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GridPos {
    x: usize,
    y: usize,
}

#[derive(Clone)]
struct GridNode {
    name: String,
    details: Vec<String>,
    metrics: String,
    child_positions: Vec<GridPos>,
}

struct RenderGrid {
    nodes: Vec<Option<GridNode>>,
    width: usize,
    height: usize,
}

impl RenderGrid {
    fn new(width: usize, height: usize) -> Self {
        let size = width.saturating_mul(height);
        // Cap at a reasonable limit (e.g., 100k cells ≈ a few MB)
        assert!(
            size <= 100_000,
            "Plan tree too large for grid rendering ({size} cells)"
        );
        Self {
            nodes: vec![None; size],
            width,
            height,
        }
    }

    fn set(&mut self, x: usize, y: usize, node: GridNode) {
        if x < self.width && y < self.height {
            self.nodes[y * self.width + x] = Some(node);
        }
    }

    fn get(&self, x: usize, y: usize) -> Option<&GridNode> {
        if x < self.width && y < self.height {
            self.nodes[y * self.width + x].as_ref()
        } else {
            None
        }
    }

    fn has_node(&self, x: usize, y: usize) -> bool {
        self.get(x, y).is_some()
    }
}

/// Formats an execution plan as an ASCII tree with federation and metric annotations.
pub struct PlanTreeFormatter {
    /// Show federation indicators ([PUSHED] markers)
    pub show_federation: bool,
    /// Show pushdown details (filters, projections, limits)
    pub show_pushdown: bool,
    /// Show metrics (timing, row counts, etc.) if available
    pub show_metrics: bool,
    /// Elide single-child RepartitionExec nodes (reduces visual clutter)
    pub elide_repartition: bool,
}

impl Default for PlanTreeFormatter {
    fn default() -> Self {
        Self {
            show_federation: true,
            show_pushdown: true,
            show_metrics: true,
            elide_repartition: false,
        }
    }
}

impl PlanTreeFormatter {
    /// Creates a formatter with all features enabled (federation markers, pushdown details, metrics).
    pub fn new() -> Self {
        Self::default()
    }

    /// Renders the execution plan as a boxed ASCII tree.
    pub fn format(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        let (width, height) = self.compute_size(plan);
        let mut grid = RenderGrid::new(width, height);
        self.place_in_grid(&mut grid, plan, 0, 0);

        // Dynamic node width based on terminal limit
        let mut node_width = 45usize;
        while grid.width * node_width > 240 && node_width > 15 {
            node_width -= 2;
        }

        self.render_grid(&grid, node_width)
    }

    fn compute_size(&self, plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
        if self.elide_repartition
            && plan.as_any().is::<RepartitionExec>()
            && plan.children().len() == 1
        {
            return self.compute_size(plan.children()[0]);
        }
        let children = plan.children();
        if children.is_empty() {
            return (1, 1);
        }
        let mut width = 0;
        let mut height = 0;
        for c in children {
            let (w, h) = self.compute_size(c);
            width += w;
            height = height.max(h);
        }
        (width, height + 1)
    }

    fn place_in_grid(
        &self,
        grid: &mut RenderGrid,
        plan: &Arc<dyn ExecutionPlan>,
        x: usize,
        y: usize,
    ) -> usize {
        if self.elide_repartition
            && plan.as_any().is::<RepartitionExec>()
            && plan.children().len() == 1
        {
            return self.place_in_grid(grid, plan.children()[0], x, y);
        }

        let children = plan.children();
        if children.is_empty() {
            grid.set(x, y, self.build_node(plan));
            return 1;
        }

        let mut width = 0;
        let mut child_positions = Vec::new();
        for c in children {
            let cx = x + width;
            let cy = y + 1;
            child_positions.push(GridPos { x: cx, y: cy });
            width += self.place_in_grid(grid, c, cx, cy);
        }

        let mut node = self.build_node(plan);
        node.child_positions = child_positions;
        grid.set(x, y, node);
        width
    }

    fn build_node(&self, plan: &Arc<dyn ExecutionPlan>) -> GridNode {
        let name = self.get_node_name(plan);
        let fed = self.get_federation_marker(plan);
        let header = if fed.is_empty() {
            name.to_uppercase()
        } else {
            format!("{} {}", name.to_uppercase(), fed)
        };

        const MAX_DETAIL_WIDTH: usize = 40;
        let details_raw = self.get_node_details_list(plan);
        let mut details = Vec::new();
        for d in details_raw {
            details.extend(self.wrap_text(&d, MAX_DETAIL_WIDTH));
        }

        let metrics = self.get_metrics(plan);
        let metrics_str = if metrics.is_empty() {
            String::new()
        } else {
            format!("~{}", metrics)
        };

        GridNode {
            name: header,
            details,
            metrics: metrics_str,
            child_positions: Vec::new(),
        }
    }

    fn render_grid(&self, grid: &RenderGrid, node_width: usize) -> String {
        let mut lines = Vec::new();
        let box_w = node_width - 2; // interior width between │ borders
        let half = node_width / 2;

        for y in 0..grid.height {
            // ── 1. Top layer ──
            let mut top = String::with_capacity(grid.width * node_width);
            for x in 0..grid.width {
                if grid.has_node(x, y) {
                    top.push('┌');
                    top.push_str(&"─".repeat(half - 1));
                    if y > 0 {
                        top.push('┴');
                    } else {
                        top.push('─');
                    }
                    top.push_str(&"─".repeat(node_width - half - 2));
                    top.push('┐');
                } else {
                    let has_right = (x + 1..grid.width)
                        .any(|rx| grid.has_node(rx, y) || grid.has_node(rx, y + 1));
                    if has_right || self.should_render_whitespace(grid, x, y) {
                        for _ in 0..node_width {
                            top.push(' ');
                        }
                    }
                }
            }
            lines.push(top);

            // ── 2. Box content ──
            let mut max_extra = 0;
            for x in 0..grid.width {
                if let Some(node) = grid.get(x, y) {
                    let mut h = 1; // name
                    if !node.details.is_empty() || !node.metrics.is_empty() {
                        h += 1; // separator
                    }
                    h += node.details.len();
                    if !node.metrics.is_empty() {
                        h += 1;
                    }
                    max_extra = max_extra.max(h);
                }
            }
            let halfway = max_extra / 2;

            for render_y in 0..max_extra {
                let mut row = String::with_capacity(grid.width * node_width);
                for x in 0..grid.width {
                    if let Some(node) = grid.get(x, y) {
                        row.push('│');
                        let text: Cow<'_, str> = if render_y == 0 {
                            Cow::Borrowed(&node.name)
                        } else if render_y == 1
                            && (!node.details.is_empty() || !node.metrics.is_empty())
                        {
                            Cow::Owned("─".repeat(box_w))
                        } else {
                            let d_idx = if !node.details.is_empty() || !node.metrics.is_empty() {
                                render_y as i32 - 2
                            } else {
                                render_y as i32 - 1
                            };
                            if d_idx >= 0 && (d_idx as usize) < node.details.len() {
                                Cow::Borrowed(node.details[d_idx as usize].as_str())
                            } else if d_idx >= 0
                                && (d_idx as usize) == node.details.len()
                                && !node.metrics.is_empty()
                            {
                                Cow::Borrowed(node.metrics.as_str())
                            } else {
                                Cow::Borrowed("")
                            }
                        };
                        row.push_str(&self.pad_center(&text, box_w));
                        if render_y == halfway && node.child_positions.len() > 1 {
                            row.push('├');
                        } else {
                            row.push('│');
                        }
                    } else {
                        let has_child_below = grid.has_node(x, y + 1);
                        let needs_whitespace = self.should_render_whitespace(grid, x, y);

                        if render_y == halfway {
                            if has_child_below {
                                if needs_whitespace {
                                    row.push_str(&"─".repeat(half));
                                    row.push('┬');
                                    row.push_str(&"─".repeat(node_width - half - 1));
                                } else {
                                    row.push_str(&"─".repeat(half));
                                    row.push('┐');
                                    for _ in 0..(node_width - half - 1) {
                                        row.push(' ');
                                    }
                                }
                            } else if needs_whitespace {
                                row.push_str(&"─".repeat(node_width));
                            } else {
                                for _ in 0..node_width {
                                    row.push(' ');
                                }
                            }
                        } else if render_y > halfway && has_child_below {
                            for _ in 0..half {
                                row.push(' ');
                            }
                            row.push('│');
                            for _ in 0..(node_width - half - 1) {
                                row.push(' ');
                            }
                        } else {
                            for _ in 0..node_width {
                                row.push(' ');
                            }
                        }
                    }
                }
                lines.push(row);
            }

            // ── 3. Bottom layer ──
            let mut bottom = String::with_capacity(grid.width * node_width);
            for x in 0..grid.width {
                if let Some(node) = grid.get(x, y) {
                    bottom.push('└');
                    bottom.push_str(&"─".repeat(half - 1));
                    if !node.child_positions.is_empty() {
                        bottom.push('┬');
                    } else {
                        bottom.push('─');
                    }
                    bottom.push_str(&"─".repeat(node_width - half - 2));
                    bottom.push('┘');
                } else {
                    let has_child_below = grid.has_node(x, y + 1);
                    let needs_whitespace = self.should_render_whitespace(grid, x, y);
                    if has_child_below {
                        for _ in 0..half {
                            bottom.push(' ');
                        }
                        bottom.push('│');
                        for _ in 0..(node_width - half - 1) {
                            bottom.push(' ');
                        }
                    } else if needs_whitespace {
                        for _ in 0..node_width {
                            bottom.push(' ');
                        }
                    }
                }
            }
            lines.push(bottom);
        }

        lines.join("\n")
    }

    fn should_render_whitespace(&self, grid: &RenderGrid, x: usize, y: usize) -> bool {
        let mut found_children = 0;
        let mut scan_x = x as isize;
        while scan_x >= 0 {
            if grid.has_node(scan_x as usize, y + 1) {
                found_children += 1;
            }
            if let Some(node) = grid.get(scan_x as usize, y) {
                if node.child_positions.len() > 1 && found_children < node.child_positions.len() {
                    return true;
                }
                return false;
            }
            scan_x -= 1;
        }
        false
    }

    fn pad_center(&self, text: &str, width: usize) -> String {
        let text_width = UnicodeWidthStr::width(text);
        if text_width > width {
            let mut cols = 0;
            let end = text
                .char_indices()
                .find(|&(_, c)| {
                    cols += unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
                    cols > width
                })
                .map(|(i, _)| i)
                .unwrap_or(text.len());
            return text[..end].to_string();
        }
        let total = width - text_width;
        let left = total / 2 + total % 2;
        let right = total / 2;
        format!("{}{}{}", " ".repeat(left), text, " ".repeat(right))
    }

    fn wrap_text(&self, text: &str, width: usize) -> Vec<String> {
        if UnicodeWidthStr::width(text) <= width {
            return vec![text.to_string()];
        }

        let mut lines = Vec::new();
        let mut remaining = text;

        while !remaining.is_empty() {
            if UnicodeWidthStr::width(remaining) <= width {
                lines.push(remaining.to_string());
                break;
            }

            // Find best split point based on display width
            let mut split_at = 0;
            let mut current_width = 0;
            for (idx, c) in remaining.char_indices() {
                let char_width = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
                if current_width + char_width > width {
                    break;
                }
                current_width += char_width;
                split_at = idx + c.len_utf8();
            }

            if split_at == 0 {
                // If the first character is wider than the limit, take it to avoid infinite loop
                if let Some(c) = remaining.chars().next() {
                    split_at = c.len_utf8();
                } else {
                    break;
                }
            }

            let mut head = &remaining[..split_at];
            let mut tail = &remaining[split_at..];

            // Try to split at space if possible for cleaner wrapping
            if !tail.is_empty()
                && !tail.starts_with(' ')
                && let Some(space_idx) = head.rfind(' ')
                && space_idx > 0
            {
                split_at = space_idx;
                head = &remaining[..split_at];
                tail = &remaining[split_at..];
            }

            lines.push(head.trim().to_string());
            remaining = tail.trim_start();
        }
        lines
    }

    fn get_node_details_list(&self, plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let mut lines = Vec::new();
        let any = plan.as_any();

        // Federated Nodes
        if let Some(fed) = federated::as_federated_plan(plan.as_ref()) {
            if let Some(sql) = fed.pushed_sql() {
                lines.push(format!("sql: {}", sql));
            }
            return lines;
        }

        // Filter
        if let Some(filter) = any.downcast_ref::<FilterExec>() {
            let pred = format!("{}", filter.predicate());
            // Check if this filter is above a pushed-down source
            let pushed = plan
                .children()
                .first()
                .is_some_and(|c| self.get_federation_marker(c).contains("PUSHED"));
            lines.push(format!("filter: {}", pred));
            if !pushed {
                lines.push("⚠ Executed Locally".to_string());
            }
            return lines;
        }

        // Projection
        if let Some(proj) = any.downcast_ref::<ProjectionExec>() {
            for item in proj.expr() {
                if item.alias.is_empty() {
                    lines.push(format!("{}", item.expr));
                } else {
                    lines.push(format!("{} as {}", item.expr, item.alias));
                }
            }
            return lines;
        }

        // Joins
        if let Some(join) = any.downcast_ref::<HashJoinExec>() {
            lines.push(format!("join_type: {:?}", join.join_type()));
            for (l, r) in join.on() {
                lines.push(format!("{} = {}", l, r));
            }
            return lines;
        }
        if let Some(nl) = any.downcast_ref::<NestedLoopJoinExec>() {
            lines.push(format!("join_type: {:?}", nl.join_type()));
            return lines;
        }
        if any.is::<CrossJoinExec>() {
            lines.push("join_type: Cross".to_string());
            return lines;
        }

        // Limit
        if let Some(fetch) = plan.fetch() {
            lines.push(format!("limit: {}", fetch));
            return lines;
        }

        // DataSource
        if let Some(ds) = any.downcast_ref::<DataSourceExec>() {
            if let Some((_, parquet_source)) = ds.downcast_to_file_source::<ParquetSource>()
                && let Some(filter) = parquet_source.filter()
            {
                lines.push(format!("filter: {}", filter));
            }
            let schema = plan.schema();
            let fields: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            lines.push(format!("projection: [{}]", fields.join(", ")));
            return lines;
        }

        // Generic Scan Fallback
        let name = plan.name();
        if name.contains("DataSource") || name.contains("Scan") {
            let schema = plan.schema();
            let field_names: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            if !field_names.is_empty() {
                lines.push(format!("projection: [{}]", field_names.join(", ")));
            }
        }

        lines
    }

    fn get_node_name<'a>(&self, plan: &'a Arc<dyn ExecutionPlan>) -> Cow<'a, str> {
        let name = plan.name();

        // Clean up common suffixes for readability
        if let Some(stripped) = name.strip_suffix("Exec") {
            Cow::Owned(stripped.to_string())
        } else {
            Cow::Borrowed(name)
        }
    }

    fn get_federation_marker(&self, plan: &Arc<dyn ExecutionPlan>) -> &'static str {
        if !self.show_federation {
            return "";
        }

        // 1. Check for remote federation [PUSHED] SQL
        if let Some(fed) = federated::as_federated_plan(plan.as_ref())
            && fed.is_federated()
        {
            return "[PUSHED]";
        }

        // 2. Check for local data source filter pushdown
        if let Some(ds) = plan.as_any().downcast_ref::<DataSourceExec>()
            && let Some((_, parquet_source)) = ds.downcast_to_file_source::<ParquetSource>()
            && parquet_source.filter().is_some()
        {
            return "[PUSHED]";
        }

        ""
    }

    fn get_metrics(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
        if !self.show_metrics {
            return String::new();
        }

        if let Some(metrics) = plan.metrics() {
            let rows = metrics.output_rows().map(|r| format!("{} rows", r));
            let bytes = metrics.sum_by_name("output_bytes").and_then(|v| match v {
                datafusion::physical_plan::metrics::MetricValue::Count { count, .. } => {
                    Some(format_bytes(count.value() as u64))
                }
                _ => None,
            });
            let time = metrics.elapsed_compute().map(|nanos| {
                let ms = nanos as f64 / 1_000_000.0;
                if ms > 0.0 {
                    format!("{:.1}ms", ms)
                } else {
                    format!("{}ns", nanos)
                }
            });

            let mut parts = Vec::new();
            if let Some(r) = rows {
                parts.push(r);
            }
            if let Some(b) = bytes {
                parts.push(b);
            }
            if let Some(t) = time {
                parts.push(t);
            }

            if !parts.is_empty() {
                return parts.join(", ");
            }
        }

        String::new()
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format an execution plan as a detailed ASCII tree.
pub fn format_plan_tree(plan: &Arc<dyn ExecutionPlan>) -> String {
    PlanTreeFormatter::new().format(plan)
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use std::any::Any;

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

        // The new format includes the filter in a box
        assert!(output.contains("FILTER"));
        assert!(output.contains("true"));
        assert!(output.contains("⚠ Executed Locally"));
    }

    #[test]
    fn test_datasource_projection_fallback() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Utf8, true),
        ]));
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        let scan = Arc::new(MockScanExec { schema, properties });

        let formatter = PlanTreeFormatter::default();
        let output = formatter.format(&(scan as Arc<dyn ExecutionPlan>));

        assert!(output.contains("MOCKSCAN"));
    }

    /// A mock execution plan node for scanning.
    #[derive(Debug)]
    struct MockScanExec {
        /// The schema of the mock scan.
        schema: SchemaRef,
        /// The properties of the mock scan.
        properties: Arc<PlanProperties>,
    }
    impl DisplayAs for MockScanExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockScanExec")
        }
    }
    impl ExecutionPlan for MockScanExec {
        fn name(&self) -> &str {
            "MockScanExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _: usize,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
            unimplemented!()
        }
    }
    #[test]
    fn test_wrap_text_panic() {
        let formatter = PlanTreeFormatter::default();
        // "🚀" is 4 bytes. If width is 1, 2, or 3, it should not panic.
        let text = "🚀🚀🚀";
        let _ = formatter.wrap_text(text, 1);
        let _ = formatter.wrap_text(text, 2);
        let _ = formatter.wrap_text(text, 3);
    }

    #[test]
    fn test_pad_center_cjk() {
        let formatter = PlanTreeFormatter::default();
        let text = "你好"; // 2 chars, 6 bytes, 4 display width
        let padded = formatter.pad_center(text, 10);
        // Expected: 3 spaces on each side if using display width (4).
        // Resulting byte length: 3 + 6 + 3 = 12.
        assert_eq!(padded.len(), 12);
    }

    #[test]
    fn test_binary_node_rendering() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        let left = Arc::new(MockScanExec {
            schema: schema.clone(),
            properties: properties.clone(),
        });
        let right = Arc::new(MockScanExec {
            schema: schema.clone(),
            properties: properties.clone(),
        });

        // Using a simple binary node mock that returns two children
        /// A mock execution plan node with two children.
        #[derive(Debug)]
        struct MockBinaryExec {
            /// The properties of the mock node.
            properties: Arc<PlanProperties>,
            /// The child execution plans.
            children: Vec<Arc<dyn ExecutionPlan>>,
        }
        impl DisplayAs for MockBinaryExec {
            fn fmt_as(
                &self,
                _: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "MOCKBINARY")
            }
        }
        impl ExecutionPlan for MockBinaryExec {
            fn name(&self) -> &str {
                "MockBinaryExec"
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn schema(&self) -> SchemaRef {
                self.children[0].schema()
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                &self.properties
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                self.children.iter().collect()
            }
            fn with_new_children(
                self: Arc<Self>,
                _: Vec<Arc<dyn ExecutionPlan>>,
            ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }
            fn execute(
                &self,
                _: usize,
                _: Arc<datafusion::execution::TaskContext>,
            ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream>
            {
                unimplemented!()
            }
        }

        let binary = Arc::new(MockBinaryExec {
            properties,
            children: vec![left, right],
        });
        let formatter = PlanTreeFormatter::default();
        let output = formatter.format(&(binary as Arc<dyn ExecutionPlan>));

        // Verify that it contains the horizontal connector arm components
        assert!(output.contains("┬"));
        assert!(output.contains("├"));
        assert!(output.contains("─"));
        assert!(output.contains("┐"));
        assert!(output.contains("MOCKBINARY"));
        assert!(output.contains("MOCKSCAN"));
    }
}
