//! # SQL Generator Translators
//!
//! This module contains the core logic for translating DataFusion logical plans
//! into SQL `Query` objects using `sqlparser-rs`.
//!
//! ## Overview
//!
//! The [`SqlGenerator`] orchestrates the translation by recursively walking the
//! plan and dispatching to specialized node handlers (e.g., [`scan::handle_table_scan`]).
//!
//! ## Normalization
//!
//! A key feature of this generator is its ability to produce stable, bare SQL
//! across different dialects. It achieves this by stripping source-level qualifiers
//! from columns and tables, and applying consistent aliasing.
//!
//! ## Safety
//!
//! Recursion depth is limited by [`MAX_RECURSION_DEPTH`] to prevent stack overflow
//! on deeply nested plans.
//!
//! ## Usage
//!
//! ```rust
//! use strake_sql::sql_generator::SqlGenerator;
//! use strake_sql::dialect_router::route_dialect;
//! use datafusion::logical_expr::LogicalPlan;
//!
//! # fn test(plan: &LogicalPlan) -> Result<(), Box<dyn std::error::Error>> {
//! let dialect_path = route_dialect("postgres");
//! if let strake_sql::dialect_router::DialectPath::Native(d, c, t) = dialect_path {
//!     let gen_dialect = strake_sql::sql_generator::dialect::GeneratorDialect::new(
//!         d.as_ref(), None, c, t, "postgres"
//!     );
//!     let mut generator = SqlGenerator::new(gen_dialect);
//!     let sql = generator.generate(plan)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Column Resolution**: O(scopes × columns) per query.
//! - **Aliasing**: O(1) via monotonic counter.
//! - **Recursion**: Stack depth is limited by [`MAX_RECURSION_DEPTH`] (50) to prevent overflow.

use crate::sql_generator::context::{ColumnEntry, GeneratorContext, Scope};
use crate::sql_generator::dialect::GeneratorDialect;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::LogicalPlan;
use sqlparser::ast::{
    Expr as SqlExpr, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use strake_error::StrakeError;

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod projection;
pub(crate) mod scan;
pub(crate) mod set_ops;
pub(crate) mod sort;

/// Maximum allowed recursion depth for logical plan translation.
pub const MAX_RECURSION_DEPTH: usize = 50;

/// The main orchestrator for translating DataFusion logical plans into SQL queries.
pub struct SqlGenerator<'a> {
    /// Context used to track scopes, aliases, and column IDs during translation.
    pub context: GeneratorContext,
    /// Dialect-specific configuration and capabilities.
    pub dialect: GeneratorDialect<'a>,
    /// Internal counter to prevent infinite recursion on nested plans.
    pub(crate) recursion_level: Arc<AtomicUsize>,
}

impl<'a> SqlGenerator<'a> {
    /// Creates a new [`SqlGenerator`] instance with the specified dialect.
    pub fn new(dialect: GeneratorDialect<'a>) -> Self {
        Self {
            context: GeneratorContext::new(),
            dialect,
            recursion_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Generates a SQL string from a DataFusion [`LogicalPlan`].
    #[must_use = "SQL generation produces the primary output and should not be ignored"]
    pub fn generate(&mut self, plan: &LogicalPlan) -> Result<String, StrakeError> {
        tracing::debug!(
            target: "sql_generator",
            plan = %plan.display(),
            supports_limit = %self.dialect.capabilities.supports_limit_clause(),
            supports_fetch = %self.dialect.capabilities.supports_fetch_clause(),
            "Generating SQL from plan"
        );
        self.plan_to_query(plan)
            .map(|q| {
                let sql = q.to_string();
                tracing::debug!(target: "sql_generator", sql = %sql, "Generated SQL");
                sql
            })
            .map_err(|e: SqlGenError| e.to_strake_error(self.dialect.source_type.as_str()))
    }

    /// Returns a skeleton SQL [`Query`] structure (equivalent to `SELECT *`).
    ///
    /// This is used as a base template for building more complex queries.
    pub(crate) fn create_skeleton_query(&self) -> Query {
        static SKELETON: OnceLock<Query> = OnceLock::new();

        SKELETON
            .get_or_init(|| {
                let dialect = GenericDialect {};
                let parser = Parser::new(&dialect);
                *parser
                    .try_with_sql("SELECT *")
                    .unwrap()
                    .parse_query()
                    .unwrap()
            })
            .clone()
    }

    /// Returns a skeleton SQL [`Select`] structure.
    pub fn create_skeleton_select(&self) -> Select {
        let query = self.create_skeleton_query();
        match *query.body {
            SetExpr::Select(select) => *select,
            _ => panic!("SKELETON query invariant violated"),
        }
    }

    pub(crate) fn plan_to_query(&mut self, plan: &LogicalPlan) -> Result<Query, SqlGenError> {
        let _guard = RecursionGuard::new(self.recursion_level.clone())?;

        match plan {
            LogicalPlan::TableScan(scan) => scan::handle_table_scan(self, scan),
            LogicalPlan::Projection(proj) => projection::handle_projection(self, proj),
            LogicalPlan::SubqueryAlias(alias) => projection::handle_subquery_alias(self, alias),
            LogicalPlan::Filter(filter) => projection::handle_filter(self, filter),
            LogicalPlan::Join(join) => join::handle_join(self, join),
            LogicalPlan::Aggregate(agg) => aggregate::handle_aggregate(self, agg),
            LogicalPlan::Sort(sort) => sort::handle_sort(self, sort),
            LogicalPlan::Limit(limit) => set_ops::handle_limit(self, limit),
            LogicalPlan::Window(window) => aggregate::handle_window(self, window),
            LogicalPlan::Union(union) => set_ops::handle_union(self, union),
            LogicalPlan::Distinct(distinct) => set_ops::handle_distinct(self, distinct),
            LogicalPlan::EmptyRelation(empty) => set_ops::handle_empty_relation(self, empty),
            LogicalPlan::Values(values) => set_ops::handle_values(self, values),
            LogicalPlan::RecursiveQuery(recursive) => {
                set_ops::handle_recursive_query(self, recursive)
            }
            LogicalPlan::Extension(ext) => {
                if let Some(nary) = ext
                    .node
                    .as_any()
                    .downcast_ref::<crate::optimizer::join_flattener::NaryJoinNode>()
                {
                    join::handle_nary_join(self, nary)
                } else {
                    Err(SqlGenError::UnsupportedPlan {
                        message: format!("Unsupported extension node: {}", ext.node.name()),
                        node_type: "Extension".to_string(),
                    })
                }
            }
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Logical plan node: {}", plan.display()),
                node_type: "Unknown".to_string(),
            }),
        }
    }

    pub(crate) fn apply_limit_offset(
        &self,
        query: &mut Query,
        limit: Option<sqlparser::ast::Expr>,
        offset: Option<sqlparser::ast::Offset>,
    ) {
        if self.dialect.capabilities.supports_limit_clause() {
            tracing::debug!(target: "sql_generator", "Applying LIMIT clause");
            query.limit_clause = Some(sqlparser::ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by: vec![],
            });
        } else if self.dialect.capabilities.supports_fetch_clause() {
            tracing::debug!(target: "sql_generator", "Applying FETCH FIRST clause");
            if let Some(o) = offset {
                query.limit_clause = Some(sqlparser::ast::LimitClause::LimitOffset {
                    limit: None,
                    offset: Some(o),
                    limit_by: vec![],
                });
            } else {
                query.limit_clause = None;
            }
            if let Some(l) = limit {
                query.fetch = Some(sqlparser::ast::Fetch {
                    with_ties: false,
                    percent: false,
                    quantity: Some(l),
                });
            }
        }
    }

    /// Translates `plan` to SQL, wrapping it as a stable derived table when
    /// necessary, and discarding all input scopes pushed above the checkpoint
    /// when the wrap happens.
    pub(crate) fn plan_to_stable_query(
        &mut self,
        plan: &LogicalPlan,
        checkpoint: crate::sql_generator::context::Checkpoint,
    ) -> Result<Query, SqlGenError> {
        let mut query = self.plan_to_query(plan)?;

        let is_complex = match &*query.body {
            SetExpr::Select(select) => {
                let has_group_by = match &select.group_by {
                    sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
                    sqlparser::ast::GroupByExpr::All(_) => true,
                };
                select.from.len() > 1
                    || !select.from[0].joins.is_empty()
                    || has_group_by
                    || select.distinct.is_some()
            }
            _ => true,
        };

        // Wrap the query if it's complex or has a limit, to ensure that parent
        // nodes (like Filter or Sort) apply to the results of this query rather
        // than its internal components. Simple SELECTs from TableScan are already
        // stable and aliased, so they don't need wrapping.
        let should_wrap = is_complex
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || self.dialect.capabilities.always_wrap_subqueries();

        if should_wrap {
            let wrapper_alias = self.context.next_alias();
            let relation =
                self.extract_relation(&mut query, Some(wrapper_alias.to_string()), checkpoint)?;
            let mut select = self.create_skeleton_select();
            select.from = vec![TableWithJoins {
                relation,
                joins: vec![],
            }];

            let scope =
                self.context
                    .scope_stack
                    .last()
                    .ok_or_else(|| SqlGenError::UnsupportedPlan {
                        message: "Missing scope".to_string(),
                        node_type: "StableQuery".to_string(),
                    })?;
            select.projection = scope
                .columns
                .iter()
                .map(|c| {
                    Ok(SelectItem::ExprWithAlias {
                        expr: SqlExpr::CompoundIdentifier(vec![
                            safe_ident(c.source_alias.as_ref())?,
                            safe_ident(c.name.as_ref())?,
                        ]),
                        alias: safe_ident(c.name.as_ref())?,
                    })
                })
                .collect::<Result<Vec<_>, SqlGenError>>()?;

            let mut out_query = self.create_skeleton_query();
            out_query.body = Box::new(SetExpr::Select(Box::new(select)));

            // Move limit/fetch to out_query if present
            let limit_clause = query.limit_clause.take();
            let fetch = query.fetch.take();

            if let Some(lc) = limit_clause {
                match lc {
                    sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } => {
                        self.apply_limit_offset(&mut out_query, limit, offset);
                    }
                    _ => {
                        out_query.limit_clause = Some(lc);
                    }
                }
            } else if fetch.is_some() {
                self.apply_limit_offset(&mut out_query, fetch.and_then(|f| f.quantity), None);
            }
            Ok(out_query)
        } else {
            Ok(query)
        }
    }

    /// Wraps a query as a derived table with a stable alias.
    ///
    /// Pops all scopes pushed above the checkpoint, re-aliases the query's
    /// projection to match the popped scope's column names, pushes a
    /// replacement scope with the same alias, and returns the
    /// `TableFactor::Derived`.
    ///
    /// Every scope pushed above the checkpoint (i.e. all intermediate scopes
    /// left behind by the input subplan's translation) is discarded, so the
    /// stack holds exactly one scope per translated subplan. Callers capture
    /// the checkpoint before translating their input.
    ///
    /// **Net scope stack depth change: zero** (pop N, push one; N = 1 when the
    /// input subplan left exactly one scope above the checkpoint). Callers
    /// relying on this invariant (e.g., `handle_aggregate`) assert that the
    /// stack is non-empty afterwards.
    pub(crate) fn extract_relation(
        &mut self,
        query: &mut Query,
        alias: Option<String>,
        checkpoint: crate::sql_generator::context::Checkpoint,
    ) -> Result<TableFactor, SqlGenError> {
        let inner_scope = self.context.pop_to_checkpoint(checkpoint).ok_or_else(|| {
            SqlGenError::UnsupportedPlan {
                message: "Missing scope".to_string(),
                node_type: "ExtractRelation".to_string(),
            }
        })?;
        let sub_alias = alias.unwrap_or_else(|| inner_scope.alias.to_string());
        tracing::debug!(target: "sql_generator", sub_alias = %sub_alias, col_count = inner_scope.columns.len(), "Extracting relation");

        let max_id_len = self.dialect.capabilities.max_identifier_length();
        let mut seen_names = std::collections::HashSet::new();
        let mut column_names: Vec<Arc<str>> = Vec::with_capacity(inner_scope.columns.len());
        for c in inner_scope.columns.iter() {
            let base_name = derive_bare_name(c.name.as_ref());
            let name = disambiguate_identifier(base_name.as_ref(), &mut seen_names, max_id_len);
            column_names.push(name);
        }

        if let SetExpr::Select(ref mut select) = *query.body {
            for (i, item) in select.projection.iter_mut().enumerate() {
                if i < column_names.len() {
                    let expr = match item {
                        SelectItem::UnnamedExpr(e) => e.clone(),
                        SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                        _ => continue,
                    };
                    let alias_str = column_names[i].to_string();
                    let alias = safe_ident(&alias_str)?;
                    tracing::debug!(target: "sql_generator", index = i, alias = %alias, "Aliasing projection item");
                    *item = SelectItem::ExprWithAlias { expr, alias };
                }
            }
        }

        let new_columns_vec: Vec<ColumnEntry> = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let (data_type, provenance, qualified_provenance) =
                    if let Some(inner_col) = inner_scope.columns.get(i) {
                        let mut prov = vec![sub_alias.clone(), inner_col.name.to_string()];
                        prov.extend(inner_col.provenance.clone());
                        let mut qprov = inner_col.qualified_provenance.clone();
                        qprov.push(crate::sql_generator::context::ColumnProvenance {
                            qualifier: Some(sub_alias.clone()),
                            column_name: inner_col.name.to_string(),
                        });
                        (inner_col.data_type.clone(), prov, qprov)
                    } else {
                        (
                            datafusion::arrow::datatypes::DataType::Null,
                            vec![sub_alias.clone()],
                            vec![],
                        )
                    };
                ColumnEntry {
                    name: name.clone(),
                    name_lower: Arc::from(name.to_lowercase().as_str()),
                    data_type,
                    source_alias: sub_alias.clone().into(),
                    provenance,
                    qualified_provenance,
                    unique_id: self.context.next_column_id(),
                }
            })
            .collect();
        let new_columns: Arc<[ColumnEntry]> = Arc::from(new_columns_vec.into_boxed_slice());

        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query.clone()),
            alias: Some(TableAlias {
                name: safe_ident(&sub_alias)?,
                columns: vec![],
                explicit: self.dialect.capabilities.supports_as_alias_for_tables(),
            }),
            sample: None,
        };

        tracing::debug!(target: "sql_generator", alias = %sub_alias, "Extracted relation with stable alias");

        let mut qualifiers = vec![sub_alias.clone()];
        qualifiers.extend(inner_scope.qualifiers.clone());
        self.context.push_existing_scope(Scope {
            alias: sub_alias.clone(),
            columns: new_columns,
            is_derived: true,
            qualifiers,
        });
        Ok(derived)
    }
}

/// Truncates `base` to at most `max_len` bytes without splitting a UTF-8
/// character.
///
/// Dialect identifier limits (Oracle's 128, SQL Server's 128) are denominated in
/// bytes, so slicing `&s[..max_len]` directly is not safe: a name whose encoding
/// has no char boundary at that offset panics. Column names originate from remote
/// catalog metadata, so this is reachable from operator-controlled input.
fn truncate_bytes_on_char_boundary(base: &str, max_len: usize) -> &str {
    if base.len() <= max_len {
        return base;
    }
    let mut end = max_len;
    while end > 0 && !base.is_char_boundary(end) {
        end -= 1;
    }
    &base[..end]
}

/// Disambiguates an identifier candidate against previously taken names, capping length at `max_len` bytes.
#[must_use]
fn disambiguate_identifier(
    base: &str,
    taken: &mut std::collections::HashSet<String>,
    max_len: usize,
) -> Arc<str> {
    debug_assert!(
        max_len >= 3,
        "max_identifier_length must allow a `_N` suffix, got {max_len}"
    );
    let base_truncated = truncate_bytes_on_char_boundary(base, max_len);
    let mut candidate = base_truncated.to_string();
    let mut counter = 1usize;
    loop {
        if candidate.len() <= max_len && !taken.contains(&candidate.to_lowercase()) {
            break;
        }
        let suffix = format!("_{counter}");
        let max_base_len = max_len.saturating_sub(suffix.len());
        let prefix = truncate_bytes_on_char_boundary(base, max_base_len);
        candidate = format!("{prefix}{suffix}");
        counter += 1;
    }
    taken.insert(candidate.to_lowercase());
    candidate.into()
}

/// Derives a stable bare name from a potentially qualified schema field name.
///
/// This function normalizes field names to match what the normalized expressions produce.
/// It strips multi-part qualifiers and ensures consistent casing.
///
/// Examples:
///   "id"                            → "id"
///   "testdb.public.users.id"        → "id"
///   "SUM(amount)"                   → "sum(amount)"
///   "SUM(testdb.public.orders.amount)" → "sum(amount)"
///   "COUNT(u.id)"                   → "count(u.id)"  (single-word qualifier preserved)
/// Derives a "bare" name for a column or table by stripping all but the final qualifier.
///
/// It also handles function expressions by recursively stripping qualifiers from their arguments.
pub fn derive_bare_name(name: &str) -> Arc<str> {
    let base = if (name.contains('(') && name.ends_with(')'))
        || name.contains("PARTITION BY")
        || name.contains("partition by")
        || name.contains("ORDER BY")
        || name.contains("order by")
        || name.contains("RANGE")
        || name.contains("range")
        || name.contains("ROWS")
        || name.contains("rows")
    {
        strip_qualifiers_in_function(name)
    } else if let Some(idx) = name.rfind('.') {
        name[idx + 1..].to_string()
    } else {
        name.to_string()
    };

    // Sanitize the name for SQL identifiers (prevent STRAKE-2001)
    // Replace double-quotes with single-quotes, and other forbidden characters with underscores.
    let sanitized = base.replace('"', "'").replace([';', '\\', '`', '\0'], "_");

    sanitized.into()
}

/// Finds `needle` in `haystack` case-insensitively, returning a byte offset that
/// is valid for `haystack`.
///
/// Deliberately avoids `to_lowercase()`: that mapping is not byte-length
/// preserving (`'İ'` U+0130 is 2 bytes but lowercases to 3), so an offset located
/// in a lowercased copy cannot be used to slice the original. Comparing
/// equal-length ASCII-case-folded byte runs cannot split a char boundary.
fn find_ci(haystack: &str, needle: &str) -> Option<usize> {
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    if n.is_empty() || n.len() > h.len() {
        return None;
    }
    (0..=h.len() - n.len()).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

/// Strips all qualifiers (single or multi-part) from inside function call expressions
/// and bracketed identifiers.
///
/// "SUM(testdb.public.orders.amount)"   → "SUM(amount)"
/// "SUM(o.amount)"                      → "SUM(amount)"
/// "COUNT(DISTINCT testdb.public.u.id)" → "COUNT(DISTINCT id)"
fn strip_qualifiers_in_function(name: &str) -> String {
    let mut s = name;
    const DEFAULT_FRAMES_IN_ALIASES: &[&str] = &[
        " range between unbounded preceding and unbounded following",
        " range between unbounded preceding and current row",
        " rows between unbounded preceding and unbounded following",
        " rows between unbounded preceding and current row",
    ];
    for default_frame in DEFAULT_FRAMES_IN_ALIASES {
        if let Some(pos) = find_ci(s, default_frame) {
            s = &s[..pos];
            break;
        }
    }

    let mut result = String::with_capacity(s.len());
    let mut current_part = String::new();
    let mut paren_depth: usize = 0;
    let mut bracket_depth: usize = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                result.push_str(&current_part);
                current_part.clear();
                result.push('(');
                paren_depth += 1;
            }
            ')' => {
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(')');
                paren_depth = paren_depth.saturating_sub(1);
            }
            '[' => {
                result.push_str(&current_part);
                current_part.clear();
                result.push('[');
                bracket_depth += 1;
            }
            ']' => {
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(']');
                bracket_depth = bracket_depth.saturating_sub(1);
            }
            ',' if paren_depth > 0 || bracket_depth > 0 => {
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(',');
            }
            ' ' | '+' | '-' | '*' | '/' if paren_depth > 0 || bracket_depth > 0 => {
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(ch);
            }
            _ if paren_depth > 0 || bracket_depth > 0 => {
                current_part.push(ch);
            }
            _ => {
                result.push(ch);
            }
        }
    }
    result.push_str(&current_part);
    result
}

/// RAII guard to track and limit recursion depth.
struct RecursionGuard {
    /// Reference to the shared recursion counter.
    level: Arc<AtomicUsize>,
}
impl RecursionGuard {
    fn new(level: Arc<AtomicUsize>) -> Result<Self, SqlGenError> {
        let current = level.fetch_add(1, Ordering::SeqCst);
        if current > MAX_RECURSION_DEPTH {
            level.fetch_sub(1, Ordering::SeqCst);
            return Err(SqlGenError::MaxRecursion(MAX_RECURSION_DEPTH));
        }
        Ok(Self { level })
    }
}
impl Drop for RecursionGuard {
    fn drop(&mut self) {
        self.level.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::{
        Expr, LogicalPlanBuilder, WindowFrame, WindowFrameBound, WindowFrameUnits, col,
    };
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::unparser::dialect::PostgreSqlDialect as DataFusionPostgres;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_concurrent_generation_safety() {
        let sqlparser_dialect = DataFusionPostgres {};
        let _dialect = GeneratorDialect::new(
            &sqlparser_dialect,
            None,
            Arc::new(crate::sql_generator::dialect::DefaultDialectCapabilities),
            Arc::new(crate::sql_generator::dialect::DefaultTypeMapper),
            "postgres",
        );
        let mut tasks = vec![];
        for _ in 0..1000 {
            let level = Arc::new(AtomicUsize::new(0));
            tasks.push(tokio::spawn(async move {
                let _guard = RecursionGuard::new(level).unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[test]
    fn test_user_authored_rows_frame_under_order_by_survives_verbatim() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // Author a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING window under ORDER BY
        let custom_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        );

        let window_expr =
            Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::sum::sum_udaf(),
                ),
                params: datafusion::logical_expr::expr::WindowFunctionParams {
                    args: vec![col("x")],
                    partition_by: vec![],
                    order_by: vec![datafusion::logical_expr::expr::Sort {
                        expr: col("t"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: custom_frame,
                    null_treatment: None,
                    distinct: false,
                    filter: None,
                },
            }));

        let plan = LogicalPlanBuilder::from(scan)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        let sql = crate::sql_gen::get_sql_for_plan(&plan, "oracle")
            .unwrap()
            .unwrap();

        let sql_upper = sql.to_uppercase();
        assert!(
            sql_upper.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            "User-authored ROWS frame must survive verbatim in generated Oracle SQL: {}",
            sql
        );
    }

    #[test]
    fn test_user_authored_range_frame_under_order_by_survives_verbatim() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // Author an explicit RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING window under ORDER BY
        let custom_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        );

        let window_expr =
            Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::sum::sum_udaf(),
                ),
                params: datafusion::logical_expr::expr::WindowFunctionParams {
                    args: vec![col("x")],
                    partition_by: vec![],
                    order_by: vec![datafusion::logical_expr::expr::Sort {
                        expr: col("t"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: custom_frame,
                    null_treatment: None,
                    distinct: false,
                    filter: None,
                },
            }));

        let plan = LogicalPlanBuilder::from(scan)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        let sql = crate::sql_gen::get_sql_for_plan(&plan, "oracle")
            .unwrap()
            .unwrap();

        let sql_upper = sql.to_uppercase();
        assert!(
            sql_upper.contains("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            "User-authored explicit RANGE frame with ORDER BY must survive verbatim: {}",
            sql
        );
    }

    #[test]
    fn test_user_authored_rows_current_row_under_order_by_survives() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // This is the value DataFusion's sql planner builds from user-authored
        // `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. It is value-equal
        // to `WindowFrame::new(Some(true))`, which an earlier revision used as the
        // "synthetic default" sentinel for ordered windows -- and therefore
        // silently elided, rewriting ROWS semantics into peer-inclusive RANGE.
        let custom_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        );
        assert_eq!(
            custom_frame,
            WindowFrame::new(Some(true)),
            "guard: the sentinel this test defends against must still be value-equal"
        );

        let window_expr =
            Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::sum::sum_udaf(),
                ),
                params: datafusion::logical_expr::expr::WindowFunctionParams {
                    args: vec![col("x")],
                    partition_by: vec![],
                    order_by: vec![datafusion::logical_expr::expr::Sort {
                        expr: col("t"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: custom_frame,
                    null_treatment: None,
                    distinct: false,
                    filter: None,
                },
            }));

        let plan = LogicalPlanBuilder::from(scan)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        for dialect in ["oracle", "postgres"] {
            let sql = crate::sql_gen::get_sql_for_plan(&plan, dialect)
                .unwrap()
                .unwrap();
            let sql_upper = sql.to_uppercase();
            assert!(
                sql_upper.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
                "ROWS ... CURRENT ROW must be emitted verbatim for {dialect}, \
                 not elided into a peer-inclusive RANGE: {sql}"
            );
        }
    }

    #[test]
    fn test_ordered_window_always_emits_frame() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // The frame datafusion-sql actually synthesizes for `OVER (ORDER BY t)`
        // when the sort key is not provably unique: RANGE, non-causal.
        let planner_default = WindowFrame::new(Some(false));

        let window_expr =
            Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::sum::sum_udaf(),
                ),
                params: datafusion::logical_expr::expr::WindowFunctionParams {
                    args: vec![col("x")],
                    partition_by: vec![],
                    order_by: vec![datafusion::logical_expr::expr::Sort {
                        expr: col("t"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: planner_default,
                    null_treatment: None,
                    distinct: false,
                    filter: None,
                },
            }));

        let plan = LogicalPlanBuilder::from(scan)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        let sql = crate::sql_gen::get_sql_for_plan(&plan, "oracle")
            .unwrap()
            .unwrap();

        let sql_upper = sql.to_uppercase();
        assert!(
            sql_upper.contains("OVER (ORDER BY"),
            "Window function OVER (ORDER BY ...) must be present: {}",
            sql
        );
        assert!(
            sql_upper.contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
            "The planner's own default frame must be emitted, spelling out the \
             peer-inclusive RANGE semantics rather than relying on the remote's \
             implicit default: {}",
            sql
        );
    }

    #[test]
    fn test_unordered_window_still_elides_implicit_default() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // No ORDER BY: the implicit default (and the value DataFusion plans) is
        // ROWS UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING. Eliding is safe here
        // because it is the same expression for both sides.
        let window_expr =
            Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::sum::sum_udaf(),
                ),
                params: datafusion::logical_expr::expr::WindowFunctionParams {
                    args: vec![col("x")],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: WindowFrame::new(None),
                    null_treatment: None,
                    distinct: false,
                    filter: None,
                },
            }));

        let plan = LogicalPlanBuilder::from(scan)
            .window(vec![window_expr])
            .unwrap()
            .build()
            .unwrap();

        let sql = crate::sql_gen::get_sql_for_plan(&plan, "oracle")
            .unwrap()
            .unwrap();

        assert!(
            !sql.to_uppercase().contains("BETWEEN UNBOUNDED PRECEDING"),
            "Unordered implicit default should still be elided: {}",
            sql
        );
    }

    #[test]
    fn test_disambiguate_truncates_multibyte_on_char_boundary() {
        // 60 CJK chars = 180 bytes; byte 128 is not a char boundary.
        let base = "中".repeat(60);
        let mut taken = HashSet::new();
        let name = disambiguate_identifier(&base, &mut taken, 128);
        assert!(
            name.len() <= 128,
            "must respect the byte cap, got {}",
            name.len()
        );
        assert!(
            base.starts_with(name.as_ref()),
            "truncation must be a prefix, not a mangled byte slice"
        );
    }

    #[test]
    fn test_disambiguate_multibyte_collision_adds_suffix_without_panic() {
        let base = "中".repeat(60);
        let mut taken = HashSet::new();
        let first = disambiguate_identifier(&base, &mut taken, 128);
        let second = disambiguate_identifier(&base, &mut taken, 128);
        assert_ne!(first, second, "colliding names must be disambiguated");
        assert!(second.len() <= 128);
        assert!(second.as_ref().ends_with("_1"), "got {second}");
    }

    #[test]
    fn test_disambiguate_exact_fit_is_not_renamed() {
        let mut taken = HashSet::new();
        assert_eq!(&*disambiguate_identifier("abc", &mut taken, 128), "abc");
    }

    #[test]
    fn test_disambiguate_smallest_valid_limit_terminates() {
        // Guards the livelock: with the old loop, `candidate.len() > max_len`
        // could never become false once max_base_len saturated to 0. A
        // `max_len` below 3 is now rejected outright by the debug_assert in
        // disambiguate_identifier, since it cannot fit a `_N` suffix.
        let mut taken = HashSet::new();
        let a = disambiguate_identifier("abcdef", &mut taken, 3);
        assert!(a.len() <= 3, "got {a}");
        let b = disambiguate_identifier("abcdef", &mut taken, 3);
        assert!(b.len() <= 3, "got {b}");
        assert_ne!(a, b, "collision must still be resolved within the cap");
    }

    #[test]
    fn test_strip_qualifiers_handles_non_ascii_case_folding() {
        // 'İ' (U+0130) is 2 bytes but lowercases to 3 ("i̇"), so an offset
        // located via to_lowercase() is not an offset into the original.
        let name = format!(
            "{}abc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "İ".repeat(10)
        );
        assert_eq!(
            strip_qualifiers_in_function(&name),
            format!("{}abc", "İ".repeat(10))
        );
    }

    #[test]
    fn test_strip_qualifiers_strips_frame_from_alias() {
        assert_eq!(
            strip_qualifiers_in_function(
                "SUM(t.amount) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ),
            "SUM(amount)"
        );
    }

    #[test]
    fn test_find_ci_is_case_insensitive_and_char_safe() {
        assert_eq!(find_ci("SUM(A.b) rows between", " ROWS BETWEEN"), Some(8));
        assert_eq!(find_ci("abc", "xyz"), None);
        assert_eq!(find_ci("İİİ", " range"), None);
    }

    #[test]
    fn test_derive_bare_name_non_ascii_function_alias() {
        assert_eq!(
            derive_bare_name("SUM(中.值) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
                .as_ref(),
            "SUM(值)"
        );
    }

    #[test]
    fn test_join_qualified_column_resolution() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let scan1 = LogicalPlanBuilder::scan(
            "t1",
            Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema1)))),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let scan2 = LogicalPlanBuilder::scan(
            "t2",
            Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema2)))),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_plan = LogicalPlanBuilder::from(scan1)
            .join(
                scan2,
                datafusion::logical_expr::JoinType::Inner,
                (vec!["id"], vec!["id"]),
                None,
            )
            .unwrap()
            .project(vec![col("t1.id"), col("t2.val")])
            .unwrap()
            .sort(vec![datafusion::logical_expr::expr::Sort {
                expr: col("t1.id"),
                asc: true,
                nulls_first: false,
            }])
            .unwrap()
            .build()
            .unwrap();

        let sql = crate::sql_gen::get_sql_for_plan(&join_plan, "oracle")
            .unwrap()
            .unwrap();

        assert!(
            sql.contains("INNER JOIN"),
            "Join must be generated: {}",
            sql
        );
        assert!(
            sql.contains("ORDER BY"),
            "ORDER BY on t1.id must resolve unambiguously: {}",
            sql
        );
    }
}
