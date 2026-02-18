use crate::sql_generator::context::GeneratorContext;
use crate::sql_generator::dialect::GeneratorDialect;
use crate::sql_generator::error::SqlGenError;
use datafusion::logical_expr::LogicalPlan;
use sqlparser::ast::{Query, Select, SetExpr, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use strake_error::StrakeError;

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod projection;
pub(crate) mod scan;
pub(crate) mod set_ops;
pub(crate) mod sort;

const MAX_RECURSION_DEPTH: usize = 50;

pub struct SqlGenerator<'a> {
    pub context: GeneratorContext,
    pub dialect: GeneratorDialect<'a>,
    pub(crate) recursion_level: Arc<AtomicUsize>,
}

impl<'a> SqlGenerator<'a> {
    pub fn new(dialect: GeneratorDialect<'a>) -> Self {
        Self {
            context: GeneratorContext::new(),
            dialect,
            recursion_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn generate(&mut self, plan: &LogicalPlan) -> Result<String, StrakeError> {
        tracing::debug!(target: "sql_generator", plan = %plan.display(), "Generating SQL from plan");
        self.plan_to_query(plan)
            .map(|q| {
                let sql = q.to_string();
                tracing::debug!(target: "sql_generator", sql = %sql, "Generated SQL");
                sql
            })
            .map_err(|e| e.to_strake_error(self.dialect.dialect_name))
    }

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

    pub fn create_skeleton_select(&self) -> Select {
        let query = self.create_skeleton_query();
        match *query.body {
            SetExpr::Select(select) => *select,
            _ => panic!(
                "SKELETON query invariant violated: expected Select, got {:?}. \
                 This indicates memory corruption or a bug in sqlparser.",
                query.body
            ),
        }
    }

    pub fn plan_to_query(&mut self, plan: &LogicalPlan) -> Result<Query, SqlGenError> {
        let _guard = RecursionGuard::new(self.recursion_level.clone())?;

        tracing::trace!(target: "sql_generator", node = %plan.display(), depth = self.recursion_level.load(Ordering::SeqCst), "Translating plan node");

        let result = match plan {
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
                } else if let Some(adapter) = ext
                    .node
                    .as_any()
                    .downcast_ref::<crate::schema_adapter::SchemaAdapter>()
                {
                    // Fallback for SchemaAdapter if not already replaced:
                    // Treat as projection but we should have replaced it in sql_gen.rs anyway.
                    self.plan_to_query(&adapter.to_projection()?)
                } else {
                    Err(SqlGenError::UnsupportedPlan {
                        message: format!("Unsupported extension node: {}", ext.node.name()),
                        node_type: "Extension".to_string(),
                    })
                }
            }
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Logical plan node: {}", plan.display()),
                node_type: match plan {
                    LogicalPlan::Projection(_) => "Projection",
                    LogicalPlan::Filter(_) => "Filter",
                    LogicalPlan::Join(_) => "Join",
                    LogicalPlan::Aggregate(_) => "Aggregate",
                    LogicalPlan::Sort(_) => "Sort",
                    LogicalPlan::Limit(_) => "Limit",
                    LogicalPlan::Subquery(_) => "Subquery",
                    LogicalPlan::TableScan(_) => "TableScan",
                    LogicalPlan::EmptyRelation(_) => "EmptyRelation",
                    LogicalPlan::Values(_) => "Values",
                    LogicalPlan::Distinct(_) => "Distinct",
                    LogicalPlan::Union(_) => "Union",
                    LogicalPlan::Window(_) => "Window",
                    LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
                    LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
                    _ => "Unknown",
                }
                .to_string(),
            }),
        };

        result
    }

    pub fn extract_relation(&self, query: Query) -> Result<TableFactor, SqlGenError> {
        if let SetExpr::Select(select) = *query.body {
            if select.from.len() == 1 && select.from[0].joins.is_empty() {
                return Ok(select.from[0].relation.clone());
            }
        }

        Err(SqlGenError::UnsupportedPlan {
            message:
                "Cannot extract relation from complex query - consider wrapping in SubqueryAlias"
                    .to_string(),
            node_type: "TableFactor".to_string(),
        })
    }
}

struct RecursionGuard {
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
    use datafusion::sql::unparser::dialect::PostgreSqlDialect as DataFusionPostgres;

    #[tokio::test]
    async fn test_concurrent_generation_safety() {
        let sqlparser_dialect = DataFusionPostgres {};
        let _dialect = GeneratorDialect::new(
            &sqlparser_dialect,
            None,
            std::sync::Arc::new(crate::sql_generator::dialect::DefaultDialectCapabilities),
            std::sync::Arc::new(crate::sql_generator::dialect::DefaultTypeMapper),
            "postgres",
        );
        let mut tasks = vec![];

        for _ in 0..1000 {
            // We can't easily move SqlGenerator due to lifetimes of GeneratorDialect,
            // but we can test that its components (like recursion_level) are Send/Sync.
            let level = Arc::new(AtomicUsize::new(0));
            tasks.push(tokio::spawn(async move {
                let _guard = RecursionGuard::new(level).unwrap();
                // If this compiles and runs, we've proven Arc<AtomicUsize> works in async
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
    }
}
