//! # DuckDB Federation Provider and Executor
//!
//! This module implements the federation layer for DuckDB, allowing DataFusion to push down
//! complex sub-plans to a DuckDB instance.
//!
//! ## Overview
//!
//! The [`DuckDBFederationProvider`] integrates with DataFusion-Federation to identify
//! and wrap DuckDB-specific query fragments. The [`DuckDBExecutor`] then handles the
//! physical execution of these fragments via DuckDB's Arrow interface.
//!
//! ## Performance Characteristics
//!
//! - **Gated Federation**: The optimizer rule only wraps plans that exclusively reference
//!   DuckDB tables to avoid breaking mixed-source queries.
//! - **Memoized Reordering**: Similar to the native scanner, the executor uses memoized
//!   column indexing to handle DuckDB's asynchronous result schema with O(1) per-batch overhead.
//!
//! ## Usage
//!
//! ```rust
//! use strake_connectors::sources::sql::duckdb_federation::DuckDBExecutor;
//! use std::sync::Arc;
//!
//! // let executor = DuckDBExecutor::new(pool, db_path)?;
//! // let provider = executor.create_federation_provider();
//! ```
//!
//! ## Errors
//!
//! - SQL generation failures in `strake-sql`.
//! - DuckDB execution errors during `query_arrow`.
//! - `SqlGenError::ScopeViolation` if column resolution fails during plan unparsing.

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::optimizer::OptimizerConfig;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::sql::TableReference;
use datafusion_federation::sql::SQLExecutor;
use datafusion_federation::{
    FederatedPlanNode, FederatedTableProviderAdaptor, FederatedTableSource, FederationPlanner,
    FederationProvider,
};
use std::sync::{Arc, OnceLock};

use super::duckdb::{DuckDBPath, DuckDBPool, DuckDBTableSource, escape_literal, map_duckdb_type};

/// DuckDB SQL Executor for federated query execution.
///
/// Implements `SQLExecutor` to bridge DataFusion federation rules with DuckDB.
#[derive(Clone)]
pub struct DuckDBExecutor {
    /// Shared connection pool.
    pool: Arc<DuckDBPool>,
    /// Path to the DuckDB database file used as identity context.
    db_path: DuckDBPath,
    /// Execution metrics.
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
    /// Memoized column index mapping to avoid per-batch reordering overhead.
    /// Stores Result to propagate initialization errors safely instead of panicking.
    memoized_index_map: Arc<OnceLock<Result<Vec<usize>, datafusion::error::DataFusionError>>>,
}

impl DuckDBExecutor {
    /// Creates a new `DuckDBExecutor` with an existing connection pool.
    ///
    /// # Errors
    /// Returns an error if the pool cannot be accessed.
    pub fn new(pool: Arc<DuckDBPool>, db_path: DuckDBPath) -> anyhow::Result<Self> {
        Ok(Self {
            pool,
            db_path,
            metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
            memoized_index_map: Arc::new(OnceLock::new()),
        })
    }

    /// Deprecated: Use `new` instead.
    #[deprecated(since = "0.1.0", note = "Use `new` instead")]
    pub fn new_with_pool(pool: Arc<DuckDBPool>, db_path: DuckDBPath) -> anyhow::Result<Self> {
        Self::new(pool, db_path)
    }

    /// Returns the database path.
    pub fn db_path(&self) -> &DuckDBPath {
        &self.db_path
    }

    /// Wraps this executor in a `DuckDBFederationProvider`.
    pub fn create_federation_provider(self: Arc<Self>) -> Arc<DuckDBFederationProvider> {
        Arc::new(DuckDBFederationProvider::new(self))
    }
}

impl std::fmt::Debug for DuckDBExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBExecutor")
            .field("db_path", &self.db_path)
            .finish()
    }
}

/// Custom Federation Provider for DuckDB.
///
/// Uses `strake_sql`'s internal SQL generator for higher-fidelity pushdown
/// and better handling of subquery aliases.
pub struct DuckDBFederationProvider {
    executor: Arc<DuckDBExecutor>,
}

impl DuckDBFederationProvider {
    /// Creates a new `DuckDBFederationProvider`.
    pub fn new(executor: Arc<DuckDBExecutor>) -> Self {
        Self { executor }
    }
}

impl std::fmt::Debug for DuckDBFederationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBFederationProvider")
            .field("executor", &self.executor)
            .finish()
    }
}

#[async_trait]
impl FederationProvider for DuckDBFederationProvider {
    fn name(&self) -> &str {
        "duckdb"
    }

    fn compute_context(&self) -> Option<String> {
        self.executor.compute_context()
    }

    fn optimizer(&self) -> Option<Arc<datafusion::optimizer::optimizer::Optimizer>> {
        // Use a custom optimizer rule that leverages DuckDBFederationPlanner.
        // This planner uses strake_sql instead of the default DataFusion unparser.
        let rule = Arc::new(DuckDBFederationOptimizerRule::new(self.executor.clone()));
        Some(Arc::new(
            datafusion::optimizer::optimizer::Optimizer::with_rules(vec![rule]),
        ))
    }
}

/// Custom Optimizer Rule for DuckDB federation.
///
/// This rule identifies sub-plans that can be pushed down to DuckDB and
/// wraps them in a `FederatedPlanNode` with our custom `DuckDBFederationPlanner`.
#[derive(Debug)]
struct DuckDBFederationOptimizerRule {
    planner: Arc<DuckDBFederationPlanner>,
}

impl DuckDBFederationOptimizerRule {
    fn new(executor: Arc<DuckDBExecutor>) -> Self {
        Self {
            planner: Arc::new(DuckDBFederationPlanner::new(executor)),
        }
    }
}

impl OptimizerRule for DuckDBFederationOptimizerRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<datafusion::common::tree_node::Transformed<LogicalPlan>> {
        if let LogicalPlan::Extension(Extension { ref node }) = plan
            && node.name() == "Federated"
        {
            return Ok(datafusion::common::tree_node::Transformed::no(plan));
        }

        // Only wrap plans that exclusively contain DuckDB tables belonging to this instance.
        if !is_duckdb_federated_plan(&plan)? {
            return Ok(datafusion::common::tree_node::Transformed::no(plan));
        }

        let fed_plan = FederatedPlanNode::new(plan, self.planner.clone());
        Ok(datafusion::common::tree_node::Transformed::yes(
            LogicalPlan::Extension(Extension {
                node: Arc::new(fed_plan),
            }),
        ))
    }

    fn name(&self) -> &str {
        "duckdb_federation_rule"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

/// Custom Federated Planner for DuckDB.
///
/// Implements `FederationPlanner` to convert logical plans into physical DuckDB scans
/// using Strake's internal SQL generator.
#[derive(Debug)]
struct DuckDBFederationPlanner {
    executor: Arc<DuckDBExecutor>,
}

impl DuckDBFederationPlanner {
    fn new(executor: Arc<DuckDBExecutor>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl FederationPlanner for DuckDBFederationPlanner {
    async fn plan_federation(
        &self,
        node: &FederatedPlanNode,
        _session_state: &datafusion::execution::context::SessionState,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let plan = node.plan();

        // Strip source-level qualifiers (e.g., "testdb.", "testdb.public.") from
        // the plan and expressions before SQL generation. Strake registers sources by name,
        // but DuckDB expects bare table names.
        //
        // IMPORTANT: We only strip MULTI-PART qualifiers (containing dots), or qualifiers
        // that match the catalog/schema pattern. Single-word relations (e.g. SubqueryAlias
        // names like "o" in "o.amount") are legitimate DataFusion join aliases and must be
        // preserved for correct column resolution in the SQL generator.
        let normalized_plan = plan
            .clone()
            .transform_up(|node| {
                // 1. Strip qualifiers from all expressions in this node
                let transformed = node.map_expressions(|expr| {
                    expr.transform_down(|e| {
                        if let Expr::Column(mut col) = e {
                            let should_strip = col
                                .relation
                                .as_ref()
                                .map(|r| {
                                    let s = r.to_string();
                                    // Strip only if the relation has catalog/schema separators (dots)
                                    s.contains('.')
                                })
                                .unwrap_or(false);

                            if should_strip {
                                col.relation = None;
                                Ok(Transformed::yes(Expr::Column(col)))
                            } else {
                                Ok(Transformed::no(Expr::Column(col)))
                            }
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                })?;

                // 2. Strip qualifier from TableScan name if applicable
                if let LogicalPlan::TableScan(mut scan) = transformed.data {
                    let bare_name = scan.table_name.table().to_string();
                    scan.table_name = TableReference::bare(bare_name);
                    Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                } else {
                    Ok(transformed)
                }
            })?
            .data;

        // Custom Unparser Logic
        // Use strake_sql to generate SQL for the isolated sub-plan.
        // This ensures we avoid DataFusion unparser bugs like unnamed_subquery.
        let sql = strake_sql::sql_gen::get_sql_for_plan(&normalized_plan, "duckdb")
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "DuckDB federation SQL generation failed: {}",
                    e
                ))
            })?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to generate SQL for DuckDB plan".to_string(),
                )
            })?;

        tracing::info!(target: "federation", sql = %sql, "Generated federated SQL for DuckDB using strake_sql");

        let schema = normalized_plan.schema().as_arrow().clone();

        let exec =
            super::duckdb::DuckDBScanExec::new(self.executor.pool.clone(), sql, Arc::new(schema));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

#[async_trait]
impl SQLExecutor for DuckDBExecutor {
    fn name(&self) -> &str {
        "duckdb"
    }

    fn compute_context(&self) -> Option<String> {
        // Same db_path = same DuckDB instance = can push down joins between tables
        Some(self.db_path.to_string())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        // We still provide a dialect for metadata/simple cases,
        // though our custom FederationProvider bypasses this for complex plans.
        Arc::new(datafusion::sql::unparser::dialect::DuckDBDialect::new())
    }

    fn logical_optimizer(&self) -> Option<datafusion_federation::sql::LogicalOptimizer> {
        None
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _params: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let db_path = self.db_path.clone();
        let query_owned = query.to_string();

        tracing::info!(target: "federation", db = %db_path, "DuckDB executing federated query: {}", query);

        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 2);
        let tx = builder.tx();
        let pool = self.pool.clone();

        let metrics = datafusion::physical_plan::metrics::BaselineMetrics::new(&self.metrics, 0);
        let bytes_metrics =
            datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics).output_bytes(0);
        let index_map_cache = self.memoized_index_map.clone();

        builder.spawn_blocking(move || {
            let _timer = metrics.elapsed_compute().timer();
            let conn = pool.get()
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to get DuckDB connection: {e}")
                ))?;

            let mut stmt = conn
                .prepare(&query_owned)
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to prepare DuckDB query: {e}")
                ))?;

            let batches = stmt
                .query_arrow([])
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("DuckDB query execution failed: {e}")
                ))?;

            for batch in batches {
                // Name-based column reordering to prevent silent data corruption.
                // DuckDB might return columns in a different
                // order than expected by DataFusion. We must reorder columns by name.
                let batch = if batch.schema() != schema {
                    // Avoid per-batch HashMap allocation.
                    let batch_schema = batch.schema();
                    let index_map = index_map_cache.get_or_init(|| {
                        schema.fields().iter().map(|field| {
                            batch_schema.index_of(field.name()).map_err(|_| {
                                datafusion::error::DataFusionError::Execution(
                                    format!("Missing column '{}' in DuckDB federated result batch", field.name())
                                )
                            })
                        }).collect::<Result<Vec<_>, _>>()
                    }).as_ref().map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                    // Defensive check: Ensure the current batch is compatible with the memoized mapping.
                    if batch_schema.fields().len() <= index_map.iter().max().copied().unwrap_or(0) {
                         return Err(datafusion::error::DataFusionError::Execution(
                            "DuckDB federated batch schema is incompatible with memoized index map".to_string()
                        ));
                    }

                    let columns: Vec<_> = index_map
                        .iter()
                        .map(|&idx| batch.column(idx).clone())
                        .collect();

                    RecordBatch::try_new(schema.clone(), columns)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
                } else {
                    batch
                };

                metrics.record_output(batch.num_rows());
                bytes_metrics.add(batch.get_array_memory_size());

                if let Err(e) = tx.blocking_send(Ok(batch)) {
                    tracing::debug!(target: "federation", error = %e, "Failed to send batch to execution stream");
                    break;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to get DuckDB connection: {e}"
                ))
            })?;

            let mut stmt = conn
                .prepare(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
                )
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to prepare table lookup: {e}"
                    ))
                })?;

            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Table lookup failed: {e}"
                    ))
                })?;

            let names: Vec<String> = rows.filter_map(|r| r.ok()).collect();
            Ok(names)
        })
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Blocking task failed: {e}"))
        })?
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let pool = self.pool.clone();
        let table_name = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to get DuckDB connection: {e}"
                ))
            })?;

            let mut stmt = conn
                .prepare(&format!(
                    "PRAGMA table_info('{}')",
                    escape_literal(&table_name)
                ))
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to prepare table_info: {e}"
                    ))
                })?;

            let rows = stmt
                .query_map([], |row| {
                    let name: String = row.get("name")?;
                    let type_str: String = row.get("type")?;
                    let notnull: bool = row.get("notnull")?;
                    Ok((name, type_str, notnull))
                })
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("table_info failed: {e}"))
                })?;

            let mut fields = Vec::new();
            for row in rows {
                let (name, type_str, notnull) = row.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read schema row: {e}"
                    ))
                })?;
                let dt = map_duckdb_type(&type_str);
                fields.push(Field::new(name, dt, !notnull));
            }

            Ok(Arc::new(Schema::new(fields)) as SchemaRef)
        })
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Blocking task failed: {e}"))
        })?
    }
}

/// Returns true if the plan contains only DuckDB table sources that belong to the
/// specified executor (same database instance).
fn is_duckdb_federated_plan(
    plan: &LogicalPlan,
) -> Result<bool, datafusion::error::DataFusionError> {
    let mut has_duckdb_source = false;
    let mut all_duckdb_sources = true;

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let mut is_duckdb = false;

            // Helper to recursively check if a provider or its inner is DuckDB
            fn check_provider(provider: &Arc<dyn TableProvider>) -> bool {
                let any = provider.as_any();

                // 1. Check if it's our DuckDBTableProvider
                if any
                    .downcast_ref::<super::duckdb::DuckDBTableProvider>()
                    .is_some()
                {
                    return true;
                }

                // 2. Peel our known wrappers (Metadata, CircuitBreaker, etc.)
                if let Some(w) =
                    any.downcast_ref::<super::wrappers::MetadataEnrichedTableProvider>()
                {
                    return check_provider(&w.inner());
                }
                if let Some(w) =
                    any.downcast_ref::<super::wrappers::ConcurrencyLimitedTableProvider>()
                {
                    return check_provider(&w.inner());
                }
                if let Some(w) = any
                    .downcast_ref::<crate::resilience::circuit_breaker::CircuitBreakerTableProvider>()
                {
                    return check_provider(&w.inner());
                }
                if let Some(w) =
                    any.downcast_ref::<crate::sources::schema_drift::SchemaDriftTableProvider>()
                {
                    return check_provider(&w.inner());
                }
                if let Some(inner) = any
                    .downcast_ref::<FederatedTableProviderAdaptor>()
                    .and_then(|w| w.table_provider.as_ref())
                {
                    return check_provider(inner);
                }

                false
            }

            let source_any = scan.source.as_any();
            if let Some(s) = source_any.downcast_ref::<DuckDBTableSource>() {
                if s.federation_provider().name() == "duckdb" {
                    is_duckdb = true;
                }
            } else if let Some(default_source) =
                source_any.downcast_ref::<datafusion::datasource::DefaultTableSource>()
            {
                #[allow(clippy::collapsible_if)]
                if check_provider(&default_source.table_provider) {
                    is_duckdb = true;
                }
            }

            if is_duckdb {
                has_duckdb_source = true;
            } else {
                all_duckdb_sources = false;
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;

    Ok(has_duckdb_source && all_duckdb_sources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tempfile::tempdir;

    #[test]
    fn test_executor_compute_context() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.duckdb");
        let manager = duckdb::DuckdbConnectionManager::file(db_path.to_str().unwrap())?;
        let pool = Arc::new(r2d2::Pool::new(manager)?);
        let executor = DuckDBExecutor::new(
            pool,
            DuckDBPath::from(db_path.to_str().unwrap().to_string()),
        )?;
        assert!(executor.compute_context().is_some());
        assert_eq!(executor.name(), "duckdb");
        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_executor_execute_happy_path() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.duckdb");
        let db_path_str = db_path.to_str().unwrap();

        // Setup DuckDB with data
        {
            let conn = duckdb::Connection::open(db_path_str)?;
            conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)", [])?;
            conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')", [])?;
        }

        let manager = duckdb::DuckdbConnectionManager::file(db_path_str)?;
        let pool = Arc::new(r2d2::Pool::new(manager)?);
        let executor = DuckDBExecutor::new(pool, DuckDBPath::from(db_path_str.to_string()))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, true),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));

        let mut stream = executor.execute("SELECT * FROM test ORDER BY id", schema, &[])?;

        let batch1 = stream.next().await.unwrap()?;
        assert_eq!(batch1.num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_executor_execute_bad_path() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let db_path = dir.path().join("bad.duckdb");
        let db_path_str = db_path.to_str().unwrap();

        let manager = duckdb::DuckdbConnectionManager::file(db_path_str)?;
        let pool = Arc::new(r2d2::Pool::new(manager)?);
        let executor = DuckDBExecutor::new(pool, DuckDBPath::from(db_path_str.to_string()))?;
        let schema = Arc::new(Schema::new(Vec::<Field>::new()));

        // Invalid query
        let mut stream = executor.execute("SELECT * FROM non_existent_table", schema, &[])?;

        // Wait for error from background task
        let res = stream.next().await;
        assert!(res.is_some());
        assert!(res.unwrap().is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_normalization() -> Result<(), Box<dyn std::error::Error>> {
        // 1. Setup a qualified TableScan with a Projection
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let table_name = TableReference::parse_str("testdb.public.users");
        let scan = LogicalPlan::TableScan(datafusion::logical_expr::TableScan {
            table_name: table_name.clone(),
            source: Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
                datafusion::datasource::empty::EmptyTable::new(schema.clone()),
            ))),
            projection: None,
            projected_schema: Arc::new(datafusion::common::DFSchema::try_from_qualified_schema(
                table_name.clone(),
                schema.as_ref(),
            )?),
            filters: vec![],
            fetch: None,
        });

        // Add a Projection referencing the qualified column
        let col = Expr::Column(datafusion::common::Column::new(Some(table_name), "id"));
        let plan = LogicalPlan::Projection(datafusion::logical_expr::Projection::try_new(
            vec![col],
            Arc::new(scan),
        )?);

        // 2. Apply the same normalization logic as in plan_federation
        let normalized_plan = plan
            .transform_up(|node| {
                // 1. Strip qualifiers from all expressions in this node
                let transformed = node.map_expressions(|expr| {
                    expr.transform_down(|e| {
                        if let Expr::Column(mut col) = e {
                            let should_strip = col
                                .relation
                                .as_ref()
                                .map(|r| r.to_string().contains('.'))
                                .unwrap_or(false);
                            if should_strip {
                                col.relation = None;
                                Ok(Transformed::yes(Expr::Column(col)))
                            } else {
                                Ok(Transformed::no(Expr::Column(col)))
                            }
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                })?;

                // 2. Strip qualifier from TableScan name if applicable
                if let LogicalPlan::TableScan(mut scan) = transformed.data {
                    let bare_name = scan.table_name.table().to_string();
                    scan.table_name = TableReference::bare(bare_name);
                    Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                } else {
                    Ok(transformed)
                }
            })?
            .data;

        // 3. Generate SQL using strake_sql
        let sql = strake_sql::sql_gen::get_sql_for_plan(&normalized_plan, "duckdb")?
            .ok_or("Failed to generate SQL")?;

        // 4. Assert: Expect bare table name AND bare column name
        assert!(sql.contains("users"));
        assert!(sql.contains("\"id\"")); // Unqualified identifier
        assert!(!sql.contains("testdb"));
        assert!(!sql.contains("public"));

        Ok(())
    }
}
