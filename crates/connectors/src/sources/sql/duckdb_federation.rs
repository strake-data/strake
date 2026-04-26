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
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion_federation::sql::SQLExecutor;
use std::sync::{Arc, OnceLock};

use super::strake_federation::StrakeFederationProvider;

use super::duckdb::{DuckDBPath, DuckDBPool, escape_literal, map_duckdb_type};

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

    /// Wraps this executor in a `StrakeFederationProvider`.
    pub fn create_federation_provider(self: Arc<Self>) -> Arc<StrakeFederationProvider> {
        Arc::new(StrakeFederationProvider::new(
            self,
            crate::sources::sql::common::SqlDialect::DuckDB,
        ))
    }
}

impl std::fmt::Debug for DuckDBExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBExecutor")
            .field("db_path", &self.db_path)
            .finish()
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
}
