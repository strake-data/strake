//! DuckDB Federation Provider
//!
//! Implements the `datafusion-federation` traits to enable same-source join pushdown
//! for DuckDB tables. When multiple tables from the same DuckDB database are joined,
//! the entire join is pushed down to DuckDB instead of being executed by DataFusion.

use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider};
use std::sync::Arc;

use super::duckdb::map_duckdb_type;

/// DuckDB SQL Executor for federation
///
/// Implements `SQLExecutor` to execute federated SQL queries against a DuckDB database.
/// The `compute_context` is the database path, allowing the federation optimizer to
/// identify tables from the same database and push down joins.
#[derive(Debug, Clone)]
pub struct DuckDBExecutor {
    db_path: String,
}

impl DuckDBExecutor {
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }

    /// Get the connection string (used as compute context)
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Create a SQLFederationProvider wrapping this executor
    pub fn create_federation_provider(self) -> Arc<SQLFederationProvider> {
        Arc::new(SQLFederationProvider::new(Arc::new(self)))
    }
}

#[async_trait]
impl SQLExecutor for DuckDBExecutor {
    fn name(&self) -> &str {
        "duckdb"
    }

    fn compute_context(&self) -> Option<String> {
        // Same db_path = same DuckDB instance = can push down joins between tables
        Some(self.db_path.clone())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        // DuckDB is PostgreSQL-compatible for most SQL
        Arc::new(datafusion::sql::unparser::dialect::PostgreSqlDialect {})
    }

    fn logical_optimizer(&self) -> Option<datafusion_federation::sql::LogicalOptimizer> {
        Some(Box::new(|plan| {
            strake_sql::sql_gen::remap_plan_for_federation(plan)
        }))
    }

    fn execute(
        &self,
        query: &str,
        _schema: SchemaRef,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let db_path = self.db_path.clone();
        let query_owned = query.to_string();

        tracing::info!(target: "federation", db = %db_path, "DuckDB executing federated query: {}", query);

        // Note: Due to Arrow version mismatch between duckdb crate (arrow 56) and
        // datafusion (arrow 57), we cannot directly use duckdb's Arrow output.
        // For now, we use a workaround via text-based SQL execution through the
        // existing DuckDBTableProvider's scan method.
        //
        // TODO:
        // Upgrade duckdb crate when it supports arrow 57

        // For now, return an error indicating this needs the table provider approach
        Err(datafusion::error::DataFusionError::NotImplemented(
            format!("DuckDB federation execute not yet implemented due to Arrow version mismatch. Query: {}", query_owned)
        ))
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&db_path)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let names: Vec<String> = rows.filter_map(|r| r.ok()).collect();
            Ok(names)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let db_path = self.db_path.clone();
        let table_name = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&db_path)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info('{}')", table_name))
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows = stmt
                .query_map([], |row| {
                    let name: String = row.get("name")?;
                    let type_str: String = row.get("type")?;
                    let notnull: bool = row.get("notnull")?;
                    Ok((name, type_str, notnull))
                })
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut fields = Vec::new();
            for row in rows {
                let (name, type_str, notnull) =
                    row.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let dt = map_duckdb_type(&type_str);
                fields.push(Field::new(name, dt, !notnull));
            }

            Ok(Arc::new(Schema::new(fields)) as SchemaRef)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_compute_context() {
        let executor = DuckDBExecutor::new("/tmp/test.duckdb".to_string());
        assert_eq!(
            executor.compute_context(),
            Some("/tmp/test.duckdb".to_string())
        );
        assert_eq!(executor.name(), "duckdb");
    }
}
